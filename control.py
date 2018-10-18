############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#  http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
#                                                                          #
############################################################################

from __future__ import print_function
from __future__ import division

import time
import configfile
import dependency
import dispatch

import workspace
import database
import methods
from extras import json_save
from setupfile import update_setup
from jobid import resolve_jobid_filename, put_workspaces, get_workspace_name
from extras import DotDict, Temp

from threading import Thread
from os import unlink
from os.path import join

METHODS_CONFIGFILENAME = 'methods.conf'



class Main:
	""" This is the main controller behind the daemon. """

	def __init__(self, options, daemon_url):
		"""
		Setup objects:

		  Methods

		  WorkSpaces

		"""
		self.config = configfile.get_config(options.config, verbose=False)
		self.debug = options.debug
		self.daemon_url = daemon_url
		# check config file
		configfile.sanity_check(self.config)
		self._update_methods()
		self.target_workdir = self.config['target_workdir']
		self.source_workdirs = {self.target_workdir} | self.config.get('source_workdirs', set())
		self.workspaces = {}
		for name, data in self.config['workdir'].items():
			if name in self.source_workdirs:
				path   = data[0]
				slices = data[1]
				self.workspaces[name] = workspace.WorkSpace(name, path, slices)
		undefined_workdirs = self.source_workdirs - set(self.workspaces)
		if undefined_workdirs:
			print('\nERROR:  Workdir(s) missing definition: ' + ', '.join('\"' + x + '\"' for x in undefined_workdirs) + '.')
			exit(1)
		check_slices = set(self.workspaces[name].slices for name in self.workspaces)
		if len(check_slices) > 1:
			print('\nERROR:  Not all workdirs have the same number of slices!')
			exit(1)
		put_workspaces({k: v.path for k, v in self.workspaces.items()})
		self.DataBase = database.DataBase(self)
		self.update_database()
		self.broken = False

	def _update_methods(self):
		print('Update methods')
		# initialise methods class looking in method_directories from config file
		method_directories = self.config['method_directories']
		self.Methods = methods.SubMethods(method_directories, METHODS_CONFIGFILENAME, self.config)

	def update_methods(self):
		try:
			self._update_methods()
			self.update_database()
			self.broken = False
		except methods.MethodLoadException as e:
			self.broken = e.module_list
			return {'broken': e.module_list}


	def get_workspace_details(self):
		""" Some information about main workspace, some parts of config """
		return dict(
			[(key, getattr(self.workspaces[self.target_workdir], key),) for key in ('slices',)] +
			[(key, self.config.get(key),) for key in ('source_directory', 'result_directory', 'common_directory', 'urd',)]
		)


	def list_workspaces(self):
		""" Return list of all initiated workspaces """
		return self.workspaces


	def print_workspaces(self):
		namelen = max(len(n) for n in self.workspaces)
		templ = "    %%s %%%ds: %%s \x1b[m(%%d)" % (namelen,)
		prefix = {n: "SOURCE  " for n in self.source_workdirs}
		prefix[self.target_workdir] = "TARGET\x1b[1m  "
		print("Available workdirs:")
		names = list(self.workspaces)
		names.remove(self.target_workdir)
		names.insert(0, self.target_workdir)
		for n in names:
			w = self.workspaces[n]
			print(templ % (prefix[n], n, w.path, w.slices,))


	def add_single_jobid(self, jobid):
		ws = self.workspaces[jobid.rsplit('-', 1)[0]]
		ws.add_single_jobid(jobid)
		return self.DataBase.add_single_jobid(jobid)

	def update_database(self):
		"""Insert all new jobids (from all workspaces) in database,
		discard all deleted or with incorrect hash.
		"""
		t_l = []
		for name in self.workspaces:
			# Run all updates in parallel. This gets all (sync) listdir calls
			# running at the same time. Then each workspace will spawn processes
			# to do the post.json checking, to keep disk queues effective. But
			# try to run a reasonable total number of post.json checkers.
			parallelism = max(3, int(self.workspaces[name].slices / len(self.workspaces)))
			t = Thread(
				target=self.workspaces[name].update,
				kwargs=dict(parallelism=parallelism),
				name='Update ' + name,
			)
			t.daemon = True
			t.start()
			t_l.append(t)
		for t in t_l:
			t.join()
		# These run one at a time, but they will spawn SLICES workers for
		# reading and parsing files. (So unless workspaces are on different
		# disks this is probably better.)
		self.DataBase._update_begin()
		for name in self.source_workdirs:
			self.DataBase._update_workspace(self.workspaces[name])
		self.DataBase._update_finish(self.Methods.hash)


	def initialise_jobs(self, setup):
		""" Updata database, check deps, create jobids. """
		ws = setup.get('workdir', self.target_workdir)
		if ws not in self.workspaces:
			raise Exception("Workdir %s does not exist" % (ws,))
		return dependency.initialise_jobs(
			setup,
			self.workspaces[ws],
			self.DataBase,
			self.Methods,
		)


	def run_job(self, jobid, subjob_cookie=None, parent_pid=0):
		W = self.workspaces[get_workspace_name(jobid)]
		#
		active_workspaces = {}
		for name in self.source_workdirs:
			active_workspaces[name] = self.workspaces[name].get_path()
		slices = self.workspaces[self.target_workdir].get_slices()

		t0 = time.time()
		setup = update_setup(jobid, starttime=t0)
		prof = setup.profile or DotDict()
		new_prof, files, subjobs = dispatch.launch(W.path, setup, self.config, self.Methods, active_workspaces, slices, self.debug, self.daemon_url, subjob_cookie, parent_pid)
		if self.debug:
			delete_from = Temp.TEMP
		else:
			delete_from = Temp.DEBUG
		for filename, temp in list(files.items()):
			if temp >= delete_from:
				unlink(join(W.path, jobid, filename))
				del files[filename]
		prof.update(new_prof)
		prof.total = 0
		prof.total = sum(v for v in prof.values() if isinstance(v, (float, int)))
		data = dict(
			starttime=t0,
			endtime=time.time(),
			profile=prof,
		)
		update_setup(jobid, **data)
		data['files'] = files
		data['subjobs'] = subjobs
		json_save(data, resolve_jobid_filename(jobid, 'post.json'))


	def get_methods(self):
		return self.Methods.db


	def method_info(self, method):
		return self.Methods.db.get(method, '')
