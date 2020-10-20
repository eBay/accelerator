############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

from threading import Thread
import multiprocessing
import signal
from os import unlink
from os.path import join
import time

from accelerator import dependency
from accelerator import dispatch

from accelerator import workspace
from accelerator import database
from accelerator import methods
from accelerator.setupfile import update_setup
from accelerator.job import WORKDIRS, Job
from accelerator.extras import json_save, DotDict

METHODS_CONFIGFILENAME = 'methods.conf'



class Main:
	""" This is the main controller behind the server. """

	def __init__(self, config, options, server_url):
		"""
		Setup objects:

		  Methods

		  WorkSpaces

		"""
		self.config = config
		self.debug = options.debug
		self.server_url = server_url
		self._update_methods()
		self.target_workdir = self.config['target_workdir']
		self.workspaces = {}
		for name, path in self.config.workdirs.items():
			self.workspaces[name] = workspace.WorkSpace(name, path, config.slices)
		WORKDIRS.clear()
		WORKDIRS.update({k: v.path for k, v in self.workspaces.items()})
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
			[(key, self.config.get(key),) for key in ('input_directory', 'result_directory', 'common_directory', 'urd',)]
		)


	def list_workdirs(self):
		""" Return list of all initiated workdirs """
		return self.workspaces


	def print_workdirs(self):
		namelen = max(len(n) for n in self.workspaces)
		templ = "    %%s %%%ds: %%s \x1b[m(%%d)" % (namelen,)
		print("Available workdirs:")
		names = sorted(self.workspaces)
		names.remove(self.target_workdir)
		names.insert(0, self.target_workdir)
		for n in names:
			if n == self.target_workdir:
				prefix = 'TARGET\x1b[1m  '
			else:
				prefix = 'SOURCE  '
			w = self.workspaces[n]
			print(templ % (prefix, n, w.path, w.slices,))


	def add_single_jobid(self, jobid):
		ws = self.workspaces[jobid.rsplit('-', 1)[0]]
		ws.add_single_jobid(jobid)
		return self.DataBase.add_single_jobid(jobid)

	def update_database(self):
		"""Insert all new jobids (from all workdirs) in database,
		discard all deleted or with incorrect hash.
		"""
		if hasattr(multiprocessing, 'get_context'):
			ctx = multiprocessing.get_context('forkserver')
			Pool = ctx.Pool
		else:
			Pool = multiprocessing.Pool
		pool = Pool(initializer=_pool_init, initargs=(WORKDIRS,))
		try:
			self.DataBase._update_begin()
			def update(name):
				ws = self.workspaces[name]
				ws.update(pool)
				self.DataBase._update_workspace(ws, pool)
				failed.remove(name)
			failed = set(self.workspaces)
			t_l = []
			for name in self.workspaces:
				# Run all updates in parallel. This gets all (sync) listdir calls
				# running at the same time. Then each workspace will use the same
				# process pool to do the post.json checking.
				t = Thread(
					target=update,
					args=(name,),
					name='Update ' + name,
				)
				t.daemon = True
				t.start()
				t_l.append(t)
			for t in t_l:
				t.join()
			assert not failed, "%s failed to update" % (', '.join(failed),)
		finally:
			pool.close()
		self.DataBase._update_finish(self.Methods.hash)


	def initialise_jobs(self, setup, workdir=None):
		""" Updata database, check deps, create jobids. """
		ws = workdir or self.target_workdir
		if ws not in self.workspaces:
			raise Exception("Workdir %s does not exist" % (ws,))
		return dependency.initialise_jobs(
			setup,
			self.workspaces[ws],
			self.DataBase,
			self.Methods,
		)


	def run_job(self, jobid, subjob_cookie=None, parent_pid=0):
		W = self.workspaces[Job(jobid).workdir]
		#
		active_workdirs = {name: ws.path for name, ws in self.workspaces.items()}
		slices = self.workspaces[self.target_workdir].slices

		t0 = time.time()
		setup = update_setup(jobid, starttime=t0)
		prof = setup.get('exectime', DotDict())
		new_prof, files, subjobs = dispatch.launch(W.path, setup, self.config, self.Methods, active_workdirs, slices, self.debug, self.server_url, subjob_cookie, parent_pid)
		prefix = join(W.path, jobid) + '/'
		if not self.debug:
			for filename, temp in list(files.items()):
				if temp:
					unlink(join(prefix, filename))
					del files[filename]
		prof.update(new_prof)
		prof.total = 0
		prof.total = sum(v for v in prof.values() if isinstance(v, (float, int)))
		data = dict(
			starttime=t0,
			endtime=time.time(),
			exectime=prof,
		)
		update_setup(jobid, **data)
		data['files'] = sorted(fn[len(prefix):] if fn.startswith(prefix) else fn for fn in files)
		data['subjobs'] = subjobs
		data['version'] = 1
		json_save(data, jobid.filename('post.json'))


	def get_methods(self):
		return {k: self.method_info(k) for k in self.Methods.db}


	def method_info(self, method):
		d = self.Methods.db.get(method, None)
		if d:
			d = dict(d)
			p = self.Methods.params[method]
			for k in ('options', 'datasets', 'jobs'):
				d[k] = [v[0] if isinstance(v, (list, tuple)) else v for v in p[k]]
			d['description'] = self.Methods.descriptions[method]
			return d


def _pool_init(workdirs):
	# The pool system will send SIGTERM when the pool is closed, so
	# restore the original behaviour for that.
	signal.signal(signal.SIGTERM, signal.SIG_DFL)
	WORKDIRS.update(workdirs)
