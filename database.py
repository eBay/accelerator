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

from collections import defaultdict
from operator import attrgetter
from collections import namedtuple

from compat import iteritems, itervalues

from safe_pool import Pool
from extras import job_params, OptionEnum, OptionDefault


Job = namedtuple('Job', 'id method params optset hash time total')

_control = None # control.Main instance, global for use in _mkjob, set when DataBase is initialized.

def _mkjob(setup):
	params_with_defaults = {}
	# Fill in defaults for all methods, update with actual options
	def optfilter(d):
		res = {}
		for k, v in iteritems(d):
			if isinstance(v, OptionEnum):
				v = None
			elif isinstance(v, OptionDefault):
				v = v.default
			res[k] = v
		return res
	for method, params in iteritems(setup.params):
		if method in _control.Methods.params:
			d = {k: optfilter(v) for k, v in iteritems(_control.Methods.params[method].defaults)}
		else:
			d = {}
		for k, v in iteritems(d):
			v.update(params[k])
		params_with_defaults[method] = d
	optset = _control.Methods.params2optset(params_with_defaults)
	job = Job(
		id     = setup.jobid,
		method = setup.method,
		params = setup.params[setup.method],
		optset = optset,
		hash   = setup.hash,
		time   = setup.starttime,
		total  = setup.profile.total,
	)
	return job

def _get_params(jobid):
	try:
		return jobid, job_params(jobid)
	except:
		from traceback import print_exc
		print_exc()
		raise

class _ParamsDict(defaultdict):
	def __missing__(self, key):
		self[key] = value = _get_params(key)[1]
		return value
_paramsdict = _ParamsDict()

class DataBase:
	def __init__(self, control):
		global _control
		assert not _control, "Only one DataBase instance allowed"
		_control = control

	def _update_begin(self):
		self._fsjid = set()

	def add_single_jobid(self, jobid):
		job = _mkjob(_paramsdict[jobid])
		self.db_by_method[job.method].insert(0, job)
		return job

	def _update_workspace(self, WorkSpace, verbose=False):
		"""Insert all items in WorkSpace in database (call update_finish too)"""
		if verbose:
			print("DATABASE:  update for \"%s\"" % WorkSpace.name)
		filesystem_jobids = WorkSpace.valid_jobids
		self._fsjid.update(filesystem_jobids)
		if verbose > 1:
			print('DATABASE:  update found these jobids in workspace', filesystem_jobids)
		# Insert any new jobids, including with invalid hash
		new_jobids = filesystem_jobids.difference(_paramsdict)
		if new_jobids:
			pool = Pool(processes=WorkSpace.slices)
			_paramsdict.update(pool.imap_unordered(_get_params, new_jobids, chunksize=64))
			pool.close()
		if verbose:
			print("DATABASE:  Database \"%s\" contains %d potential items" % (WorkSpace.name, len(filesystem_jobids), ))

	def _update_finish(self, dict_of_hashes, verbose=False):
		"""Filters in-use database on valid hashes.
		Always call after (a sequence of) update_workspace calls.
		"""
		# discard cached setup.json from any gone jobs
		# (so we reload it if they reappear, and also so we don't see them here)
		for j in set(_paramsdict) - self._fsjid:
			del _paramsdict[j]
		discarded_due_to_hash_list = []
		# Keep lists of jobs per method, only with valid hashes
		self.db_by_method = defaultdict(list)
		for setup in itervalues(_paramsdict):
			if setup.hash in dict_of_hashes.get(setup.method, ()):
				job = _mkjob(setup)
				self.db_by_method[job.method].append(job)
			else:
				discarded_due_to_hash_list.append(setup.jobid)
		# Newest first
		for l in itervalues(self.db_by_method):
			l.sort(key=attrgetter('time'), reverse=True)
		if verbose:
			if discarded_due_to_hash_list:
				print("DATABASE:  discarding due to unknown hash: %s" % ', '.join(discarded_due_to_hash_list))
			print("DATABASE:  Full database contains %d items" % (sum(len(v) for v in itervalues(self.db_by_method)),))

	def match_complex(self, reqlist):
		for method, uid, opttuple in reqlist:
			# These are already sorted newest to oldest.
			for job in self.db_by_method[method]:
				if opttuple.issubset(job.optset):
					yield uid, job
					break

	def match_exact(self, reqlist):
		for method, uid, opttuple in reqlist:
			# These are already sorted newest to oldest.
			for job in self.db_by_method[method]:
				if opttuple == job.optset:
					yield uid, job
					break
