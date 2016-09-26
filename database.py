from __future__ import print_function
from __future__ import division

from collections import defaultdict
from operator import attrgetter
from collections import namedtuple

from compat import iteritems, itervalues, iterkeys

from safe_pool import Pool
from extras import job_params, OptionEnum, OptionDefault


Job = namedtuple('Job', 'id method params optset hash time')

_control = None # control.Main instance, global for use in _update, set when DataBase is initialized.

def _update(jobid):
	try:
		setup = job_params(jobid)
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
			id     = jobid,
			method = setup.method,
			params = setup.params[setup.method],
			optset = optset,
			hash   = setup.hash,
			time   = setup.starttime,
		)
		return jobid, job
	except:
		from traceback import print_exc
		print_exc()
		raise

class DataBase:
	def __init__(self, control):
		global _control
		assert not _control, "Only one DataBase instance allowed"
		_control = control
		self.db = {}

	def add_single_jobid(self, jobid):
		job = _update(jobid)[1]
		self.db[jobid] = job
		self.db_by_method[job.method].insert(0, job)

	def update_workspace(self, WorkSpace, verbose=False):
		"""Insert all items in WorkSpace in database (call update_finish too)"""
		if verbose:
			print("DATABASE:  update for \"%s\"" % WorkSpace.name)
		filesystem_jobids = WorkSpace.list_of_jobids(valid=True)
		if verbose > 1:
			print('DATABASE:  update found these jobids in workspace', filesystem_jobids)
		# Insert any new jobids, including with invalid hash
		pool = Pool(processes=WorkSpace.slices)
		new_jobids = set(filesystem_jobids).difference(self.db)
		self.db.update(pool.imap_unordered(_update, new_jobids, chunksize=64))
		pool.close()
		prefix = WorkSpace.name + '-'
		valid = set(filesystem_jobids)
		for jobid in list(iterkeys(self.db)):
			if jobid.startswith(prefix) and jobid not in valid:
				if verbose:
					print('Removed deleted job %s.' % (jobid, ))
				del self.db[jobid]
		if verbose:
			print("DATABASE:  Database \"%s\" contains %d potential items" % (WorkSpace.name, len(filesystem_jobids), ))

	def update_finish(self, dict_of_hashes, verbose=False):
		"""Filters in-use database on valid hashes.
		Always call after (a sequence of) update_workspace calls.
		"""
		discarded_due_to_hash_list = []
		# Keep lists of jobs per method, only with valid hashes
		self.db_by_method = defaultdict(list)
		for job in itervalues(self.db):
			if job.hash in dict_of_hashes.get(job.method, ()):
				self.db_by_method[job.method].append(job)
			else:
				discarded_due_to_hash_list.append(job.id)
		# Newest first
		for l in itervalues(self.db_by_method):
			l.sort(key=attrgetter('time'), reverse=True)
		if verbose:
			if discarded_due_to_hash_list:
				print("DATABASE:  discarding due to unknown hash: %s" % ', '.join(discarded_due_to_hash_list))
			print("DATABASE:  Full database contains %d items" % (len(self.db), ))

	def match_complex(self, reqlist):
		for method, uid, opttuple in reqlist:
			# These are already sorted newest to oldest.
			for job in self.db_by_method[method]:
				if opttuple.issubset(job.optset):
					yield uid, job.id
					break

	def match_exact(self, reqlist):
		for method, uid, opttuple in reqlist:
			# These are already sorted newest to oldest.
			for job in self.db_by_method[method]:
				if opttuple == job.optset:
					yield uid, job.id
					break
