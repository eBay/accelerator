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
import sys
import os
import json
from operator import itemgetter
from collections import defaultdict
from functools import partial
from types import GeneratorType
from base64 import b64encode

from compat import unicode, str_types, PY3
from compat import urlencode, urlopen, Request, URLError, HTTPError

import setupfile
from extras import json_encode, json_decode, DotDict
from dispatch import JobError
from status import print_status_stacks
import unixhttp; unixhttp # for unixhttp:// URLs, as used to talk to the daemon


class Automata:
	"""
	Launch jobs, wait for completion.
	"""

	method = '?' # fall-through case when we resume waiting for something

	def __init__(self, server_url, dataset='churn', verbose=False, flags=None, subjob_cookie=None, infoprints=False):
		"""
		Set server url and legacy dataset parameter
		"""
		self.dataset = dataset
		self.url = server_url
		self.subjob_cookie = subjob_cookie
		self.history = []
		self.verbose = verbose
		self.monitor = None
		self.flags = flags or []
		self.job_method = None
		# Workspaces should be per Automata
		from jobid import put_workspaces
		put_workspaces(self.list_workspaces())
		self.update_method_deps()
		self.clear_record()
		# Don't do this unless we are part of automatarunner
		if infoprints:
			from workarounds import SignalWrapper
			siginfo = SignalWrapper(['SIGINFO', 'SIGUSR1'])
			self.siginfo_check = siginfo.check
		else:
			self.siginfo_check = lambda: False


	def clear_record(self):
		self.record = defaultdict(JobList)
		self.jobs = self.record[None]

	def validate_response(self, response):
		# replace with homemade function,
		# this is run on bigdata response
		pass

	def _url_get(self, *path, **kw):
		url = self.url + os.path.join('/', *path)
		req = urlopen(url, **kw)
		try:
			resp = req.read()
		finally:
			req.close()
		if PY3:
			resp = resp.decode('utf-8')
		return resp

	def _url_json(self, *path, **kw):
		return json_decode(self._url_get(*path, **kw))

	def abort(self):
		return self._url_json('abort')

	def info(self):
		return self._url_json('workspace_info')

	def config(self):
		return self._url_json('config')

	def set_workspace(self, workspace):
		resp = self._url_get('set_workspace', workspace)
		print(resp)


	def new(self, method, caption=None):
		"""
		Prepare submission of a new job.
		"""
		self.params = defaultdict(lambda: {'options': {}, 'datasets': {}, 'jobids': {}})
		self.job_method = method
		if not caption:
			self.job_caption='fsm_'+method
		else:
			self.job_caption = caption

	def options(self, method, optionsdict):
		"""
		Append options for "method".
		This method could be called repeatedly for all
		included methods.
		"""
		self.params[method]['options'].update(optionsdict)

	def datasets(self, method, datasetdict):
		"""
		Similar to self.options(), but for datasets.
		"""
		self.params[method]['datasets'].update(datasetdict)

	def jobids(self, method, jobiddict):
		"""
		Similar to self.options(), but for jobids.
		"""
		self.params[method]['jobids'].update(jobiddict)

	def submit(self, wait=True, why_build=False):
		"""
		Submit job to server and conditionaly wait for completion.
		"""
		if not why_build and 'why_build' in self.flags:
			why_build = 'on_build'
		if self.monitor and not why_build:
			self.monitor.submit(self.job_method)
		data = setupfile.generate(self.job_caption, self.job_method, self.params, why_build=why_build)
		if self.subjob_cookie:
			data.subjob_cookie = self.subjob_cookie
			data.parent_pid = os.getpid()
		t0 = time.time()
		self.job_retur = self._server_submit(data)
		self.history.append((data, self.job_retur))
		#
		if wait and not self.job_retur.done:
			self.wait(t0)
		if self.monitor and not why_build:
			self.monitor.done()

	def wait(self, t0=None, ignore_old_errors=False):
		idle, status_stacks, current, last_time = self._server_idle(0, ignore_errors=ignore_old_errors)
		if idle:
			return
		if t0 is None:
			if current:
				t0 = current[0]
			else:
				t0 = time.time()
		waited = int(round(time.time() - t0)) - 1
		if self.verbose == 'dots':
			print('[' + '.' * waited, end=' ')
		while not idle:
			if self.siginfo_check():
				print()
				print_status_stacks(status_stacks)
			waited += 1
			if waited % 60 == 0 and self.monitor:
				self.monitor.ping()
			if self.verbose:
				now = time.time()
				if current:
					current = (now - t0, current[1], now - current[2],)
				else:
					current = (now - t0, self.job_method, 0,)
				if self.verbose == 'dots':
					if waited % 60 == 0:
						sys.stdout.write('[%d]\n ' % (now - t0,))
					else:
						sys.stdout.write('.')
				elif self.verbose == 'log':
					if waited % 60 == 0:
						print('%d seconds, still waiting for %s (%d seconds)' % current)
				else:
					sys.stdout.write('\r\033[K           %.1f %s %.1f' % current)
			idle, status_stacks, current, last_time = self._server_idle(1)
		if self.verbose == 'dots':
			print('(%d)]' % (last_time,))
		else:
			print('\r\033[K              %s' % (fmttime(last_time),))

	def jobid(self, method):
		"""
		Return jobid of "method"
		"""
		return self.job_retur.jobs[method].link

	def dump_history(self):
		return self.history

	def _server_idle(self, timeout=0, ignore_errors=False):
		"""ask server if it is idle, return (idle, status_stacks)"""
		path = ['status']
		if self.verbose:
			path.append('full')
		path.append('?subjob_cookie=%s&timeout=%d' % (self.subjob_cookie or '', timeout,))
		resp = self._url_json(*path)
		last_error = resp.last_error
		if last_error and not ignore_errors:
			print("\nFailed to build jobs:", file=sys.stderr)
			for jobid, method, status in last_error:
				e = JobError(jobid, method, status)
				print(e.format_msg(), file=sys.stderr)
			raise e
		return resp.idle, resp.status_stacks, resp.current, resp.last_time

	def _server_submit(self, json):
		# submit json to server
		postdata = urlencode({'json': setupfile.encode_setup(json)})
		res = self._url_json('submit', data=postdata)
		if res.error:
			raise Exception("Submit failed: " + res.error)
		if not res.why_build:
			if not self.subjob_cookie:
				self._printlist(res.jobs)
			self.validate_response(res.jobs)
		return res

	def _printlist(self, returndict):
		# print (return list) in neat format
		for method, item in sorted(returndict.items(), key=lambda x: x[1].link):
			if item.make == True:
				make_msg = 'MAKE'
			else:
				make_msg = item.make or 'link'
			print('        -  %44s' % method.ljust(44), end=' ')
			print(' %s' % (make_msg,), end=' ')
			print(' %s' % item.link, end=' ')
			if item.make != True:
				print(' %s' % fmttime(item.total_time), end=' ')
			print()

	def method_info(self, method):
		return self._url_json('method_info', method)

	def methods_info(self):
		return self._url_json('methods')

	def update_methods(self):
		resp = self._url_get('update_methods')
		self.update_method_deps()
		return resp

	def update_method_deps(self):
		info = self.methods_info()
		self.dep_methods = {str(name): set(map(str, data.get('dep', ()))) for name, data in info.items()}

	def list_workspaces(self):
		return self._url_json('list_workspaces')

	def call_method(self, method, defopt={}, defdata={}, defjob={}, options=(), datasets=(), jobids=(), record_in=None, record_as=None, why_build=False, caption=None):
		todo  = {method}
		org_method = method
		opted = set()
		self.new(method, caption)
		# options and datasets can be for just method, or {method: options, ...}.
		def dictofdicts(d):
			if method not in d:
				return {method: dict(d)}
			else:
				return dict(d)
		options  = dictofdicts(options)
		datasets = dictofdicts(datasets)
		jobids   = dictofdicts(jobids)
		def resolve_something(res_in, d):
			def resolve(name, inner=False):
				if name is None and not inner:
					return None
				if isinstance(name, JobTuple):
					names = [str(name)]
				elif isinstance(name, (list, tuple)):
					names = name
				else:
					assert isinstance(name, str_types), "%s: %s" % (key, name)
					names = [name]
				fixed_names = []
				for name in names:
					res_name = res_in.get(name, name)
					if isinstance(res_name, (list, tuple)):
						res_name = resolve(res_name, True)
					assert isinstance(res_name, str_types), "%s: %s" % (key, name) # if name was a job-name this gets a dict and dies
					fixed_names.append(res_name)
				return ','.join(fixed_names)
			for key, name in d.items():
				yield key, resolve(name)
		resolve_datasets = partial(resolve_something, defdata)
		resolve_jobids   = partial(resolve_something, defjob)
		to_record = []
		while todo:
			method = todo.pop()
			m_opts = dict(defopt.get(method, ()))
			m_opts.update(options.get(method, ()))
			self.options(method, m_opts)
			m_datas = dict(defdata.get(method, ()))
			m_datas.update(resolve_datasets(datasets.get(method, {})))
			self.datasets(method, m_datas)
			m_jobs = dict(defjob.get(method, ()))
			m_jobs.update(resolve_jobids(jobids.get(method, {})))
			self.jobids(method, m_jobs)
			opted.add(method)
			to_record.append(method)
			todo.update(self.dep_methods[method])
			todo.difference_update(opted)
		self.submit(why_build=why_build)
		if why_build: # specified by caller
			return self.job_retur.why_build
		if self.job_retur.why_build: # done by server anyway (because --flags why_build)
			print("Would have built from:")
			print("======================")
			print(setupfile.encode_setup(self.history[-1][0], as_str=True))
			print("Could have avoided build if:")
			print("============================")
			print(json_encode(self.job_retur.why_build, as_str=True))
			print()
			from inspect import stack
			stk = stack()[1]
			print("Called from %s line %d" % (stk[1], stk[2],))
			exit()
		if isinstance(record_as, str):
			record_as = {org_method: record_as}
		elif not record_as:
			record_as = {}
		for m in to_record:
			self.record[record_in].insert(record_as.get(m, m), self.jobid(m))
		return self.jobid(org_method)


def fmttime(t):
	if t == '':
		# Failures have no time information and end up here
		return ''
	unit = 'seconds'
	units = ['hours', 'minutes']
	while t > 60 * 3 and units:
		unit = units.pop()
		t /= 60
	return '%.1f %s' % (t, unit,)


class JobTuple(tuple):
	"""
	A tuple of (method, jobid) with accessor properties that gives just
	the jobid for str and etree (encode).
	"""
	def __new__(cls, *a):
		if len(a) == 1: # like tuple
			method, jobid = a[0]
		else: # like namedtuple
			method, jobid = a
		assert isinstance(method, str_types)
		assert isinstance(jobid, str_types)
		return tuple.__new__(cls, (str(method), str(jobid)))
	method = property(itemgetter(0), doc='Field 0')
	jobid  = property(itemgetter(1), doc='Field 1')
	def __str__(self):
		return self.jobid
	def encode(self, encoding=None, errors="strict"):
		"""Unicode-object compat. For etree, gives jobid."""
		return str(self).encode(encoding, errors)

class JobList(list):
	"""
	Mostly a list, but uses the jobid of the last element in str and etree (encode).
	Also provides the following properties:
	.all for an a,b,c string (jobids)
	.method for the latest method.
	.jobid for the latest jobid.
	.pretty for a pretty-printed version.
	Taking a single element gives you a (method, jobid) tuple
	(which also gives jobid in str and etree).
	Taking a slice gives a jobid,jobid,... string.
	There is also .find, for finding the latest jobid with a given method.
	"""

	def __init__(self, *a):
		if len(a) == 1:
			self.extend(a[0])
		elif a:
			self.insert(*a)

	def insert(self, method, jobid):
		list.append(self, JobTuple(method, jobid))

	def append(self, *a):
		if len(a) == 1:
			data = a[0]
		else:
			return self.insert(*a)
		if isinstance(data, str_types):
			return self.insert('', data)
		if isinstance(data, (tuple, list)):
			return self.insert(*data)
		raise ValueError("What did you try to append?", data)

	def extend(self, other):
		if isinstance(other, str_types + (JobTuple,)):
			return self.append(other)
		if not isinstance(other, (list, tuple, GeneratorType)):
			raise ValueError("Adding what?", other)
		for item in other:
			self.append(item)

	def __str__(self):
		"""Last element jobid, for convenience."""
		if self:
			return self[-1].jobid
		else:
			return ''
	def __unicode__(self):
		"""Last element jobid, for convenience."""
		return unicode(str(self))
	def __repr__(self):
		return "%s(%s)" % (type(self).__name__, list.__repr__(self))

	# this is what etree calls
	def encode(self, encoding=None, errors="strict"):
		"""Unicode-object compat. For etree, gives last element."""
		return str(self).encode(encoding, errors)
	def __getslice__(self, i, j): # This is friggin pre-python 2, and I *still* need it.
		return self[slice(i, j)]
	def __getitem__(self, item):
		if isinstance(item, slice):
			return JobList(list.__getitem__(self, item))
		elif isinstance(item, str_types):
			return self.find(item)[-1] # last matching or IndexError
		else:
			return list.__getitem__(self, item)
	def __delitem__(self, item):
		if isinstance(item, (int, slice)):
			return list.__delitem__(self, item)
		if isinstance(item, tuple):
			self[:] = [j for j in self if item != j]
		else:
			self[:] = [j for j in self if item not in j]
	def __add__(self, other):
		if not isinstance(other, list):
			raise ValueError("Adding what?", other)
		return JobList(list(self) + other)
	def __iadd__(self, other):
		self.extend(other)
		return self
	@property
	def all(self):
		"""Comma separated list of all elements' jobid"""
		return ','.join(e.jobid for e in self)

	@property
	def method(self):
		if self:
			return self[-1].method
	@property
	def jobid(self): # for symmetry
		if self:
			return self[-1].jobid

	@property
	def pretty(self):
		"""Formated for printing"""
		if not self: return 'JobList([])'
		template = '   [%%3d] %%%ds : %%s' % (max(len(i.method) for i in self),)
		return 'JobList(\n' + \
			'\n'.join(template % (i, a, b) for i, (a, b) in enumerate(self)) + \
			'\n)'

	def find(self, method):
		"""Matching elements returned as new Joblist."""
		return JobList(e for e in self if e.method == method)

	def get(self, method, default=None):
		l = self.find(method)
		return l[-1] if l else default

def profile_jobs(jobs):
	from extras import job_post
	if isinstance(jobs, str):
		jobs = [jobs]
	total = 0
	seen = set()
	for j in jobs:
		if isinstance(j, tuple):
			j = j[1]
		if j not in seen:
			total += job_post(j).profile.total
			seen.add(j)
	return total


class UrdResponse(dict):
	def __new__(cls, d):
		assert cls is UrdResponse, "Always make these through UrdResponse"
		obj = dict.__new__(UrdResponse if d else EmptyUrdResponse)
		return obj

	def __init__(self, d):
		d = dict(d or ())
		d.setdefault('caption', '')
		d.setdefault('timestamp', '0')
		d.setdefault('joblist', JobList())
		d.setdefault('deps', {})
		dict.__init__(self, d)

	__setitem__ = dict.__setitem__
	__delattr__ = dict.__delitem__
	def __getattr__(self, name):
		if name.startswith('_') or name not in self:
			raise AttributeError(name)
		return self[name]

	@property
	def as_dep(self):
		return DotDict(timestamp=self.timestamp, joblist=self.joblist, caption=self.caption, _default=lambda: None)

class EmptyUrdResponse(UrdResponse):
	# so you can do "if urd.latest('foo'):" and similar.
	# python2 version
	def __nonzero__(self):
		return False
	# python3 version
	def __bool__(self):
		return False

def _urd_typeify(d):
	if PY3 and isinstance(d, bytes):
		d = d.decode('utf-8')
	if isinstance(d, str):
		d = json.loads(d)
		if not d or isinstance(d, unicode):
			return d
	res = DotDict(_default=lambda: None)
	for k, v in d.items():
		if k == 'joblist':
			v = JobList(v)
		elif isinstance(v, dict):
			v = _urd_typeify(v)
		res[k] = v
	return res

class Urd(object):
	def __init__(self, a, info, user, password, horizon=None):
		self._a = a
		if info.urd:
			assert '://' in str(info.urd), 'Bad urd URL: %s' % (info.urd,)
		self._url = info.urd or ''
		self._user = user
		self._current = None
		self.info = info
		self.flags = set(a.flags)
		self.horizon = horizon
		self.joblist = a.jobs
		auth = '%s:%s' % (user, password,)
		if PY3:
			auth = b64encode(auth.encode('utf-8')).decode('ascii')
		else:
			auth = b64encode(auth)
		self._headers = {'Content-Type': 'application/json', 'Authorization': 'Basic ' + auth}

	def _path(self, path):
		if '/' not in path:
			path = '%s/%s' % (self._user, path,)
		return path

	def _call(self, url, data=None, fmt=_urd_typeify):
		assert self._url, "No urd configured for this daemon"
		url = url.replace(' ', '%20')
		if data is not None:
			req = Request(url, json_encode(data), self._headers)
		else:
			req = Request(url)
		tries_left = 3
		while True:
			try:
				r = urlopen(req)
				try:
					return fmt(r.read())
				finally:
					try:
						r.close()
					except Exception:
						pass
			except HTTPError as e:
				if e.code in (401, 409,):
					raise
				tries_left -= 1
				if not tries_left:
					raise
				print('Error %d from urd, %d tries left' % (e.code, tries_left,), file=sys.stderr)
			except ValueError:
				tries_left -= 1
				if not tries_left:
					raise
				print('Bad data from urd, %d tries left' % (tries_left,), file=sys.stderr)
			except URLError:
				print('Error contacting urd', file=sys.stderr)
			time.sleep(4)

	def _get(self, path, *a):
		assert self._current, "Can't record dependency with nothing running"
		path = self._path(path)
		assert path not in self._deps, 'Duplicate ' + path
		url = '/'.join((self._url, path,) + a)
		res = UrdResponse(self._call(url))
		if res:
			self._deps[path] = res.as_dep
		self._latest_joblist = res.joblist
		return res

	def _latest_str(self):
		if self.horizon:
			return '<=' + self.horizon
		else:
			return 'latest'

	def get(self, path, timestamp):
		return self._get(path, timestamp)

	def latest(self, path):
		return self.get(path, self._latest_str())

	def first(self, path):
		return self.get(path, 'first')

	def peek(self, path, timestamp):
		path = self._path(path)
		url = '/'.join((self._url, path, timestamp,))
		return UrdResponse(self._call(url))

	def peek_latest(self, path):
		return self.peek(path, self._latest_str())

	def peek_first(self, path):
		return self.peek(path, 'first')

	def since(self, path, timestamp):
		path = self._path(path)
		url = '%s/%s/since/%s' % (self._url, path, timestamp,)
		return self._call(url, fmt=json.loads)

	def begin(self, path, timestamp=None, caption=None, update=False):
		assert not self._current, 'Tried to begin %s while running %s' % (path, self._current,)
		assert self._user, "Set URD_AUTH to be able to record jobs in urd"
		self._current = self._path(path)
		self._current_timestamp = timestamp
		self._current_caption = caption
		self._update = update
		self._deps = {}
		self._a.clear_record()
		self.joblist = self._a.jobs
		self._latest_joblist = None

	def abort(self):
		self._current = None

	def finish(self, path, timestamp=None, caption=None):
		path = self._path(path)
		assert self._current, 'Tried to finish %s with nothing running' % (path,)
		assert path == self._current, 'Tried to finish %s while running %s' % (path, self._current,)
		user, automata = path.split('/')
		self._current = None
		caption = caption or self._current_caption or ''
		timestamp = timestamp or self._current_timestamp
		assert timestamp, 'No timestamp specified in begin or finish for %s' % (path,)
		data = DotDict(
			user=user,
			automata=automata,
			joblist=self.joblist,
			deps=self._deps,
			caption=caption,
			timestamp=timestamp,
		)
		if self._update:
			data.flags = ['update']
		url = self._url + '/add'
		return self._call(url, data)

	def truncate(self, path, timestamp):
		url = '%s/truncate/%s/%s' % (self._url, self._path(path), timestamp,)
		return self._call(url, '')

	def build(self, method, options={}, datasets={}, jobids={}, name=None, caption=None, why_build=False):
		return self._a.call_method(method, options={method: options}, datasets={method: datasets}, jobids={method: jobids}, record_as=name, caption=caption, why_build=why_build)

	def build_chained(self, method, options={}, datasets={}, jobids={}, name=None, caption=None, why_build=False):
		datasets = dict(datasets or {})
		assert 'previous' not in datasets, "Don't specify previous dataset to build_chained"
		assert name, "build_chained must have 'name'"
		assert self._latest_joblist is not None, "Can't build_chained without a dependency to chain from"
		datasets['previous'] = self._latest_joblist.get(name)
		return self.build(method, options, datasets, jobids, name, caption, why_build)

	def print_profile(self, verbose=True):
		from extras import job_post
		total = 0
		seen = set()
		per_method = defaultdict(int)
		for method, jid in self.joblist:
			if jid not in seen:
				seen.add(jid)
				t = job_post(jid).profile.total
				total += t
				per_method[method] += t
		if verbose and per_method:
			print("Time per method:")
			tmpl = "   %%-%ds  %%s" % (max(len(method) for method in per_method),)
			for method, t in sorted(per_method.items(), key=itemgetter(1), reverse=True):
				print(tmpl % (method, fmttime(t),))
		print("Total time", fmttime(total))
