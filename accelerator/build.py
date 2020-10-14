############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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
from datetime import date
from base64 import b64encode
from importlib import import_module
from argparse import ArgumentParser, RawTextHelpFormatter

from accelerator.compat import unicode, str_types, PY3
from accelerator.compat import urlencode
from accelerator.compat import getarglist

from accelerator import setupfile
from accelerator.extras import json_encode, DotDict, _ListTypePreserver
from accelerator.job import Job
from accelerator.statmsg import print_status_stacks
from accelerator.error import JobError, ServerError, UrdPermissionError
from accelerator import g
from accelerator.unixhttp import call


class Automata:
	"""
	Launch jobs, wait for completion.
	Don't use this directly, use the Urd object.
	"""

	method = '?' # fall-through case when we resume waiting for something

	def __init__(self, server_url, verbose=False, flags=None, subjob_cookie=None, infoprints=False, print_full_jobpath=False):
		self.url = server_url
		self.subjob_cookie = subjob_cookie
		self.history = []
		self.verbose = verbose
		self.monitor = None
		self.flags = flags or []
		self.job_method = None
		# Workspaces should be per Automata
		from accelerator.job import WORKDIRS
		WORKDIRS.update(self.list_workdirs())
		self.update_method_info()
		self.clear_record()
		# Only do this when run from shell.
		if infoprints:
			from accelerator.workarounds import SignalWrapper
			siginfo = SignalWrapper(['SIGINFO', 'SIGUSR1'])
			self.siginfo_check = siginfo.check
		else:
			self.siginfo_check = lambda: False
		self.print_full_jobpath = print_full_jobpath

	def clear_record(self):
		self.record = defaultdict(JobList)
		self.jobs = self.record[None]

	def validate_response(self, response):
		# replace with homemade function,
		# this is run on bigdata response
		pass

	def _url_json(self, *path, **kw):
		url = self.url + os.path.join('/', *path)
		return call(url, **kw)

	def abort(self):
		return self._url_json('abort')

	def info(self):
		return self._url_json('workspace_info')

	def config(self):
		return self._url_json('config')

	def _submit(self, method, options, datasets, jobs, caption=None, wait=True, why_build=False, workdir=None):
		"""
		Submit job to server and conditionaly wait for completion.
		"""
		self.job_method = method
		if not why_build and 'why_build' in self.flags:
			why_build = 'on_build'
		if self.monitor and not why_build:
			self.monitor.submit(method)
		if not caption:
			caption = ''
		params = {method: dict(options=options, datasets=datasets, jobs=jobs,)}
		data = setupfile.generate(caption, method, params, why_build=why_build)
		if self.subjob_cookie:
			data.subjob_cookie = self.subjob_cookie
			data.parent_pid = os.getpid()
		if workdir:
			data.workdir = workdir
		t0 = time.time()
		self.job_retur = self._server_submit(data)
		self.history.append((data, self.job_retur))
		#
		if wait and not self.job_retur.done:
			self.wait(t0)
		if self.monitor and not why_build:
			self.monitor.done()
		return self.jobid(method), self.job_retur

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
					current_display = (
						fmttime(current[0], True),
						current[1],
						fmttime(current[2], True),
					)
					sys.stdout.write('\r\033[K           %s %s %s' % current_display)
			idle, status_stacks, current, last_time = self._server_idle(1)
		if self.verbose == 'dots':
			print('(%d)]' % (last_time,))
		else:
			print('\r\033[K              %s' % (fmttime(last_time),))

	def jobid(self, method):
		"""
		Return jobid of "method"
		"""
		if 'jobs' in self.job_retur:
			return self.job_retur.jobs[method].link

	def dump_history(self):
		return self.history

	def _server_idle(self, timeout=0, ignore_errors=False):
		"""ask server if it is idle, return (idle, status_stacks)"""
		path = ['status']
		if self.verbose:
			path.append('full')
		path.append('?subjob_cookie=%s&timeout=%s' % (self.subjob_cookie or '', timeout,))
		resp = self._url_json(*path)
		if 'last_error' in resp and not ignore_errors:
			print("\nFailed to build jobs:", file=sys.stderr)
			for jobid, method, status in resp.last_error:
				e = JobError(jobid, method, status)
				print(e.format_msg(), file=sys.stderr)
			raise e
		return resp.idle, resp.get('status_stacks'), resp.get('current'), resp.get('last_time')

	def _server_submit(self, json):
		# submit json to server
		postdata = urlencode({'json': setupfile.encode_setup(json)})
		res = self._url_json('submit', data=postdata)
		if 'error' in res:
			raise ServerError('Submit failed: ' + res.error)
		if 'why_build' not in res:
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
			if self.print_full_jobpath:
				link_msg = Job(item.link).path
			else:
				link_msg = item.link
			msg = '   - %-35s %-5s %s' % (method, make_msg, link_msg,)
			if item.make != True and 'total_time' in item:
				msg = msg + ' ' + fmttime(item.total_time).rjust(78 - len(msg))
			print(msg)

	def method_info(self, method):
		return self._url_json('method_info', method)

	def methods_info(self):
		return self._url_json('methods')

	def update_methods(self):
		resp = self._url_json('update_methods')
		self.update_method_info()
		return resp

	def update_method_info(self):
		self._method_info = self.methods_info()

	def list_workdirs(self):
		return self._url_json('list_workdirs')

	def call_method(self, method, options={}, datasets={}, jobs={}, record_in=None, record_as=None, why_build=False, caption=None, workdir=None, **kw):
		if method not in self._method_info:
			raise Exception('Unknown method %s' % (method,))
		info = self._method_info[method]
		params = dict(options=dict(options), datasets=dict(datasets), jobs=dict(jobs))
		argmap = defaultdict(list)
		for thing in ('options', 'datasets', 'jobs'):
			for n in info[thing]:
				argmap[n].append(thing)
		for k, v in kw.items():
			if k not in argmap:
				raise Exception('Keyword %s not in options/datasets/jobs for method %s' % (k, method,))
			if len(argmap[k]) != 1:
				raise Exception('Keyword %s has several targets on method %s: %r' % (k, method, argmap[k],))
			params[argmap[k][0]][k] = v
		jid, res = self._submit(method, caption=caption, why_build=why_build, workdir=workdir, **params)
		if why_build: # specified by caller
			return res.why_build
		if 'why_build' in res: # done by server anyway (because --flags why_build)
			print("Would have built from:")
			print("======================")
			print(setupfile.encode_setup(self.history[-1][0], as_str=True))
			print("Could have avoided build if:")
			print("============================")
			print(json_encode(res.why_build, as_str=True))
			print()
			from inspect import stack
			stk = stack()[2]
			print("Called from %s line %d" % (stk[1], stk[2],))
			exit()
		jid = Job(jid, record_as or method)
		self.record[record_in].append(jid)
		return jid


def fmttime(t, short=False):
	if t is None:
		return None
	if short:
		fmts = ['%.0fs', '%.1fm', '%.2fh', '%.2fd']
	else:
		fmts = ['%.1f seconds', '%.1f minutes', '%.2f hours', '%.2f days']
	sizes = [60, 60, 24, 1]
	for fmt, size in zip(fmts, sizes):
		if t < size * 2:
			break
		t /= size
	res = fmt % (t,)
	if res == '1.0 seconds':
		return '1 second'
	return res


class JobList(_ListTypePreserver):
	"""
	A list of Jobs with some convenience methods.
	.find(method) a new JobList with only jobs with that method in it.
	.get(method, default=None) latest Job with that method.
	[method] Same as .get but error if no job with that method is in the list.
	.as_tuples The same list but as (method, jid) tuples.
	.pretty a pretty-printed version (string).
	"""

	def __getitem__(self, item):
		if isinstance(item, slice):
			return self.__class__(list.__getitem__(self, item))
		elif isinstance(item, str_types):
			return self.find(item)[-1] # last matching or IndexError
		else:
			return list.__getitem__(self, item)

	@property
	def pretty(self):
		"""Formated for printing"""
		if not self: return 'JobList([])'
		template = '   [%%3d] %%%ds : %%s' % (max(len(i.method) for i in self),)
		return 'JobList(\n' + \
			'\n'.join(template % (i, j.method, j) for i, j in enumerate(self)) + \
			'\n)'

	@property
	def as_tuples(self):
		return [(e.method, e) for e in self]

	def find(self, method):
		"""Matching elements returned as new Joblist."""
		return self.__class__(e for e in self if e.method == method)

	def get(self, item, default=None):
		try:
			return self[item]
		except IndexError:
			return default

	@property
	def exectime(self):
		total = 0
		seen = set()
		per_method = defaultdict(int)
		for jid in self:
			if jid not in seen:
				seen.add(jid)
				t = jid.post.exectime.total
				total += t
				per_method[jid.method] += t
		return total, per_method

	def print_exectimes(self, verbose=True):
		total, per_method = self.exectime
		if verbose and per_method:
			print("Time per method:")
			tmpl = "   %%-%ds  %%s  (%%d%%%%)" % (max(len(method) for method in per_method),)
			total_time = sum(per_method.values())
			for method, t in sorted(per_method.items(), key=itemgetter(1), reverse=True):
				print(tmpl % (method, fmttime(t), round(100 * t / total_time) if total_time else 0.0))
		print("Total time", fmttime(total))


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
		return DotDict(timestamp=self.timestamp, joblist=self.joblist.as_tuples, caption=self.caption)

class EmptyUrdResponse(UrdResponse):
	# so you can do "if urd.latest('foo'):" and similar.
	# python2 version
	def __nonzero__(self):
		return False
	# python3 version
	def __bool__(self):
		return False

def _urd_typeify(d):
	if isinstance(d, str):
		d = json.loads(d)
		if not d or isinstance(d, unicode):
			return d
	res = DotDict()
	for k, v in d.items():
		if k == 'joblist':
			v = JobList(Job(e[1], e[0]) for e in v)
		elif isinstance(v, dict):
			v = _urd_typeify(v)
		res[k] = v
	return res

def _tsfix(ts):
	if ts is None:
		return None
	errmsg = 'Specify timestamps as strings, ints, datetimes or (timestamp, integer), not %r' % (ts,)
	if isinstance(ts, (tuple, list,)):
		assert len(ts) == 2, errmsg
		ts, integer = ts
		assert isinstance(integer, int), errmsg
	else:
		integer = None
	if isinstance(ts, (int, date,)):
		ts = str(ts)
	assert isinstance(ts, str_types), errmsg
	assert ts, errmsg
	if integer is None:
		return ts
	else:
		return '%s+%d' % (ts, integer,)

class Urd(object):
	def __init__(self, a, info, user, password, horizon=None, default_workdir=None):
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
		self.workdir = None
		self.default_workdir = default_workdir
		auth = '%s:%s' % (user, password,)
		if PY3:
			auth = b64encode(auth.encode('utf-8')).decode('ascii')
		else:
			auth = b64encode(auth)
		self._headers = {'Content-Type': 'application/json', 'Authorization': 'Basic ' + auth}
		self._auth_tested = False
		self._warnings = []

	def _path(self, path):
		if '/' not in path:
			path = '%s/%s' % (self._user, path,)
		return path

	def _call(self, url, data=None, fmt=_urd_typeify):
		assert self._url, "No urd configured for this server"
		url = url.replace(' ', '%20')
		return call(url, data=data, fmt=fmt, headers=self._headers, server_name='urd')

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
		return self._get(path, _tsfix(timestamp))

	def latest(self, path):
		return self.get(path, self._latest_str())

	def first(self, path):
		return self.get(path, 'first')

	def peek(self, path, timestamp):
		path = self._path(path)
		url = '/'.join((self._url, path, _tsfix(timestamp),))
		return UrdResponse(self._call(url))

	def peek_latest(self, path):
		return self.peek(path, self._latest_str())

	def peek_first(self, path):
		return self.peek(path, 'first')

	def since(self, path, timestamp):
		path = self._path(path)
		url = '%s/%s/since/%s' % (self._url, path, _tsfix(timestamp),)
		return self._call(url, fmt=json.loads)

	def list(self):
		url = '/'.join((self._url, 'list'))
		return self._call(url, fmt=json.loads)

	def begin(self, path, timestamp=None, caption=None, update=False):
		assert not self._current, 'Tried to begin %s while running %s' % (path, self._current,)
		if not self._auth_tested:
			try:
				self._call('%s/test/%s' % (self._url, self._user,), True)
			except UrdPermissionError:
				raise Exception('Urd says permission denied, did you forget to set URD_AUTH?')
			self._auth_tested = True
		self._current = self._path(path)
		self._current_timestamp = _tsfix(timestamp)
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
		user, build = path.split('/')
		self._current = None
		caption = caption or self._current_caption or ''
		if timestamp is None:
			timestamp = self._current_timestamp
		else:
			timestamp = _tsfix(timestamp)
		assert timestamp, 'No timestamp specified in begin or finish for %s' % (path,)
		data = DotDict(
			user=user,
			build=build,
			joblist=self.joblist.as_tuples,
			deps=self._deps,
			caption=caption,
			timestamp=timestamp,
		)
		if self._update:
			data.flags = ['update']
		url = self._url + '/add'
		return self._call(url, data)

	def truncate(self, path, timestamp):
		url = '%s/truncate/%s/%s' % (self._url, self._path(path), _tsfix(timestamp),)
		return self._call(url, '')

	def set_workdir(self, workdir):
		"""Build jobs in this workdir, None to restore default"""
		self.workdir = workdir

	def build(self, method, options={}, datasets={}, jobs={}, name=None, caption=None, why_build=False, workdir=None, **kw):
		return self._a.call_method(method, options=options, datasets=datasets, jobs=jobs, record_as=name, caption=caption, why_build=why_build, workdir=workdir or self.workdir or self.default_workdir, **kw)

	def build_chained(self, method, options={}, datasets={}, jobs={}, name=None, caption=None, why_build=False, workdir=None, **kw):
		assert 'previous' not in set(datasets) | set(jobs) | set(kw), "Don't specify previous to build_chained"
		assert name, "build_chained must have 'name'"
		assert self._latest_joblist is not None, "Can't build_chained without a dependency to chain from"
		kw = dict(kw)
		kw['previous'] = self._latest_joblist.get(name)
		return self.build(method, options, datasets, jobs, name, caption, why_build, workdir, **kw)

	def warn(self, line=''):
		"""Add a warning message to be displayed at the end of the build"""
		self._warnings.extend(l.rstrip() for l in line.expandtabs().split('\n'))

	def _show_warnings(self):
		if self._warnings:
			from itertools import chain
			from accelerator.compat import terminal_size
			max_width = max(34, terminal_size().columns - 6)
			def reflow(line):
				indent = ''
				for c in line:
					if c.isspace():
						indent += c
					else:
						break
				width = max(max_width - len(indent), 25)
				current = ''
				between = ''
				for word in line[len(indent):].split(' '):
					if len(current + word) >= width:
						if len(word) > width / 2 and word.startswith('"/') and (word.endswith('"') or word.endswith('",')):
							for pe in word.split('/'):
								if len(current + pe) >= width:
									if current:
										yield indent + current
										current = ('/' if between == '/' else '') + pe
									else:
										yield indent + between + pe
								else:
									current += between + pe
								between = '/'
						else:
							if current:
								yield indent + current
								current = word
							else:
								yield indent + word
					else:
						current += between + word
					between = ' '
				if current:
					yield indent + current
			warnings = list(chain.from_iterable(reflow(w) for w in self._warnings))
			print()
			width = max(len(line) for line in warnings)
			print('\x1b[35m' + ('#' * (width + 6)) + '\x1b[m')
			for line in warnings:
				print('\x1b[35m##\x1b[m', line.ljust(width), '\x1b[35m##\x1b[m')
			print('\x1b[35m' + ('#' * (width + 6)) + '\x1b[m')
			self._warnings = []


def find_automata(a, package, script):
	all_packages = sorted(a.config()['method_directories'])
	if package:
		if package in all_packages:
			package = [package]
		else:
			for cand in all_packages:
				if cand.endswith('.' + package):
					package = [cand]
					break
			else:
				raise Exception('No method directory found for %r in %r' % (package, all_packages))
	else:
		package = all_packages
	if not script.startswith('build'):
		script = 'build_' + script
	for p in package:
		module_name = p + '.' + script
		try:
			module_ref = import_module(module_name)
			print(module_name)
			return module_ref
		except ImportError as e:
			if PY3:
				if not e.msg[:-1].endswith(script):
					raise
			else:
				if not e.message.endswith(script):
					raise
	raise Exception('No build script "%s" found in {%s}' % (script, ', '.join(package)))

def run_automata(options, cfg):
	g.running = 'build'
	a = Automata(cfg.url, verbose=options.verbose, flags=options.flags.split(','), infoprints=True, print_full_jobpath=options.fullpath)

	try:
		a.wait(ignore_old_errors=not options.just_wait)
	except JobError:
		# An error occured in a job we didn't start, which is not our problem.
		pass

	if options.just_wait:
		return

	module_ref = find_automata(a, options.package, options.script)

	assert getarglist(module_ref.main) == ['urd'], "Only urd-enabled automatas are supported"
	if 'URD_AUTH' in os.environ:
		assert ':' in os.environ['URD_AUTH'], "Set $URD_AUTH to user:password"
		user, password = os.environ['URD_AUTH'].split(':', 1)
	else:
		user = os.environ.get('USER')
		if not user:
			user = 'NO-USER'
			print("No $URD_AUTH or $USER in environment, using %r" % (user,), file=sys.stderr)
		password = ''
	info = a.info()
	urd = Urd(a, info, user, password, options.horizon, options.workdir)
	if options.quick:
		a.update_method_info()
	else:
		a.update_methods()
	module_ref.main(urd)
	urd._show_warnings()


def main(argv, cfg):
	parser = ArgumentParser(
		prog=argv.pop(0),
		usage="%(prog)s [options] [script]",
		formatter_class=RawTextHelpFormatter,
	)
	parser.add_argument('-f', '--flags',    default='',          help="comma separated list of flags", )
	parser.add_argument('-q', '--quick',    action='store_true', help="skip method updates and checking workdirs for new jobs", )
	parser.add_argument('-w', '--workdir',  default=None,        help="build in this workdir\nset_workdir() and workdir= override this.", )
	parser.add_argument('-W', '--just_wait',action='store_true', help="just wait for running job, don't run any build script", )
	parser.add_argument('-F', '--fullpath', action='store_true', help="print full path to jobdirs")
	parser.add_argument('--verbose',        default='status',    help="verbosity style {no, status, dots, log}")
	parser.add_argument('--quiet',          action='store_true', help="same as --verbose=no")
	parser.add_argument('--horizon',        default=None,        help="time horizon - dates after this are not visible in\nurd.latest")
	parser.add_argument('script',           default='build'   ,  help="build script to run. default \"build\".\nsearches under all method directories in alphabetical\norder if it does not contain a dot.\nprefixes build_ to last element unless specified.\npackage name suffixes are ok.\nso for example \"test_methods.tests\" expands to\n\"accelerator.test_methods.build_tests\".", nargs='?')

	options = parser.parse_args(argv)

	if '.' in options.script:
		options.package, options.script = options.script.rsplit('.', 1)
	else:
		options.package = None

	options.verbose = {'no': False, 'status': True, 'dots': 'dots', 'log': 'log'}[options.verbose]
	if options.quiet: options.verbose = False

	try:
		run_automata(options, cfg)
		return 0
	except (JobError, ServerError):
		# If it's a JobError we don't care about the local traceback,
		# we want to see the job traceback, and maybe know what line
		# we built the job on.
		# If it's a ServerError we just want the line and message.
		print_minimal_traceback()
	return 1


def print_minimal_traceback():
	build_fn = __file__
	if build_fn[-4:] in ('.pyc', '.pyo',):
		# stupid python2
		build_fn = build_fn[:-1]
	blacklist_fns = {build_fn}
	_, e, tb = sys.exc_info()
	last_interesting = tb
	while tb is not None:
		code = tb.tb_frame.f_code
		if code.co_filename not in blacklist_fns:
			last_interesting = tb
		tb = tb.tb_next
	lineno = last_interesting.tb_lineno
	filename = last_interesting.tb_frame.f_code.co_filename
	if isinstance(e, JobError):
		print("Failed to build job %s on %s line %d" % (e.jobid, filename, lineno,))
	else:
		print("Server returned error on %s line %d:\n%s" % (filename, lineno, e.args[0]))
