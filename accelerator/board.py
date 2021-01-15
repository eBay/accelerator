############################################################################
#                                                                          #
# Copyright (c) 2020-2021 Carl Drougge                                     #
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

import bottle
import json
import sys
import os
import tarfile
import itertools
import collections
import functools

from accelerator.job import Job
from accelerator.dataset import Dataset
from accelerator.unixhttp import call, WaitressServer
from accelerator.build import fmttime
from accelerator.configfile import resolve_listen
from accelerator.shell.workdir import job_data, workdir_jids
from accelerator.compat import setproctitle, url_quote

def get_job(jobid):
	if jobid.endswith('-LATEST'):
		base = jobid.rsplit('-', 1)[0]
		jobid = os.readlink(Job(base + '-0').path[:-2] + '-LATEST')
	return Job(jobid)

# why wasn't Accept specified in a sane manner (like sending it in preference order)?
def get_best_accept(*want):
	d = {want[0]: -1} # fallback to first specified
	# {'a/*': 'a/exact'}, reversed() so earlier win
	want_short = {w.split('/', 1)[0] + '/*': w for w in reversed(want)}
	want = set(want)
	want.update(want_short)
	for accept in bottle.request.headers.get('Accept', '').split(','):
		accept = accept.split(';')
		mimetype = accept[0].strip()
		if mimetype in want:
			d[mimetype] = 1.0
			for p in accept[1:]:
				p = p.strip()
				if p.startswith('q='):
					try:
						d[mimetype] = float(p[2:])
					except ValueError:
						pass
	# include k in sort key as well so /* gets lower priority
	_, best = sorted(((v, k) for k, v in d.items()), reverse=True)[0]
	return want_short.get(best, best)

class JSONEncoderWithSet(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, set):
			return list(o)
		return json.JSONEncoder.default(o)

json_enc = JSONEncoderWithSet(indent=4, ensure_ascii=False).encode

def view(name, subkey=None):
	def view_decorator(func):
		@functools.wraps(func)
		def view_wrapper(**kw):
			res = func(**kw)
			if isinstance(res, dict):
				accept = get_best_accept('application/json', 'text/json', 'text/html')
				if accept == 'text/html':
					return bottle.template(name, **res)
				else:
					bottle.response.content_type = accept + '; charset=UTF-8'
					if callable(subkey):
						res = subkey(res)
					elif subkey:
						res = res[subkey]
					return [json_enc(res), '\n']
			return res
		return view_wrapper
	return view_decorator

def fix_stacks(stacks, report_t):
	pid2pid = {}
	pid2jid = {}
	pid2part = {}
	job_pid = None
	for pid, indent, msg, t in stacks:
		if pid not in pid2pid and pid not in pid2jid:
			if msg.startswith('analysis('):
				pid2part[pid] = ''.join(c for c in msg if c.isdigit())
				pid2pid[pid] = job_pid
			else:
				pid2jid[pid] = msg.split(' ', 1)[0]
				job_pid = pid
		elif pid not in pid2part:
			pid2part[pid] = msg if msg in ('prepare', 'synthesis') else 'analysis'
		jobpid = pid
		while jobpid in pid2pid:
			jobpid = pid2pid[jobpid]
		jid = pid2jid[jobpid]
		if indent < 0:
			msg = msg.split('\n')
			start = len(msg) - 1
			while start and sum(map(bool, msg[start:])) < 5:
				start -= 1
			msg = [line.rstrip('\r') for line in msg[start:]]
			t = fmttime(report_t - t)
		else:
			t = fmttime(report_t - t, short=True)
		yield (jid, pid, indent, pid2part.get(pid), msg, t)

# datasets aren't dicts, so can't be usefully json encoded
def ds_json(d):
	ds = d['ds']
	keys = ('job', 'name', 'parent', 'filename', 'previous', 'hashlabel', 'lines')
	res = {k: getattr(ds, k) for k in keys}
	res['method'] = ds.job.method
	res['columns'] = {k: c.type for k, c in ds.columns.items()}
	return res

def main(argv, cfg):
	prog = argv.pop(0)
	if '-h' in argv or '--help' in argv or len(argv) not in (0, 1):
		print('usage: %s [listen_on]' % (prog,))
		print('runs a web server on listen_on (default localhost:8520, can be socket path)')
		print('for displaying results (result_directory)')
		return
	if argv:
		listen = argv[0]
	else:
		listen = 'localhost:8520'
	cfg.board_listen = resolve_listen(listen)[0]
	if isinstance(cfg.board_listen, str):
		# The listen path may be relative to the directory the user started us
		# from, but the reloader will exec us from the project directory, so we
		# have to be a little gross.
		cfg.board_listen = os.path.join(cfg.user_cwd, cfg.board_listen)
		sys.argv[sys.argv.index(listen)] = cfg.board_listen
	run(cfg, from_shell=True)

def run(cfg, from_shell=False):
	project = os.path.split(cfg.project_directory)[1]
	setproctitle('ax board for %s on %s' % (project, cfg.board_listen,))

	def call_s(*path):
		return call(os.path.join(cfg.url, *map(url_quote, path)))

	def call_u(*path):
		return call(os.path.join(cfg.urd, *map(url_quote, path)), server_name='urd')

	@bottle.get('/')
	@view('main')
	def main_page():
		return dict(
			project=project,
			workdirs=cfg.workdirs,
		)

	@bottle.get('/results')
	def results():
		res = {}
		for fn in os.listdir(cfg.result_directory):
			if fn.endswith('_'):
				continue
			ffn = os.path.join(cfg.result_directory, fn)
			try:
				jobid, name = os.readlink(ffn).split('/')[-2:]
				res[fn] = dict(
					jobid=jobid,
					name=name,
					ts=os.lstat(ffn).st_mtime,
					size=os.stat(ffn).st_size,
				)
			except OSError:
				continue
		bottle.response.content_type = 'application/json; charset=UTF-8'
		bottle.response.set_header('Cache-Control', 'no-cache')
		return json.dumps(res)

	@bottle.get('/results/<name>')
	def file(name):
		return bottle.static_file(name, root=cfg.result_directory)

	@bottle.get('/status')
	@view('status')
	def status():
		status = call_s('status/full')
		if 'short' in bottle.request.query:
			if status.idle:
				return 'idle'
			else:
				t, msg, _ = status.current
				return '%s (%s)' % (msg, fmttime(status.report_t - t, short=True),)
		else:
			status.tree = list(fix_stacks(status.pop('status_stacks', ()), status.report_t))
			return status

	@bottle.get('/last_error')
	@view('last_error')
	def last_error():
		return call_s('last_error')

	@bottle.get('/job/<jobid>/method.tar.gz/')
	@bottle.get('/job/<jobid>/method.tar.gz/<name:path>')
	def job_method(jobid, name=None):
		job = get_job(jobid)
		with tarfile.open(job.filename('method.tar.gz'), 'r:gz') as tar:
			if name:
				info = tar.getmember(name)
			else:
				members = [info for info in tar.getmembers() if info.isfile()]
				if len(members) == 1 and not name:
					info = members[0]
				else:
					return bottle.template('job_method_list', members=members, job=job)
			bottle.response.content_type = 'text/plain; charset=UTF-8'
			return tar.extractfile(info).read()

	@bottle.get('/job/<jobid>/<name:path>')
	def job_file(jobid, name):
		job = get_job(jobid)
		res = bottle.static_file(name, root=job.path)
		if not res.content_type and res.status_code < 400:
			# bottle default is text/html, which is probably wrong.
			res.content_type = 'text/plain'
		return res

	@bottle.get('/job/<jobid>')
	@bottle.get('/job/<jobid>/')
	@view('job')
	def job(jobid):
		job = get_job(jobid)
		try:
			post = job.post
		except IOError:
			post = None
		if post:
			aborted = False
			files = [fn for fn in job.files() if fn[0] != '/']
			subjobs = [Job(jobid) for jobid in post.subjobs]
			current = call_s('job_is_current', job)
		else:
			aborted = True
			current = False
			files = None
			subjobs = None
		return dict(
			job=job,
			aborted=aborted,
			current=current,
			output=os.path.exists(job.filename('OUTPUT')),
			datasets=job.datasets,
			params=job.params,
			subjobs=subjobs,
			files=files,
		)

	@bottle.get('/dataset/<dsid:path>')
	@view('dataset', ds_json)
	def dataset(dsid):
		ds = Dataset(dsid.rstrip('/'))
		q = bottle.request.query
		if q.column:
			lines = int(q.lines or 10)
			it = ds.iterate(None, q.column)
			it = itertools.islice(it, lines)
			t = ds.columns[q.column].type
			if t in ('datetime', 'date', 'time',):
				it = map(str, it)
			elif t in ('bytes', 'pickle',):
				it = map(repr, it)
			res = list(it)
			bottle.response.content_type = 'application/json; charset=UTF-8'
			return json.dumps(res)
		else:
			return dict(ds=ds)

	def load_workdir(jobs, name):
		known = call_s('workdir', name)
		jobs[name + '-LATEST'] = None # Sorts first
		try:
			latest = os.readlink(os.path.join(cfg.workdirs[name], name + '-LATEST'))
		except OSError:
			latest = None
		for jid in workdir_jids(cfg, name):
			jobs[jid] = job_data(known, jid)
		if latest in jobs:
			jobs[name + '-LATEST'] = jobs[latest]
		else:
			del jobs[name + '-LATEST']
		return jobs

	@bottle.get('/workdir/<name>')
	@view('workdir', 'jobs')
	def workdir(name):
		return dict(name=name, jobs=load_workdir(collections.OrderedDict(), name))

	@bottle.get('/workdir')
	@bottle.get('/workdir/')
	@view('workdir', 'jobs')
	def all_workdirs():
		jobs = collections.OrderedDict()
		for name in sorted(cfg.workdirs):
			load_workdir(jobs, name)
		return dict(name='ALL', jobs=jobs)

	@bottle.get('/methods')
	@view('methods')
	def methods():
		methods = call_s('methods')
		by_package = collections.defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		return dict(methods=methods, by_package=by_package)

	@bottle.get('/method/<name>')
	@view('method', 'data')
	def method(name):
		methods = call_s('methods')
		if name not in methods:
			return bottle.HTTPError(404, 'Method %s not found' % (name,))
		return dict(name=name, data=methods[name], cfg=cfg)

	@bottle.get('/urd')
	@bottle.get('/urd/')
	@view('urd', 'lists')
	def urd():
		return dict(
			lists=call_u('list'),
			project=project,
		)

	@bottle.get('/urd/<user>/<build>')
	@bottle.get('/urd/<user>/<build>/')
	@view('urdlist', 'timestamps')
	def urdlist(user, build):
		key = user + '/' + build
		return dict(
			key=key,
			timestamps=call_u(key, 'since/0'),
		)

	@bottle.get('/urd/<user>/<build>/<ts>')
	@view('urditem', 'entry')
	def urditem(user, build, ts):
		key = user + '/' + build + '/' + ts
		d = call_u(key)
		return dict(key=key, entry=d)

	bottle.TEMPLATE_PATH = [os.path.join(os.path.dirname(__file__), 'board')]
	if from_shell:
		kw = {'reloader': True}
	else:
		kw = {'quiet': True}
	kw['server'] = WaitressServer
	listen = cfg.board_listen
	if isinstance(listen, tuple):
		kw['host'], kw['port'] = listen
	else:
		from accelerator.server import check_socket
		check_socket(listen)
		kw['host'] = listen
		kw['port'] = 0
	bottle.run(**kw)
