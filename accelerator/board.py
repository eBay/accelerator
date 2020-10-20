############################################################################
#                                                                          #
# Copyright (c) 2020 Carl Drougge                                          #
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
import time

from accelerator.job import Job
from accelerator.dataset import Dataset
from accelerator.unixhttp import call, WaitressServer
from accelerator.build import fmttime
from accelerator.configfile import resolve_listen
from accelerator.shell.workdir import job_data, workdir_jids
from accelerator.compat import setproctitle

def get_job(jobid):
	if jobid.endswith('-LATEST'):
		base = jobid.rsplit('-', 1)[0]
		jobid = os.readlink(Job(base + '-0').path[:-2] + '-LATEST')
	return Job(jobid)

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

	@bottle.get('/')
	@bottle.view('main')
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
	def status():
		url = cfg.url + '/status/full'
		status = call(url)
		if 'short' in bottle.request.query:
			if status.idle:
				return 'idle'
			else:
				t, msg, _ = status.current
				return '%s (%s)' % (msg, fmttime(time.time() - t, short=True),)
		else:
			return bottle.template('status', **status)

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
	@bottle.view('job')
	def job(jobid):
		job = get_job(jobid)
		try:
			post = job.post
		except IOError as e:
			post = None
		if post:
			aborted = False
			files = [fn for fn in job.files() if fn[0] != '/']
			subjobs = [Job(jobid) for jobid in post.subjobs]
		else:
			aborted = True
			files = None
			subjobs = None
		return dict(
			job=job,
			aborted=aborted,
			output=os.path.exists(job.filename('OUTPUT')),
			datasets=job.datasets,
			params=job.params,
			subjobs=subjobs,
			files=files,
		)

	@bottle.get('/dataset/<dsid:path>')
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
			return bottle.template('dataset', ds=ds)

	def load_workdir(jobs, name):
		known = call(cfg.url + '/workdir/' + name)
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
	@bottle.view('workdir')
	def workdir(name):
		return dict(name=name, jobs=load_workdir(collections.OrderedDict(), name))

	@bottle.get('/workdir')
	@bottle.get('/workdir/')
	@bottle.view('workdir')
	def all_workdirs():
		jobs = collections.OrderedDict()
		for name in sorted(cfg.workdirs):
			load_workdir(jobs, name)
		return dict(name='ALL', jobs=jobs)

	@bottle.get('/methods')
	@bottle.view('methods')
	def methods():
		methods = call(cfg.url + '/methods')
		by_package = collections.defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		return dict(methods=methods, by_package=by_package)

	@bottle.get('/method/<name>')
	@bottle.view('method')
	def method(name):
		methods = call(cfg.url + '/methods')
		if name not in methods:
			return bottle.HTTPError(404, 'Method %s not found' % (name,))
		return dict(name=name, data=methods[name], cfg=cfg)

	@bottle.get('/urd')
	@bottle.get('/urd/')
	@bottle.view('urd')
	def urd():
		return dict(
			lists=call(cfg.urd + '/list'),
			project=project,
		)

	@bottle.get('/urd/<user>/<build>')
	@bottle.get('/urd/<user>/<build>/')
	@bottle.view('urdlist')
	def urdlist(user, build):
		key = user + '/' + build
		return dict(
			key=key,
			timestamps=call(cfg.urd + '/' + key + '/since/0'),
		)

	@bottle.get('/urd/<user>/<build>/<ts>')
	@bottle.view('urditem')
	def urditem(user, build, ts):
		key = user + '/' + build + '/' + ts
		return dict(
			key=key,
			entry=call(cfg.urd + '/' + key),
		)

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
