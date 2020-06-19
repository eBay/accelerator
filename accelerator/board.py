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
import os
import tarfile

from accelerator.job import Job
from accelerator.dataset import Dataset

def main(argv, cfg):
	prog = argv.pop(0)
	if '-h' in argv or '--help' in argv or len(argv) not in (0, 1):
		print('usage: %s [port]' % (prog,))
		print('runs a web server on port (default 8520)')
		print('for displaying results (result_directory)')
		return
	port = int(argv[0]) if argv else 8520

	@bottle.get('/')
	@bottle.view('main')
	def main_page():
		return dict(project=os.path.split(cfg.project_directory)[1])

	@bottle.get('/results')
	def results():
		res = {}
		for fn in os.listdir(cfg.result_directory):
			if fn.endswith('_'):
				continue
			ffn = os.path.join(cfg.result_directory, fn)
			try:
				jobid = os.readlink(ffn).split('/')[-2]
				res[fn] = dict(
					jobid=jobid,
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

	@bottle.get('/job/<jobid>/method.tar.gz/')
	@bottle.get('/job/<jobid>/method.tar.gz/<name:path>')
	def job_method(jobid, name=None):
		job = Job(jobid)
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
		job = Job(jobid)
		return bottle.static_file(name, root=job.path)

	@bottle.get('/job/<jobid>')
	@bottle.get('/job/<jobid>/')
	@bottle.view('job')
	def job(jobid):
		job = Job(jobid)
		try:
			prefix = job.path + '/'
			files = [fn[len(prefix):] for fn in job.files() if fn.startswith(prefix)]
		except OSError:
			# This happens for jobs that didn't finish.
			files = []
		return dict(
			job=job,
			output=os.path.exists(job.filename('OUTPUT')),
			datasets=job.datasets,
			params=job.params,
			files=files,
		)

	@bottle.get('/dataset/<dsid:path>')
	@bottle.view('dataset')
	def dataset(dsid):
		return dict(ds=Dataset(dsid.rstrip('/')), max_lines=25)

	bottle.TEMPLATE_PATH = [os.path.join(os.path.dirname(__file__), 'board')]
	bottle.run(port=port, reloader=True)
