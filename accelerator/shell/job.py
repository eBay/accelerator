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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from traceback import print_exc
from os.path import split, realpath
from datetime import datetime
import errno

from accelerator.setupfile import encode_setup
from accelerator.job import Job, WORKDIRS
from accelerator.compat import FileNotFoundError

def show(path):
	if '/' not in path:
		job = Job(path)
	else:
		path, jid = split(realpath(path))
		job = Job(jid)
		WORKDIRS[job.workdir] = path
	print(job.path)
	print('=' * len(job.path))
	setup = job.json_load('setup.json')
	setup.pop('_typing', None)
	setup.starttime = str(datetime.fromtimestamp(setup.starttime))
	if 'endtime' in setup:
		setup.endtime = str(datetime.fromtimestamp(setup.endtime))
	print(encode_setup(setup, as_str=True))
	try:
		with job.open('datasets.txt') as fh:
			print()
			print('datasets:')
			for line in fh:
				print('    %s/%s' % (job, line[:-1],))
	except IOError:
		pass
	try:
		post = job.json_load('post.json')
	except FileNotFoundError:
		print('\x1b[31mWARNING: Job did not finish\x1b[m')
		post = None
	if post and post.subjobs:
		print()
		print('subjobs:')
		for sj in sorted(post.subjobs):
			print('   ', sj)
	if post and post.files:
		print()
		print('files:')
		for fn in sorted(post.files):
			print('   ', job.filename(fn))
	print()

def main(argv, cfg):
	prog = argv.pop(0)
	if '--help' in argv or '-h' in argv or not argv:
		print('usage: %s [jobid or path] [...]' % (prog,))
		print('show setup.json, dataset list, etc for jobs')
		return
	for path in argv:
		try:
			show(path)
		except Exception as e:
			if isinstance(e, IOError) and e.errno == errno.EPIPE:
				raise
			print_exc()
			print("Failed to show %r" % (path,))
