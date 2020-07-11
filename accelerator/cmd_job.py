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
from os.path import join, split, normpath
from datetime import datetime

from accelerator.extras import json_load
from accelerator.setupfile import encode_setup
from accelerator.job import Job
from accelerator.compat import open

def show(path):
	if '/' not in path:
		path = Job(path).path
	path = normpath(path)
	print(path)
	print('=' * len(path))
	jid = split(path)[1]
	setup = json_load(join(path, 'setup.json'))
	setup.pop('_typing', None)
	setup.starttime = str(datetime.fromtimestamp(setup.starttime))
	setup.endtime = str(datetime.fromtimestamp(setup.endtime))
	print(encode_setup(setup, as_str=True))
	try:
		with open(join(path, 'datasets.txt'), 'r', encoding='utf-8') as fh:
			print()
			print('datasets:')
			for line in fh:
				print('    %s/%s' % (jid, line[:-1],))
	except IOError:
		pass
	post = json_load(join(path, 'post.json'))
	if post.subjobs:
		print()
		print('subjobs:')
		for sj in sorted(post.subjobs):
			print('   ', sj)
	if post.files:
		print()
		print('files:')
		for fn in sorted(post.files):
			print('   ', fn)
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
		except Exception:
			print_exc()
			print("Failed to show %r" % (path,))
