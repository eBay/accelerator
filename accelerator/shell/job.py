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
import argparse
import re

from accelerator.setupfile import encode_setup
from accelerator.job import Job, WORKDIRS
from accelerator.compat import FileNotFoundError, parse_intermixed_args
from accelerator.unixhttp import call

def show(job, show_output):
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
	out = job.output()
	if show_output:
		if out:
			print('output (use --just-output/-O to see only the output):')
			print(out)
			if not out.endswith('\n'):
				print()
		else:
			print(job, 'produced no output')
			print()
	elif out:
		print('%s produced %d bytes of output, use --output/-o to see it' % (job, len(out),))
		print()

def main(argv, cfg):
	descr = 'show setup.json, dataset list, etc for jobs'
	parser = argparse.ArgumentParser(prog=argv.pop(0), description=descr)
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-o', '--output', action='store_true', help='show job output')
	group.add_argument('-O', '--just-output', action='store_true', help='show only job output')
	group.add_argument('-P', '--just-path', action='store_true', help='show only job path')
	parser.add_argument('jobid', nargs='+', metavar='jobid or path')
	args = parse_intermixed_args(parser, argv)
	for path in args.jobid:
		try:
			if '/' not in path:
				if re.search(r'-\d+$', path):
					job = Job(path)
				else:
					found = call(cfg.url + '/find_latest/' + path)
					if not found:
						raise Exception('No job with method %s available.' % (path,))
					job = Job(found.id)
			else:
				path, jid = split(realpath(path))
				job = Job(jid)
				WORKDIRS[job.workdir] = path
			if args.just_output:
				out = job.output()
				print(out, end='' if out.endswith('\n') else '\n')
			elif args.just_path:
				print(job.path)
			else:
				show(job, args.output)
		except Exception as e:
			if isinstance(e, IOError) and e.errno == errno.EPIPE:
				raise
			print_exc()
			print("Failed to show %r" % (path,))
