############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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

import sys
from os import environ
from os.path import join
from accelerator.build import JobList
from accelerator.job import Job


def main(argv, cfg):
	prog = argv.pop(0)
	user = environ.get('USER', 'NO-USER')
	if '--help' in argv or '-h' in argv or not argv:
		fh = sys.stdout if argv else sys.stderr
		print('usage: %s path [path [...]]' % (prog,), file=fh)
		print(file=fh)
		print('path is an optionally shortened path to an urd list, using the', file=fh)
		print('same rules as :urdlist: job-specifiers. You can put :: around', file=fh)
		print('the path here too, if you want.', file=fh)
		print('use "path/since/ts" or just "path/" to list timestamps', file=fh)
		print('use "/" to list all lists', file=fh)
		print(file=fh)
		print('examples:', file=fh)
		print('  "%s example" is "%s %s/example/latest"' % (prog, prog, user,), file=fh)
		print('  "%s :example:" is also "%s %s/example/latest"' % (prog, prog, user,), file=fh)
		print('  "%s example/2021-04-14" is "%s %s/example/2021-04-14"' % (prog, prog, user,), file=fh)
		print('  "%s :foo/bar/first:" is "%s foo/bar/first"' % (prog, prog,), file=fh)
		print('  "%s example/" is "%s %s/example/since/0"' % (prog, prog, user,), file=fh)
		return not argv
	def call(*path):
		from accelerator.unixhttp import call
		return call(join(cfg.urd, *path), server_name='urd')
	def resolve(path):
		if path.startswith(':'):
			if not path.endswith(':'):
				print('%r should either end with : or not start with :' % (path,), file=sys.stderr)
				return None
			path = path[1:-1]
		if not path:
			return None
		if path == '/':
			return ['list']
		path = path.split('/')
		if path[-1] == '':
			path.pop()
			since = ['since', '0']
		elif len(path) > 2 and path[-2] == 'since':
			since = path[-2:]
			path = path[:-2]
		else:
			since = None
		if len(path) < 3 - bool(since):
			path.insert(0, user)
		if since:
			path.append(since[0])
			path.append(since[1] + '?captions')
		elif len(path) < 3:
			path.append('latest')
		return path
	for path in argv:
		path = resolve(path)
		if not path:
			continue
		res = call(*path)
		print(fmt(res))

def fmt(res):
	if not res:
		return ''
	def fmt_caption(path, caption):
		return template % (path, caption,) if caption else path
	if isinstance(res, list):
		if isinstance(res[0], list):
			tlen = max(len(ts) for ts, _ in res)
			template = '%%-%ds : %%s' % (tlen,)
			return '\n'.join(fmt_caption(*item) for item in res)
		else:
			return '\n'.join(res)
	if res['deps']:
		deps = sorted(
			('%s/%s' % (k, v['timestamp'],), v['caption'],)
			for k, v in res['deps'].items()
		)
		if len(deps) > 1:
			plen = max(len(path) for path, _ in deps)
			template = '%%-%ds : %%s' % (plen,)
			deps = '\n           '.join(fmt_caption(*dep) for dep in deps)
		else:
			template = '%s : %s'
			deps = fmt_caption(*deps[0])
	else:
		deps = ''
	joblist = JobList(Job(j, m) for m, j in res['joblist'])
	return "timestamp: %s\ncaption  : %s\ndeps     : %s\n%s" % (
		res['timestamp'],
		res['caption'],
		deps,
		joblist.pretty,
	)
