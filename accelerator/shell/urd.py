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
from accelerator.build import JobList
from accelerator.job import Job
from accelerator.shell.parser import split_tildes, urd_call_w_tildes
from accelerator.error import UrdError
from accelerator.compat import url_quote


def main(argv, cfg):
	prog = argv.pop(0)
	user = environ.get('USER', 'NO-USER')
	if '--help' in argv or '-h' in argv:
		fh = sys.stdout if argv else sys.stderr
		print('usage: %s path [path [...]]' % (prog,), file=fh)
		print(file=fh)
		print('path is an optionally shortened path to an urd list.', file=fh)
		print('one element is a list name, two elements are list and timestamp.', file=fh)
		print('(and three elements are the whole thing, user/list/timestamp.)', file=fh)
		print(file=fh)
		print('use "path/since/ts" or just "path/" to list timestamps', file=fh)
		print('use "/" (or nothing) to list all lists', file=fh)
		print(file=fh)
		print('you can also use :urdlist:[entry] job specifiers. urdlist follows the same', file=fh)
		print('path rules as above, entry is an optional argument to joblist.get() printing', file=fh)
		print('just the resultant jobid.', file=fh)
		print(file=fh)
		print('in job specifiers you can use ~ to move to forwards (later, down) along a', file=fh)
		print('list of timestamps and ^ to move backwards (earlier, up).', file=fh)
		print(file=fh)
		print('examples:', file=fh)
		print('  "%s example" is "%s %s/example/latest"' % (prog, prog, user,), file=fh)
		print('  "%s :example:" is also "%s %s/example/latest"' % (prog, prog, user,), file=fh)
		print('  "%s example/2021-04-14" is "%s %s/example/2021-04-14"' % (prog, prog, user,), file=fh)
		print('  "%s :foo/bar/first:" is "%s foo/bar/first"' % (prog, prog,), file=fh)
		print('  "%s :bar/first~:" is the second timestamp in %s/bar' % (prog, user,), file=fh)
		print('  "%s example/" is "%s %s/example/since/0"' % (prog, prog, user,), file=fh)
		return not argv
	def resolve_path_part(path):
		if not path:
			return []
		if path == '/':
			return ['list']
		path = [url_quote(el) for el in path.split('/')]
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
	def urd_get(path):
		if path.startswith(':'):
			a = path[1:].split(':', 1)
			if len(a) == 1:
				print('%r should have two or no :' % (path,), file=sys.stderr)
				return None, None
			path = a[0]
			try:
				entry = int(a[1], 10)
			except ValueError:
				entry = a[1] or None
			path, tildes = split_tildes(path)
		else:
			entry = tildes = None
		path = resolve_path_part(path)
		if len(path) != 3 and tildes:
			print("path %r isn't walkable (~^)" % ('/'.join(path),), file=sys.stderr)
			return None, None
		if len(path) != 3 and entry is not None:
			print("path %r doesn't take an entry (%r)" % ('/'.join(path), entry,), file=sys.stderr)
			return None, None
		try:
			res = urd_call_w_tildes(cfg, '/'.join(path), tildes)
		except UrdError as e:
			print(e, file=sys.stderr)
			res = None
		return res, entry
	for path in argv or ('/',):
		res, entry = urd_get(path)
		if not res:
			continue
		print(fmt(res, entry))

def fmt(res, entry):
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
	joblist = JobList(Job(j, m) for m, j in res['joblist'])
	if entry:
		return joblist.get(entry, '')
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
	return "timestamp: %s\ncaption  : %s\ndeps     : %s\n%s" % (
		res['timestamp'],
		res['caption'],
		deps,
		joblist.pretty,
	)
