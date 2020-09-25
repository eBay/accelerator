############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

# grep in a dataset(chain)

from __future__ import division, print_function

import sys
import re
from argparse import ArgumentParser
from multiprocessing import Process, JoinableQueue
from itertools import chain
import errno
from os import write

from accelerator.compat import unicode
from accelerator.dscmdhelper import name2ds
from accelerator import g

def main(argv):
	usage = "%(prog)s [options] pattern ds [ds [...]] [column [column [...]]"
	parser = ArgumentParser(usage=usage, prog=argv.pop(0))
	parser.add_argument('-c', '--chain',       dest="chain",      action='store_true', help="follow dataset chains", )
	parser.add_argument('-i', '--ignore-case', dest="ignorecase", action='store_true', help="case insensitive pattern", )
	parser.add_argument('-H', '--headers',     dest="headers",    action='store_true', help="print column names before output (and on each change)", )
	parser.add_argument('-o', '--ordered',     dest="ordered",    action='store_true', help="Output in order (one slice at a time)", )
	parser.add_argument('-s', '--slice',       dest="slice",      action='append',     help="grep this slice only, can be specified multiple times",  type=int)
	parser.add_argument('pattern')
	parser.add_argument('dataset')
	parser.add_argument('columns', nargs='*', default=[])
	args = parser.parse_args(argv)

	pat_s = re.compile(args.pattern                , re.IGNORECASE if args.ignorecase else 0)
	pat_b = re.compile(args.pattern.encode('utf-8'), re.IGNORECASE if args.ignorecase else 0)
	datasets = [name2ds(args.dataset)]
	columns = []

	for ds_or_col in args.columns:
		if columns:
			columns.append(ds_or_col)
		else:
			try:
				datasets.append(name2ds(ds_or_col))
			except Exception:
				columns.append(ds_or_col)

	if not datasets:
		parser.print_help(file=sys.stderr)
		return 1

	if args.slice:
		want_slices = []
		for s in args.slice:
			assert 0 <= s < g.slices, "Slice %d not available" % (s,)
			if s not in want_slices:
				want_slices.append(s)
	else:
		if args.ordered:
			want_slices = [None]
		else:
			want_slices = list(range(g.slices))

	if args.chain:
		datasets = chain.from_iterable(ds.chain() for ds in datasets)

	def grep(ds, sliceno):
		chk_b = pat_b.search
		chk_s = pat_s.search
		def match(items):
			for item in items:
				if isinstance(item, bytes):
					if chk_b(item):
						return True
				else:
					if chk_s(str(item)):
						return True
		def fmt(v):
			if not isinstance(v, (unicode, bytes)):
				v = str(v)
			if isinstance(v, unicode):
				v = v.encode('utf-8', 'replace')
			return v
		for items in ds.iterate(sliceno, columns):
			if match(items):
				# This will be atomic if the line is not too long
				# (at least up to PIPE_BUF bytes, should be at least 512).
				write(1, b'\t'.join(map(fmt, items)) + b'\n')

	def one_slice(sliceno, q):
		try:
			if q:
				for ds in datasets:
					q.get()
					grep(ds, sliceno)
					q.task_done()
			else:
				for ds in datasets:
					grep(ds, sliceno)
		except KeyboardInterrupt:
			return
		except IOError as e:
			if e.errno == errno.EPIPE:
				return
			else:
				raise

	queues = []
	children = []
	if not args.ordered:
		q = None
		for sliceno in want_slices[1:]:
			if args.headers:
				q = JoinableQueue()
				queues.append(q)
			p = Process(
				target=one_slice,
				args=(sliceno, q,),
				name='slice-%d' % (sliceno,),
				daemon=True,
			)
			p.start()
			children.append(p)
		want_slices = want_slices[:1]

	headers = []
	try:
		for ds in datasets:
			if args.headers:
				new_headers = columns or sorted(ds.columns)
				if new_headers != headers:
					headers = new_headers
					print('\x1b[34m' + '\t'.join(headers) + '\x1b[m')
			for q in queues:
				q.put(None)
			for sliceno in want_slices:
				grep(ds, sliceno)
			for q in queues:
				q.join()
		for c in children:
			c.join()
	except KeyboardInterrupt:
		print()
