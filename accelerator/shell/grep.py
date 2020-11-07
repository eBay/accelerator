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
from itertools import chain, repeat
import errno
from os import write

from accelerator.compat import unicode, izip, imap, parse_intermixed_args
from .dscmdhelper import name2ds
from accelerator import g

def main(argv):
	usage = "%(prog)s [options] pattern ds [ds [...]] [column [column [...]]"
	parser = ArgumentParser(usage=usage, prog=argv.pop(0))
	parser.add_argument('-c', '--chain',        action='store_true', help="follow dataset chains", )
	parser.add_argument('-C', '--color',        action='store_true', help="color matched text", )
	parser.add_argument('-i', '--ignore-case',  action='store_true', help="case insensitive pattern", )
	parser.add_argument('-H', '--headers',      action='store_true', help="print column names before output (and on each change)", )
	parser.add_argument('-o', '--ordered',      action='store_true', help="output in order (one slice at a time)", )
	parser.add_argument('-g', '--grep',         action='append',     help="grep this column only, can be specified multiple times", metavar='COLUMN')
	parser.add_argument('-s', '--slice',        action='append',     help="grep this slice only, can be specified multiple times",  type=int)
	parser.add_argument('-t', '--separator', help="field separator (default tab)", default='\t')
	parser.add_argument('-D', '--show-dataset', action='store_true', help="show dataset on matching lines", )
	parser.add_argument('-S', '--show-sliceno', action='store_true', help="show sliceno on matching lines", )
	parser.add_argument('-L', '--show-lineno',  action='store_true', help="show lineno (per slice) on matching lines", )
	parser.add_argument('pattern')
	parser.add_argument('dataset')
	parser.add_argument('columns', nargs='*', default=[])
	args = parse_intermixed_args(parser, argv)

	pat_s = re.compile(args.pattern                , re.IGNORECASE if args.ignore_case else 0)
	pat_b = re.compile(args.pattern.encode('utf-8'), re.IGNORECASE if args.ignore_case else 0)
	datasets = [name2ds(args.dataset)]
	columns = []

	separator_s = args.separator
	separator_b = separator_s.encode('utf-8')

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

	grep_columns = set(args.grep or ())
	if grep_columns == set(columns):
		grep_columns = None

	if args.slice:
		want_slices = []
		for s in args.slice:
			assert 0 <= s < g.slices, "Slice %d not available" % (s,)
			if s not in want_slices:
				want_slices.append(s)
	else:
		want_slices = list(range(g.slices))

	if args.chain:
		datasets = list(chain.from_iterable(ds.chain() for ds in datasets))

	def grep(ds, sliceno):
		# Use bytes for everything if anything is bytes, str otherwise. (For speed.)
		if any(ds.columns[col].backing_type == 'bytes' for col in (grep_columns or columns or ds.columns)):
			def strbytes(v):
				return str(v).encode('utf-8', 'replace')
			def mk_iter(col):
				if ds.columns[col].backing_type in ('bytes', 'unicode', 'ascii',):
					return ds._column_iterator(sliceno, col, _type='bytes')
				else:
					return imap(strbytes, ds._column_iterator(sliceno, col))
			chk = pat_b.search
		else:
			def mk_iter(col):
				if ds.columns[col].backing_type in ('unicode', 'ascii',):
					return ds._column_iterator(sliceno, col, _type='unicode')
				else:
					return imap(str, ds._column_iterator(sliceno, col))
			chk = pat_s.search
		def fmt(v):
			if not isinstance(v, (unicode, bytes)):
				v = str(v)
			if isinstance(v, unicode):
				v = v.encode('utf-8', 'replace')
			return v
		def color(item):
			pos = 0
			parts = []
			for m in pat_b.finditer(item):
				a, b = m.span()
				parts.extend((item[pos:a], b'\x1b[31m', item[a:b], b'\x1b[m'))
				pos = b
			parts.append(item[pos:])
			return b''.join(parts)
		prefix = []
		if args.show_dataset:
			prefix.append(ds.encode('utf-8'))
		if args.show_sliceno:
			prefix.append(str(sliceno).encode('utf-8'))
		prefix = tuple(prefix)
		def show(prefix, items):
			items = map(fmt, items)
			if args.color:
				items = map(color, items)
			# This will be atomic if the line is not too long
			# (at least up to PIPE_BUF bytes, should be at least 512).
			write(1, separator_b.join(prefix + tuple(items)) + b'\n')
		if grep_columns and grep_columns != set(columns or ds.columns):
			grep_iter = izip(*(mk_iter(col) for col in grep_columns))
			lines_iter = ds.iterate(sliceno, columns)
		else:
			grep_iter = repeat(None)
			lines_iter = izip(*(mk_iter(col) for col in (columns or sorted(ds.columns))))
		lines = izip(grep_iter, lines_iter)
		if args.show_lineno:
			for lineno, (grep_items, items) in enumerate(lines):
				if any(imap(chk, grep_items or items)):
					show(prefix + (str(lineno).encode('utf-8'),), items)
		else:
			for grep_items, items in lines:
				if any(imap(chk, grep_items or items)):
					show(prefix, items)

	def one_slice(sliceno, q, wait_for):
		try:
			if q:
				q.get()
			for ds in datasets:
				if ds in wait_for:
					q.task_done()
					q.get()
				grep(ds, sliceno)
		except KeyboardInterrupt:
			return
		except IOError as e:
			if e.errno == errno.EPIPE:
				return
			else:
				raise
		finally:
			# Make sure we are joinable
			try:
				q.task_done()
			except Exception:
				pass

	headers_prefix = []
	if args.show_dataset:
		headers_prefix.append('[DATASET]')
	if args.show_sliceno:
		headers_prefix.append('[SLICE]')
	if args.show_lineno:
		headers_prefix.append('[LINE]')

	headers = {}
	if args.headers:
		if columns:
			current_headers = columns
		else:
			current_headers = None
			for ds in datasets:
				candidate_headers = sorted(ds.columns)
				if candidate_headers != current_headers:
					headers[ds] = current_headers = candidate_headers
			current_headers = headers.pop(datasets[0])
		def show_headers(headers):
			print('\x1b[34m' + separator_s.join(headers_prefix + headers) + '\x1b[m')
		show_headers(current_headers)

	queues = []
	children = []
	if not args.ordered:
		q = None
		wait_for = set(headers)
		for sliceno in want_slices[1:]:
			if wait_for:
				q = JoinableQueue()
				q.put(None)
				queues.append(q)
			p = Process(
				target=one_slice,
				args=(sliceno, q, wait_for),
				name='slice-%d' % (sliceno,),
			)
			p.daemon = True
			p.start()
			children.append(p)
		want_slices = want_slices[:1]

	try:
		for ds in datasets:
			if ds in headers:
				for q in queues:
					q.join()
				show_headers(headers.pop(ds))
				for q in queues:
					q.put(None)
			for sliceno in want_slices:
				grep(ds, sliceno)
		for c in children:
			c.join()
	except KeyboardInterrupt:
		print()
