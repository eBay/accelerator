############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2021 Carl Drougge                       #
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
from multiprocessing import Process, JoinableQueue
from itertools import chain, repeat
import errno
from os import write
import json
import datetime

from accelerator.compat import ArgumentParser
from accelerator.compat import unicode, izip, PY2
from accelerator.colourwrapper import colour
from .parser import name2ds
from accelerator import g

def main(argv, cfg):
	parser = ArgumentParser(
		usage="%(prog)s [options] pattern ds [ds [...]] [column [column [...]]",
		prog=argv.pop(0),
	)
	parser.add_argument('-c', '--chain',        action='store_true', help="follow dataset chains", )
	parser.add_argument(      '--colour', '--color', nargs='?', const='always', choices=['auto', 'never', 'always'], type=str.lower, help="colour matched text. can be auto, never or always", metavar='WHEN', )
	parser.add_argument('-i', '--ignore-case',  action='store_true', help="case insensitive pattern", )
	parser.add_argument('-H', '--headers',      action='store_true', help="print column names before output (and on each change)", )
	parser.add_argument('-O', '--ordered',      action='store_true', help="output in order (one slice at a time)", )
	parser.add_argument('-g', '--grep',         action='append',     help="grep this column only, can be specified multiple times", metavar='COLUMN')
	parser.add_argument('-s', '--slice',        action='append',     help="grep this slice only, can be specified multiple times",  type=int)
	parser.add_argument('-D', '--show-dataset', action='store_true', help="show dataset on matching lines", )
	parser.add_argument('-S', '--show-sliceno', action='store_true', help="show sliceno on matching lines", )
	parser.add_argument('-L', '--show-lineno',  action='store_true', help="show lineno (per slice) on matching lines", )
	supported_formats = ('csv', 'raw', 'json',)
	parser.add_argument('-f', '--format', default='csv', choices=supported_formats, help="output format, csv (default) / " + ' / '.join(supported_formats[1:]), metavar='FORMAT', )
	parser.add_argument('-t', '--separator', help="field separator, default tab / tab-like spaces", )
	parser.add_argument('pattern')
	parser.add_argument('dataset', help='can be specified in the same ways as for "ax ds"')
	parser.add_argument('columns', nargs='*', default=[])
	args = parser.parse_intermixed_args(argv)

	pat_s = re.compile(args.pattern, re.IGNORECASE if args.ignore_case else 0)
	datasets = [name2ds(cfg, args.dataset)]
	columns = []

	for ds_or_col in args.columns:
		if columns:
			columns.append(ds_or_col)
		else:
			try:
				datasets.append(name2ds(cfg, ds_or_col))
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

	if columns or grep_columns:
		bad = False
		need_cols = set(columns)
		if grep_columns:
			need_cols.update(grep_columns)
		for ds in datasets:
			missing = need_cols - set(ds.columns)
			if missing:
				print('ERROR: %s does not have columns %r' % (ds, missing,), file=sys.stderr)
				bad = True
		if bad:
			return 1

	# never and always override env settings, auto (default) sets from env/tty
	if args.colour == 'never':
		colour.disable()
		highlight_matches = False
	elif args.colour == 'always':
		colour.enable()
		highlight_matches = True
	else:
		highlight_matches = colour.enabled

	# Don't highlight everything when just trying to cat
	if args.pattern == '':
		highlight_matches = False

	separator = args.separator
	if separator is None and not sys.stdout.isatty():
		separator = '\t'

	if separator is None:
		# special case where we try to be like a tab, but with spaces.
		# this is useful because terminals typically don't style tabs.
		def separate(items, lens):
			things = []
			for item, item_len in zip(items, lens):
				things.append(item)
				spaces = 8 - (item_len % 8)
				things.append(colour(' ' * spaces, 'cyan', 'underline'))
			return ''.join(things[:-1])
		separator = '\t'
	else:
		separator_coloured = colour(separator, 'cyan', 'underline')
		def separate(items, lens):
			return separator_coloured.join(items)

	def json_default(obj):
		if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
			return str(obj)
		elif isinstance(obj, complex):
			return [obj.real, obj.imag]
		else:
			return repr(obj)

	if args.format == 'csv':
		def escape_item(item):
			if item and (separator in item or item[0] in '\'"' or item[-1] in '\'"'):
				return '"' + item.replace('\n', '\\n').replace('"', '""') + '"'
			else:
				return item.replace('\n', '\\n')
		errors = 'surrogatepass'
	else:
		escape_item = None
		errors = 'replace' if PY2 else 'surrogateescape'

	def grep(ds, sliceno):
		def no_conv(v):
			return v
		def mk_conv(col):
			if ds.columns[col].type in ('bytes', 'unicode', 'ascii',):
				if not ds.columns[col].none_support:
					return no_conv
			return unicode
		chk = pat_s.search
		def mk_iter(col):
			if ds.columns[col].type == 'ascii':
				it = ds._column_iterator(sliceno, col, _type='unicode')
			else:
				it = ds._column_iterator(sliceno, col)
			if ds.columns[col].type == 'bytes':
				errors = 'replace' if PY2 else 'surrogateescape'
				if ds.columns[col].none_support:
					it = (None if v is None else v.decode('utf-8', errors) for v in it)
				else:
					it = (v.decode('utf-8', errors) for v in it)
			return it
		def colour_item(item):
			pos = 0
			parts = []
			for m in pat_s.finditer(item):
				a, b = m.span()
				parts.extend((item[pos:a], colour.red(item[a:b])))
				pos = b
			parts.append(item[pos:])
			return ''.join(parts)
		if args.format == 'json':
			prefix = {}
			dumps = json.JSONEncoder(ensure_ascii=False, default=json_default).encode
			if args.show_dataset:
				prefix['dataset'] = ds
			if args.show_sliceno:
				prefix['sliceno'] = sliceno
			def show():
				d = dict(zip(used_columns, items))
				if args.show_lineno:
					prefix['lineno'] = lineno
				if prefix:
					prefix['data'] = d
					d = prefix
				return dumps(d).encode('utf-8', 'surrogatepass')
		else:
			prefix = []
			if args.show_dataset:
				prefix.append(ds)
			if args.show_sliceno:
				prefix.append(str(sliceno))
			prefix = tuple(prefix)
			def show():
				data = list(prefix)
				if args.show_lineno:
					data.append(unicode(lineno))
				if PY2:
					show_items = (v if isinstance(v, unicode) else str(v).decode('utf-8', 'replace') for v in items)
				else:
					show_items = map(str, items)
				show_items = list(show_items)
				lens = (len(item) for item in data + show_items)
				if highlight_matches:
					show_items = list(map(colour_item, show_items))
				if escape_item:
					lens_unesc = (len(item) for item in data + show_items)
					show_items = list(map(escape_item, show_items))
					lens_esc = (len(item) for item in data + show_items)
					lens = (l + esc - unesc for l, unesc, esc in zip(lens, lens_unesc, lens_esc))
				data.extend(show_items)
				return separate(data, lens).encode('utf-8', errors)
		used_columns = columns or sorted(ds.columns)
		if grep_columns and grep_columns != set(used_columns):
			grep_iter = izip(*(mk_iter(col) for col in grep_columns))
			conv_items = [mk_conv(col) for col in grep_columns]
		else:
			grep_iter = repeat(None)
			conv_items = [mk_conv(col) for col in used_columns]
		lines_iter = izip(*(mk_iter(col) for col in used_columns))
		for lineno, (grep_items, items) in enumerate(izip(grep_iter, lines_iter)):
			if any(chk(conv(item)) for conv, item in izip(conv_items, grep_items or items)):
				# This will be atomic if the line is not too long
				# (at least up to PIPE_BUF bytes, should be at least 512).
				write(1, show() + b'\n')

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
			if args.format != 'json':
				show_items = headers_prefix + headers
				if escape_item:
					show_items = list(map(escape_item, show_items))
				print(separate(map(colour.blue, show_items), map(len, show_items)))
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
