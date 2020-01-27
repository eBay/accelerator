############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Carl Drougge                            #
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
from multiprocessing import Process
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

	def grep(lines):
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
		for items in lines:
			if match(items):
				# This will be atomic if the line is not too long
				# (at least up to PIPE_BUF bytes, should be at least 512).
				write(1, b'\t'.join(map(fmt, items)) + b'\n')

	def one_slice(sliceno):
		try:
			for ds in datasets:
				if args.chain:
					f = ds.iterate_chain
				else:
					f = ds.iterate
				grep(f(sliceno, columns))
		except KeyboardInterrupt:
			return
		except IOError as e:
			if e.errno == errno.EPIPE:
				return
			else:
				raise

	try:
		children = []
		want_slices = set(args.slice) if args.slice else range(g.slices)
		for sliceno in want_slices:
			p = Process(target=one_slice, args=(sliceno,), name='slice-%d' % (sliceno,))
			p.start()
			children.append(p)
		for p in children:
			p.join()
	except KeyboardInterrupt:
		print()
