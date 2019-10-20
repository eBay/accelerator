#!/usr/bin/env python3

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Carl Drougge                            #
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

from compat import unicode
from dscmdhelper import name2ds
import g

def main(argv, usage_extra=''):
	usage = "Usage: %%(prog)s%s [options] pattern ds [ds [...]] [column [column [...]]" % (usage_extra,)
	parser = ArgumentParser(usage=usage)
	parser.add_argument('-c', '--chain',       dest="chain",      action='store_true', help="Follow dataset chains", )
	parser.add_argument('-i', '--ignore-case', dest="ignorecase", action='store_true', help="Case insensitive pattern", )
	parser.add_argument('pattern')
	parser.add_argument('ds_or_column', nargs='+')
	args = parser.parse_args(argv)

	pat_s = re.compile(args.pattern                , re.IGNORECASE if args.ignorecase else 0)
	pat_b = re.compile(args.pattern.encode('utf-8'), re.IGNORECASE if args.ignorecase else 0)
	datasets = []
	columns = []
	
	for ds_or_col in args.ds_or_column:
		try:
			assert not columns, "Everything after the first column is a column"
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
		for sliceno in range(g.SLICES):
			p = Process(target=one_slice, args=(sliceno,), name='slice-%d' % (sliceno,))
			p.start()
			children.append(p)
		for p in children:
			p.join()
	except KeyboardInterrupt:
		print()

if __name__ == '__main__':
	from accelerator.dscmdhelper import init
	init()
	main(sys.argv[1:])
