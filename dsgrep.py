#!/usr/bin/env python3

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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
from optparse import OptionParser
from multiprocessing import Process
import errno

import dscmdhelper
import g

dscmdhelper.init()

parser = OptionParser(usage="Usage: %prog [options] pattern ds [ds [...]] [column [column [...]]")
parser.add_option('-c', '--chain',       dest="chain",      action='store_true', help="Follow dataset chains", )
parser.add_option('-i', '--ignore-case', dest="ignorecase", action='store_true', help="Case insensitive pattern", )

options, args = parser.parse_args(sys.argv[1:])

def badargs():
	parser.print_help(file=sys.stderr)
	exit(1)

if len(args) < 2:
	badargs()

pat_s = re.compile(args[0]                , re.IGNORECASE if options.ignorecase else 0)
pat_b = re.compile(args[0].encode('utf-8'), re.IGNORECASE if options.ignorecase else 0)
datasets = []
columns = []

for ds_or_col in args[1:]:
	try:
		assert not columns, "Everything after the first column is a column"
		datasets.append(dscmdhelper.name2ds(ds_or_col))
	except Exception:
		columns.append(ds_or_col)

if not datasets:
	badargs()

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
	for items in lines:
		if match(items):
			items = (item.decode('utf-8', 'replace') if isinstance(item, bytes) else str(item) for item in items)
			print('\t'.join(items))

def one_slice(sliceno):
	try:
		for ds in datasets:
			if options.chain:
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
