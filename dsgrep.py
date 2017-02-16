#!/usr/bin/env python3
#
# Print information about a dataset(chain)

from __future__ import division, print_function

import sys
import re
from optparse import OptionParser

import dscmdhelper

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

pat = re.compile(args[0], re.IGNORECASE if options.ignorecase else 0)
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
	for items in lines:
		match = False
		p_items = []
		for item in items:
			if not isinstance(item, (bytes, str)):
				item = str(item)
			p_items.append(item)
			if pat.search(item):
				match = True
		if match:
			print('\t'.join(p_items))

for ds in datasets:
	if options.chain:
		f = ds.iterate_chain
	else:
		f = ds.iterate
	grep(f(None, columns))
