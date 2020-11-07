#!/usr/bin/env python

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
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

from __future__ import division, print_function

import argparse
import locale
from datetime import datetime, time, date
from math import ceil, floor, log10, isinf, isnan

from accelerator.compat import terminal_size, parse_intermixed_args
from .dscmdhelper import name2ds
from accelerator.dataset import Dataset
from accelerator.error import NoSuchWhateverError
from accelerator.job import Job

MINMAXWIDTH = 13 # minimum number of characters reserved for min/max values
COLUMNS, LINES = terminal_size()

def quote(x):
	if (x is None) or ({' ', '"', "'"} & set(x)):
		return repr(x)
	else:
		return x

def colwidth(rows):
	# find max string len per column
	return [max(len(s) for s in col) for col in zip(*rows)]

def printcolwise(data, template, printfunc, minrows=8, indent=4):
	if not data:
		return
	cols = (COLUMNS - indent) // (len(template.format(*printfunc(data[0]))) + 2)
	n = int(ceil(len(data) / cols))
	n = max(n, minrows)
	for r in range(n):
		v = data[r::n]
		if v:
			print(' ' * indent + '  '.join(template.format(*printfunc(x)) for x in v))

def main(argv):
	usage = "%(prog)s [options] ds [ds [...]]"
	parser = argparse.ArgumentParser(prog=argv.pop(0), usage=usage)
	parser.add_argument('-c', '--chain', action='store_true', help='list all datasets in a chain')
	parser.add_argument('-C', '--non_empty_chain', action='store_true', help='list all non-empty datasets in a chain')
	parser.add_argument('-l', '--list', action='store_true', help='list all datasets in a job with number of rows')
	parser.add_argument('-L', '--chainedlist', action='store_true', help='list all datasets in a job with number of chained rows')
	parser.add_argument('-m', '--suppress_minmax', action='store_true', help='do not print min/max column values')
	parser.add_argument('-n', '--suppress_columns', action='store_true', help='do not print columns')
	parser.add_argument('-q', '--suppress_errors', action='store_true', help='silently ignores bad input datasets/jobids')
	parser.add_argument('-s', '--slices', action='store_true', help='list relative number of lines per slice in sorted order')
	parser.add_argument('-S', '--chainedslices', action='store_true', help='same as -s but for full chain')
	parser.add_argument("dataset", nargs='+')
	args = parse_intermixed_args(parser, argv)
	args.chain = args.chain or args.non_empty_chain

	def finish(badinput):
		if badinput and not args.suppress_errors:
			print('Error, dataset(s) does not exist: ' + ', '.join(repr(x) for x in badinput))
			exit(1)
		exit()

	badinput = []

	if args.list or args.chainedlist:
		for n in args.dataset:
			try:
				if '/' in n:
					dsvec = Dataset(n).job.datasets
				else:
					dsvec = Job(n).datasets
			except Exception:
				dsvec = None
			if dsvec:
				print('%s' % (n,))
				v = []
				for ds in dsvec:
					if args.chainedlist:
						lines = sum(sum(x.lines) for x in ds.chain())
					else:
						lines = sum(ds.lines)
					v.append((ds.name, '{:n}'.format(lines)))
				len_n, len_l = colwidth(v)
				template = "{0:%d}  ({1:>%d})" % (len_n, len_l)
				for name, numlines in sorted(v):
					print('    ' + template.format(name, numlines))
			else:
				badinput.append(n)
				continue
		finish(badinput)

	for n in args.dataset:
		try:
			ds = name2ds(n)
		except NoSuchWhateverError:
			badinput.append(n)
			continue

		print(quote("%s/%s" % (ds.job, ds.name,)))
		if ds.parent:
			if isinstance(ds.parent, tuple):
				print("    Parents:")
				max_n = max(len(quote(x)) for x in ds.parent)
				template = "{1:%d}" % (max_n,)
				data = tuple((None, quote(x)) for ix, x in enumerate(ds.parent))
				data = sorted(data, key = lambda x: x[1])
				printcolwise(data, template, lambda x: x, minrows=8, indent=8)
			else:
				print("    Parent:", quote(ds.parent))
		print("    Method:", quote(ds.job.method))
		if ds.filename:
			print("    Filename:", quote(ds.filename))
		if ds.previous:
			print("    Previous:", quote(ds.previous))
		if ds.hashlabel:
			print("    Hashlabel:", quote(ds.hashlabel))

		def prettyminmax(minval, maxval):
			if args.suppress_minmax:
				return ''
			s = '[%%%ds, %%%ds]' % (MINMAXWIDTH, MINMAXWIDTH)
			if minval is None:
				return ''
			elif isinstance(minval, float):
				def intdigits(x):
					if isinf(x) or isnan(x):
						return 3
					return min(MINMAXWIDTH - 2, floor(log10(abs(x)) + 1)) if x else (MINMAXWIDTH - 2)//2
				ints = max(intdigits(minval), intdigits(maxval))
				if ints > 0:
					format = "%% %d.%df" % (ints, MINMAXWIDTH - ints - 2)
				elif ints < -4:
					format = "%% .%de" % (MINMAXWIDTH - 7,)
				else:
					format = "%% .%df" % (MINMAXWIDTH - 3,)
				def format_or_int(v):
					try:
						i = int(v)
						if v == i:
							return i
					except OverflowError:
						pass
					return locale.format_string(format, v)
				return s % (format_or_int(minval), format_or_int(maxval))
			elif isinstance(minval, int):
				return s % (minval, maxval)
			elif isinstance(minval, (date, time, datetime)):
				return s % (minval, maxval)
			else:
				return s % (minval, maxval)

		if not args.suppress_columns:
			print("    Columns:")
			name2typ = {n: c.type + '+None' if c.none_support else c.type for n, c in ds.columns.items()}
			len_n, len_t = colwidth((quote(n), name2typ[n]) for n, c in ds.columns.items())
			template = "{2} {0:%d}  {1:%d} " % (len_n, len_t,)
			for n, c in sorted(ds.columns.items()):
				if args.chainedslices or args.chain:
					chain = ds.chain()
					minval, maxval = chain.min(n), chain.max(n)
				else:
					minval, maxval = c.min, c.max
				hashdot = "\x1b[1m*\x1b[m" if n == ds.hashlabel else " "
				print(' ' * 8 + template.format(quote(n), name2typ[n], hashdot), prettyminmax(minval, maxval))
			print("    {0:n} columns".format(len(ds.columns)))
		print("    {0:n} lines".format(sum(ds.lines)))

		if ds.previous or args.chain:
			chain = ds.chain()
			if args.non_empty_chain:
				print("    Full chain length {0:n}, from {1} to {2}".format(len(chain), chain[0], chain[-1]))
				chain = [ds for ds in chain if sum(ds.lines)]
				print("    Filtered chain length {0:n}".format(len(chain)))
			if chain:
				if not args.non_empty_chain:
					print("    Chain length {0:n}, from {1} to {2}".format(len(chain), chain[0], chain[-1]))
				if args.chain:
					data = tuple((ix, "%s/%s" % (x.job, x.name), "{:n}".format(sum(x.lines))) for ix, x in enumerate(chain))
					max_n, max_l = colwidth(x[1:] for x in data)
					template = "{0:3}: {1:%d} ({2:>%d})" % (max_n, max_l)
					printcolwise(data, template, lambda x: (x[0], x[1], x[2]), minrows=8, indent=8)

		if args.slices or args.chainedslices:
			if args.chainedslices and ds.previous:
				data = ((ix, '{:n}'.format(sum(x)), sum(x)) for ix, x in enumerate(zip(*(x.lines for x in ds.chain()))))
				print('    Balance, lines per slice, full chain:')
			else:
				data = ((ix, '{:n}'.format(x), x) for ix, x in enumerate(ds.lines))
				if ds.previous:
					print('    Balance, lines per slice, tip dataset:')
				else:
					print('    Balance, lines per slice:')
			data = sorted(data, key=lambda x: -x[2])
			s = sum(x[2] for x in data)
			len_n = max(len(x[1]) for x in data)
			template = "{0:3}: {1!s}%% ({2:>%d})" % (len_n,)
			printcolwise(data, template, lambda x: (x[0], locale.format_string("%6.2f", (100 * x[2] / (s or 1e20))), x[1]), minrows=8, indent=8)
			print("    Max to average ratio: " + locale.format_string("%2.3f", (max(x[2] for x in data) / ((s or 1e20) / len(data)),) ))

		if ds.previous:
			print("    {0:n} total lines in chain".format(sum(sum(ds.lines) for ds in chain)))

	finish(badinput)
