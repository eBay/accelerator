#!/usr/bin/env python
#
# Print information about a dataset(chain)

from __future__ import division, print_function

import sys

import dscmdhelper

dscmdhelper.init()

# for every name argument
for n in sys.argv[1:]:
	ds = dscmdhelper.name2ds(n)
	print("Parent:", ds.parent)
	print("Columns:")
	# there's a better way to do this, right?
	len_n = len_t = 0
	for n, c in ds.columns.items():
		len_n = max(len_n, len(n))
		len_t = max(len_t, len(c.type))
	template = "    {0:%d}  {1:%d}" % (len_n, len_t,)
	for n, c in ds.columns.items():
		print(template.format(n, c.type))
	print("{0:n} lines".format(sum(ds.lines)))
	if ds.previous:
		chain = ds.chain()
		print("Chain length {0:n}, from {1} to {2}".format(len(chain), chain[0], chain[-1]))
		print("{0:n} total lines".format(sum(sum(ds.lines) for ds in chain)))
