from __future__ import division
from __future__ import print_function

description = r'''
Take a dataset and make a checksum of one or more columns.

This is a debugging aid, so you can compare datasets across machines,
even with different slicing.

If you set options.sort=False it's faster, but your datasets have to have
the same slicing and order to compare equal.

options.columns defaults to all columns in the source dataset.

Note that this uses about 64 bytes of RAM per line, so you can't sum huge
datasets. (So one GB per 20M lines or so.)
'''

from hashlib import md5
from itertools import chain
from extras import DotDict

options = dict(
	columns      = set(),
	sort         = True,
)

datasets = ('source',)

def prepare():
	return sorted(options.columns or datasets.source.columns)

def analysis(sliceno, prepare_res):
	columns = prepare_res
	src = datasets.source.iterate(sliceno, columns)
	return [md5('\0'.join(map(str, line))).digest() for line in src]

def synthesis(prepare_res, analysis_res):
	all = chain.from_iterable(analysis_res)
	if options.sort:
		all = sorted(all)
	res = md5(''.join(all)).hexdigest()
	print("%s: %s" % (datasets.source, res,))
	return DotDict(sum=int(res, 16), sort=options.sort, columns=prepare_res, source=datasets.source)
