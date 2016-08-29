from __future__ import division

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

from dataset import dataset
from chaining import iterate_datasets
from hashlib import md5
from itertools import chain
from extras import DotDict

options = dict(
	columns      = set(),
	sort         = True,
)

datasets = ('source',)

def prepare():
	if options.columns:
		columns = options.columns
	else:
		d = dataset()
		d.load(datasets.source)
		columns = d.name_type_dict().keys()
	return sorted(columns)

def analysis(sliceno, prepare_res):
	columns = prepare_res
	src = iterate_datasets(sliceno, columns, datasets.source)
	return [md5('\0'.join(map(str, line))).digest() for line in src]

def synthesis(prepare_res, analysis_res):
	all = chain.from_iterable(analysis_res)
	if options.sort:
		all = sorted(all)
	res = md5(''.join(all)).hexdigest()
	print "%s: %s" % (datasets.source, res,)
	return DotDict(sum=int(res, 16), sort=options.sort, columns=prepare_res, source=datasets.source)
