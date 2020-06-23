############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

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
from json import JSONEncoder
from heapq import merge

from accelerator.extras import DotDict
from accelerator.compat import PY2

options = dict(
	columns      = set(),
	sort         = True,
)

datasets = ('source',)

if PY2:
	bytesrepr = repr
else:
	def bytesrepr(v):
		return repr(v).encode('utf-8')

def bytesstr(v):
	return v.encode('utf-8')

def bytesstr_none(v):
	return b'None\xff' if v is None else v.encode('utf-8')

def self_none(v):
	return b'None\xff' if v is None else v

jsonenc = JSONEncoder(
	sort_keys=True,
	ensure_ascii=False,
	check_circular=False,
	separators=(',', ':'),
).encode
def sortdicts(v):
	return jsonenc(v).encode('utf-8')

def prepare():
	columns = sorted(options.columns or datasets.source.columns)
	translators = {}
	# Special casing the string types here is just a performance
	# optimisation. (Which changes the hash, but not how good it is.)
	# Special casing json is needed because the iteration order of
	# dicts is not fixed (depending on python version).
	# Same with pickle, but worse (many picklable values will break this).
	for n in columns:
		col = datasets.source.columns[n]
		if col.type == 'bytes' or (col.type == 'ascii' and PY2):
			# doesn't need any encoding, but might need None-handling.
			if col.none_support:
				translators[n] = self_none
		elif col.type in ('unicode', 'ascii'):
			if col.none_support:
				translators[n] = bytesstr_none
			else:
				translators[n] = bytesstr
		elif col.type == 'json':
			translators[n] = sortdicts
		elif col.type == 'pickle':
			translators[n] = sortdicts
			print('WARNING: Column %s is pickle, may not work' % (n,))
		else:
			translators[n] = bytesrepr
	return columns, translators

def analysis(sliceno, prepare_res):
	columns, translators = prepare_res
	src = datasets.source.iterate(sliceno, columns, translators=translators)
	res = []
	for line in src:
		m = md5()
		for item in line:
			m.update(item)
			m.update(b'\0') # Some separator is better than nothing.
		res.append(m.digest())
	if options.sort:
		res.sort()
	return res

def synthesis(prepare_res, analysis_res):
	if options.sort:
		all = merge(*analysis_res)
	else:
		all = chain.from_iterable(analysis_res)
	res = md5(b''.join(all)).hexdigest()
	print("%s: %s" % (datasets.source, res,))
	return DotDict(sum=int(res, 16), sort=options.sort, columns=prepare_res[0], source=datasets.source)
