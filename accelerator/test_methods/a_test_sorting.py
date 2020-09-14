############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
Test dataset_sort with various options on a dataset with all types.
'''

from itertools import chain
from math import isnan
from functools import cmp_to_key

from accelerator.dataset import Dataset
from accelerator import subjobs
from . import test_data

# ideally the subjobs should be part of staleness detection, but currently
# they are not, so we put gendata here explicitly.
depend_extra = (test_data, "a_test_sorting_gendata.py",)

def noneninf(v):
	if v is None:
		return float('-inf')
	return v

def unnan(l):
	return [
		tuple('NaN' if isinstance(v, float) and isnan(v) else v for v in t)
	for t in l]

def check_one(slices, key, source, reverse=False):
	jid = subjobs.build(
		"dataset_sort",
		options=dict(
			sort_columns=key,
			sort_order="descending" if reverse else "ascending",
		),
		datasets=dict(source=source),
	)
	ds = Dataset(jid)
	key_off = sorted(test_data.data).index(key)
	# This provides better separation than the replacement values
	# used in the actual sort method (but this is slow).
	if 'date' in key or 'time' in key:
		nonepos = 1
	else:
		nonepos = -1
	def cmp(a, b):
		a = a[key_off]
		b = b[key_off]
		if a is None:
			if b is None:
				return 0
			return nonepos
		if b is None:
			return -nonepos
		if isinstance(a, float):
			if isnan(a):
				if isnan(b):
					return 0
				return 1
			if isnan(b):
				return -1
		if a < b:
			return -1
		return a > b
	keycmp = cmp_to_key(cmp)
	for sliceno in range(slices):
		good = sorted(test_data.sort_data_for_slice(sliceno), key=keycmp, reverse=reverse)
		check = list(ds.iterate(sliceno))
		assert unnan(check) == unnan(good), "Slice %d sorted on %s bad (%s)" % (sliceno, key, jid,)

def synthesis(params):
	source = Dataset(subjobs.build("test_sorting_gendata"))
	# Test that all datatypes work for sorting
	for key in test_data.data:
		if not key.startswith('complex'):
			check_one(params.slices, key, source)
	# Check reverse sorting
	check_one(params.slices, "int32", source, reverse=True)
	# Check that sorting across slices and by two columns works
	jid = subjobs.build(
		"dataset_sort",
		options=dict(
			sort_columns=["int64", "int32"],
			sort_order="descending",
			sort_across_slices=True,
		),
		datasets=dict(source=source),
	)
	int64_off = sorted(test_data.data).index("int64")
	int32_off = sorted(test_data.data).index("int32")
	all_data = chain.from_iterable(test_data.sort_data_for_slice(sliceno) for sliceno in range(params.slices))
	good = sorted(all_data, key=lambda t: (noneninf(t[int64_off]), noneninf(t[int32_off]),), reverse=True)
	ds = Dataset(jid)
	check = list(ds.iterate(None))
	assert unnan(check) == unnan(good), "Sorting across slices on [int64, int32] bad (%s)" % (jid,)
