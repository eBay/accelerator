############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
Verify the dataset_hashpart method with various options.
'''

from datetime import date

from accelerator import subjobs
from accelerator.dataset import DatasetWriter, Dataset
from accelerator.gzwrite import typed_writer

data = [
	{"a column": "text", "also a column": b"octets", "number": 10, "date": date(1978, 1, 1)},
	{"a column": "a", "also a column": b"0", "number": -900, "date": date(2009, 1, 1)},
	{"a column": "b", "also a column": b"1", "number": -901, "date": date(2009, 1, 2)},
	{"a column": "c", "also a column": b"2", "number": -902, "date": date(2009, 1, 3)},
	{"a column": "d", "also a column": b"3", "number": -903, "date": date(2009, 1, 4)},
	{"a column": "e", "also a column": b"4", "number": -904, "date": date(2009, 1, 5)},
	{"a column": "f", "also a column": b"5", "number": -905, "date": date(2009, 1, 6)},
	{"a column": "g", "also a column": b"6", "number": -906, "date": date(2009, 1, 7)},
	{"a column": "h", "also a column": b"7", "number": -907, "date": date(2009, 1, 8)},
	{"a column": "i", "also a column": b"8", "number": -908, "date": date(2009, 1, 9)},
	{"a column": "j", "also a column": b"9", "number": -909, "date": date(2009, 2, 1)},
	{"a column": "k", "also a column": b"Z", "number": -999, "date": date(2009, 3, 1)},
	{"a column": "l", "also a column": b"Y", "number": -989, "date": date(2009, 4, 1)},
	{"a column": "m", "also a column": b"X", "number": -979, "date": date(2009, 5, 1)},
	{"a column": "n", "also a column": b"W", "number": -969, "date": date(2009, 6, 1)},
	{"a column": "o", "also a column": b"V", "number": -959, "date": date(2009, 7, 1)},
	{"a column": "p", "also a column": b"A", "number": -949, "date": date(2009, 8, 1)},
	{"a column": "q", "also a column": b"B", "number": -939, "date": date(2009, 9, 1)},
	{"a column": "r", "also a column": b"C", "number": -929, "date": None},
	{"a column": "s", "also a column": b"D", "number": None, "date": date(1970, 1, 1)},
	{"a column": "B", "also a column": None, "number": -242, "date": date(1970, 2, 3)},
	{"a column": None, "also a column": b"F", "number": -123, "date": date(1970, 4, 1)},
]
bonus_data = [
	{"a column": "foo", "also a column": b"bar", "number": 42, "date": date(2019, 4, 10)},
]
columns = {
	"a column": ("ascii", True),
	"also a column": ("bytes", True),
	"number": ("int32", True),
	"date": ("date", True),
}

def write(data, **kw):
	dw = DatasetWriter(columns=columns, **kw)
	w = dw.get_split_write_dict()
	for values in data:
		w(values)
	return dw.finish()

def verify(slices, data, source, previous=None, **options):
	jid = subjobs.build(
		"dataset_hashpart",
		datasets=dict(source=source, previous=previous),
		options=options,
	)
	hl = options["hashlabel"]
	h = typed_writer(columns[hl][0]).hash
	ds = Dataset(jid)
	good = {row[hl]: row for row in data}
	names = list(source.columns)
	for slice in range(slices):
		for row in ds.iterate_chain(slice, names):
			row = dict(zip(names, row))
			assert h(row[hl]) % slices == slice, "row %r is incorrectly in slice %d in %s" % (row, slice, ds)
			want = good[row[hl]]
			assert row == want, '%s (rehashed from %s) did not contain the right data for "%s".\nWanted\n%r\ngot\n%r' % (ds, source, hl, want, row)
	return ds

def synthesis(params):
	ds = write(data)
	for colname in data[0]:
		verify(params.slices, data, ds, hashlabel=colname)
	# ok, all the hashing stuff works out, let's test the chaining options.
	bonus_ds = write(bonus_data, name="bonus", previous=ds)
	# no chaining options - full chain
	verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date")
	# just the bonus ds
	verify(params.slices, bonus_data, bonus_ds, hashlabel="date", length=1)
	# built as a chain
	verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date", as_chain=True)
	# normal chaining
	a = verify(params.slices, data, ds, hashlabel="date")
	b = verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date", previous=a)
	assert b.chain() == [a, b], "chain of %s is not [%s, %s] as expected" % (b, a, b)
	# as_chain sparseness
	dw = DatasetWriter(columns=columns, name="empty")
	dw.get_split_write()
	ds = verify(params.slices, [], dw.finish(), hashlabel="date", as_chain=True)
	assert len(ds.chain()) == 1, ds + ": dataset_hashpart on empty dataset with as_chain=True did not produce a single dataset"
	# two populated slices with the same data, should end up in two datasets.
	dw = DatasetWriter(columns=columns, name="0 and 2")
	dw.set_slice(0)
	dw.write_dict(data[0])
	dw.set_slice(1)
	dw.set_slice(2)
	dw.write_dict(data[0])
	for s in range(3, params.slices):
		dw.set_slice(s)
	ds = verify(params.slices, [data[0]], dw.finish(), hashlabel="date", as_chain=True)
	got_slices = len(ds.chain())
	assert got_slices == 2, "%s (built with as_chain=True) has %d datasets in chain, expected 2." % (ds, got_slices,)
