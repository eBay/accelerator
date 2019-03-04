############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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
Write datasets with old and new backing types, verify both read back
and type correctly.

There's no writer for the old json type, so that's not tested.
'''

from dataset import Dataset, DatasetWriter
from compat import uni, PY2, PY3
import subjobs

raw_data = [
	"this is not a number",
	42,
	10000000000000000000000000,
	0.1,
	None,
	100,
	-5,
]

str_data = [
	None if v is None else str(v) for v in raw_data
]
paddedstr_data = [
	None if v is None else "\t%s  " % (v,) for v in raw_data
]

columns = dict(
	ascii_new='ascii',
	ascii_old='_v2_ascii',
	bytes_new='bytes',
	bytes_old='_v2_bytes',
	unicode_new='unicode',
	unicode_old='_v2_unicode',
)

def do_one(params, name, data):
	dw = DatasetWriter(name=name, columns=columns)
	dw.set_slice(0)
	for v in data:
		if v is None:
			d = dict(
				ascii_new=None,
				ascii_old=None,
				bytes_new=None,
				bytes_old=None,
				unicode_new=None,
				unicode_old=None,
			)
		else:
			d = dict(
				ascii_new=v,
				ascii_old=v,
				bytes_new=uni(v).encode("ascii"),
				bytes_old=uni(v).encode("ascii"),
				unicode_new=uni(v),
				unicode_old=uni(v),
			)
		dw.write_dict(d)
	# We don't really want the other slices, but write one thing to
	# each, to make sure it doesn't show up in slice 0.
	# (Small slice merging will put it in the same file, so this is
	# a real risk.)
	for sliceno in range(1, params.slices):
		dw.set_slice(sliceno)
		dw.write_dict(d)
	dw.finish()

	# verify we got what we asked for
	me_ds = Dataset(params.jobid, name)
	for colname, coltype in columns.items():
		col = me_ds.columns[colname]
		assert col.type == coltype.split("_")[-1], colname
		assert col.backing_type == coltype, colname
		for want, got in zip(data, me_ds.iterate(0, colname)):
			if want is not None:
				if PY2 and "unicode" in coltype:
					want = uni(want)
				if PY3 and "bytes" in coltype:
					want = want.encode("ascii")
			assert want == got, "%s in %s did not contain the expected value. Wanted %r but got %r." % (colname, me_ds, want, got)

	# check that both types of bytes filter correctly through typing
	jid = subjobs.build("dataset_type", datasets=dict(source=me_ds), options=dict(
		column2type=dict(
			ascii_new="bytes",
			ascii_old="number", # fails on the string, so that gets filtered out everywhere
			bytes_new="bytes",
			bytes_old="bytes",
		),
		filter_bad=True,
	))
	ds = Dataset(jid)
	# verify the number first
	data_it = iter(raw_data)
	next(data_it) # skip the filtered out string
	for got in ds.iterate(0, "ascii_old"):
		want = next(data_it)
		if want is None:
			# Becomes 0 because the typer (unfortunately) sees it as an empty string
			want = 0
		assert want == got, "ascii_old in %s did not type correctly as number. Wanted %r but got %r." % (ds, want, got)
	# now verify all the bytes ones are ok, no longer containing the string.
	for colname in ("ascii_new", "bytes_new", "bytes_old",):
		data_it = iter(data)
		next(data_it) # skip the filtered out string
		for got in ds.iterate(0, colname):
			want = next(data_it)
			if want is not None:
				want = want.encode("ascii")
			assert want == got, "%s in %s did not roundtrip correctly as bytes. Wanted %r but got %r." % (colname, ds, want, got)

	# and now check that the Nones are ok after making bytes from ascii and unicode from bytes.
	jid = subjobs.build("dataset_type", datasets=dict(source=me_ds), options=dict(
		column2type=dict(
			ascii_new="bytes",
			ascii_old="bytes",
			bytes_new="unicode:ascii",
			bytes_old="unicode:ascii",
		),
	))
	ds = Dataset(jid)
	for colname in ("ascii_new", "ascii_old", "bytes_new", "bytes_old",):
		for want, got in ds.iterate(0, ["unicode_new", colname]):
			assert uni(want) == uni(got), "%s in %s did not roundtrip correctly as bytes. Wanted %r but got %r." % (colname, ds, want, got)

def synthesis(params):
	do_one(params, "str", str_data)
	do_one(params, "paddedstr", paddedstr_data)
