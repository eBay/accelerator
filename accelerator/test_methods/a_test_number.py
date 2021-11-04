############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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

import gzip
import struct
from accelerator.dsutil import typed_writer
from accelerator.subjobs import build

description = r'''
Test the number column type, verifying that both DatasetWriter and
dataset_type produce the expected bitstream.
'''

def synthesis(job, slices):
	# All the representations we want to verify.
	values = [
		# 1 byte values
		[i, '=B', i + 128 + 5]
		for i in range(-5, 123)
	] + [
		# 3 bytes values
		[-6, '=bh', 2, -6],
		[123, '=bh', 2, 123],
		[-0x8000, '=bh', 2, -0x8000],
		[0x7fff, '=bh', 2, 0x7fff],
		# 5 byte values
		[-0x8001, '=bi', 4, -0x8001],
		[0x8000, '=bi', 4, 0x8000],
		[-0x80000000, '=bi', 4, -0x80000000],
		[0x7fffffff, '=bi', 4, 0x7fffffff],
		# 9 byte values
		[-0x80000001, '=bq', 8, -0x80000001],
		[0x80000000, '=bq', 8, 0x80000000],
		[-0x8000000000000000, '=bq', 8, -0x8000000000000000],
		[0x7fffffffffffffff, '=bq', 8, 0x7fffffffffffffff],
		# special values
		[None, '=b', 0],
		[0.1, '=bd', 1, 0.1],
	]

	# Verify each value through a manual typed_writer.
	# Also write to a dataset, a csv and a value2bytes dict.
	value2bytes = {}
	dw = job.datasetwriter()
	dw.add('num', 'number', none_support=True)
	write = dw.get_split_write()
	with job.open('data.csv', 'wt') as csv_fh:
		csv_fh.write('num\n')
		for v in values:
			value = v[0]
			write(value)
			csv_fh.write('%s\n' % (value,))
			want_bytes = struct.pack(*v[1:])
			value2bytes[value] = want_bytes
			with typed_writer('number')('tmp', compression='gzip', none_support=True) as w:
				w.write(value)
			with gzip.open('tmp', 'rb') as fh:
				got_bytes = fh.read()
			assert want_bytes == got_bytes, "%r gave %r, wanted %r" % (value, got_bytes, want_bytes,)

	# Make sure we get the same representation through a dataset.
	# Assumes that the column is merged (a single file for all slices).
	ds = dw.finish()
	just_values = set(v[0] for v in values)
	assert set(ds.iterate(None, 'num')) == just_values, "Dataset contains wrong values"
	want_bytes = b''.join(value2bytes[v] for v in ds.iterate(None, 'num'))
	with gzip.open(ds.column_filename('num'), 'rb') as fh:
		got_bytes = fh.read()
	assert want_bytes == got_bytes, "All individual encoding are right, but not in a dataset?"

	# csvimport and dataset_type the same thing,
	# verify we got the same bytes
	jid = build('csvimport', filename=job.filename('data.csv'))
	jid = build('dataset_type', source=jid, column2type={'num': 'number'}, defaults={'num': None})
	with gzip.open(jid.dataset().column_filename('num'), 'rb') as fh:
		got_bytes = fh.read()
	assert want_bytes == got_bytes, "csvimport + dataset_type (%s) gave different bytes" % (jid,)
