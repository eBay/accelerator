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

description = r'''
Tests the parsed:type writers.
With plain values, parsable values and unparsable values.
'''

# this is just to make nan values compare equal, but the easiest way
# to do that is to just stringify everything.
# since we aren't comparing different types, this is safe.
def nanfix(lst):
	return [str(v) for v in lst]

def hashfilter(typ, values, sliceno):
	from accelerator.g import slices
	if typ == 'json':
		return values[sliceno::slices]
	else:
		from accelerator.dsutil import typed_writer
		h = typed_writer(typ).hash
		return [v for v in values if h(v) % slices == sliceno]

def synthesis(job, slices):
	def test(typ, write_values, want_values, bad_values=[], allow_none=True):
		dw = job.datasetwriter(
			name=typ,
			columns={'value': ('parsed:' + typ, allow_none)},
			hashlabel=None if typ == 'json' else 'value',
		)
		write = dw.get_split_write()
		for value in write_values:
			try:
				write(value)
			except Exception as e:
				raise Exception('Failed to write %r to %s column: %r' % (value, typ, e))
		for value in ['foo', '1 two'] + bad_values:
			try:
				write(value)
				raise Exception('writer for parsed:%s allowed %r' % (typ, value,))
			except (ValueError, OverflowError):
				pass
		ds = dw.finish()
		for sliceno in range(slices):
			want_slice = hashfilter(typ, want_values, sliceno)
			got_slice = list(ds.iterate(sliceno, 'value'))
			assert nanfix(got_slice) == nanfix(want_slice), "%s got %r, wanted %r in slice %d" % (typ, got_slice, want_slice, sliceno)

	inf = float('inf')
	nan = float('nan')

	test('number',
		['42.0', '42', inf, '-inf', None, '9'*200,  'nan', nan, ' 1e55 ', '+1'],
		[ 42.0,   42,  inf,  -inf,  None, 10**200-1, nan,  nan,   1e55,     1],
		['1+1', '+', '-', ''],
	)
	test('complex32',
		[42-0.5j, '0.5+infj',        None, 'j', '9',  '-inf+nanj',       '+1'],
		[42-0.5j, complex(0.5, inf), None,  1j,  9+0j, complex(-inf, nan), 1+0j],
		['1+1', '+', '-', ''],
	)
	test('complex64',
		[-0.99j, 'inf-1j',         None, '  nan-infj ',      'nan+nanj',        '-j'],
		[-0.99j, complex(inf, -1), None, complex(nan, -inf), complex(nan, nan), 0-1j],
		['1-1', '+', '-', ''],
	)
	test('float32',
		[-0.75, '0.5', '2e3',   None, '1e100', 'nan', nan, '\n\t+5.\r', '011'],
		[-0.75,  0.5,   2000.0, None,  inf,     nan,  nan,       5.0,     11.0],
		['0x1.5p+5', '1+1', '+', '-', ''],
	)
	test('float64',
		[-0.99, '-0.1', None, '1e100', '-inf', 'nan', inf, '1e+2', ' -011 \n'],
		[-0.99,  -0.1,  None,  1e100,   -inf,   nan,  inf,  100.0,   -11.0],
		['0x5', '1+1', '+', '-', ''],
	)
	test('int32',
		[None, '42', 0, '-99', '1', 2, 3, 4, 5, ' 9 ', '011', '-0'],
		[None,  42,  0,  -99,   1,  2, 3, 4, 5,   9,     11,    0],
		['1.', '.1', '1.0', '1+1', '+', '-', ''],
	)
	test('int64',
		[None, '42', 0, '-99', '1', 2, 3, 4, 5, '\r9\n', '011', '+0'],
		[None,  42,  0,  -99,   1,  2, 3, 4, 5,   9,       11,    0],
		['1.', '.1', '1.0', '1+1', '+', '-', ''],
	)
	test('bits32',
		['42', 0, '-0', '1', 2, 3, 4, 5, '\r9\n', '011', '+0'],
		[ 42,  0,   0,   1,  2, 3, 4, 5,   9,       11,    0],
		['1.', '.1', '1.0', '1+1', '+', '-', '', '-99'],
		allow_none=False,
	)
	test('bits64',
		['42', 0, '-0', '1', 2, 3, 4, 5, '\r9\n', '011', '+0',],
		[ 42,  0,   0,   1,  2, 3, 4, 5,   9,       11,    0],
		['1.', '.1', '1.0', '1+1', '+', '-', '', '-99'],
		allow_none=False,
	)
	test('json',
		[[1, 2], '[3, 4]', '"foo"', None, 'null', 'NaN', nan, '{"foo": [-Infinity]}'],
		[[1, 2],  [3, 4],   "foo",  None,  None,   nan,  nan,  {'foo': [-inf]}],
		['"foo', '1+1', '', 'None'],
	)
