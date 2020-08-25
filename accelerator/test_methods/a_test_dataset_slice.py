############################################################################
#                                                                          #
# Copyright (c) 2020 Carl Drougge                                          #
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
Test dataset iteration slicing.
'''

from accelerator.error import DatasetError

def synthesis(job, slices):
	dw = job.datasetwriter({'a': 'int32'}, name='first')
	dw.set_slice(0)
	dw.write(0)
	dw.write(1)
	dw.write(2)
	dw.set_slice(1)
	dw.set_slice(2)
	for ix in range(3, 100):
		dw.write(ix)
	for sliceno in range(3, slices):
		dw.set_slice(sliceno)
	ds = dw.finish()
	expect = list(range(100))
	def get(sliceno, slice, columns='a'):
		return list(ds.iterate(sliceno, columns, slice=slice))
	def get_chain(sliceno, slice, columns='a'):
		res = list(ds.iterate_chain(sliceno, columns, slice=slice))
		assert res == list(ds.chain().iterate(sliceno, columns, slice=slice))
		return res
	assert get(None, None) == expect
	assert get(None, -5) == expect[-5:]
	assert get(None, 97) == expect[97:]
	assert get(None, slice(93)) == expect[:93]
	assert get(None, slice(-97)) == expect[:-97]
	assert get(None, slice(0, -15)) == expect[:-15]
	assert get(None, slice(None, -15)) == expect[:-15]
	assert get(None, slice(10, 50, 3)) == expect[10:50:3]
	assert get(None, slice(None, None, 7)) == expect[::7]
	assert get(None, slice(1, 3)) == expect[1:3]
	assert get(None, slice(-30, -3)) == expect[-30:-3]
	assert get(None, -100) == expect
	assert get(None, 0) == expect
	assert get(None, slice(0, None)) == expect
	assert get(None, slice(None, None)) == expect
	assert get(None, 100) == []
	assert get(0, None) == expect[:3]
	assert get(0, -2) == expect[1:3]
	assert get(2, 3) == expect[6:]
	assert get(2, -97) == expect[-97:]
	assert get('roundrobin', 3) == [4, 2] + expect[5:]
	assert get('roundrobin', -40) == expect[-40:]
	assert get('roundrobin', slice(3)) == [0, 3, 1]
	assert get('roundrobin', slice(-10)) == [0, 3, 1, 4, 2] + expect[5:-10]
	assert get('roundrobin', 100) == []
	def assert_fails(sliceno, slice, func=get, columns='a'):
		try:
			func(sliceno, slice, columns)
			raise Exception("Iterating with slice %r should have failed" % (slice,))
		except DatasetError:
			pass
	assert_fails(None, -101)
	assert_fails(None, slice(20, 10))
	assert_fails(None, slice(20, 10, -1))
	assert_fails(None, slice(0, None, -1))
	assert_fails(None, 101)
	assert_fails(0, 4)
	assert_fails(2, -98)
	assert_fails('roundrobin', 101)
	dw = job.datasetwriter({'a': 'int32', 'b': 'int32'}, previous=ds, name='second')
	write = dw.get_split_write()
	write(100, -1)
	write(101, -2)
	write(102, -3)
	ds = dw.finish()
	expect2 = [(100, -1), (101, -2), (102, -3)]
	expect.extend(v[0] for v in expect2)
	assert get(None, None, None) == expect2
	assert get_chain(None, None) == expect
	assert get_chain(0, None) == [0, 1, 2, 100]
	assert get_chain(0, 2) == [2, 100]
	assert get_chain(0, -2) == [2, 100]
	assert get_chain(0, -1, ['a', 'b']) == [(100, -1)] # no error because we slice off 'first'
	assert_fails(0, -2, get_chain, ['a', 'b']) # error because 'first' has no b column.
	assert get_chain(None, slice(90, -2)) == expect[90:-2]
	assert get_chain(None, slice(99, -4)) == []
	assert_fails(None, slice(99, -5), get_chain)
	assert get_chain(None, slice(2, -2, 2)) == expect[2:-2:2]
