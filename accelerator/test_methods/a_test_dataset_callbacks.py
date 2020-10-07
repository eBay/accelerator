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
Tests many variations of dataset iteration callbacks with skipping.
'''

from accelerator import SkipDataset, SkipSlice


def synthesis(job, slices):
	# Chain of four, (2, 1, 3, 1) slices with data, only second one has column c.
	def mk_ds(name, previous, *data, **columns):
		dw = job.datasetwriter(name=name, columns=columns, previous=previous)
		w = dw.get_split_write_list()
		for v in data:
			w(v)
		return dw.finish()
	ds1 = mk_ds('1', None, (0, 0), (1, 10), a='int32', b='int32')
	ds2 = mk_ds('2', ds1, (2, 20, 200), a='int32', b='int32', c='int32')
	ds3 = mk_ds('3', ds2, (3, 30), (4, 40), (5, 50), a='int32', b='int32')
	ds4 = mk_ds('4', ds3, (6, 60), a='int32', b='int32')

	# Sanity check.
	assert list(ds4.iterate_chain(0, 'a')) == [0, 2, 3, 6]
	assert list(ds4.iterate_chain(0, 'b')) == [0, 20, 30, 60]
	assert list(ds4.iterate_chain(1, 'b')) == [10, 40]
	assert list(ds4.iterate_chain(None, 'b')) == [0, 10, 20, 30, 40, 50, 60]

	# Don't skip anything, just check pre_callback happens at the right times.
	# First with just ds.
	current = []
	current_expect = []
	expect = {
		'1': [],
		'2': [0, 1],
		'3': [2],
		'4': [3, 4, 5],
	}
	seen_ds = set()
	def chk_pre_ds(ds):
		assert ds.name not in seen_ds
		seen_ds.add(ds.name)
		current_expect.extend(expect[ds.name])
		assert current_expect == current
	for v in ds4.iterate_chain(None, 'a', pre_callback=chk_pre_ds):
		current.append(v)
	assert seen_ds == set('1234')

	# Then with ds and sliceno.
	current = []
	current_expect = []
	expect = {
		('1', 0): 0,
		('1', 1): 1,
		('2', 0): 2,
		('3', 0): 3,
		('3', 1): 4,
		('3', 2): 5,
		('4', 0): 6,
	}
	seen_ds = set()
	def chk_pre_ds_s(ds, sliceno):
		t = (ds.name, sliceno)
		assert t not in seen_ds
		seen_ds.add(t)
		assert current_expect == current
		if t in expect:
			current_expect.append(expect[t])
	for v in ds4.iterate_chain(None, 'a', pre_callback=chk_pre_ds_s):
		current.append(v)
	assert seen_ds == set.union(*(set((n, s) for s in range(slices)) for n in '1234'))

	# Same for post_callback
	# First with just ds.
	current = []
	current_expect = []
	expect = {
		'1': [0, 1],
		'2': [2],
		'3': [3, 4, 5],
		'4': [6],
	}
	seen_ds = set()
	def chk_post_ds(ds):
		assert ds.name not in seen_ds
		seen_ds.add(ds.name)
		current_expect.extend(expect[ds.name])
		assert current_expect == current
	for v in ds4.iterate_chain(None, 'a', post_callback=chk_post_ds):
		current.append(v)
	assert seen_ds == set('1234')

	# Then with ds and sliceno.
	current = []
	current_expect = []
	expect = {
		('1', 0): 0,
		('1', 1): 1,
		('2', 0): 2,
		('3', 0): 3,
		('3', 1): 4,
		('3', 2): 5,
		('4', 0): 6,
	}
	seen_ds = set()
	def chk_post_ds_s(ds, sliceno):
		t = (ds.name, sliceno)
		assert t not in seen_ds
		seen_ds.add(t)
		if t in expect:
			current_expect.append(expect[t])
			assert current_expect == current
	for v in ds4.iterate_chain(None, 'a', post_callback=chk_post_ds_s):
		current.append(v)
	assert seen_ds == set.union(*(set((n, s) for s in range(slices)) for n in '1234'))

	# And now both together. With sliceno on pre_callback, without on post_callback.
	current = []
	current_expect = []
	expect = {
		('1', 1): 0,
		('1', 2): 1,
		('2', 1): 2,
		('3', 1): 3,
		('3', 2): 4,
		('3', 3): 5,
		('4', 1): 6,
	}
	seen_pre = set()
	seen_post = set()
	def chk_both_pre(ds, sliceno):
		t = (ds.name, sliceno)
		assert t not in seen_pre
		seen_pre.add(t)
		if t in expect:
			current_expect.append(expect[t])
	def chk_both_post(ds):
		assert ds.name not in seen_post
		seen_post.add(ds.name)
		if slices == 3 and ds.name == '3':
			# ('3', 3) never happens, so fake it.
			current_expect.append(5)
		assert current_expect == current, '%s %r %r'%(ds,current_expect, current)
	for v in ds4.iterate_chain(None, 'a', pre_callback=chk_both_pre, post_callback=chk_both_post):
		current.append(v)
	assert seen_pre == set.union(*(set((n, s) for s in range(slices)) for n in '1234'))
	assert seen_post == set('1234')

	# They seem to work, let's test skipping.
	# Skip dataset 2. (SkipDataset works.)
	def skip_2(ds):
		if ds.name == '2':
			raise SkipDataset
	assert list(ds4.iterate_chain(0, 'a', pre_callback=skip_2)) == [0, 3, 6]

	# Neither ds4 (which we call) not ds1 (which we start on) have column c.
	# We end up keeping only ds2. (Skipping uniterable works.)
	def skip_without_c(ds):
		if 'c' not in ds.columns:
			raise SkipDataset
	assert list(ds4.iterate_chain(None, 'c', pre_callback=skip_without_c)) == [200]

	# Skip slice 0. (SkipSlice works.)
	def skip_slice_0(ds, sliceno):
		if sliceno == 0:
			raise SkipSlice
	assert list(ds4.iterate_chain(None, 'a', pre_callback=skip_slice_0)) == [1, 4, 5]

	# Skip slice 0 in ds1, skip all of ds3. (Combining SkipSlice and SkipDataset works.)
	def skip_1s0_and_2(ds, sliceno):
		if ds.name == '1' and sliceno == 0:
			raise SkipSlice
		if ds.name == '3':
			raise SkipDataset
	assert list(ds4.iterate_chain(None, 'b', pre_callback=skip_1s0_and_2)) == [10, 20, 60]

	# Skip ds3 after first slice. (SkipDataset works within dataset.)
	def skip_ds3_after_s0(ds, sliceno):
		if ds.name == '3' and sliceno == 1:
			raise SkipDataset
	assert list(ds4.iterate_chain(None, 'b', pre_callback=skip_ds3_after_s0)) == [0, 10, 20, 30, 60]

	# Stop after first slice of ds3.
	def stop_after_ds3_s0_pre(ds, sliceno):
		if ds.name == '3' and sliceno == 1:
			raise StopIteration
	assert list(ds4.iterate_chain(None, 'b', pre_callback=stop_after_ds3_s0_pre)) == [0, 10, 20, 30]

	# Same thing but by using post_callback.
	def stop_after_ds3_s0_post(ds, sliceno):
		if ds.name == '3':
			raise StopIteration
	assert list(ds4.iterate_chain(None, 'b', post_callback=stop_after_ds3_s0_post)) == [0, 10, 20, 30]

	# And without sliceno, so we get all of ds3.
	def stop_after_ds3_post(ds):
		if ds.name == '3':
			raise StopIteration
	assert list(ds4.iterate_chain(None, 'b', post_callback=stop_after_ds3_post)) == [0, 10, 20, 30, 40, 50]

	# And finally both callbacks on plain .iterate.
	def skip_s1(ds, sliceno):
		if sliceno == 1:
			raise SkipSlice
	current = []
	at_last_called = [0]
	def at_last(ds):
		assert ds == ds3
		assert current == [30, 50]
		at_last_called[0] += 1
	for v in ds3.iterate(None, 'b', pre_callback=skip_s1, post_callback=at_last):
		current.append(v)
	assert at_last_called == [1]
	assert current == [30, 50]
