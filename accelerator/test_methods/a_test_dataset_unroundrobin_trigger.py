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
Test that dataset_unroundrobin with trigger_column produces the
correct order and switches slice at the expected point.
'''

from accelerator import subjobs

def assert_slice(ds, sliceno, want):
	got = list(ds.iterate(sliceno))
	assert got == want, "slice %s in %s (from %s, trigger %s) gave\n\t%r,\nwanted\n\t%r" % (sliceno, ds, ds.job.params.datasets.source, ds.job.params.options.trigger_column, got, want,)

def synthesis(job, slices):
	unrr_values = [
		(0, 'a', 'A', 42),
		(0, 'a', 'A', 42),
		(0, 'a', 'A', 42),
		(1, 'a', 'A', 42),
		(1, 'b', 'A', 42),
		(1, 'b', 'B', 42),
		(1, 'b', 'C', 42),
		(2, 'b', 'D', 42),
		(2, 'c', 'E', 42),
	]

	# Use only slice 0 and 2, with 4 and 5 lines.
	dw = job.datasetwriter(name='0 and 2', columns=dict(a='int32', b='ascii', c='ascii', d='int32'))
	for sliceno in range(slices):
		dw.set_slice(sliceno)
		if sliceno == 0:
			dw.write_list(unrr_values[0])
			dw.write_list(unrr_values[2])
			dw.write_list(unrr_values[4])
			dw.write_list(unrr_values[6])
		if sliceno == 2:
			dw.write_list(unrr_values[1])
			dw.write_list(unrr_values[3])
			dw.write_list(unrr_values[5])
			dw.write_list(unrr_values[7])
			dw.write_list(unrr_values[8])
	src_ds = dw.finish()

	for col, split_point in (
		('a', 7,),
		('b', 4,),
		('c', 5,),
		('d', 9,),
	):
		ds_unrr = subjobs.build('dataset_unroundrobin', source=src_ds, trigger_column=col).dataset()
		assert_slice(ds_unrr, None, unrr_values)
		assert_slice(ds_unrr, 0, unrr_values[:split_point])
		assert_slice(ds_unrr, 2, unrr_values[split_point:])

	# Plain round robin.
	dw = job.datasetwriter(name='rr', columns=dict(a='int32', b='ascii', c='ascii', d='int32'))
	for sliceno in range(slices):
		dw.set_slice(sliceno)
		for v in unrr_values[sliceno::slices]:
			dw.write_list(v)
	src_ds = dw.finish()
	# The lines we expect in a slice
	def lines_for_slice(sliceno):
		if src_ds.lines[sliceno] == 0:
			return
		start_pos = sum(src_ds.lines[:sliceno])
		if start_pos == 0:
			trigger_v = object() # Not equal to anything else
		else:
			trigger_v = unrr_values[start_pos - 1][ix]
		cand_values = unrr_values[start_pos:]
		for v in cand_values[:src_ds.lines[sliceno]]:
			if v[ix] != trigger_v: # assumes values don't return to trigger_v
				yield v
		if v[ix] == trigger_v:
			# If we reached the end of "our" values without starting we will
			# not keep going until the next trigger change.
			return
		trigger_v = v[ix]
		for v in cand_values[src_ds.lines[sliceno]:]:
			if v[ix] != trigger_v:
				break
			yield v
	for ix, col in enumerate('abcd'):
		ds_unrr = subjobs.build('dataset_unroundrobin', source=src_ds, trigger_column=col).dataset()
		assert_slice(ds_unrr, None, unrr_values)
		for sliceno in range(slices):
			assert_slice(ds_unrr, sliceno, list(lines_for_slice(sliceno)))
