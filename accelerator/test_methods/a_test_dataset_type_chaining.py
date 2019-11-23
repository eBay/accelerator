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
Verify the various dataset_type chaining options:
Building a chain with and without extra datasets in the source.
Using the length option to get only some of the extra datasets.
Using the as_chain option to get one dataset per slice.
'''

from accelerator.dataset import Dataset
from accelerator.extras import DotDict
from accelerator import subjobs

def synthesis(job, slices):
	def verify(a, b):
		for col in 'abcd':
			for sliceno in range(slices):
				a_data = list(Dataset.iterate_list(sliceno, col, a))
				b_data = list(map(str, Dataset.iterate_list(sliceno, col, b)))
				assert a_data == b_data, '%r has different contents to %r in slice %d column %s' % (a, b, sliceno, col,)
	def verify_sorted(a, b):
		for col in 'abcd':
			a_data = list(Dataset.iterate_list(None, col, a))
			b_data = list(map(str, Dataset.iterate_list(None, col, b)))
			a_data.sort()
			b_data.sort()
			assert a_data == b_data, '%r has different contents to %r in column %s' % (a, b, col,)
	def write(name, previous, low, high, filter=lambda ix: True):
		dw = job.datasetwriter(
			name=name,
			previous=previous,
			columns={'a': 'unicode', 'b': 'unicode', 'c': 'unicode', 'd': 'unicode',},
		)
		w = dw.get_split_write()
		for ix in range(low, high):
			if filter(ix):
				w('%d' % (ix,), '%d.2' % (ix,), '%d%s' % (ix, '.5' if ix % 2 else ''), '[%d]' % (ix,))
		return dw.finish()
	untyped_A = write('A', None, 0, 100)
	untyped_B = write('B', untyped_A, 100, 1000)
	untyped_C = write('C', untyped_B, 1000, 2000)
	untyped_D = write('D', untyped_C, 2000, 10000)
	untyped_E = write('E', untyped_D, 10000, 10100)

	# All four different classes of converters
	opts = DotDict(column2type=dict(a='int32_10', b='number', c='ascii', d='json'), as_chain=False)
	src_chain = []
	simple_chain = []
	previous = None
	for src in (untyped_A, untyped_B, untyped_C, untyped_D, untyped_E):
		previous = subjobs.build('dataset_type', datasets=dict(source=src, previous=previous), options=opts)
		simple_chain.append(previous)
		src_chain.append(src)
		verify([src], [previous])
		assert simple_chain == Dataset(previous).chain(), previous
		verify(src_chain, simple_chain)
	typed_B = simple_chain[1]
	typed_D = simple_chain[3]

	# No previous -> should contain both A and B
	typed_AB = subjobs.build('dataset_type', datasets=dict(source=untyped_B), options=opts)
	verify(src_chain[:2], [typed_AB])
	typed_CDE = subjobs.build('dataset_type', datasets=dict(source=untyped_E, previous=typed_B), options=opts)
	verify(src_chain[2:], [typed_CDE])
	verify(src_chain, Dataset(typed_CDE).chain())
	# A and B through typed_B, but length=2 only gets D and E, not C.
	opts.length = 2
	typed_DE_noC = subjobs.build('dataset_type', datasets=dict(source=untyped_E, previous=typed_B), options=opts)
	del opts.length
	verify((untyped_A, untyped_B, untyped_D, untyped_E), Dataset(typed_DE_noC).chain())

	# with as_chain (and a hashlabel so as_chain happens)
	opts.as_chain = True
	opts.hashlabel = 'a'
	previous = None
	for ix, src in enumerate(src_chain, 1):
		previous = subjobs.build('dataset_type', datasets=dict(source=src, previous=previous), options=opts)
		ds = Dataset(previous)
		assert len(ds.chain()) == ix * slices, ds
		verify_sorted([src], ds.chain(length=slices))
		verify_sorted(src_chain[:ix], ds.chain())

	# And one with as_chain just on the last job, discarding half the rows from bad typing.
	opts.column2type['b'] = 'ascii'
	opts.column2type['c'] = 'number:int'
	opts.filter_bad = True
	typed_and_hashed_Ehalf = subjobs.build('dataset_type', datasets=dict(source=untyped_E, previous=typed_D), options=opts)
	typed_and_hashed_Ehalf = Dataset(typed_and_hashed_Ehalf)
	assert len(typed_and_hashed_Ehalf.chain()) == slices + 4, typed_and_hashed_Ehalf
	untyped_Ehalf = write('Ehalf', untyped_D, 10000, 10100, filter=lambda ix: ix % 2 == 0)
	verify_sorted([untyped_Ehalf], typed_and_hashed_Ehalf.chain(length=slices))
