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
Test dataset_sort as a chain, across a chain and as a chain merging
only two datasets of the original chain.
'''

from accelerator import subjobs
from accelerator.dataset import Dataset, DatasetWriter

def synthesis():
	dw_a = DatasetWriter(name='a', columns={'num': 'int32'})
	dw_b = DatasetWriter(name='b', columns={'num': 'int32'}, previous=dw_a)
	dw_c = DatasetWriter(name='c', columns={'num': 'int32'}, previous=dw_b)
	w = dw_a.get_split_write()
	w(3)
	w(2)
	w = dw_b.get_split_write()
	w(2)
	w(1)
	w = dw_c.get_split_write()
	w(0)
	a = dw_a.finish()
	b = dw_b.finish()
	c = dw_c.finish()

	opts = dict(
		sort_columns='num',
		sort_across_slices=True,
	)

	# sort as a chain
	jid = subjobs.build('dataset_sort', options=opts, datasets=dict(source=a, previous=None))
	assert list(Dataset(jid).iterate(None, 'num')) == [2, 3]
	sorted_a = jid
	jid = subjobs.build('dataset_sort', options=opts, datasets=dict(source=b, previous=jid))
	assert list(Dataset(jid).iterate_chain(None, 'num')) == [2, 3, 1, 2]
	jid = subjobs.build('dataset_sort', options=opts, datasets=dict(source=c, previous=jid))
	assert list(Dataset(jid).iterate_chain(None, 'num')) == [2, 3, 1, 2, 0]

	# sort all as a single dataset
	jid = subjobs.build('dataset_sort', options=opts, datasets=dict(source=c, previous=None))
	assert list(Dataset(jid).iterate_chain(None, 'num')) == [0, 1, 2, 2, 3]

	# merge b and c but not a
	jid = subjobs.build('dataset_sort', options=opts, datasets=dict(source=c, previous=sorted_a))
	# test with new style job.dataset
	assert list(jid.dataset().iterate(None, 'num')) == [0, 1, 2]
	assert list(jid.dataset().iterate_chain(None, 'num')) == [2, 3, 0, 1, 2]
