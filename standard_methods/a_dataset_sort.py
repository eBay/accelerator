############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

from __future__ import division
from __future__ import absolute_import

description = r'''
Stable sort a dataset based on one or more columns.
You'll have to type the sort column(s) approprietly.
'''

from functools import partial

from extras import OptionEnum, OptionString
from dataset import Dataset, DatasetWriter

OrderEnum = OptionEnum('ascending descending')

options = {
	'sort_columns'           : [OptionString],
	'sort_order'             : OrderEnum.ascending,
	'sort_across_slices'     : False, # normally only sort within slices
}
datasets = ('source', 'previous',)


def sort(columniter):
	lst = list(columniter(options.sort_columns))
	reverse = (options.sort_order == 'descending')
	return sorted(range(len(lst)), key=lst.__getitem__, reverse=reverse)

def prepare(params):
	d = datasets.source
	ds_list = d.chain(stop_ds={datasets.previous: 'source'})
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, datasets=ds_list)
		sort_idx = sort(columniter)
	else:
		sort_idx = None
	if options.sort_across_slices:
		hashlabel = None
	else:
		hashlabel = d.hashlabel
	if len(ds_list) == 1:
		filename = d.filename
	else:
		filename = None
	dw = DatasetWriter(
		columns=d.columns,
		caption=params.caption,
		hashlabel=hashlabel,
		filename=filename,
	)
	return dw, ds_list, sort_idx

def analysis(sliceno, params, prepare_res):
	dw, ds_list, sort_idx = prepare_res
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, datasets=ds_list)
		per_slice = len(sort_idx) // params.slices
		if sliceno + 1 ==  params.slices:
			sort_idx = sort_idx[per_slice * sliceno:]
		else:
			sort_idx = sort_idx[per_slice * sliceno:per_slice * (sliceno + 1)]
	else:
		columniter = partial(Dataset.iterate_list, sliceno, datasets=ds_list)
		sort_idx = sort(columniter)
	for column in datasets.source.columns:
		lst = list(columniter(column))
		w = dw.writers[column].write
		for idx in sort_idx:
			w(lst[idx])
