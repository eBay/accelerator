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

# Stable sort a dataset based on one or more columns.
# You'll have to type the sort column approprietly.

from __future__ import division

from numpy import lexsort
from os import symlink
from functools import partial

from extras import OptionEnum, OptionString
from jobid import resolve_jobid_filename
from dataset import Dataset, DatasetWriter

OrderEnum = OptionEnum('ascending descending')

options = {
	'sort_columns'           : [OptionString],
	'sort_order'             : OrderEnum.ascending,
	'sort_across_slices'     : False, # normally only sort within slices
}
datasets = ('source', 'previous',)


def sort(columniter):
	def sortable_columnlist(column):
		if datasets.source.columns[column] in ('datetime', 'date', 'time',):
			return map(str, columniter(column))
		else:
			return list(columniter(column))
	lst = [sortable_columnlist(c) for c in reversed(options.sort_columns)]
	if options.sort_order == 'descending':
		# Stupid lexsort doesn't take a direction, and we want stable sorting.
		[e.reverse() for e in lst]
		l = len(lst[0]) - 1
		sort_idx = [l - i for i in reversed(lexsort(lst))]
	else:
		sort_idx = list(lexsort(lst)) # stable
	return sort_idx

def prepare(params):
	d = datasets.source
	jobs = d.chain(stop_jobid={datasets.previous: 'source'})
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, jobids=jobs)
		sort_idx = sort(columniter)
	else:
		sort_idx = None
	if options.sort_across_slices:
		hashlabel = None
	else:
		hashlabel = d.hashlabel
	if len(jobs) == 1:
		filename = d.filename
	else:
		filename = None
	dw = DatasetWriter(
		columns=d.columns,
		caption=params.caption,
		hashlabel=hashlabel,
		filename=filename,
	)
	return dw, jobs, sort_idx

def analysis(sliceno, params, prepare_res):
	dw, jobs, sort_idx = prepare_res
	single_job = (len(jobs) == 1)
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, jobids=jobs)
		per_slice = len(sort_idx) // params.slices
		if sliceno + 1 ==  params.slices:
			sort_idx = sort_idx[per_slice * sliceno:]
		else:
			sort_idx = sort_idx[per_slice * sliceno:per_slice * (sliceno + 1)]
	else:
		columniter = partial(Dataset.iterate_list, sliceno, jobids=jobs)
		sort_idx = sort(columniter)
	if single_job and not options.sort_across_slices and sort_idx == sorted(sort_idx):
		# this slice is fully sorted as is.
		slice_dir = '%02d' % (sliceno,)
		symlink(resolve_jobid_filename(datasets.source, slice_dir), slice_dir)
		return len(sort_idx)
	for column in datasets.source.columns:
		lst = list(columniter(column))
		w = dw.writers[column].write
		for idx in sort_idx:
			w(lst[idx])
