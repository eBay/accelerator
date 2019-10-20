############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2019 Carl Drougge                       #
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
from __future__ import print_function

description = r'''
Stable sort a dataset based on one or more columns.
You'll have to type the sort column(s) approprietly.

None and NaN values will sort the same as the smallest/largest
value possible in a comparable type.
'''

from functools import partial
import datetime
from math import isnan

from accelerator.compat import PY2, izip

from accelerator.extras import OptionEnum, OptionString
from accelerator.dataset import Dataset, DatasetWriter
from accelerator.status import status

OrderEnum = OptionEnum('ascending descending')

options = {
	'sort_columns'           : [OptionString],
	'sort_order'             : OrderEnum.ascending,
	'sort_across_slices'     : False, # normally only sort within slices
}

datasets = ('source', 'previous',)


# These types don't need/can't use any special handling of None-values.
nononehandling_types = ('json', 'bits64', 'bits32',)
if PY2:
	# These types sort None before everything else on py2.
	nononehandling_types += ('bytes', 'ascii', 'unicode', 'int64', 'int32', 'bool',)

def filter_unsortable(column, it):
	coltype = datasets.source.columns[column].type
	if coltype == 'bytes':
		nonev = b''
	elif coltype in ('ascii', 'unicode',):
		nonev = u''
	elif coltype in ('int64', 'int32',):
		nonev = float('-inf')
	elif coltype == 'bool':
		nonev = -1
	elif coltype == 'datetime':
		nonev = datetime.datetime.max
	elif coltype == 'date':
		nonev = datetime.date.max
	elif coltype == 'time':
		nonev = datetime.time.max
	else:
		nanv = float('inf')
		nonev = float('-inf')
		return (nonev if v is None else nanv if isnan(v) else v for v in it)
	return (nonev if v is None else v for v in it)

def sort(columniter):
	with status('Determining sort order'):
		info = datasets.source.columns
		if sum(info[column].type not in nononehandling_types for column in options.sort_columns):
			# At least one sort column can have unsortable values
			first = True
			iters = []
			for column in options.sort_columns:
				it = columniter(column, status_reporting=first)
				first = False
				if info[column].type not in nononehandling_types:
					it = filter_unsortable(column, it)
				iters.append(it)
			if len(iters) == 1:
				# Special case to not make tuples when there is only one column.
				lst = list(iters[0])
			else:
				lst = list(izip(*iters))
		else:
			columns = options.sort_columns
			if len(columns) == 1:
				# Special case to not make tuples when there is only one column.
				columns = columns[0]
			lst = list(columniter(columns))
		reverse = (options.sort_order == 'descending')
		with status('Creating sort list'):
			return sorted(range(len(lst)), key=lst.__getitem__, reverse=reverse)

def prepare(params):
	d = datasets.source
	ds_list = d.chain(stop_ds={datasets.previous: 'source'})
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, datasets=ds_list)
		sort_idx = sort(columniter)
		total = len(sort_idx)
		per_slice = [total // params.slices] * params.slices
		extra = total % params.slices
		if extra:
			# spread the left over length over pseudo-randomly selected slices
			# (using the start of sort_idx to select slices).
			# this will always select the first slices if data is already sorted
			# but at least it's deterministic.
			selector = sorted(range(min(params.slices, total)), key=sort_idx.__getitem__)
			for sliceno in selector[:extra]:
				per_slice[sliceno] += 1
		# change per_slice to be the actual sort indexes
		start = 0
		for ix, num in enumerate(per_slice):
			end = start + num
			per_slice[ix] = sort_idx[start:end]
			start = end
		assert sum(len(part) for part in per_slice) == total # all rows used
		assert len(set(len(part) for part in per_slice)) < 3 # only 1 or 2 lengths possible
		sort_idx = per_slice
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
		previous=datasets.previous,
	)
	return dw, ds_list, sort_idx

def analysis(sliceno, params, prepare_res):
	dw, ds_list, sort_idx = prepare_res
	if options.sort_across_slices:
		columniter = partial(Dataset.iterate_list, None, datasets=ds_list)
		sort_idx = sort_idx[sliceno]
	else:
		columniter = partial(Dataset.iterate_list, sliceno, datasets=ds_list)
		sort_idx = sort(columniter)
	for ix, column in enumerate(datasets.source.columns, 1):
		colstat = '%r (%d/%d)' % (column, ix, len(datasets.source.columns),)
		with status('Reading ' + colstat):
			lst = list(columniter(column))
		with status('Writing ' + colstat):
			w = dw.writers[column].write
			for idx in sort_idx:
				w(lst[idx])
		# Delete the list before making a new one, so we use less memory.
		del lst
