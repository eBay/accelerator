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

from __future__ import division
from __future__ import absolute_import

description = r'''
Orders a dataset so new_ds.iterate(None) gives the same order as
old_ds.iterate('roundrobin'). You always get the same number of lines
for each slice as in old_ds.

This is useful to get the original file order from csvimport, one
slice at a time.
'''

datasets = ('source', 'previous',)

def synthesis(job):
	d = datasets.source
	dw = job.datasetwriter(
		columns=d.columns,
		caption=d.caption,
		filename=d.filename,
		previous=datasets.previous,
	)

	per_slice = iter(d.lines)
	to_go = 0
	sliceno = -1
	for values in d.iterate('roundrobin'):
		while not to_go:
			sliceno += 1
			dw.set_slice(sliceno)
			to_go = next(per_slice)
		dw.write_list(values)
		to_go -= 1

	# The writer will complain if not all slices were written, so we
	# have to set_slice any trailing empty slices.
	for _ in per_slice:
		sliceno += 1
		dw.set_slice(sliceno)
	assert d.lines == dw.finish().lines, "Did not get the expected line counts"
