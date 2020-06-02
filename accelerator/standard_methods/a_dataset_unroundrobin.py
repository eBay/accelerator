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

def prepare(job):
	d = datasets.source
	return job.datasetwriter(
		columns=d.columns,
		caption=d.caption,
		filename=d.filename,
		previous=datasets.previous,
	)

def analysis(sliceno, prepare_res):
	write = prepare_res.write_list
	d = datasets.source
	it = d.iterate('roundrobin')
	# Skip until the lines that belong in this slice
	for _ in range(sum(d.lines[:sliceno])):
		next(it)
	# write the lines belonging here
	for _ in range(d.lines[sliceno]):
		write(next(it))

def synthesis(prepare_res):
	want = datasets.source.lines
	got = prepare_res.finish().lines
	assert want == got, "Did not get the expected line counts"
