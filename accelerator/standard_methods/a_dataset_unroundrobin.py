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
new_ds.iterate(None) gives the same order as old_ds.iterate('roundrobin').

This is useful to get the original file order from csvimport, one
slice at a time.

If you don't specify trigger_column you get the same number of lines per
slice as old_ds. If you do specify trigger_column slice switching is
delayed until the value in trigger_column changes. This is so jobs using
new_ds get all lines with the same trigger_column value in the same
slice, useful for a number of algorithms. (Assuming all occurances of
the same value are together.)
'''

from accelerator.compat import izip

options = dict(trigger_column=None)

datasets = ('source', 'previous',)

def prepare(job):
	d = datasets.source
	dw = job.datasetwriter(
		columns=d.columns,
		caption=d.caption,
		filename=d.filename,
		previous=datasets.previous,
	)
	if options.trigger_column:
		assert options.trigger_column in datasets.source.columns, "Trigger column %r not in %s" % (options.trigger_column, datasets.source,)
		ix = sorted(datasets.source.columns).index(options.trigger_column)
	else:
		ix = -1
	return dw, ix

def analysis(sliceno, prepare_res):
	write = prepare_res[0].write_list
	ix = prepare_res[1]
	d = datasets.source
	to_copy = d.lines[sliceno]
	if to_copy == 0:
		# bail out empty slices right away
		return
	to_skip = sum(d.lines[:sliceno])
	if to_skip:
		it = d.iterate('roundrobin', slice=to_skip - bool(options.trigger_column))
		if options.trigger_column:
			trigger_v = next(it)[ix]
			# keep skipping until trigger value changes
			for v in it:
				to_copy -= 1
				if v[ix] != trigger_v:
					write(v)
					break
				if to_copy == 0:
					return # no lines left for this slice
	else:
		it = d.iterate('roundrobin')
	# write the lines belonging here
	# (zip so we don't have to count down to_copy manually)
	for _, v in izip(range(to_copy), it):
		write(v)
	if options.trigger_column:
		trigger_v = v[ix]
		# keep copying until trigger value changes or lines run out
		for v in it:
			if trigger_v != v[ix]:
				break
			write(v)

def synthesis(prepare_res):
	want = datasets.source.lines
	got = prepare_res[0].finish().lines
	if options.trigger_column:
		# Lines will have moved, but the total must still be the same.
		want = sum(want)
		got = sum(got)
	assert want == got, "Did not get the expected line counts"
