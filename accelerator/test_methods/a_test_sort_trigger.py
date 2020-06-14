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
Test dataset_sort with trigger_column
'''

from operator import itemgetter

from accelerator import subjobs

def sort(src, ix, **kw):
	ds = subjobs.build('dataset_sort', source=src, sort_across_slices=True, **kw).dataset()
	want = sorted(src.iterate(None), key=itemgetter(ix))
	assert list(ds.iterate(None)) == want, '%s != sorted(%s)' % (ds, src,)
	return ds

def synthesis(job):
	dw = job.datasetwriter()
	dw.add('a', 'int32')
	dw.add('b', 'int32')
	dw.add('c', 'int32')
	w = dw.get_split_write()
	for ix in range(1, 11):
		w(1, 2, ix * 10 + 5)
		w(1, 2, ix * 10 + 3)
		w(1, 2, ix * 10 + 4)
		w(1, 3, ix * 1000 + 1)
		w(1, 3, ix * 1000 + 2)
		w(1, 3, ix * 1000 + 0)
	src = dw.finish()
	# Unchanging trigger
	a = sort(src, 0, sort_columns='a', trigger_column='a')
	assert set(a.lines) == {0, 60}, '%s %r' % (a, a.lines,)
	# Just two trigger values
	b = sort(src, 1, sort_columns='b', trigger_column='b')
	assert set(b.lines) == {0, 30}, '%s %r' % (b, b.lines,)
	# Trigger value changes every time - trigger_column should affect nothing
	c = sort(src, 2, sort_columns='c')
	ct = sort(src, 2, sort_columns='c', trigger_column='c')
	assert c.lines == ct.lines, '%s %r != %s %r' % (c, c.lines, ct, ct.lines,)
	# check that using second sort column as trigger works
	bc = sort(src, 2, sort_columns=['c', 'b'], trigger_column='b')
	assert set(bc.lines) == {0, 30}, '%s %r' % (bc, bc.lines,)
