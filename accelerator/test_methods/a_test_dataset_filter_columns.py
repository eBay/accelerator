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
Test the dataset_filter_columns method.
'''

from accelerator import subjobs

def synthesis(job):
	dw = job.datasetwriter()
	dw.add('a', 'int32')
	dw.add('b', 'int32')
	dw.add('c', 'int32')
	dw.add('d', 'int32')
	w = dw.get_split_write()
	w(1, 2, 3, 4)
	w(97, 98, 99, 100)
	ds = dw.finish()
	def chk(j, *want):
		ds = j.dataset()
		want = set(want)
		got = set(ds.columns)
		assert got == want, "%s should have had columns %r but had %r" % (ds, want, got,)
		want = list(zip(*[(ord(c) - 96, ord(c)) for c in sorted(want)]))
		got = list(ds.iterate(None))
		assert got == want, "%s should have had %r but had %r" % (ds, want, got,)
	chk(job, 'a', 'b', 'c', 'd')
	j = subjobs.build('dataset_filter_columns', source=ds, keep_columns=['a'])
	chk(j, 'a')
	j = subjobs.build('dataset_filter_columns', source=ds, keep_columns=['b', 'c'])
	chk(j, 'b', 'c')
	j = subjobs.build('dataset_filter_columns', source=ds, discard_columns=['d', 'c', 'b'])
	chk(j, 'a')
	j = subjobs.build('dataset_filter_columns', source=ds, discard_columns=['b'])
	chk(j, 'a', 'c', 'd')
	# Discarding a non-existant column is ok.
	j = subjobs.build('dataset_filter_columns', source=ds, discard_columns=['nah', 'b'])
	chk(j, 'a', 'c', 'd')
	j = subjobs.build('dataset_filter_columns', source=ds, keep_columns=['b', 'b', 'b'])
	chk(j, 'b')
