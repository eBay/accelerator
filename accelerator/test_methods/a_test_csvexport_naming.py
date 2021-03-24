############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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
Verify filename (sliced and unsliced) and gzip in csvexport.
'''

from accelerator import subjobs
import gzip

def synthesis(job):
	dw = job.datasetwriter(name='a')
	dw.add('a', 'int32')
	w = dw.get_split_write()
	w(0)
	w(1)
	w(2)
	a = dw.finish()

	for filename, sliced, out_filename, open_func in (
		('name', False, 'name', open),
		('name', True, 'name.1', open),
		('name%dend', True, 'name1end', open),
		('name.gz', False, 'name.gz', gzip.open),
		('name.gz', True, 'name.gz.1', gzip.open),
		('a%02d.gz.b', True, 'a01.gz.b', gzip.open),
		('a%02d.gz.b', False, 'a%02d.gz.b', gzip.open),
		('name.gzonk', False, 'name.gzonk', open),
	):
		job = subjobs.build('csvexport', filename=filename, sliced=sliced, source=a, labels=['a'])
		fn = job.filename(out_filename)
		with open_func(fn, mode='rb') as fh:
			got = fh.read()
		if sliced:
			want = b'a\n1\n'
		else:
			want = b'a\n0\n1\n2\n'
		assert want == got, 'wanted %r, got %r in %s' % (want, got, fn)
