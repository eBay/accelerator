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
Verify that lists of chains are handled in csvexport, including changing
column types.
'''

from accelerator import subjobs

def verify(want, **kw):
	job = subjobs.build('csvexport', labelsonfirstline=False, **kw)
	with job.open('result.csv') as fh:
		got = fh.read()
	assert want == got, 'wanted %r, got %r' % (want, got,)
	return job

def synthesis(job):
	dw = job.datasetwriter(name='a')
	dw.add('fixed', 'ascii')
	dw.add('changing', 'ascii')
	dw.get_split_write()('a', 'a')
	a = dw.finish()
	dw = job.datasetwriter(name='b', previous=a)
	dw.add('fixed', 'ascii')
	dw.add('changing', 'bytes')
	dw.get_split_write()('b', b'\xc3\xa5')
	b = dw.finish()
	dw = job.datasetwriter(name='c', previous=b)
	dw.add('fixed', 'ascii')
	dw.add('changing', 'json')
	dw.get_split_write()('c', None)
	c = dw.finish()

	# first alone just to see that later failures are not something trivial
	verify('a,a\n', source=a)
	verify('\xe5,b\n', source=b)
	verify('null,c\n', source=c)

	verify('a\nb\nc\n', labels=['fixed'], source=[a, b, c])
	verify('a\na\nb\nc\n', labels=['fixed'], source=[a, c], chain_source=True)
	verify('a\nb\nc\n', labels=['fixed'], source=c, chain_source=True)
	verify('a\na\n\xe5\nnull\n', labels=['changing'], source=[a, c], chain_source=True)
	ab = verify('a\n\xe5\n', labels=['changing'], source=[a, b])
	verify('\xe5\nnull\nnull\n', labels=['changing'], source=[c, c], chain_source=True, previous=ab)
