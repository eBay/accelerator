############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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
Test JobWithFile file loading.
Pickle and json, sliced and unsliced.
'''

from accelerator.extras import JobWithFile, json_save
from accelerator import subjobs
from accelerator import blob

options = dict(
	file=JobWithFile,
	inner=False,
)

def analysis(sliceno):
	data = {sliceno}
	if options.inner:
		if options.file.sliced:
			value = options.file.load(sliceno)
			assert value == data
		else:
			try:
				options.file.load(sliceno)
				raise Exception("Allowed sliced load of unsliced file")
			except AssertionError:
				pass
		blob.save({'inner': sliceno}, 'inner.pickle', sliceno, temp=False)
		json_save({'inner': sliceno}, 'inner.json', sliceno)
	else:
		blob.save(data, 'data', sliceno, temp=False)

def verify(params, jwf):
	jid = subjobs.build('test_jobwithfile', options=dict(inner=True, file=jwf))
	for sliceno in range(params.slices):
		assert jid.load('inner.pickle', sliceno) == {'inner': sliceno}
		assert jid.json_load('inner.json', sliceno) == {'inner': sliceno}
	assert jid.load('inner.pickle') == {'inner': None}
	assert jid.json_load('inner.json') == {'inner': None}

def synthesis(params, job):
	data = {'foo'}
	if options.inner:
		if options.file.sliced:
			try:
				options.file.load()
				raise Exception("Allowed unsliced load of sliced file")
			except AssertionError:
				pass
		else:
			value = options.file.load()
			assert value == data
		blob.save({'inner': None}, 'inner.pickle')
		json_save({'inner': None}, 'inner.json')
	else:
		blob.save(data, 'data')
		# use different ways to construct the jwf so both get tested.
		verify(params, JobWithFile(params.jobid, 'data'))
		verify(params, job.withfile('data', True))
