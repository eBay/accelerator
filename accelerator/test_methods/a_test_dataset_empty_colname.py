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

from accelerator import subjobs

def synthesis(job):
	dw = job.datasetwriter()
	dw.add('', 'number')
	dw.add('word', 'ascii')
	w = dw.get_split_write()
	w(0, 'foo')
	w(1, 'bar')
	ds = dw.finish()
	assert set(ds.columns) == {'', 'word'}
	assert list(ds.iterate(None, '')) == [0, 1]
	assert list(ds.iterate(None)) == [(0, 'foo'), (1, 'bar')]
	job = subjobs.build('csvexport', source=ds, filename='out.csv')
	job = subjobs.build('csvimport', filename=job.filename('out.csv'))
	job = subjobs.build('dataset_type', source=job, column2type={'': 'number', 'word': 'ascii'})
	assert list(job.dataset().iterate(None)) == list(ds.iterate(None))
