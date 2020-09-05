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
Exercise different valid and invalid parent/hashlabel combinations in
DatasetWriter.
'''

from accelerator import DatasetUsageError, NoSuchDatasetError

def synthesis(slices, job):
	parent = job.datasetwriter(name='parent', columns={'a': 'ascii'}, hashlabel='a')
	w = parent.get_split_write()
	w('a')
	w('aa')
	w('aaa')
	parent = parent.finish()
	def test_good(name, specified_parent):
		dw = job.datasetwriter(name=name, columns={'b': 'ascii'}, parent=specified_parent)
		for sliceno in range(slices):
			dw.set_slice(sliceno)
			for v in parent.iterate(sliceno, 'a'):
				dw.write(v + v)
		ds = dw.finish()
		assert set(ds.iterate(None)) == {('a', 'aa'), ('aa', 'aaaa'), ('aaa', 'aaaaaa')}
	test_good('ok', parent)
	test_good('also ok', (job, 'parent')) # different way of specifying parent
	dw = job.datasetwriter(columns={'b': 'ascii'}, parent=parent)
	try:
		dw.get_split_write()
		raise Exception('get_split_write allowed when not writing hash label')
	except DatasetUsageError:
		pass
	dw.discard()
	try:
		dw = job.datasetwriter(name='fail', columns={'b': 'ascii'}, parent=parent, hashlabel='b')
		raise Exception('Allowed changing hashlabel without hashlabel_override')
	except DatasetUsageError:
		pass
	# This is permitted, though kind of questionable since we pretty much have to write the same thing in b as in a
	dw = job.datasetwriter(name='overridden', columns={'b': 'ascii'}, parent=parent, hashlabel='b', hashlabel_override=True)
	w = dw.get_split_write()
	w('a')
	w('aa')
	w('aaa')
	ds = dw.finish()
	assert set(ds.iterate(None)) == {('a', 'a'), ('aa', 'aa'), ('aaa', 'aaa')}
	dw = job.datasetwriter(columns={'b': 'ascii'}, parent=parent, hashlabel='a')
	try:
		dw.set_slice(0)
		raise Exception('set_slice allowed when not writing hash label')
	except DatasetUsageError:
		pass
	dw.discard()
	dup = job.datasetwriter(name='dup')
	try:
		job.datasetwriter(name='dup')
		raise Exception('dataset name reuse allowed')
	except DatasetUsageError:
		pass
	dup.discard()
	try:
		job.datasetwriter(name='parent')
		raise Exception('already finished dataset name reuse allowed')
	except OSError:
		pass
	try:
		job.datasetwriter(parent=(job, 'nah'))
		raise Exception('allowed non-existant parent')
	except NoSuchDatasetError:
		pass
