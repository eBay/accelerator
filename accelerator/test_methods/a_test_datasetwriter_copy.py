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
Test copy_mode in DatasetWriter, and three ways to specify the column
types (columns={}, add(n, Datasetcolums), .add(n, (t, none_support)).
'''

from accelerator.dataset import DatasetWriter

jobs = ('source',)


def prepare(job):
	job.datasetwriter(columns=jobs.source.dataset().columns, copy_mode=True)
	DatasetWriter(name='named', columns=jobs.source.dataset('named').columns, copy_mode=True)
	dw_passed = job.datasetwriter(name='passed', copy_mode=True)
	# DatasetColumn in .add
	for n, c in sorted(jobs.source.dataset('passed').columns.items()):
		dw_passed.add(n, c)
	# verify that .add(none_support=) takes precedence over coltype
	dw_nonetest_removed = job.datasetwriter(name='nonetest_removed')
	for n, c in sorted(jobs.source.dataset('nonetest').columns.items()):
		dw_nonetest_removed.add(n, c, none_support=(n == 'unicode'))
	return dw_passed, dw_nonetest_removed

def analysis(sliceno, prepare_res, job):
	dw_default = DatasetWriter()
	dw_named = job.datasetwriter(name='named')
	dw_passed, _ = prepare_res
	for name, dw in [('default', dw_default), ('named', dw_named), ('passed', dw_passed)]:
		for data in jobs.source.dataset(name).iterate(sliceno, copy_mode=True):
			dw.write(*data)

def synthesis(job, slices, prepare_res):
	for name in ['synthesis_split', 'synthesis_manual', 'nonetest']:
		dw = job.datasetwriter(name=name, copy_mode=True)
		# (type, none_support) in .add
		for n, c in sorted(jobs.source.dataset(name).columns.items()):
			dw.add(n, (c.type, c.none_support))
		for sliceno in range(slices):
			dw.set_slice(sliceno)
			for data in jobs.source.dataset(name).iterate(sliceno, copy_mode=True):
				dw.write(*data)
	_, dw_nonetest_removed = prepare_res
	ds = dw_nonetest_removed.finish()
	for name, col in ds.columns.items():
		if name == 'unicode':
			assert col.none_support, "%s:%s should have none_support" % (ds, name,)
		else:
			assert not col.none_support, "%s:%s shouldn't have none_support" % (ds, name,)
