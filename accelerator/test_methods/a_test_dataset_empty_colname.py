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
from accelerator.error import DatasetUsageError

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
	jid = subjobs.build('csvexport', source=ds, filename='out.csv')
	jid = subjobs.build('csvimport', filename=jid.filename('out.csv'))
	jid = subjobs.build('dataset_type', source=jid, column2type={'': 'number', 'word': 'ascii'})
	assert list(jid.dataset().iterate(None)) == list(ds.iterate(None))

	# verify that names really get resolved/used in added order, for all orders
	# and sneak in a hashlabel='' check as well.
	# this should really be in test_dataset_column_names, but I wanted to
	# include the hashlabel='' check too, so here it is.
	def chk_order(names, want):
		dw = job.datasetwriter(name='order' + ','.join(names), hashlabel='')
		for name in names:
			dw.add(name, 'ascii')
		w = dw.get_split_write()
		# write in the specified order
		w(*names)
		# write with the expected (mangled) name
		w(**{v: k for k, v in want.items()})
		ds = dw.finish()
		for col in want:
			# '' hashes to 0, so if the hashlabel worked both are in slice 0.
			assert list(ds.iterate(0, col)) == [col, col], '%r bad in %s' % (col, ds,)
		return ds
	chk_order(['@', '_', ''], {'@': '_', '_': '__', '': '___'})
	chk_order(['@', '', '_'], {'@': '_', '': '__', '_': '___'})
	chk_order(['', '@', '_'], {'': '_', '@': '__', '_': '___'})
	chk_order(['_', '@', ''], {'_': '_', '@': '__', '': '___'})
	ds = chk_order(['', '_', '@'], {'': '_', '_': '__', '@': '___'})

	# check that hashlabel '' is correctly handled in the mismatch check
	def chk_mismatch(ds, good, bad):
		list(ds.iterate(0, '_', hashlabel=good))
		try:
			list(ds.iterate(0, '_', hashlabel=bad))
		except DatasetUsageError:
			pass
		else:
			raise Exception('%s accepted hashlabel %r' % (ds, bad,))
	chk_mismatch(ds, '', '@')
	dw = job.datasetwriter(name='hl_', hashlabel='_')
	dw.add('_', 'int32')
	dw.add('', 'int32')
	w = dw.get_split_write()
	for ix in range(64):
		w(ix, 0)
	ds = dw.finish()
	chk_mismatch(ds, '_', '')

	# check that re-hashing on '' works
	# 0 hashes to 0, so all should be in slice 0
	got = set(ds.iterate(0, '_', hashlabel='', rehash=True))
	want = set(range(64))
	assert got == want, 'Wanted {0..63}, got %r from %s' % (got, ds,)
