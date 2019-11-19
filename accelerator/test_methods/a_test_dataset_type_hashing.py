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
Verify that using dataset_type with a hashlabel gives the same result as
first typing and then rehashing for various hashlabel types, including
with renamed columns and bad lines.

Also verify that hashing discards untyped columns, and that parent
hashlabel is inherited or discarded as appropriate.
'''

from itertools import cycle

from accelerator.compat import unicode
from accelerator import subjobs
from accelerator.extras import DotDict

def synthesis(job):
	for hl in (None, 'a', 'b', 'c'):
		dw = job.datasetwriter(name='hashed on %s' % (hl,), columns={'a': 'unicode', 'b': 'unicode', 'c': 'unicode'}, hashlabel=hl)
		w = dw.get_split_write()
		for ix in range(1000):
			w(unicode(ix), '%d.%d' % (ix, ix % 5 == 0), ('{"a": %s}' if ix % 3 else '%d is bad') % (ix,))
		src_ds = dw.finish()
		test(src_ds, dict(column2type={'a': 'int32_10', 'b': 'number:int'}, filter_bad=True), 800)
		test(src_ds, dict(column2type={'a': 'int64_10', 'b': 'number', 'c': 'json'}, filter_bad=True), 666)
		test(src_ds, dict(column2type={'a': 'floatint32ei', 'b': 'number:int', 'c': 'json'}, filter_bad=True), 533)
		test(src_ds, dict(column2type={'from_a': 'number', 'from_b': 'float64', 'from_c': 'ascii'}, rename=dict(a='from_a', b='from_b', c='from_c')), 1000)
		test(src_ds, dict(column2type={'c': 'bits32_16', 'a': 'float32', 'b': 'bytes'}, rename=dict(a='c', b='a', c='b')), 1000)

	# this doesn't test as many permutations, it's just to test more column types.
	dw = job.datasetwriter(name='more types')
	cols = {
		'floatbooli': cycle(['1.42 or so', '0 maybe', '1 (exactly)']),
		'datetime:%Y%m%d %H:%M': ['2019%02d%02d 17:%02d' % (t % 12 + 1, t % 28 + 1, t % 60) for t in range(1000)],
		'date:%Y%m%d': ['2019%02d%02d' % (t % 12 + 1, t % 28 + 1,) for t in range(1000)],
		'time:%H:%M': ['%02d:%02d' % (t // 60, t % 60) for t in range(1000)],
		'timei:%H:%M': ['%02d:%02d%c' % (t // 60, t % 60, chr(t % 26 + 65)) for t in range(1000)],
	}
	gens = []
	for coltype, gen in cols.items():
		dw.add(coltype.split(':')[0], 'ascii')
		gens.append(iter(gen))
	dw.add('half', 'bytes')
	gens.append(cycle([b'1', b'no']))
	w = dw.get_split_write()
	for _ in range(1000):
		w(*map(next, gens))
	src_ds = dw.finish()
	column2type = {t.split(':')[0]: t for t in cols}
	for hl in column2type:
		hashed = subjobs.build('dataset_type', options=dict(column2type=column2type, hashlabel=hl), datasets=dict(source=src_ds)).dataset()
		unhashed = subjobs.build('dataset_type', options=dict(column2type=column2type), datasets=dict(source=src_ds)).dataset()
		rehashed = subjobs.build('dataset_rehash', options=dict(hashlabel=hl), datasets=dict(source=unhashed)).dataset()
		assert hashed.lines == rehashed.lines
		assert sum(hashed.lines) == 1000
		assert set(hashed.columns.keys()) == set(unhashed.columns.keys()) == set(rehashed.columns.keys())
		# and again with a bad column
		column2type['half'] = 'float32'
		hashed = subjobs.build('dataset_type', options=dict(column2type=column2type, hashlabel=hl, filter_bad=True), datasets=dict(source=src_ds)).dataset()
		unhashed = subjobs.build('dataset_type', options=dict(column2type=column2type, filter_bad=True), datasets=dict(source=src_ds)).dataset()
		rehashed = subjobs.build('dataset_rehash', options=dict(hashlabel=hl), datasets=dict(source=unhashed)).dataset()
		del column2type['half']
		assert hashed.lines == rehashed.lines
		assert sum(hashed.lines) == 500
		assert set(hashed.columns.keys()) == set(unhashed.columns.keys()) == set(rehashed.columns.keys())

def test(src_ds, opts, expect_lines):
	opts = DotDict(opts)
	def rename(colname):
		return opts.get('rename', {}).get(colname, colname)
	cols = set(opts.column2type)
	opts.discard_untyped = True
	msg = 'Testing with types %s' % (', '.join(v for k, v in sorted(opts.column2type.items())),)
	if src_ds.hashlabel and opts.column2type.get(src_ds.hashlabel) == 'json':
		# json is not hashable, so we have to override the hashlabel to nothing in this case.
		opts.hashlabel = ''
		msg += ' (clearing hashlabel)'
	elif src_ds.hashlabel:
		msg += ' (hashed on %s)' % (opts.column2type.get(src_ds.hashlabel, '<untyped column>'),)
	print(msg)
	just_typed = subjobs.build('dataset_type', options=opts, datasets=dict(source=src_ds)).dataset()
	assert set(just_typed.columns) == cols
	assert sum(just_typed.lines) == expect_lines
	if rename(src_ds.hashlabel) not in opts.column2type or opts.get('hashlabel') == '':
		assert just_typed.hashlabel is None
	else:
		assert just_typed.hashlabel == rename(src_ds.hashlabel)
	del opts.discard_untyped
	for hashlabel in cols:
		if opts.column2type[hashlabel] == 'json':
			# not hashable
			continue
		opts['hashlabel'] = hashlabel
		print('%s rehashed on %s' % (msg, opts.column2type[hashlabel],))
		hashed_by_type = subjobs.build('dataset_type', options=opts, datasets=dict(source=src_ds)).dataset()
		assert set(hashed_by_type.columns) == cols
		assert sum(hashed_by_type.lines) == expect_lines
		hashed_after = subjobs.build('dataset_rehash', options=dict(hashlabel=hashlabel), datasets=dict(source=just_typed)).dataset()
		assert set(hashed_after.columns) == cols
		assert sum(hashed_after.lines) == expect_lines
		if src_ds.hashlabel:
			# if src_ds has a hashlabel then just_typed will actually already be hashed, so hashed_after
			# will have been hashed twice and therefore have a different order than hashed_by_type.
			if rename(src_ds.hashlabel) == hashlabel:
				# These should be the same though.
				subjobs.build('test_compare_datasets', datasets=dict(a=hashed_by_type, b=just_typed))
			hashed_by_type = subjobs.build('dataset_sort', options=dict(sort_columns=rename('a')), datasets=dict(source=hashed_by_type))
			hashed_after = subjobs.build('dataset_sort', options=dict(sort_columns=rename('a')), datasets=dict(source=hashed_after))
		subjobs.build('test_compare_datasets', datasets=dict(a=hashed_by_type, b=hashed_after))
