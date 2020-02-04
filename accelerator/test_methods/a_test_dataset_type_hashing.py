############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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
from datetime import date, time, datetime

from accelerator.compat import unicode
from accelerator import subjobs
from accelerator.extras import DotDict
from accelerator.gzwrite import typed_writer

def synthesis(job, slices):
	# Test keeping untyped columns.
	dw = job.datasetwriter(name='a', columns={'a': 'unicode', 'b': ('bytes', True), 'c': ('ascii', True), 'd': ('number', True)})
	write = dw.get_split_write()
	write('A', None, None, None)
	write('a', b'b', 'c', 0)
	a = dw.finish()
	assert a.hashlabel == None
	typed_a = subjobs.build('dataset_type', options=dict(hashlabel='a', column2type={'a': 'ascii'}), datasets=dict(source=a)).dataset()
	assert typed_a.hashlabel == 'a'
	assert set(typed_a.iterate(None)) == {('A', None, None, None), ('a', b'b', 'c', 0)}, typed_a

	# Test hashing on a column not explicitly typed.
	dw = job.datasetwriter(name='b', columns={'a': 'unicode', 'b': 'ascii', 'c': 'bytes', 'd': 'unicode'}, previous=a)
	write = dw.get_split_write()
	write('A', 'B', b'C', '1')
	b = dw.finish()
	assert b.hashlabel == None
	typed_b = subjobs.build('dataset_type', options=dict(hashlabel='a', column2type={'b': 'ascii'}), datasets=dict(source=b)).dataset()
	assert typed_b.hashlabel == 'a'
	assert set(typed_b.iterate(None)) == {('a', 'b'), ('A', None), ('A', 'B')}, typed_b

	# Test renaming over the original hashlabel
	dw = job.datasetwriter(name='c', columns={'a': 'unicode', 'b': 'ascii', 'c': 'bytes', 'd': 'unicode'}, hashlabel='a')
	write = dw.get_split_write()
	write('\xe5', 'b', b'c', '0')
	c = dw.finish()
	assert c.hashlabel == 'a'
	typed_c = subjobs.build('dataset_type', options=dict(column2type={'a': 'ascii', 'd': 'number'}, rename={'c': 'a'}), datasets=dict(source=c)).dataset()
	assert typed_c.hashlabel == None
	assert list(typed_c.iterate(None)) == [('c', 'b', b'c', 0)], typed_c

	# Test using the original names but for different columns (keeping hashlabel under new name)
	dw = job.datasetwriter(name='d', columns={'a': 'unicode', 'b': 'ascii', 'c': 'bytes', 'd': 'unicode'}, hashlabel='a')
	write = dw.get_split_write()
	write('\xc5', 'B', B'C', '1')
	d = dw.finish()
	assert d.hashlabel == 'a'
	typed_d = subjobs.build('dataset_type', options=dict(column2type={'a': 'bytes', 'b': 'ascii', 'c': 'int32_10', 'd': 'bytes'}, rename={'b': 'a', 'c': 'b', 'd': 'c', 'a': 'd'}), datasets=dict(source=d)).dataset()
	assert typed_d.hashlabel == 'd'
	assert list(typed_d.iterate(None)) == [(b'B', 'C', 1, b'\xc3\x85')], typed_c

	# Test various types for hashing and discarding of bad lines.
	for hl in (None, 'a', 'b', 'c'):
		dw = job.datasetwriter(name='hashed on %s' % (hl,), columns={'a': 'unicode', 'b': 'unicode', 'c': 'unicode'}, hashlabel=hl)
		w = dw.get_split_write()
		for ix in range(1000):
			w(unicode(ix), '%d.%d' % (ix, ix % 5 == 0), ('{"a": %s}' if ix % 3 else '%d is bad') % (ix,))
		src_ds = dw.finish()
		assert src_ds.hashlabel == hl
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
	assert src_ds.hashlabel == None
	column2type = {t.split(':')[0]: t for t in cols}
	for hl in column2type:
		hashed = subjobs.build('dataset_type', options=dict(column2type=column2type, hashlabel=hl), datasets=dict(source=src_ds)).dataset()
		assert hashed.hashlabel == hl
		unhashed = subjobs.build('dataset_type', options=dict(column2type=column2type), datasets=dict(source=src_ds)).dataset()
		assert unhashed.hashlabel == None
		rehashed = subjobs.build('dataset_hashpart', options=dict(hashlabel=hl), datasets=dict(source=unhashed)).dataset()
		assert rehashed.hashlabel == hl
		assert hashed.lines == rehashed.lines
		assert sum(hashed.lines) == 1000
		assert set(hashed.columns.keys()) == set(unhashed.columns.keys()) == set(rehashed.columns.keys())
		# and again with a bad column
		column2type['half'] = 'float32'
		hashed = subjobs.build('dataset_type', options=dict(column2type=column2type, hashlabel=hl, filter_bad=True), datasets=dict(source=src_ds)).dataset()
		assert hashed.hashlabel == hl
		unhashed = subjobs.build('dataset_type', options=dict(column2type=column2type, filter_bad=True), datasets=dict(source=src_ds)).dataset()
		assert unhashed.hashlabel == None
		rehashed = subjobs.build('dataset_hashpart', options=dict(hashlabel=hl), datasets=dict(source=unhashed)).dataset()
		assert rehashed.hashlabel == hl
		del column2type['half']
		assert hashed.lines == rehashed.lines
		assert sum(hashed.lines) == 500
		assert set(hashed.columns.keys()) == set(unhashed.columns.keys()) == set(rehashed.columns.keys())

	# test rehashing on a column we don't type, over all types.
	dw = job.datasetwriter(name='rehash all types', columns={
		'2type'   : ('ascii', True),
		'ascii'   : ('ascii', True),
		'bits32'  : ('bits32', False),
		'bits64'  : ('bits64', False),
		'bool'    : ('bool', True),
		'bytes'   : ('bytes', True),
		'date'    : ('date', True),
		'datetime': ('datetime', True),
		'float32' : ('float32', True),
		'float64' : ('float64', True),
		'int32'   : ('int32', True),
		'int64'   : ('int64', True),
		'json'    : ('json', True),
		'number'  : ('number', True),
		'time'    : ('time', True),
		'unicode' : ('unicode', True),
	})
	write = dw.get_split_write()
	data = {
		'42': ('ascii string', 100, 1000,  True, b'bytes string', date(2019, 12, 11), datetime(2019, 12, 11, 20, 7, 21), 1.5, 0.00000001, 99, -11, {"a": "b"}, 1e100, time(20, 7, 21), 'unicode string'),
		None: (          None,   0,    0,  None,            None,               None,                              None, None,      None, None, None,     None,  None,            None,             None),
		'18': ('ASCII STRING', 111, 1111, False, b'BYTES STRING', date(1868,  1,  3), datetime(1868,  1,  3, 13, 14, 5), 2.5, -0.0000001, 67, -99, [42, ".."], 5e100, time(13, 14, 5), 'UNICODE STRING'),
	}
	write('42', *data['42'])
	write(None, *data[None])
	write('18', *data['18'])
	src_ds = dw.finish()
	data['None'] = data.pop(None)
	type2type = {
		'ascii'   : 'unicode:ascii',
		'bool'    : 'unicode:ascii',
		'date'    : 'unicode:ascii',
		'datetime': 'unicode:ascii',
		'time'    : 'unicode:ascii',
		'bits32'  : 'bits32_10',
		'bits64'  : 'bits64_10',
		'bytes'   : 'bytes',
		'float32' : 'float32',
		'float64' : 'float64',
		'int32'   : 'int32_10',
		'int64'   : 'int64_10',
		'number'  : 'number',
		'unicode' : 'unicode:ascii',
	}
	for hl, typeas in sorted(type2type.items()):
		ds = subjobs.build('dataset_type', column2type={'2type': typeas}, hashlabel=hl, source=src_ds).dataset()
		seen = set()
		hl_hash = typed_writer(hl).hash
		for sliceno in range(slices):
			for line in ds.iterate(sliceno, None):
				key = line[0] or None
				if isinstance(key, float):
					key = int(key)
				if isinstance(key, bytes):
					key = key.decode('ascii')
				else:
					key = unicode(key)
				assert data.get(key) == line[1:], "%s (hl %s) didn't have the right data for line %r" % (ds, hl, line[0],)
				hv = line[sorted(src_ds.columns).index(hl)]
				assert hl_hash(hv) % slices == sliceno, "%s (hl %s) didn't hash %r correctly" % (ds, hl, hv,)
				assert key not in seen, "%s (hl %s) repeated line %s" % (ds, hl, line[0],)
				seen.add(key)
		assert seen == {'42', 'None', '18'}, "%s didn't have all lines (%r)" % (ds, seen,)

def test(src_ds, opts, expect_lines):
	opts = DotDict(opts)
	def rename(colname):
		return opts.get('rename', {}).get(colname, colname)
	cols = set(opts.column2type)
	opts.discard_untyped = True
	msg = 'Testing with types %s' % (', '.join(v for k, v in sorted(opts.column2type.items())),)
	expect_hl = None
	if src_ds.hashlabel and opts.column2type.get(src_ds.hashlabel) == 'json':
		# json is not hashable, so we have to override the hashlabel to nothing in this case.
		opts.hashlabel = ''
		msg += ' (clearing hashlabel)'
	elif src_ds.hashlabel:
		expect_hl = rename(src_ds.hashlabel)
		if expect_hl in opts.column2type:
			msg += ' (hashed on %s)' % (opts.column2type[expect_hl],)
		else:
			expect_hl = None
			msg += ' (hashed on <untyped column>)'
	print(msg)
	just_typed = subjobs.build('dataset_type', options=opts, datasets=dict(source=src_ds)).dataset()
	assert just_typed.hashlabel == expect_hl, just_typed
	assert set(just_typed.columns) == cols, just_typed
	assert sum(just_typed.lines) == expect_lines, just_typed
	if rename(src_ds.hashlabel) not in opts.column2type or opts.get('hashlabel') == '':
		assert just_typed.hashlabel is None, just_typed
	else:
		assert just_typed.hashlabel == rename(src_ds.hashlabel), just_typed
	del opts.discard_untyped
	rev_rename = {v: k for k, v in opts.get('rename', {}).items()}
	discard = set(src_ds.columns) - set(rev_rename.get(n, n) for n in cols)
	if discard:
		d = opts.get('rename', {})
		d.update({k: None for k in discard})
		opts.rename = d
	for hashlabel in cols:
		if opts.column2type[hashlabel] == 'json':
			# not hashable
			continue
		opts['hashlabel'] = hashlabel
		print('%s rehashed on %s' % (msg, opts.column2type[hashlabel],))
		hashed_by_type = subjobs.build('dataset_type', options=opts, datasets=dict(source=src_ds)).dataset()
		assert hashed_by_type.hashlabel == hashlabel, hashed_by_type
		assert set(hashed_by_type.columns) == cols, hashed_by_type
		assert sum(hashed_by_type.lines) == expect_lines, hashed_by_type
		hashed_after = subjobs.build('dataset_hashpart', options=dict(hashlabel=hashlabel), datasets=dict(source=just_typed)).dataset()
		assert hashed_after.hashlabel == hashlabel, hashed_after
		assert set(hashed_after.columns) == cols, hashed_after
		assert sum(hashed_after.lines) == expect_lines, hashed_after
		if src_ds.hashlabel:
			# if src_ds has a hashlabel then just_typed will actually already be hashed, so hashed_after
			# will have been hashed twice and therefore have a different order than hashed_by_type.
			if rename(src_ds.hashlabel) == hashlabel:
				# These should be the same though.
				subjobs.build('test_compare_datasets', datasets=dict(a=hashed_by_type, b=just_typed))
			hashed_by_type = subjobs.build('dataset_sort', options=dict(sort_columns=rename('a')), datasets=dict(source=hashed_by_type))
			hashed_after = subjobs.build('dataset_sort', options=dict(sort_columns=rename('a')), datasets=dict(source=hashed_after))
		subjobs.build('test_compare_datasets', datasets=dict(a=hashed_by_type, b=hashed_after))
