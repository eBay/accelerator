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
Test Dataset.merge() and the dataset_merge method.
'''

from accelerator.dataset import DatasetWriter, DatasetUsageError
from accelerator import subjobs
from accelerator.dispatch import JobError

def mkds(name, columns, data, **kw):
	columns = dict.fromkeys(columns, 'int32')
	dw = DatasetWriter(name=name, columns=columns, **kw)
	write = dw.get_split_write()
	for v in data:
		write(*v)
	return dw.finish()

merges = {}
def merge(name, a, b, **kw):
	merges[name] = ((a, b), kw)
	return a.merge(b, name=name, **kw)

failed_merges = []
def fail_merge(a, b, **kw):
	failed_merges.append(((a, b), kw,))
	try:
		a.merge(b, name='failme', **kw)
	except DatasetUsageError:
		return
	raise Exception("Merging %s and %s with %r didn't fail as it should have" % (a, b, kw,))

checks = {}
def check(ds, want):
	if '/' in ds:
		checks[ds.name] = want
	got = list(ds.iterate_chain(None))
	got.sort()
	assert got == want, "%s contained %r not %r as expetected" % (ds, got, want,)

def synthesis(params):
	a0 = mkds('a0', ['0', '1'], [(1, 2), (3, 4), (5, 6)])
	a1 = mkds('a1', ['0', '1'], [(7, 8), (9, 10), (11, 12)], previous=a0)
	b0 = mkds('b0', ['1', '2'], [(13, 14), (15, 16), (17, 18)], parent=a0)
	b1 = mkds('b1', ['1', '2'], [(19, 20), (21, 22), (23, 24)], parent=a1, previous=b0)
	c0 = mkds('c0', ['3'], [(25,), (26,), (27,)], parent=a0)
	c1 = mkds('c1', ['3'], [(28,), (29,), (30,)], parent=a1, previous=c0)
	# a contains columns 0 and 1
	# b contains columns 0, 1 and 2 with 0 from a
	# c contains columns 0, 1 and 3 with 0 and 1 from a
	bc0 = merge('bc0', b0, c0) # simple merge, one overlapping column
	# bc contains columns 0, 1, 2 and 3, with 0 and 1 from a (via c), 2 from b and 3 from c
	check(bc0, [(1, 2, 14, 25), (3, 4, 16, 26), (5, 6, 18, 27)])
	bc1 = merge('bc1', b1, c1, previous=bc0) # chained
	check(bc1, [(1, 2, 14, 25), (3, 4, 16, 26), (5, 6, 18, 27), (7, 8, 20, 28), (9, 10, 22, 29), (11, 12, 24, 30)])
	cb0 = merge('cb0', c0, b0) # other direction, getting the other "1" column.
	# cb contains columns 0, 1, 2 and 3, with 0 from a (via b), 1 and 2 from b and 3 from c
	check(cb0, [(1, 13, 14, 25), (3, 15, 16, 26), (5, 17, 18, 27)])
	d0 = mkds('d0', ['4'], [(37,), (38,), (39,)], parent=c0)
	bd0 = merge('bd0', b0, d0) # immediate parents are not shared
	# bd contains columns 0, 1, 2, 3 and 4, with 0 and 1 from a (via d -> c -> a), 2 from b, 3 from c and 4 from d
	check(bd0, [(1, 2, 14, 25, 37), (3, 4, 16, 26, 38), (5, 6, 18, 27, 39)])
	# more than two datasets with complex parent relationship
	# merged in two stages here, but a single dataset_merge job later.
	cbd0 = merge('cbd0', c0, bd0)
	del merges['cbd0']
	cbdb0 = merge('cbdb0', cbd0, b0)
	merges['cbdb0'] = ((c0, bd0, b0), {})
	# cbdb contains columns 0, 1, 2, 3 and 4, with 0 from a (via d -> c -> a), 1 and 2 from b, 3 from c and 4 from d
	check(cbdb0, [(1, 13, 14, 25, 37), (3, 15, 16, 26, 38), (5, 17, 18, 27, 39)])
	fail_merge(a0, a1) # no parents
	fail_merge(b0, b1) # parents not shared
	fail_merge(b0, b0) # merge with self
	other = mkds('other', ['5'], [(31,), (32,), (33,)])
	fail_merge(a0, other) # parents not shared
	aother = merge('aother', a0, other, allow_unrelated=True)
	# aother contains 0 and 1 from a, 5 from other
	check(aother, [(1, 2, 31), (3, 4, 32), (5, 6, 33)])

	# check hashed datasets too
	ab_a = mkds('ab_a', ['a', 'b'], [(1, 2), (3, 4), (5, 6)], hashlabel='a')
	ab_b = mkds('ab_b', ['a', 'b'], [(7, 8), (9, 10), (11, 12)], hashlabel='b')
	ac_a = mkds('ac_a', ['a', 'c'], [(1, 14), (3, 15), (5, 16)], hashlabel='a') # a values must match ab_a
	fail_merge(ab_a, ab_b, allow_unrelated=True) # different hashlabels
	abac_a = merge('abac_a', ab_a, ac_a, allow_unrelated=True)
	assert abac_a.hashlabel == 'a'
	check(abac_a, [(1, 2, 14), (3, 4, 15), (5, 6, 16)])

	# merge hashed with unhashed (which we align with the hashlabel manually)
	dw = DatasetWriter(name='d_none', columns={'d': 'number'})
	for sliceno in range(params.slices):
		dw.set_slice(sliceno)
		for v in ab_a.iterate(sliceno, 'a'):
			dw.write(v + 16)
	d_none = dw.finish()
	abd_a = merge('abd_a', ab_a, d_none, allow_unrelated=True)
	assert abd_a.hashlabel == 'a'
	check(abd_a, [(1, 2, 17), (3, 4, 19), (5, 6, 21)])
	# other way round should affect nothing here
	dab_a = merge('dab_a', ab_a, d_none, allow_unrelated=True)
	assert dab_a.hashlabel == 'a'
	check(dab_a, [(1, 2, 17), (3, 4, 19), (5, 6, 21)])

	# the same test but with the lines in the wrong slices:
	dw = DatasetWriter(name='e_none', columns={'e': 'number'})
	e_done = False
	for sliceno in range(params.slices):
		dw.set_slice(sliceno)
		# there are 3 lines in total, some slice will not have all of them.
		if ab_a.lines[sliceno] != 3 and not e_done:
			dw.write(17)
			dw.write(19)
			dw.write(21)
			e_done = True
	assert e_done
	e_none = dw.finish()
	fail_merge(ab_a, e_none, allow_unrelated=True)

	# and finally test all we tested above using the dataset_merge method too
	for name, (parents, kw) in merges.items():
		a_ds = dict(source=parents)
		if 'previous' in kw:
			a_ds['previous'] = kw.pop('previous')
		jid = subjobs.build('dataset_merge', datasets=a_ds, options=kw)
		check(jid.dataset(), checks[name])
	for parents, kw in failed_merges:
		try:
			subjobs.build('dataset_merge', datasets=dict(source=parents), options=kw)
		except JobError:
			continue
		raise Exception("dataset_merge incorrectly allowed %r with %r" % (parents, kw))
