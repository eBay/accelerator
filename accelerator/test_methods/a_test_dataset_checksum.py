############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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
Test dataset_checksum[_chain].
'''

from accelerator.dataset import DatasetWriter
from accelerator import subjobs
from accelerator import blob
from accelerator.compat import PY3

test_data = [
	("a", b"A", b"0", 0.42, 18, [1, 2, 3], u"a", u"A"),
	("b", b"B", b"1", 0.18, 42, "1, 2, 3", u"b", u"B"),
	# And one with difficult values.
	("c", b"\xe4", None, float('NaN'), -1, {1: 2, "3": 4, "foo": "bar", "bar": "|"}, u"\xe4", None),
]

def prepare():
	columns = dict(
		ascii="ascii",
		bytes="bytes",
		bytes_none=("bytes", True),
		float="float64",
		int="int64",
		json="json",
		unicode="unicode",
		unicode_none=("unicode", True),
	)
	if PY3:
		# z so it sorts last
		columns['zpickle'] = 'pickle'
		for ix, v in enumerate(test_data):
			test_data[ix] = v + ([ix, 'line %d' % (ix,), {'line': ix}, 42],)
		test_data[-1][-1][-1] = float('-inf')
	a = DatasetWriter(name="a", columns=columns)
	b = DatasetWriter(name="b", columns=columns, previous=a)
	c = DatasetWriter(name="c", columns=columns)
	return a, b, c, test_data

def analysis(sliceno, prepare_res):
	a, b, c, test_data = prepare_res
	if sliceno == 0:
		for data in test_data[:2]:
			a.write_list(data)
		for data in test_data[:1]:
			b.write_list(data)
		for data in test_data[::-1]:
			c.write_list(data)
	if sliceno == 1:
		for data in test_data[2:]:
			a.write_list(data)
		for data in test_data[-1:0:-1]:
			b.write_list(data)
		for data in test_data:
			c.write_list(data)

def ck(jid, method="dataset_checksum", **kw):
	jid = subjobs.build(method, datasets=dict(source=jid), options=kw)
	return blob.load(jobid=jid).sum

def synthesis(prepare_res):
	a, b, c, _ = prepare_res
	a = a.finish()
	b = b.finish()
	c = c.finish()
	a_sum = ck(a)
	b_sum = ck(b)
	c_sum = ck(c)
	assert a_sum == b_sum # only order differs, so sorted they are the same.
	assert a_sum != c_sum # each line twice in c
	ab_sum = ck(b, "dataset_checksum_chain")
	assert ab_sum == 0 # a_sum and b_sum are the same, so a_sum ^ b_sum == 0
	assert ab_sum != a_sum
	assert ab_sum != c_sum # chains and lines are handled differently.
	cc_sum = ck(c, "dataset_checksum_chain")
	assert cc_sum == c_sum # but a chain of one should be equal to that one.
	a_uns_sum = ck(a, sort=False)
	b_uns_sum = ck(b, sort=False)
	assert a_uns_sum != b_uns_sum # they are not the same order
	if PY3:
		# Check that the pickle column really was included and works.
		a_p_sum = ck(a, columns={'zpickle'})
		b_p_sum = ck(b, columns={'zpickle'})
		assert a_p_sum == b_p_sum # same values
		a_uns_p_sum = ck(a, columns={'zpickle'}, sort=False)
		b_uns_p_sum = ck(b, columns={'zpickle'}, sort=False)
		assert a_uns_p_sum != b_uns_p_sum # but they are not the same order
