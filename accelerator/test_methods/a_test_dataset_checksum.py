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
Test dataset_checksum[_chain].
'''

from accelerator.dataset import DatasetWriter
from accelerator import subjobs
from accelerator import blob

test_data = [
	(b"a", 0.42, 18, [1, 2, 3], u"a"),
	(b"b", 0.18, 42, "1, 2, 3", u"b"),
	(b"c", 1.23, 0, {1: 2, 3: 4}, u"c"),
]

def prepare():
	columns = dict(
		bytes="bytes",
		float="float64",
		int="int64",
		json="json",
		unicode="unicode",
	)
	a = DatasetWriter(name="a", columns=columns)
	b = DatasetWriter(name="b", columns=columns, previous=a)
	c = DatasetWriter(name="c", columns=columns)
	return a, b, c

def analysis(sliceno, prepare_res):
	a, b, c = prepare_res
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
	a, b, c = prepare_res
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
