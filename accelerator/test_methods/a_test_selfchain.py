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
Tests creating several chained datasets in one job.
Exercises DatasetWriter.finish and the chaining logic
including callbacks with SkipDataset.
'''

from accelerator.dataset import DatasetWriter, SkipDataset
from accelerator.extras import DotDict

def prepare(params):
	dws = {}
	prev = None
	for name in "abcdefgh":
		dw = DatasetWriter(name=name, previous=prev)
		dw.add("ds", "ascii")
		dw.add("num", "number")
		dws[name] = dw
		prev = dw
	return dws

def analysis(sliceno, prepare_res):
	# First slice goes down, datasets c - h contain negative numbers.
	if sliceno == 0:
		value = 3
		add = -2
	else:
		value = sliceno
		add = sliceno
	for name, dw in sorted(prepare_res.items()):
		dw.write(name, value)
		value += add

def synthesis(prepare_res, params):
	ds = DotDict()
	# Must be finished in order (.previous must be finished when .finish is called.)
	for name, dw in sorted(prepare_res.items()):
		ds[name] = dw.finish()
	last = ds.h
	assert last.chain() == sorted(ds.values())
	ds.last = last
	test_partial_chains(ds)
	test_filters(ds)

def test_partial_chains(ds):
	alles = list(ds.last.iterate_chain(None))
	part1 = list(ds.b.iterate_chain(None))
	part2 = list(ds.e.iterate_chain(None, length=3))
	def only_f_and_g_cb(d):
		if d[-2:] not in ("/f", "/g"):
			raise SkipDataset
	seen = []
	def record_cb(d):
		seen.append(d)
	part3 = list(ds.g.iterate_chain(None, pre_callback=only_f_and_g_cb, post_callback=record_cb))
	assert seen == ds.g.chain(length=2)
	part4 = list(ds.last.iterate(None))
	assert alles == part1 + part2 + part3 + part4
	two_by_length = ds.d.chain(length=2)
	two_by_id = ds.d.chain(stop_ds=ds.b)
	assert two_by_length == two_by_id
	def stop_on(name):
		def cb(d):
			if d == name:
				raise StopIteration
		return cb
	just_two = list(ds.last.iterate_list(None, None, [ds.a, ds.b]))
	two_by_pre_stop = list(ds.last.iterate_chain(None, pre_callback=stop_on(ds.c)))
	assert two_by_pre_stop == just_two
	two_by_post_stop = list(ds.last.iterate_chain(None, post_callback=stop_on(ds.b)))
	assert two_by_post_stop == just_two

def test_filters(ds):
	just_c = list(ds.c.iterate(None))
	filtered_to_c = list(ds.last.iterate_chain(None, filters={"ds": "c".__eq__}))
	assert just_c == filtered_to_c
	just_c_0 = list(ds.c.iterate(0))
	# Can't use (0).__gt__ in py2.
	just_c_filtered_negative = list(ds.c.iterate(None, filters={"num": lambda v: v < 0}))
	assert just_c_0 == just_c_filtered_negative
	# test two filters
	alles_filtered_c_negative = list(ds.last.iterate_chain(None, filters={"num": lambda v: v < 0, "ds": "c".__eq__}))
	assert just_c_0 == alles_filtered_c_negative
	# test single tuple filter
	alles_filtered_c_negative_again = list(ds.last.iterate_chain(None, filters=lambda t: t[0] == "c" and t[1] < 0))
	assert alles_filtered_c_negative == alles_filtered_c_negative_again
	# test filtering something translated
	# Can't use (2).__add__ in (some) py2.
	just_0_plus_2_pos = list(ds.last.iterate_chain(0, filters={"num": lambda v: v > 0}, translators={"num": lambda v: v + 2}))
	just_0 = list(ds.last.iterate_chain(0))
	just_0_plus_2_pos_manual = [(t[0], t[1] + 2,) for t in just_0 if t[1] + 2 > 0]
	assert just_0_plus_2_pos == just_0_plus_2_pos_manual
	# Test filtering with (non-tupled) single column
	just_0_num_plus_2_pos = list(ds.last.iterate_chain(0, columns="num", filters={"num": lambda v: v > 0}, translators={"num": lambda v: v + 2}))
	assert just_0_num_plus_2_pos == [t[1] for t in just_0_plus_2_pos]
