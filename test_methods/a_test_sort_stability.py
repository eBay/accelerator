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
Test that dataset_sort sorts stably.

Also tests DatasetWriter (again), and also DatasetWriter.finish()
'''

from accelerator.dataset import Dataset, DatasetWriter
from accelerator import subjobs

def prepare():
	dw = DatasetWriter()
	dw.add("str", "ascii")
	dw.add("num", "number")
	return dw

def analysis(sliceno, prepare_res):
	dw = prepare_res
	s = str(sliceno)
	# we expect to get a repeating str of range(slices) in "str" after sorting
	for ix in range(64):
		dw.write(s, ix)
	# less regular test case
	if sliceno == 0:
		dw.write("a", -1)
		dw.write("b", -1)
		dw.write("c", -2)
		dw.write("d", -1)
		dw.write("e", -1)
		dw.write("f", -1)
		dw.write("g", -2)
	if sliceno == 1:
		dw.write("h", -2)
		dw.write("i", -1)
		dw.write("j", -2)

def synthesis(params, prepare_res):
	dw = prepare_res
	source = dw.finish()
	jid = subjobs.build(
		"dataset_sort",
		options=dict(
			sort_columns="num",
			sort_across_slices=True,
		),
		datasets=dict(source=source),
	)
	ds = Dataset(jid)
	data = list(ds.iterate(None, "str"))
	good = list("cghjabdefi") + \
	       [str(sliceno) for sliceno in range(params.slices)] * 64
	assert data == good
