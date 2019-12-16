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
Test DatasetWriter, exercising the different ways to create,
pass and populate the dataset.
'''

from datetime import date

from accelerator.dataset import DatasetWriter
from . import test_data

depend_extra=(test_data,)

def prepare(job, slices):
	assert slices >= test_data.value_cnt
	dw_default = DatasetWriter()
	dw_default.add("a", "number")
	dw_default.add("b", "ascii")
	DatasetWriter(name="named", columns={"c": "bool", "d": "date"})
	dw_passed = job.datasetwriter(name="passed", columns=test_data.columns)
	return dw_passed, 42

def analysis(sliceno, prepare_res, job):
	dw_default = job.datasetwriter()
	dw_named = DatasetWriter(name="named")
	dw_passed, num = prepare_res
	dw_default.write(a=sliceno, b="a")
	dw_default.write_list([num, str(sliceno)])
	dw_named.write(True, date(1536, 12, min(sliceno + 1, 31)))
	dw_named.write_dict({"c": False, "d": date(2236, 5, min(sliceno + 1, 31))})
	# slice 0 is written in synthesis
	if 0 < sliceno < test_data.value_cnt:
		dw_passed.write_dict({k: v[sliceno] for k, v in test_data.data.items()})

def synthesis(prepare_res, slices, job):
	dw_passed, _ = prepare_res
	# Using set_slice on a dataset that was written in analysis is not
	# actually supported, but since it currently works (as long as that
	# particular slice wasn't written in analysis) let's test it.
	dw_passed.set_slice(0)
	dw_passed.write(**{k: v[0] for k, v in test_data.data.items()})
	dw_synthesis_split = DatasetWriter(name="synthesis_split", hashlabel="a")
	dw_synthesis_split.add("a", "int32")
	dw_synthesis_split.add("b", "unicode")
	dw_synthesis_split.get_split_write()(1, "a")
	dw_synthesis_split.get_split_write_list()([2, "b"])
	dw_synthesis_split.get_split_write_dict()({"a": 3, "b": "c"})
	dw_synthesis_manual = job.datasetwriter(name="synthesis_manual", columns={"sliceno": "int32"})
	dw_nonetest = job.datasetwriter(name="nonetest", columns=test_data.columns)
	for sliceno in range(slices):
		dw_synthesis_manual.set_slice(sliceno)
		dw_synthesis_manual.write(sliceno)
		dw_nonetest.set_slice(sliceno)
		dw_nonetest.write(**{k: v[0] if k in test_data.not_none_capable else None for k, v in test_data.data.items()})
