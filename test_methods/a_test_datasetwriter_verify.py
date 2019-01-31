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
Verify that each slice contains the expected data after test_datasetwriter.
'''

from datetime import date

from dataset import Dataset
from . import test_data

depend_extra=(test_data,)

datasets = ('source',)

def analysis(sliceno, params):
	assert list(datasets.source.iterate(sliceno, "a")) == [sliceno, 42]
	assert list(datasets.source.iterate(sliceno, "b")) == ["a", str(sliceno)]
	named = Dataset(datasets.source, "named")
	assert list(named.iterate(sliceno, "c")) == [True, False]
	assert list(named.iterate(sliceno, "d")) == [date(1536, 12, min(sliceno + 1, 31)), date(2236, 5, min(sliceno + 1, 31))]
	if sliceno < test_data.value_cnt:
		passed = Dataset(datasets.source, "passed")
		good = tuple(v[sliceno] for _, v in sorted(test_data.data.items()))
		assert list(passed.iterate(sliceno)) == [good]
