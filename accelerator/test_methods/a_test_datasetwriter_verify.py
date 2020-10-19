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
Verify that each slice contains the expected data after test_datasetwriter.
'''

from datetime import date
from sys import version_info

from accelerator.dataset import Dataset
from accelerator.gzwrite import typed_writer
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
		if version_info > (3, 6, 0):
			want_fold = (sliceno == 1)
			assert next(passed.iterate(sliceno, "datetime")).fold == want_fold
			assert next(passed.iterate(sliceno, "time")).fold == want_fold
	synthesis_split = Dataset(datasets.source, "synthesis_split")
	values = zip((1, 2, 3,), "abc")
	hash = typed_writer("int32").hash
	good = [v for v in values if hash(v[0]) % params.slices == sliceno]
	assert list(synthesis_split.iterate(sliceno)) == good
	synthesis_manual = Dataset(datasets.source, "synthesis_manual")
	assert list(synthesis_manual.iterate(sliceno, "sliceno")) == [sliceno]
	nonetest = Dataset(datasets.source, "nonetest")
	good = tuple(v[0] if k in test_data.not_none_capable else None for k, v in sorted(test_data.data.items()))
	assert list(nonetest.iterate(sliceno)) == [good]
