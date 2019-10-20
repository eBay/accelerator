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
Type datasets.untyped the same as datasets.typed in a subjob, then verify
(in another subjob) the the results are correct.
'''

from accelerator import subjobs

datasets=("typed", "untyped")

def synthesis():
	typerename = dict(
		int64="int64_10",
		int32="int32_10",
		bits64="bits64_10",
		bits32="bits32_10",
		bool="strbool",
		datetime="datetime:%Y-%m-%d %H:%M:%S.%f",
		date="date:%Y-%m-%d",
		time="time:%H:%M:%S.%f",
		unicode="unicode:utf-8",
	)
	columns = {k: typerename.get(v.type, v.type) for k, v in datasets.typed.columns.items()}
	retyped = subjobs.build(
		"dataset_type",
		options=dict(column2type=columns),
		datasets=dict(source=datasets.untyped)
	)
	subjobs.build("test_compare_datasets", datasets=dict(a=datasets.typed, b=retyped))
