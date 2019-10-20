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
Test writing a dataset in prepare, verifying that it is usable in
analysis and synthesis with no manual .finish()
'''

from accelerator.dataset import DatasetWriter, Dataset

def prepare():
	dw = DatasetWriter(columns={"data": "ascii"})
	write = dw.get_split_write()
	write("foo")
	write("bar")

def analysis(sliceno, params):
	ds = Dataset(params.jobid)
	assert set(ds.iterate(None, "data")) == {"foo", "bar"}

def synthesis(params):
	ds = Dataset(params.jobid)
	assert set(ds.iterate(None, "data")) == {"foo", "bar"}
