############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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
Test that missing set_slice is an error without allow_missing_slices
but not with.
'''

from accelerator.error import DatasetUsageError

def synthesis(job):
	dw = job.datasetwriter(name='fails')
	dw.add('a', 'ascii')
	dw.set_slice(1)
	dw.write('a')
	try:
		dw.finish()
		raise Exception("DatasetWriter allowed finishing without writing all slices")
	except DatasetUsageError:
		dw.discard()

	dw = job.datasetwriter(name='fails', allow_missing_slices=True)
	dw.add('a', 'ascii')
	try:
		dw.get_split_write()
		raise Exception("DatasetWriter allowed get_split_write with allow_missing_slices")
	except DatasetUsageError:
		dw.discard()

	dw = job.datasetwriter(name='succeeds', allow_missing_slices=True)
	dw.add('a', 'ascii')
	dw.set_slice(1)
	dw.write('a')
	dw.finish()
