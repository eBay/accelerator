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
Verify that subjobs are allowed to nest (exactly) five levels.
'''

from accelerator import subjobs
from accelerator.dispatch import JobError

options = {'level': 0}

def synthesis():
	assert options.level < 5, "Too deep subjob nesting allowed"
	try:
		subjobs.build('test_subjobs_nesting', options={'level': options.level + 1})
	except JobError:
		assert options.level == 4, "Not enough subjob nesting allowed"
