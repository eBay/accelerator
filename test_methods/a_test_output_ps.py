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

from . import a_test_output

description = r'''
Run from test_output to check that output handling works correctly
when only prepare and synthesis are defined.
'''

depend_extra = (a_test_output,)

options = dict(
	prefix='',
	p='',
	s='',
)

def prepare():
	a_test_output.sub_part('p', options)

def synthesis():
	a_test_output.sub_part('s', options)
