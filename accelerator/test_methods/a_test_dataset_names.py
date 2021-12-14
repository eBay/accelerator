# -*- coding: utf-8 -*-
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
Verify that some potentially problematic dataset names work.
'''

def synthesis(job):
	def mk(name):
		dw = job.datasetwriter(name=name, columns={'name': 'unicode'})
		dw.get_split_write()(name)
		ds = dw.finish()
		assert ds.name == name
		return job.dataset(name)
	names = [
		'',            # empty is not a valid fs name
		'.',           # . is a reserved fs name
		'..',          # .. is a reserved fs name
		'\0',          # many things have problems with \0 ("string terminator")
		'\n',          # newlines are difficult
		' ',           # sometimes spaces are too
		'\xa0',        # this is non-breaking space
		'dataset.txt', # name collides with the legacy list of dataset names
		'LIST',        # name collides with the pickle of dataset names
		'ᛞᚨᛏᚨᛊᛖᛏ',     # non-ascii
		'😁',          # this character doesn't fit in 16 bits
	]
	for name in names:
		assert list(mk(name).iterate(None, 'name')) == [name], name
	return names
