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
Create some jobs and datasets to test the shell parser with.

Creates a default dataset with previous from datasets.previous,
and a j dataset with previous from j in jobs.previous.
'''

datasets = ('previous', 'parent',)
jobs = ('previous',)

def synthesis(job):
	dw = job.datasetwriter(columns={'a': 'bool'}, previous=datasets.previous)
	dw.get_split_write()
	previous = jobs.previous.dataset('j') if jobs.previous else None
	dw = job.datasetwriter(name='j', columns={'b': 'bool'}, previous=previous, parent=datasets.parent)
	dw.get_split_write()
	dw = job.datasetwriter(name='name/with/slash', columns={'b': 'bool'})
	dw.get_split_write()
