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

description = r'''
Merge two or more datasets.
The datasets must have the same number of lines in each slice
and if they do not have a common ancestor you must set
allow_unrelated=True.

Columns from later datasets override columns of the same name
from earlier datasets.
'''

options = dict(
	allow_unrelated=False,
)
datasets = (['source'], 'previous',)

def synthesis():
	assert len(datasets.source) >= 2, 'Must have at least two datasets to join'
	current = datasets.source[0]
	for ix, ds in enumerate(datasets.source[1:-1]):
		current = current.merge(ds, name=str(ix), allow_unrelated=options.allow_unrelated)
	current.merge(datasets.source[-1], allow_unrelated=options.allow_unrelated, previous=datasets.previous)
