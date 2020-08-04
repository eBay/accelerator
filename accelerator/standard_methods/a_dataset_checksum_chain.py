############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

description = r'''
Take a chain of datasets and make a checksum of one or more columns.
See dataset_checksum.description for more information.

datasets.source is mandatory, datasets.stop is optional.
options.chain_length defaults to -1.

Sort does not sort across datasets.
'''

from accelerator import DotDict, build

options = dict(
	chain_length = -1,
	columns      = set(),
	sort         = True,
)

datasets = ('source', 'stop',)

def synthesis():
	sum = 0
	jobs = datasets.source.chain(length=options.chain_length, stop_ds=datasets.stop)
	for src in jobs:
		data = build('dataset_checksum', columns=options.columns, sort=options.sort, source=src).load()
		sum ^= data.sum
	print("Total: %016x" % (sum,))
	return DotDict(sum=sum, columns=data.columns, sort=options.sort, sources=jobs)
