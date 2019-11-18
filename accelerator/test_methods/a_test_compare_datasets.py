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

datasets = ("a", "b",)

def analysis(sliceno):
	assert sorted(datasets.a.columns) == sorted(datasets.b.columns)
	iter_a = datasets.a.iterate(sliceno)
	iter_b = datasets.b.iterate(sliceno, status_reporting=False)
	# This doesn't use zip because then we can't see if one iterator is shorter
	while True:
		try:
			a = next(iter_a)
		except StopIteration:
			try:
				next(iter_b)
				raise Exception("dataset b is longer than a in slice %d" % (sliceno,))
			except StopIteration:
				break
		try:
			b = next(iter_b)
		except StopIteration:
			raise Exception("dataset a is longer than b in slice %d" % (sliceno,))
		assert a == b
