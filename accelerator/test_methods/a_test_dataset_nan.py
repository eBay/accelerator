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
Test that NaN does not end up in min/max unless it's the only value.
'''

from math import isnan

nan = float('nan')
inf = float('inf')

def prepare(job):
	def create(name, none_support=False):
		dw = job.datasetwriter(name=name)
		dw.add('float32', 'float32', none_support=none_support)
		dw.add('float64', 'float64', none_support=none_support)
		dw.add('number', 'number', none_support=none_support)
		return dw
	return create('a', True), create('b'), create('c')

def analysis(sliceno, prepare_res):
	a, b, c = prepare_res
	if sliceno < 2:
		for ds in prepare_res:
			ds.write(nan, nan, nan)
		if sliceno == 1:
			a.write(None, None, None)
	if sliceno == 2:
		b.write(nan, 2, nan)
		c.write(nan, nan, nan)
		c.write(inf, 1, 1)
		c.write(0, 1, 2)
		c.write(nan, nan, nan)

def synthesis(prepare_res):
	a, b, c = prepare_res
	def eq(a, b):
		if isinstance(a, float) and isinstance(b, float) and isnan(a) and isnan(b):
			return True
		return a == b
	def check(dw, want_min, want_max):
		ds = dw.finish()
		for colname, want_min, want_max in zip(['float32', 'float64', 'number'], want_min, want_max):
			col = ds.columns[colname]
			assert eq(col.min, want_min), "%s.%s should have had min value %s, but had %s" % (ds, colname, want_min, col.min)
			assert eq(col.max, want_max), "%s.%s should have had max value %s, but had %s" % (ds, colname, want_max, col.max)
	check(a, [nan, nan, nan], [nan, nan, nan])
	check(b, [nan, 2, nan], [nan, 2, nan])
	check(c, [0, 1, 1], [inf, 1, 2])
