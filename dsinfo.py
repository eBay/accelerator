#!/usr/bin/env python

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

# Print information about a dataset(chain)

from __future__ import division, print_function

import sys

import dscmdhelper

dscmdhelper.init()

# for every name argument
for n in sys.argv[1:]:
	ds = dscmdhelper.name2ds(n)
	print("Parent:", ds.parent)
	print("Hashlabel:", ds.hashlabel)
	print("Columns:")
	# there's a better way to do this, right?
	len_n = len_t = 0
	for n, c in ds.columns.items():
		len_n = max(len_n, len(n))
		len_t = max(len_t, len(c.type))
	template = "  {2} {0:%d} {3} {1:%d}" % (len_n, len_t,)
	for n, c in ds.columns.items():
		if n == ds.hashlabel:
			print(template.format(n, c.type, "\x1b[1m*", "\x1b[m"))
		else:
			print(template.format(n, c.type, " ", ""))
	print("{0:n} columns".format(len(ds.columns)))
	print("{0:n} lines".format(sum(ds.lines)))
	if ds.previous:
		chain = ds.chain()
		print("Chain length {0:n}, from {1} to {2}".format(len(chain), chain[0], chain[-1]))
		print("{0:n} total lines".format(sum(sum(ds.lines) for ds in chain)))
