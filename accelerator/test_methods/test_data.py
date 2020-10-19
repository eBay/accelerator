############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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

# Test data for use in dataset testing

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from datetime import date, time, datetime
from sys import version_info

from accelerator.compat import first_value, num_types

# Constraints on this data:
#    Each tuple must be the same length.
#    The tests will not work with less than len(tuple) slices.
#    Time values may not have a 0 microsecond, because str() will omit that.
#    No tuple should contain duplicate values (but bool has to).
#    All values must be sortable (within the same tuple).
#       Which means no tuple may contain None, even for types that support None.
#    Numeric types other than int64 must have a low-ish first value.
#    Most of the above doesn't apply to json (because it's handled specially.)
#    complex* are also unsortable. (first value is sortable though.)
# It's supposed to contain all types, but it doesn't really have to.
data = {
	"float64": (1/3, 1e100, -9.0),
	"float32": (100.0, -0.0, 2.0),
	"int64": (9223372036854775807, -9223372036854775807, 100),
	"int32": (-2147483647, 2147483647, -1),
	"bits64": (0, 18446744073709551615, 0x55aa55aa55aa55aa),
	"bits32": (0, 4294967295, 0xaa55aa55),
	"bool": (True, False, True,),
	"datetime": (datetime(1916, 2, 29, 23, 59, 59, 999999), datetime(1916, 2, 29, 23, 59, 59, 999998), datetime(1970, 1, 1, 0, 0, 0, 1)),
	"date": (date(2016, 2, 29), date(2016, 2, 28), date(2017, 6, 27),),
	"time": (time(12, 0, 0, 999999), time(12, 0, 0, 999998), time(0, 1, 2, 3)),
	"bytes": (b"foo", b"bar", b"blutti",),
	"unicode": ("bl\xe5", "bl\xe4", "bla",),
	"ascii": ("foo", "bar", "blutti",),
	# big value - will change if it roundtrips through (any type of) float, semibig to find 32bit issues, and a float.
	"number": (1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, 13578058080989382, 1/3),
	"json": ({"a": [1, 2, {"b": {}}]}, None, "bl\xe4"),
	"complex64": (1.5, -1e100+0.00000002j, 5.3j),
	"complex32": (-1, 1+2j, -0.25j),
}

if version_info > (3, 6, 0):
	for name in ("datetime", "time",):
		tmp = list(data[name])
		tmp[1] = tmp[1].replace(fold=1)
		data[name] = tuple(tmp)

value_cnt = {len(v) for v in data.values()}
assert len(value_cnt) == 1, "All tuples in data must have the same length."
value_cnt = first_value(value_cnt)

not_none_capable = {"bits64", "bits32",}

columns = {t: t if t in not_none_capable else (t, True) for t in data}

def sort_data_for_slice(sliceno):
	# numeric types use only (modified) v[0], other types cycle through their values.
	# int64 goes down one every other line,
	# all other numeric columns go up sliceno + 1 every line.
	# json starts as 0 (pretending to be a numeric type).
	# complex* is also pretending to be a real number type here.
	def add(offset):
		res = []
		for k, v in sorted(data.items()):
			if k == "json":
				v = [0]
			if offset == 42 and k.startswith('float'):
				# Should sort last.
				v = float('NaN')
			elif isinstance(v[0], num_types) and k != "bool":
				v = v[0]
				if k == "int64":
					v -= offset // 2
				else:
					v += offset * (sliceno + 1)
			else:
				v = v[offset % len(v)]
			res.append(v)
		return tuple(res)
	for offset in range(sliceno + 2):
		yield add(offset)
	# Add some Nones in the middle
	# (Sorts first except for date/times where they sort last.)
	res = []
	for k in sorted(data):
		if k in not_none_capable:
			res.append(data[k][0])
		elif k == 'json':
			res.append(0)
		else:
			res.append(None)
	yield tuple(res)
	for offset in range(sliceno, 128):
		yield add(offset)
