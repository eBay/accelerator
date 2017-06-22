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

from __future__ import print_function
from __future__ import division

import gzutil

assert gzutil.version >= (2, 8, 1) and gzutil.version[0] == 2, gzutil.version

from compat import PY3

type2iter = {
	'number'  : gzutil.GzNumber,
	'float64' : gzutil.GzFloat64,
	'float32' : gzutil.GzFloat32,
	'int64'   : gzutil.GzInt64,
	'int32'   : gzutil.GzInt32,
	'bits64'  : gzutil.GzBits64,
	'bits32'  : gzutil.GzBits32,
	'bool'    : gzutil.GzBool,
	'datetime': gzutil.GzDateTime,
	'date'    : gzutil.GzDate,
	'time'    : gzutil.GzTime,
	'bytes'   : gzutil.GzBytesLines,
	'ascii'   : gzutil.GzAsciiLines,
	'unicode' : gzutil.GzUnicodeLines,
}

def _mklistreader(inner_type, seq_type):
	from compat import builtins
	reader = type2iter[inner_type.lower()]
	mk = getattr(builtins, seq_type.lower())
	class GzXList(object):
		def __init__(self, *a, **kw):
			if 'max_count' in kw:
				kw['max_count'] += 1
			self.fh = reader(*a, **kw)
		def __next__(self):
			llen = next(self.fh)
			if llen is None:
				return None
			return mk(next(self.fh) for _ in range(int(llen)))
		next = __next__
		def close(self):
			self.fh.close()
		def __iter__(self):
			return self
		def __enter__(self):
			return self
		def __exit__(self, type, value, traceback):
			self.close()
	GzXList.__name__ = 'Gz' + inner_type + seq_type
	return GzXList

for _seq_t in ('List', 'Set',):
	for _t in ('Number', 'Ascii', 'Unicode',):
		type2iter[(_t + _seq_t).lower()] = _mklistreader(_t, _seq_t)

from ujson import loads
class GzJson(object):
	def __init__(self, *a, **kw):
		if 'max_count' in kw:
			kw['max_count'] += 1
		if PY3:
			self.fh = gzutil.GzUnicodeLines(*a, **kw)
		else:
			self.fh = gzutil.GzBytesLines(*a, **kw)
	def __next__(self):
		return loads(next(self.fh))
	next = __next__
	def close(self):
		self.fh.close()
	def __iter__(self):
		return self
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
type2iter['json'] = GzJson

def typed_reader(typename):
	if typename not in type2iter:
		raise ValueError("Unknown reader for type %s" % (typename,))
	return type2iter[typename]
