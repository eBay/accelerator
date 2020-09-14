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

from __future__ import print_function
from __future__ import division

from accelerator import gzutil
from accelerator.compat import PY3

type2iter = {
	'number'  : gzutil.GzNumber,
	'complex64': gzutil.GzComplex64,
	'complex32': gzutil.GzComplex32,
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
	'bytes'   : gzutil.GzBytes,
	'ascii'   : gzutil.GzAscii,
	'unicode' : gzutil.GzUnicode,
}

from json import JSONDecoder
class GzJson(object):
	def __init__(self, *a, **kw):
		if PY3:
			self.fh = gzutil.GzUnicode(*a, **kw)
		else:
			self.fh = gzutil.GzBytes(*a, **kw)
		self.decode = JSONDecoder().decode
	def __next__(self):
		return self.decode(next(self.fh))
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

from pickle import loads
class GzPickle(object):
	def __init__(self, *a, **kw):
		assert PY3, "Pickle columns require python 3, sorry"
		self.fh = gzutil.GzBytes(*a, **kw)
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
type2iter['pickle'] = GzPickle

def typed_reader(typename):
	if typename not in type2iter:
		raise ValueError("Unknown reader for type %s" % (typename,))
	return type2iter[typename]
