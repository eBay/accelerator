############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2021 Carl Drougge                       #
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

from accelerator import _dsutil
from accelerator.compat import str_types, PY3

_convfuncs = {
	'number'   : _dsutil.GzWriteNumber,
	'complex64': _dsutil.GzWriteComplex64,
	'complex32': _dsutil.GzWriteComplex32,
	'float64'  : _dsutil.GzWriteFloat64,
	'float32'  : _dsutil.GzWriteFloat32,
	'int64'    : _dsutil.GzWriteInt64,
	'int32'    : _dsutil.GzWriteInt32,
	'bits64'   : _dsutil.GzWriteBits64,
	'bits32'   : _dsutil.GzWriteBits32,
	'bool'     : _dsutil.GzWriteBool,
	'datetime' : _dsutil.GzWriteDateTime,
	'date'     : _dsutil.GzWriteDate,
	'time'     : _dsutil.GzWriteTime,
	'bytes'    : _dsutil.GzWriteBytes,
	'ascii'    : _dsutil.GzWriteAscii,
	'unicode'  : _dsutil.GzWriteUnicode,
	'parsed:number'   : _dsutil.GzWriteParsedNumber,
	'parsed:complex64': _dsutil.GzWriteParsedComplex64,
	'parsed:complex32': _dsutil.GzWriteParsedComplex32,
	'parsed:float64'  : _dsutil.GzWriteParsedFloat64,
	'parsed:float32'  : _dsutil.GzWriteParsedFloat32,
	'parsed:int64'    : _dsutil.GzWriteParsedInt64,
	'parsed:int32'    : _dsutil.GzWriteParsedInt32,
	'parsed:bits64'   : _dsutil.GzWriteParsedBits64,
	'parsed:bits32'   : _dsutil.GzWriteParsedBits32,
}

_type2iter = {
	'number'  : _dsutil.GzNumber,
	'complex64': _dsutil.GzComplex64,
	'complex32': _dsutil.GzComplex32,
	'float64' : _dsutil.GzFloat64,
	'float32' : _dsutil.GzFloat32,
	'int64'   : _dsutil.GzInt64,
	'int32'   : _dsutil.GzInt32,
	'bits64'  : _dsutil.GzBits64,
	'bits32'  : _dsutil.GzBits32,
	'bool'    : _dsutil.GzBool,
	'datetime': _dsutil.GzDateTime,
	'date'    : _dsutil.GzDate,
	'time'    : _dsutil.GzTime,
	'bytes'   : _dsutil.GzBytes,
	'ascii'   : _dsutil.GzAscii,
	'unicode' : _dsutil.GzUnicode,
}

def typed_writer(typename):
	if typename not in _convfuncs:
		raise ValueError("Unknown writer for type %s" % (typename,))
	return _convfuncs[typename]

def typed_reader(typename):
	if typename not in _type2iter:
		raise ValueError("Unknown reader for type %s" % (typename,))
	return _type2iter[typename]

from json import JSONEncoder, JSONDecoder, loads as json_loads
class GzWriteJson(object):
	min = max = None
	def __init__(self, *a, **kw):
		assert 'default' not in kw, "default not supported for Json, sorry"
		if PY3:
			self.fh = _dsutil.GzWriteUnicode(*a, **kw)
			self.encode = JSONEncoder(ensure_ascii=False, separators=(',', ':')).encode
		else:
			self.fh = _dsutil.GzWriteBytes(*a, **kw)
			self.encode = JSONEncoder(ensure_ascii=True, separators=(',', ':')).encode
	def write(self, o):
		self.fh.write(self.encode(o))
	@property
	def count(self):
		return self.fh.count
	def close(self):
		self.fh.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_convfuncs['json'] = GzWriteJson

class GzWriteParsedJson(GzWriteJson):
	"""This assumes strings are the object you wanted and parse them as json.
	If they are unparseable you get an error."""
	def write(self, o):
		if isinstance(o, str_types):
			o = json_loads(o)
		self.fh.write(self.encode(o))
_convfuncs['parsed:json'] = GzWriteParsedJson

class GzJson(object):
	def __init__(self, *a, **kw):
		if PY3:
			self.fh = _dsutil.GzUnicode(*a, **kw)
		else:
			self.fh = _dsutil.GzBytes(*a, **kw)
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
_type2iter['json'] = GzJson

from pickle import dumps as pickle_dumps, loads as pickle_loads
class GzWritePickle(object):
	min = max = None
	def __init__(self, *a, **kw):
		assert PY3, "Pickle columns require python 3, sorry"
		assert 'default' not in kw, "default not supported for Pickle, sorry"
		self.fh = _dsutil.GzWriteBytes(*a, **kw)
	def write(self, o):
		self.fh.write(pickle_dumps(o, 4))
	@property
	def count(self):
		return self.fh.count
	def close(self):
		self.fh.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_convfuncs['pickle'] = GzWritePickle

class GzPickle(object):
	def __init__(self, *a, **kw):
		assert PY3, "Pickle columns require python 3, sorry"
		self.fh = _dsutil.GzBytes(*a, **kw)
	def __next__(self):
		return pickle_loads(next(self.fh))
	next = __next__
	def close(self):
		self.fh.close()
	def __iter__(self):
		return self
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_type2iter['pickle'] = GzPickle
