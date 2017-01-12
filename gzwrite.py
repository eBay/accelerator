from __future__ import print_function
from __future__ import division

import gzutil
from compat import unicode, str_types

GzWrite = gzutil.GzWrite

_convfuncs = {
	'number'   : gzutil.GzWriteNumber,
	'float64'  : gzutil.GzWriteFloat64,
	'float32'  : gzutil.GzWriteFloat32,
	'int64'    : gzutil.GzWriteInt64,
	'int32'    : gzutil.GzWriteInt32,
	'bits64'   : gzutil.GzWriteBits64,
	'bits32'   : gzutil.GzWriteBits32,
	'bool'     : gzutil.GzWriteBool,
	'datetime' : gzutil.GzWriteDateTime,
	'date'     : gzutil.GzWriteDate,
	'time'     : gzutil.GzWriteTime,
	'bytes'    : gzutil.GzWriteBytesLines,
	'ascii'    : gzutil.GzWriteAsciiLines,
	'unicode'  : gzutil.GzWriteUnicodeLines,
	'parsed:number'   : gzutil.GzWriteParsedNumber,
	'parsed:float64'  : gzutil.GzWriteParsedFloat64,
	'parsed:float32'  : gzutil.GzWriteParsedFloat32,
	'parsed:int64'    : gzutil.GzWriteParsedInt64,
	'parsed:int32'    : gzutil.GzWriteParsedInt32,
	'parsed:bits64'   : gzutil.GzWriteParsedBits64,
	'parsed:bits32'   : gzutil.GzWriteParsedBits32,
}

def typed_writer(typename):
	if typename not in _convfuncs:
		raise ValueError("Unknown writer for type %s" % (typename,))
	return _convfuncs[typename]

def typed_reader(typename):
	from sourcedata import type2iter
	if typename not in type2iter:
		raise ValueError("Unknown reader for type %s" % (typename,))
	return type2iter[typename]

def _mklistwriter(inner_type, seq_type, len_type):
	class GzWriteXList(object):
		min = max = None
		def __init__(self, *a, **kw):
			assert 'default' not in kw, "default not supported for %s%s, sorry" % (inner_type, seq_type,)
			self.fh = _convfuncs[inner_type.lower()](*a, **kw)
			self.fh.write(len_type(42001)) # version marker
			self.count = 0
		def write(self, lst):
			self.count += 1
			if lst is None:
				self.fh.write(None)
				return
			llen = len(lst)
			assert llen < 65536, 'List too long (max 65535 elements)'
			w = self.fh.write
			w(len_type(llen))
			for v in lst:
				w(v)
		def close(self):
			self.fh.close()
		def __enter__(self):
			return self
		def __exit__(self, type, value, traceback):
			self.close()
	GzWriteXList.__name__ = 'GzWrite%s%s' % (inner_type, seq_type,)
	return GzWriteXList
for _seq_type in ('List', 'Set',):
	_convfuncs['number'  + _seq_type.lower()] = _mklistwriter('Number'  , _seq_type, int)
	_convfuncs['ascii'   + _seq_type.lower()] = _mklistwriter('Ascii'   , _seq_type, str)
	_convfuncs['unicode' + _seq_type.lower()] = _mklistwriter('Unicode' , _seq_type, unicode)

from ujson import dumps, loads
class GzWriteJson(object):
	min = max = None
	def __init__(self, *a, **kw):
		assert 'default' not in kw, "default not supported for Json, sorry"
		self.fh = gzutil.GzWriteBytesLines(*a, **kw)
		self.fh.write(b"json0") # version marker
		self.count = 0
	def write(self, o):
		self.count += 1
		self.fh.write(dumps(o, ensure_ascii=False))
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
			o = loads(o)
		self.count += 1
		self.fh.write(dumps(o, ensure_ascii=False))
_convfuncs['parsed:json'] = GzWriteParsedJson
