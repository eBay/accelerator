import gzutil

assert gzutil.version >= (2, 5, 0) and gzutil.version[0] == 2, gzutil.version

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
	import __builtin__
	reader = type2iter[inner_type.lower()]
	mk = getattr(__builtin__, seq_type.lower())
	class GzXList(object):
		def __init__(self, *a, **kw):
			self.fh = reader(*a, **kw)
			assert int(next(self.fh)) == 42001, "Wrong version"
		def next(self):
			llen = next(self.fh)
			if llen is None:
				return None
			return mk(next(self.fh) for _ in range(int(llen)))
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
		self.fh = gzutil.GzBytesLines(*a, **kw)
		assert next(self.fh) == "json0", "Wrong version"
	def next(self):
		return loads(next(self.fh))
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
