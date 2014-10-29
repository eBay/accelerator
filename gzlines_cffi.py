from cffi import FFI

ffi = FFI()
ffi.cdef("""
	typedef struct gzFile_s *gzFile;
	gzFile gzopen(const char *path, const char *mode);
	int gzclose(gzFile);
	int gzread (gzFile file, void *buf, unsigned len);
""")
C = ffi.verify("""#include <zlib.h>""", libraries=["z"])
Z = 128 * 1024

_gzclosemaybe_done = set()
@ffi.callback("void(gzFile)")
def _gzclosemaybe(fh):
	i = intptr(fh)
	if i in _gzclosemaybe_done:
		_gzclosemaybe_done.remove(i)
	else:
		C.gzclose(fh)

def intptr(ptr):
	return int(ffi.cast("intptr_t", ptr))

class gzlines:
	def __init__(self, fn):
		self.fh = ffi.gc(C.gzopen(fn, "rb"), _gzclosemaybe)
		if not self.fh:
			raise IOError("gzopen " + fn)
		self.cbuf = ffi.new("char[]", Z)
		self.spill = ""
		self.lines = []
	
	def _read(self):
		buflen = C.gzread(self.fh, self.cbuf, Z)
		buf = self.spill
		if buflen <= 0:
			self.spill = ""
			if buf:
				self.lines = [buf]
				return True
			return False
		else:
			buf += ffi.buffer(self.cbuf, buflen)[:]
		self.lines = buf.split("\n")
		self.spill = self.lines.pop()
		self.lines.reverse()
		return True
	
	def close(self):
		_gzclosemaybe_done.add(intptr(self.fh))
		C.gzclose(self.fh)
		self.fh = self.cbuf = None
	
	def __enter__(self):
		return self
	
	def __exit__(self, type, value, traceback):
		self.close()
	
	def __iter__(self):
		return self
	
	def next(self):
		while not self.lines:
			if not self._read():
				raise StopIteration
		return self.lines.pop()
