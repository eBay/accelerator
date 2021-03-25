/**
 * Copyright (c) 2017 eBay Inc.
 * Modifications copyright (c) 2018-2021 Carl Drougge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

#define PY_SSIZE_T_CLEAN 1
#include <Python.h>
#include <bytesobject.h>
#include <datetime.h>
#include <structmember.h>

#include <zlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>


// Choose some python number functions based on the size of long.
#if LONG_MAX == INT64_MAX
#  define pyLong_AsS64   PyInt_AsLong
#  define pyLong_AsU64   PyLong_AsUnsignedLong
#  define pyInt_FromS32  PyInt_FromLong
#  define pyInt_FromU32  PyInt_FromLong
#  define pyInt_FromS64  PyInt_FromLong
#  define pyInt_FromU64  PyLong_FromUnsignedLong
#  define py64T          long
#elif PY_LLONG_MAX == INT64_MAX
#  define pyLong_AsS64   PyLong_AsLongLong
#  define pyLong_AsU64   PyLong_AsUnsignedLongLong
#  define pyInt_FromS32  PyInt_FromLong
#  define pyInt_FromU32  PyLong_FromUnsignedLong
#  define pyInt_FromS64  PyLong_FromLongLong
#  define pyInt_FromU64  PyLong_FromUnsignedLongLong
#  define py64T          PY_LONG_LONG
#else
#  error "Unable to find a 64 bit type for PyLong*"
#endif
#if LONG_MAX == INT32_MAX
// If not these will be defined as functions further down
#  define pyLong_AsS32 PyInt_AsLong
#  define pyLong_AsU32 PyLong_AsUnsignedLong
#endif

// Must be a multiple of the largest fixed size type
#define Z (128 * 1024)

// Up to +-(2**1007 - 1). Don't increase this.
#define GZNUMBER_MAX_BYTES 127

#define BOM_STR "\xef\xbb\xbf"

#define err1(v) if (v) goto err

typedef struct gzread {
	PyObject_HEAD
	char *name;
	char *encoding;
	char *errors;
	PyObject *(*decodefunc)(const char *, Py_ssize_t, const char *);
	PyObject *hashfilter;
	PyObject *callback;
	PY_LONG_LONG want_count;
	PY_LONG_LONG count;
	PY_LONG_LONG break_count;
	PY_LONG_LONG callback_interval;
	PY_LONG_LONG callback_offset;
	uint64_t spread_None;
	gzFile fh;
	int error;
	int pos, len;
	unsigned int sliceno;
	unsigned int slices;
	char buf[Z + 1];
} GzRead;

#define FREE(p) do { PyMem_Free(p); (p) = 0; } while (0)

static int gzread_close_(GzRead *self)
{
	FREE(self->name);
	FREE(self->errors);
	FREE(self->encoding);
	Py_CLEAR(self->hashfilter);
	self->count = 0;
	self->want_count = -1;
	self->break_count = -1;
	Py_CLEAR(self->callback);
	self->callback_interval = 0;
	self->callback_offset = 0;
	if (self->fh) {
		gzclose(self->fh);
		self->fh = 0;
		return 0;
	}
	return 1;
}

#if PY_MAJOR_VERSION < 3
#  define BYTES_NAME      "str"
#  define UNICODE_NAME    "unicode"
#  define EITHER_NAME     "str or unicode"
#  define INITFUNC        initgzutil
#  define Integer_Check(o) (PyInt_Check(o) || PyLong_Check(o))
#else
#  define BYTES_NAME      "bytes"
#  define UNICODE_NAME    "str"
#  define EITHER_NAME     "str or bytes"
#  define PyInt_FromLong  PyLong_FromLong
#  define PyInt_AsLong    PyLong_AsLong
#  define PyNumber_Int    PyNumber_Long
#  define INITFUNC        PyInit_gzutil
#  define Integer_Check(o) PyLong_Check(o)
#endif

// Stupid forward declarations
static int gzread_read_(GzRead *self, int itemsize);
static PyTypeObject GzBytesLines_Type;
static PyTypeObject GzAsciiLines_Type;
static PyTypeObject GzUnicodeLines_Type;
static PyTypeObject GzWriteUnicodeLines_Type;
static PyTypeObject GzNumber_Type;
static PyTypeObject GzDateTime_Type;
static PyTypeObject GzDate_Type;
static PyTypeObject GzTime_Type;
static PyTypeObject GzBool_Type;

typedef Py_complex complex64;
typedef struct {
	float real, imag;
} complex32;

static const uint8_t hash_k[16] = {94, 70, 175, 255, 152, 30, 237, 97, 252, 125, 174, 76, 165, 112, 16, 9};

int siphash(uint8_t *out, const uint8_t *in, uint64_t inlen, const uint8_t *k);
static uint64_t hash(const void *ptr, const uint64_t len)
{
	uint64_t res;
	if (!len) return 0;
	siphash((uint8_t *)&res, ptr, len, hash_k);
	return res;
}
static uint64_t hash_datetime(const void *ptr)
{
	struct { uint32_t i0, i1; } tmp;
	memcpy(&tmp, ptr, 8);
	// ignore .fold, because python does.
	tmp.i0 &= 0xfffffff;
	return hash(&tmp, 8);
}
static uint64_t hash_32bits(const void *ptr)
{
	return hash(ptr, 4);
}
static uint64_t hash_bool(const void *ptr)
{
	return !!*(uint8_t *)ptr;
}
static uint64_t hash_integer(const void *ptr)
{
	uint64_t i = *(uint64_t *)ptr;
	if (!i) return 0;
	return hash(ptr, 8);
}
static uint64_t hash_double(const void *ptr)
{
	double  d = *(double *)ptr;
	int64_t i = d;
	if (i == d) return hash_integer(&i);
	return hash(&d, sizeof(d));
}
static uint64_t hash_complex64(const void *ptr)
{
	complex64 *p = (complex64 *)ptr;
	if (p->imag == 0.0) return hash_double(&p->real);
	return hash(p, sizeof(*p));
}
static uint64_t hash_complex32(const void *ptr)
{
	complex32 *p = (complex32 *)ptr;
	complex64 v64;
	v64.real = p->real;
	v64.imag = p->imag;
	return hash_complex64(&v64);
}

static int parse_hashfilter(PyObject *hashfilter, PyObject **r_hashfilter, unsigned int *r_sliceno, unsigned int *r_slices, uint64_t *r_spread_None)
{
	Py_CLEAR(*r_hashfilter);
	*r_slices = 0;
	*r_sliceno = 0;
	*r_spread_None = 0;
	if (!hashfilter || hashfilter == Py_None) return 0;
	int spread_None = 0;
	if (!PyArg_ParseTuple(hashfilter, "II|i", r_sliceno, r_slices, &spread_None)) {
		PyErr_Clear();
		PyErr_SetString(PyExc_ValueError, "hashfilter should be a tuple (sliceno, slices) or (sliceno, slices, spread_None)");
		return 1;
	}
	if (*r_slices == 0 || *r_sliceno >= *r_slices) {
		PyErr_Format(PyExc_ValueError, "Bad hashfilter (%d, %d)", *r_sliceno, *r_slices);
		return 1;
	}
	*r_spread_None = !!spread_None;
	*r_hashfilter = Py_BuildValue("(IIO)", *r_sliceno, *r_slices, spread_None ? Py_True : Py_False);
	return !*r_hashfilter;
}

static int gzread_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	int res = -1;
	GzRead *self = (GzRead *)self_;
	char *name = 0;
	int strip_bom = 0;
	int fd = -1;
	PY_LONG_LONG seek = 0;
	PyObject *hashfilter = 0;
	PyObject *callback = 0;
	PY_LONG_LONG callback_interval = 0;
	PY_LONG_LONG callback_offset = 0;
	gzread_close_(self);
	self->error = 0;
	if (self_->ob_type == &GzBytesLines_Type) {
		static char *kwlist[] = {"name", "strip_bom", "seek", "want_count", "hashfilter", "callback", "callback_interval", "callback_offset", "fd", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|iLLOOLLi", kwlist, Py_FileSystemDefaultEncoding, &name, &strip_bom, &seek, &self->want_count, &hashfilter, &callback, &callback_interval, &callback_offset, &fd)) return -1;
	} else if (self_->ob_type == &GzUnicodeLines_Type) {
		static char *kwlist[] = {"name", "encoding", "errors", "strip_bom", "seek", "want_count", "hashfilter", "callback", "callback_interval", "callback_offset", "fd", 0};
		char *errors = 0;
		char *encoding = 0;
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|etetiLLOOLLi", kwlist, Py_FileSystemDefaultEncoding, &name, "ascii", &encoding, "ascii", &errors, &strip_bom, &seek, &self->want_count, &hashfilter, &callback, &callback_interval, &callback_offset, &fd)) return -1;
		self->errors = errors;
		self->encoding = encoding;
	} else {
		static char *kwlist[] = {"name", "seek", "want_count", "hashfilter", "callback", "callback_interval", "callback_offset", "fd", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|LLOOLLi", kwlist, Py_FileSystemDefaultEncoding, &name, &seek, &self->want_count, &hashfilter, &callback, &callback_interval, &callback_offset, &fd)) return -1;
	}
	self->name = name;
	if (callback && callback != Py_None) {
		if (!PyCallable_Check(callback)) {
			PyErr_SetString(PyExc_ValueError, "callback must be callable");
			goto err;
		}
		if (callback_interval <= 0) {
			PyErr_SetString(PyExc_ValueError, "callback interval must be > 0");
			goto err;
		}
		Py_INCREF(callback);
		self->callback = callback;
		self->callback_interval = callback_interval;
		self->callback_offset = callback_offset;
	}
	if (fd == -1) {
		fd = open(self->name, O_RDONLY);
		if (fd < 0) {
			PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
			goto err;
		}
	}
	if (seek && lseek(fd, seek, 0) != seek) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	self->fh = gzdopen(fd, "rb");
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	fd = -1; // belongs to self->fh now
	unsigned int buf_kb = 64;
	if (self->want_count >= 0) {
		self->break_count = self->want_count;
		if (self->want_count < 100000) buf_kb = 16;
	}
	if (self->callback_interval > 0) {
		if (self->callback_interval < self->break_count || self->break_count < 0) {
			self->break_count = self->callback_interval;
		}
	}
	gzbuffer(self->fh, buf_kb * 1024);
	self->pos = self->len = 0;
	if (self_->ob_type == &GzAsciiLines_Type) {
		self->decodefunc = PyUnicode_DecodeASCII;
	}
	if (self_->ob_type == &GzUnicodeLines_Type) {
		if (self->encoding) {
			PyObject *decoder = PyCodec_Decoder(self->encoding);
			err1(!decoder);
			self->decodefunc = 0;
			PyObject *test_decoder;
			test_decoder = PyCodec_Decoder("utf-8");
			if (decoder == test_decoder) self->decodefunc = PyUnicode_DecodeUTF8;
			Py_XDECREF(test_decoder);
			test_decoder = PyCodec_Decoder("latin-1");
			if (decoder == test_decoder) self->decodefunc = PyUnicode_DecodeLatin1;
			Py_XDECREF(test_decoder);
			test_decoder = PyCodec_Decoder("ascii");
			if (decoder == test_decoder) self->decodefunc = PyUnicode_DecodeASCII;
			Py_XDECREF(test_decoder);
			Py_DECREF(decoder);
			if (!self->decodefunc) {
				PyErr_Format(PyExc_LookupError, "Unsupported encoding '%s'", self->encoding);
				goto err;
			}
		} else {
			self->decodefunc = PyUnicode_DecodeUTF8;
			self->encoding = PyMem_Malloc(6);
			strcpy(self->encoding, "utf-8");
		}
	}
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	gzread_read_(self, 8);
	if (strip_bom) {
		if (self->len >= 3 && !memcmp(self->buf, BOM_STR, 3)) {
			self->pos = 3;
		}
	}
	res = 0;
err:
	if (fd >= 0) close(fd);
	if (res) {
		gzread_close_(self);
		self->error = 1;
	}
	return res;
}

static void gzread_dealloc(GzRead *self)
{
	gzread_close_(self);
	PyObject_Del(self);
}

static PyObject *err_closed(void)
{
	PyErr_SetString(PyExc_ValueError, "I/O operation on closed file");
	return 0;
}

static PyObject *gzread_close(GzRead *self)
{
	if (gzread_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *gzread_self(GzRead *self)
{
	if (!self->fh) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static int gzread_read_(GzRead *self, int itemsize)
{
	if (!self->error) {
		unsigned len = Z;
		if (self->want_count >= 0) {
			PY_LONG_LONG count_left = self->want_count - self->count;
			PY_LONG_LONG candidate = count_left * itemsize + itemsize;
			if (candidate < len) len = candidate;
		}
		self->len = gzread(self->fh, self->buf, len);
		if (self->len <= 0) {
			(void) gzerror(self->fh, &self->error);
		}
	}
	if (self->error) {
		PyErr_SetString(PyExc_ValueError, "File format error");
		return 1;
	}
	if (self->len <= 0) {
		if (self->want_count >= 0 && self->want_count != self->count) {
			PyErr_Format(PyExc_ValueError, "\"%s\" ended after %lld items, expected %lld", self->name, self->count, self->want_count);
		}
		return 1;
	}
	self->buf[self->len] = 0;
	self->pos = 0;
	return 0;
}

// Likely item size to avoid over-read. (Exact when possible)
#define SIZE_Bytes    20
#define SIZE_Ascii    20
#define SIZE_Unicode  20
#define SIZE_Number   9
#define SIZE_DateTime 8
#define SIZE_Time     8
#define SIZE_Date     4
#define SIZE_complex64 16
#define SIZE_complex32 8
#define SIZE_double   8
#define SIZE_float    4
#define SIZE_int64_t  8
#define SIZE_int32_t  4
#define SIZE_uint64_t 8
#define SIZE_uint32_t 4
#define SIZE_uint8_t  1

static inline int do_callback(GzRead *self)
{
	PyObject *res = PyObject_CallFunction(self->callback, "L", self->count + self->callback_offset);
	if (res) {
		Py_DECREF(res);
		PY_LONG_LONG bc = self->break_count + self->callback_interval;
		if (self->want_count > 0 && bc > self->want_count) {
			bc = self->want_count;
		}
		self->break_count = bc;
		return 0;
	} else {
		PyObject *extype = PyErr_Occurred();
		if (!extype) {
			PyErr_SetString(PyExc_ValueError, "Callback error");
		} else if (PyErr_GivenExceptionMatches(extype, PyExc_StopIteration)) {
			PyErr_Clear();
		}
		return 1;
	}
}

#define ITERPROLOGUE(typename)                               	\
	do {                                                 	\
		if (!self->fh) return err_closed();          	\
		if (self->count == self->break_count) {      	\
			if (self->count == self->want_count) {	\
				return 0;                    	\
			}                                    	\
			if (do_callback(self)) {             	\
				return 0;                    	\
			}                                    	\
		}                                            	\
		if (self->error || self->pos >= self->len) { 	\
			if (gzread_read_(self, SIZE_ ## typename)) return 0; \
		}                                            	\
		self->count++;                               	\
	} while (0)

#define HC_RETURN_NONE do {                                                  	\
	if (self->slices) {                                                  	\
		if (self->spread_None) {                                     	\
			if (self->spread_None++ % self->slices == self->sliceno) {\
				Py_RETURN_TRUE;                              	\
			} else {                                             	\
				Py_RETURN_FALSE;                             	\
			}                                                    	\
		} else if (self->sliceno) {                                  	\
			Py_RETURN_FALSE;                                     	\
		} else {                                                     	\
			Py_RETURN_TRUE;                                      	\
		}                                                            	\
	} else {                                                             	\
		Py_RETURN_NONE;                                              	\
	}                                                                    	\
} while(0)

#define HC_CHECK(hash) do {                                  	\
	if (self->slices) {                                  	\
		if (hash % self->slices == self->sliceno) {  	\
			Py_RETURN_TRUE;                      	\
		} else {                                     	\
			Py_RETURN_FALSE;                     	\
		}                                            	\
	}                                                    	\
} while(0)

static inline PyObject *mkBytes(GzRead *self, const char *ptr, int len)
{
	if (len == 1 && *ptr == 0) {
		HC_RETURN_NONE;
	}
	if (len && ptr[len - 1] == '\r') len--;
	HC_CHECK(hash(ptr, len));
	return PyBytes_FromStringAndSize(ptr, len);
}
static inline PyObject *mkUnicode(GzRead *self, const char *ptr, int len)
{
	if (len == 1 && *ptr == 0) {
		HC_RETURN_NONE;
	}
	if (len && ptr[len - 1] == '\r') len--;
	HC_CHECK(hash(ptr, len));
	return self->decodefunc(ptr, len, self->errors);
}

#define MKLINEITER(name, typename) \
	static PyObject *name ## _iternext(GzRead *self)                                 	\
	{                                                                                	\
		ITERPROLOGUE(typename);                                                  	\
		char *ptr = self->buf + self->pos;                                       	\
		char *end = memchr(ptr, '\n', self->len - self->pos);                    	\
		if (!end) {                                                              	\
			int linelen = self->len - self->pos;                             	\
			char line[Z + linelen];                                          	\
			memcpy(line, self->buf + self->pos, linelen);                    	\
			if (gzread_read_(self, SIZE_ ## typename)) {                     	\
				if (self->error) return 0;                                      \
				return mk ## typename(self, line, linelen);              	\
			}                                                                	\
			end = memchr(self->buf, '\n', self->len);                        	\
			if (!end) {                                                      	\
				size_t longlen = linelen + self->len;                    	\
				char *longbuf = malloc(longlen);                         	\
				if (!longbuf) return PyErr_NoMemory();                   	\
				memcpy(longbuf, line, linelen);                          	\
				memcpy(longbuf + linelen, self->buf, self->len);         	\
				while (1) {                                              	\
					if (gzread_read_(self, SIZE_ ## typename)) break;	\
					int copylen = self->len;                         	\
					char *end = memchr(self->buf, '\n', copylen);    	\
					if (end) copylen = end - self->buf;              	\
					char *tmp = realloc(longbuf, longlen + copylen); 	\
					if (!tmp) {                                      	\
						free(longbuf);                           	\
						return PyErr_NoMemory();                 	\
					}                                                	\
					longbuf = tmp;                                   	\
					memcpy(longbuf + longlen, self->buf, copylen);   	\
					longlen += copylen;                              	\
					self->pos = copylen + 1;                         	\
					if (end) break;                                  	\
				}                                                        	\
				if (self->error) {                                              \
					free(longbuf);                                          \
					return 0;                                               \
				}                                                               \
				PyObject *res = mk ## typename(self, longbuf, longlen);  	\
				free(longbuf);                                           	\
				return res;                                              	\
			}                                                                	\
			self->pos = end - self->buf + 1;                                 	\
			memcpy(line + linelen, self->buf, self->pos - 1);                	\
			return mk ## typename(self, line, linelen + self->pos - 1);      	\
		}                                                                        	\
		int linelen = end - ptr;                                                 	\
		self->pos += linelen + 1;                                                	\
		return mk ## typename(self, ptr, linelen);                               	\
	}
MKLINEITER(GzBytesLines  , Bytes);
#if PY_MAJOR_VERSION < 3
#  define GzAsciiLines_iternext GzBytesLines_iternext
#else
#  define GzAsciiLines_iternext GzUnicodeLines_iternext
#endif
MKLINEITER(GzUnicodeLines, Unicode);

#define MKmkBlob(name, decoder) \
	static inline PyObject *mkblob ## name(GzRead *self, const char *ptr, int len)   	\
	{                                                                                	\
		HC_CHECK(hash(ptr, len));                                                	\
		return decoder;                                                          	\
	}
MKmkBlob(Bytes  , PyBytes_FromStringAndSize(ptr, len))
MKmkBlob(Unicode, PyUnicode_DecodeUTF8(ptr, len, 0))
#if PY_MAJOR_VERSION < 3
  MKmkBlob(Ascii, PyBytes_FromStringAndSize(ptr, len))
#else
  MKmkBlob(Ascii, PyUnicode_DecodeASCII(ptr, len, 0))
#endif

#define MKBLOBITER(name, typename) \
	static PyObject *name ## _iternext(GzRead *self)                                 	\
	{                                                                                	\
		ITERPROLOGUE(typename);                                                  	\
		uint32_t size = ((uint8_t *)self->buf)[self->pos];                       	\
		self->pos++;                                                             	\
		char *ptr = self->buf + self->pos;                                       	\
		uint32_t left_in_buf = self->len - self->pos;                            	\
		if (!left_in_buf && size) {                                              	\
			if (gzread_read_(self, SIZE_ ## typename)) goto fferror;         	\
			left_in_buf = self->len;                                         	\
			ptr = self->buf;                                                 	\
		}                                                                        	\
		if (size == 255) {                                                       	\
			/* Special case - more than 254 or NUL. */                       	\
			if (left_in_buf < 4) { /* sigh.. */                              	\
				char *size_ptr = (char *)&size;                          	\
				int need_more = 4 - left_in_buf;                         	\
				memcpy(size_ptr, ptr, left_in_buf);                      	\
				size_ptr += left_in_buf;                                 	\
				if (gzread_read_(self, SIZE_ ## typename)) goto fferror; 	\
				if (self->len < need_more) goto fferror;                 	\
				memcpy(size_ptr, self->buf, need_more);                  	\
				self->pos = need_more;                                   	\
			} else {                                                         	\
				memcpy(&size, ptr, 4);                                   	\
				self->pos += 4;                                          	\
			}                                                                	\
			if (size == 0) {                                                 	\
				/* Special case - 0 as long len means NUL */             	\
				HC_RETURN_NONE;                                          	\
			}                                                                	\
			if (size < 255) { /* Should have had short length */             	\
				goto fferror;                                            	\
			}                                                                	\
			ptr = self->buf + self->pos;                                     	\
			left_in_buf = self->len - self->pos;                             	\
		}                                                                        	\
		if (size > Z) {                                                          	\
			char *tmp = malloc(size);                                        	\
			if (!tmp) {                                                      	\
				return PyErr_NoMemory();                                 	\
			}                                                                	\
			memcpy(tmp, ptr, left_in_buf);                                   	\
			self->pos = self->len;                                           	\
			const int want_len = size - left_in_buf;                         	\
			int read_len = gzread(self->fh, tmp + left_in_buf, want_len);    	\
			if (read_len != want_len) {                                      	\
				free(tmp);                                               	\
				(void) gzerror(self->fh, &self->error);                  	\
				goto fferror;                                            	\
			}                                                                	\
			PyObject *res = mkblob ## typename(self, tmp, size);             	\
			free(tmp);                                                       	\
			return res;                                                      	\
		}                                                                        	\
		if (size > left_in_buf) {                                                	\
			memmove(self->buf, ptr, left_in_buf);                            	\
			ptr = self->buf + left_in_buf;                                   	\
			int read_len = gzread(self->fh, ptr, Z - left_in_buf);           	\
			if (read_len <= 0) {                                             	\
				(void) gzerror(self->fh, &self->error);                  	\
				goto fferror;                                            	\
			}                                                                	\
			if (read_len + left_in_buf < size) goto fferror;                 	\
			self->len = read_len + left_in_buf;                              	\
			self->pos = 0;                                                   	\
			ptr = self->buf;                                                 	\
		}                                                                        	\
		self->pos += size;                                                       	\
		return mkblob ## typename(self, ptr, size);                              	\
fferror:                                                                                 	\
		PyErr_SetString(PyExc_ValueError, "File format error");                  	\
		return 0;                                                                	\
	}
MKBLOBITER(GzBytes  , Bytes);
MKBLOBITER(GzAscii  , Ascii);
MKBLOBITER(GzUnicode, Unicode);


// These are signaling NaNs with extra DEADness in the significand
static unsigned char noneval_double[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
static unsigned char noneval_float[4] = {0xde, 0xad, 0x80, 0xff};
static unsigned char noneval_complex64[16] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff, 0, 0, 0, 0, 0, 0, 0, 0};
static unsigned char noneval_complex32[8] = {0xde, 0xad, 0x80, 0xff, 0, 0, 0, 0};
static const unsigned char BE_noneval_double[8] = {0xff, 0xf0, 0xde, 0xad, 0xde, 0xad, 0xde, 0xad};
static const unsigned char BE_noneval_float[4] = {0xff, 0x80, 0xde, 0xad};
static const unsigned char BE_noneval_complex64[16] = {0xff, 0xf0, 0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0, 0, 0, 0, 0, 0, 0, 0};
static const unsigned char BE_noneval_complex32[8] = {0xff, 0x80, 0xde, 0xad, 0, 0, 0, 0};

// The smallest value is one less than -biggest, so that seems like a good signal value.
static const int64_t noneval_int64_t = INT64_MIN;
static const int32_t noneval_int32_t = INT32_MIN;

// The uint types don't support None, but the datetime types do.
static const uint64_t noneval_uint64_t = 0;
static const uint32_t noneval_uint32_t = 0;

// This is bool
static const uint8_t noneval_uint8_t = 255;

#define MKITER(name, T, conv, hash, HT, withnone)                            	\
	static PyObject * name ## _iternext(GzRead *self)                    	\
	{                                                                    	\
		ITERPROLOGUE(T);                                             	\
		/* Z is a multiple of sizeof(T), so this never overruns. */  	\
		const char *ptr = self->buf + self->pos;                     	\
		self->pos += sizeof(T);                                      	\
		if (withnone && !memcmp(ptr, &noneval_ ## T, sizeof(T))) {   	\
			HC_RETURN_NONE;                                      	\
		}                                                            	\
		T res;                                                       	\
		memcpy(&res, ptr, sizeof(T));                                	\
		if (self->slices) {                                          	\
			HT v = res;                                          	\
			HC_CHECK(hash(&v));                                  	\
		}                                                            	\
		return conv(res);                                            	\
	}

static PyObject *pyComplex_From32(complex32 v)
{
	return PyComplex_FromDoubles(v.real, v.imag);
}

MKITER(GzComplex64, complex64, PyComplex_FromCComplex, hash_complex64, complex64, 1)
MKITER(GzComplex32, complex32, pyComplex_From32      , hash_complex32, complex32, 1)
MKITER(GzFloat64, double  , PyFloat_FromDouble     , hash_double , double  , 1)
MKITER(GzFloat32, float   , PyFloat_FromDouble     , hash_double , double  , 1)
MKITER(GzInt64  , int64_t , pyInt_FromS64          , hash_integer, int64_t , 1)
MKITER(GzInt32  , int32_t , pyInt_FromS32          , hash_integer, int64_t , 1)
MKITER(GzBits64 , uint64_t, pyInt_FromU64          , hash_integer, uint64_t, 0)
MKITER(GzBits32 , uint32_t, pyInt_FromU32          , hash_integer, uint64_t, 0)
MKITER(GzBool   , uint8_t , PyBool_FromLong        , hash_bool   , uint8_t , 1)

static PyObject *GzNumber_iternext(GzRead *self)
{
	ITERPROLOGUE(Number);
	int is_float = 0;
	int len = self->buf[self->pos];
	self->pos++;
	if (!len) HC_RETURN_NONE;
	if (len == 1) {
		len = 8;
		is_float = 1;
	}
	if (len >= GZNUMBER_MAX_BYTES || len < 8) {
		PyErr_SetString(PyExc_ValueError, "File format error");
		return 0;
	}
	unsigned char buf[GZNUMBER_MAX_BYTES];
	const int avail = self->len - self->pos;
	if (avail >= len) {
		memcpy(buf, self->buf + self->pos, len);
		self->pos += len;
	} else {
		memcpy(buf, self->buf + self->pos, avail);
		unsigned char * const ptr = buf + avail;
		const int morelen = len - avail;
		if (gzread_read_(self, GZNUMBER_MAX_BYTES) || morelen > self->len) {
			self->error = 1;
			PyErr_SetString(PyExc_ValueError, "File format error");
			return 0;
		}
		memcpy(ptr, self->buf, morelen);
		self->pos = morelen;
	}
	if (is_float) {
		double v;
		memcpy(&v, buf, sizeof(v));
		HC_CHECK(hash_double(&v));
		return PyFloat_FromDouble(v);
	}
	if (len == 8) {
		int64_t v;
		memcpy(&v, buf, sizeof(v));
		HC_CHECK(hash_integer(&v));
		return pyInt_FromS64(v);
	}
	HC_CHECK(hash(buf, len));
	return _PyLong_FromByteArray(buf, len, 1, 1);
}

static inline PyObject *unfmt_datetime(const uint32_t i0, const uint32_t i1)
{
	if (!i0) Py_RETURN_NONE;
	const int Y = i0 >> 14 & 0x2fff;
	const int m = i0 >> 10 & 0x0f;
	const int d = i0 >> 5 & 0x1f;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
#if PY_VERSION_HEX >= 0x03060000
	const int fold = !!(i0 & 0x10000000);
	return PyDateTime_FromDateAndTimeAndFold(Y, m, d, H, M, S, u, fold);
#else
	return PyDateTime_FromDateAndTime(Y, m, d, H, M, S, u);
#endif
}

static PyObject *GzDateTime_iternext(GzRead *self)
{
	ITERPROLOGUE(DateTime);
	/* Z is a multiple of 8, so this never overruns. */
	uint32_t a[2];
	memcpy(a, self->buf + self->pos, 8);
	self->pos += 8;
	if (!a[0]) HC_RETURN_NONE;
	HC_CHECK(hash_datetime(self->buf + self->pos - 8));
	return unfmt_datetime(a[0], a[1]);
}

static inline PyObject *unfmt_date(const uint32_t i0)
{
	if (!i0) Py_RETURN_NONE;
	const int Y = i0 >> 9;
	const int m = i0 >> 5 & 0x0f;
	const int d = i0 & 0x1f;
	return PyDate_FromDate(Y, m, d);
}

static PyObject *GzDate_iternext(GzRead *self)
{
	ITERPROLOGUE(Date);
	/* Z is a multiple of 4, so this never overruns. */
	uint32_t i0;
	memcpy(&i0, self->buf + self->pos, 4);
	self->pos += 4;
	if (!i0) HC_RETURN_NONE;
	HC_CHECK(hash_32bits(self->buf + self->pos - 4));
	return unfmt_date(i0);
}

static inline PyObject *unfmt_time(const uint32_t i0, const uint32_t i1)
{
	if (!i0) Py_RETURN_NONE;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
#if PY_VERSION_HEX >= 0x03060000
	const int fold = !!(i0 & 0x10000000);
	return PyTime_FromTimeAndFold(H, M, S, u, fold);
#else
	return PyTime_FromTime(H, M, S, u);
#endif
}

static PyObject *GzTime_iternext(GzRead *self)
{
	ITERPROLOGUE(Time);
	/* Z is a multiple of 8, so this never overruns. */
	uint32_t a[2];
	memcpy(a, self->buf + self->pos, 8);
	self->pos += 8;
	if (!a[0]) HC_RETURN_NONE;
	HC_CHECK(hash_datetime(self->buf + self->pos - 8));
	return unfmt_time(a[0], a[1]);
}

static PyObject *gzany_exit(PyObject *self, PyObject *args)
{
	PyObject *ret = PyObject_CallMethod(self, "close", NULL);
	if (!ret) return 0;
	Py_DECREF(ret);
	Py_RETURN_NONE;
}

static PyMethodDef gzread_methods[] = {
	{"__enter__", (PyCFunction)gzread_self, METH_NOARGS,  NULL},
	{"__exit__",  (PyCFunction)gzany_exit, METH_VARARGS, NULL},
	{"close",     (PyCFunction)gzread_close, METH_NOARGS,  NULL},
	{NULL, NULL, 0, NULL}
};

#define MKTYPE(name, members)                                        	\
	static PyTypeObject name ## _Type = {                        	\
		PyVarObject_HEAD_INIT(NULL, 0)                       	\
		#name,                          /*tp_name          */	\
		sizeof(GzRead),                 /*tp_basicsize     */	\
		0,                              /*tp_itemsize      */	\
		(destructor)gzread_dealloc,     /*tp_dealloc       */	\
		0,                              /*tp_print         */	\
		0,                              /*tp_getattr       */	\
		0,                              /*tp_setattr       */	\
		0,                              /*tp_compare       */	\
		0,                              /*tp_repr          */	\
		0,                              /*tp_as_number     */	\
		0,                              /*tp_as_sequence   */	\
		0,                              /*tp_as_mapping    */	\
		0,                              /*tp_hash          */	\
		0,                              /*tp_call          */	\
		0,                              /*tp_str           */	\
		0,                              /*tp_getattro      */	\
		0,                              /*tp_setattro      */	\
		0,                              /*tp_as_buffer     */	\
		Py_TPFLAGS_DEFAULT,             /*tp_flags         */	\
		0,                              /*tp_doc           */	\
		0,                              /*tp_traverse      */	\
		0,                              /*tp_clear         */	\
		0,                              /*tp_richcompare   */	\
		0,                              /*tp_weaklistoffset*/	\
		(getiterfunc)gzread_self,       /*tp_iter          */	\
		(iternextfunc)name ## _iternext,/*tp_iternext      */	\
		gzread_methods,                 /*tp_methods       */	\
		members,                        /*tp_members       */	\
		0,                              /*tp_getset        */	\
		0,                              /*tp_base          */	\
		0,                              /*tp_dict          */	\
		0,                              /*tp_descr_get     */	\
		0,                              /*tp_descr_set     */	\
		0,                              /*tp_dictoffset    */	\
		gzread_init,                    /*tp_init          */	\
		PyType_GenericAlloc,            /*tp_alloc         */	\
		PyType_GenericNew,              /*tp_new           */	\
		PyObject_Del,                   /*tp_free          */	\
		0,                              /*tp_is_gc         */	\
	}
static PyMemberDef r_default_members[] = {
	{"name"      , T_STRING   , offsetof(GzRead, name       ), READONLY},
	{"hashfilter", T_OBJECT_EX, offsetof(GzRead, hashfilter ), READONLY},
	{0}
};
static PyMemberDef r_unicode_members[] = {
	{"name"      , T_STRING   , offsetof(GzRead, name       ), READONLY},
	{"hashfilter", T_OBJECT_EX, offsetof(GzRead, hashfilter ), READONLY},
	{"encoding"  , T_STRING   , offsetof(GzRead, encoding   ), READONLY},
	{"errors"    , T_STRING   , offsetof(GzRead, errors     ), READONLY},
	{0}
};
MKTYPE(GzBytes, r_default_members);
MKTYPE(GzAscii, r_default_members);
MKTYPE(GzUnicode, r_default_members);
MKTYPE(GzBytesLines, r_default_members);
MKTYPE(GzAsciiLines, r_default_members);
MKTYPE(GzUnicodeLines, r_unicode_members);
MKTYPE(GzNumber, r_default_members);
MKTYPE(GzComplex64, r_default_members);
MKTYPE(GzComplex32, r_default_members);
MKTYPE(GzFloat64, r_default_members);
MKTYPE(GzFloat32, r_default_members);
MKTYPE(GzInt64, r_default_members);
MKTYPE(GzInt32, r_default_members);
MKTYPE(GzBits64, r_default_members);
MKTYPE(GzBits32, r_default_members);
MKTYPE(GzBool, r_default_members);
MKTYPE(GzDateTime, r_default_members);
MKTYPE(GzDate, r_default_members);
MKTYPE(GzTime, r_default_members);


typedef union {
	double   as_double;
	float    as_float;
	int32_t  as_int32_t;
	int64_t  as_int64_t;
	uint8_t  as_uint8_t;
	uint32_t as_uint32_t;
	uint64_t as_uint64_t;
} minmax_u;

// Same thing but with the larger complex64 type which isn't needed in minmax.
typedef union {
	double   as_double;
	float    as_float;
	int32_t  as_int32_t;
	int64_t  as_int64_t;
	uint8_t  as_uint8_t;
	uint32_t as_uint32_t;
	uint64_t as_uint64_t;
	complex32 as_complex32;
	complex64 as_complex64;
} default_u;

typedef struct gzwrite {
	PyObject_HEAD
	gzFile fh;
	char *name;
	default_u *default_value;
	unsigned PY_LONG_LONG count;
	PyObject *hashfilter;
	PyObject *default_obj;
	PyObject *min_obj;
	PyObject *max_obj;
	minmax_u min_u;
	minmax_u max_u;
	uint64_t spread_None;
	unsigned int sliceno;
	unsigned int slices;
	int closed;
	int none_support;
	int len;
	char mode[4];
	char buf[Z];
} GzWrite;

static int gzwrite_ensure_open(GzWrite *self)
{
	if (self->fh) return 0;
	if (self->closed) {
		(void) err_closed();
		return 1;
	}
	self->fh = gzopen(self->name, self->mode);
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		return 1;
	}
	return 0;
}

static int gzwrite_flush_(GzWrite *self)
{
	if (!self->len) return 0;
	if (gzwrite_ensure_open(self)) return 1;
	const int len = self->len;
	self->len = 0;
	if (gzwrite(self->fh, self->buf, len) != len) {
		PyErr_SetString(PyExc_IOError, "Write failed");
		return 1;
	}
	return 0;
}

static PyObject *gzwrite_flush(GzWrite *self)
{
	if (gzwrite_ensure_open(self)) return 0;
	if (gzwrite_flush_(self)) return 0;
	Py_RETURN_NONE;
}

static int gzwrite_close_(GzWrite *self)
{
	if (self->default_value) {
		free(self->default_value);
		self->default_value = 0;
	}
	FREE(self->name);
	Py_CLEAR(self->hashfilter);
	Py_CLEAR(self->default_obj);
	Py_CLEAR(self->min_obj);
	Py_CLEAR(self->max_obj);
	if (self->closed) return 1;
	if (!self->fh) return 0;
	int err = gzwrite_flush_(self);
	err |= gzclose(self->fh);
	self->fh = 0;
	self->closed = 1;
	return err;
}

// Make sure mode matches [wa]b?(\d.?)?
static int mode_fixup(const char * const mode, char buf[static 4])
{
	const char *modeptr;
	if (mode && *mode) {
		modeptr = mode;
	} else {
		modeptr = "w";
	}
	if (modeptr[0] != 'w' && modeptr[0] != 'a') goto bad;
	*(buf++) = *(modeptr++);
	if (*modeptr == 'b') modeptr++;
	if (strlen(modeptr) > 2) goto bad;
	if (*modeptr && (*modeptr < '0' || *modeptr > '9')) goto bad;
	strcpy(buf, modeptr);
	return 0;
bad:
	PyErr_Format(PyExc_IOError, "Bad mode '%s'", mode);
	return 1;
}

static int gzwrite_init_GzWrite(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", 0};
	GzWrite *self = (GzWrite *)self_;
	char *name = 0;
	const char *mode = 0;
	gzwrite_close_(self);
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|s", kwlist, Py_FileSystemDefaultEncoding, &name, &mode)) return -1;
	self->name = name;
	err1(mode_fixup(mode, self->mode));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

static int gzwrite_init_GzWriteLines(PyObject *self_, PyObject *args, PyObject *kwds)
{
	GzWrite *self = (GzWrite *)self_;
	char *name = 0;
	const char *mode = 0;
	PyObject *hashfilter = 0;
	int write_bom = 0;
	gzwrite_close_(self);
	if (self_->ob_type == &GzWriteUnicodeLines_Type) {
		static char *kwlist[] = {"name", "mode", "hashfilter", "none_support", "write_bom", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOii", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &hashfilter, &self->none_support, &write_bom)) return -1;
	} else {
		static char *kwlist[] = {"name", "mode", "hashfilter", "none_support", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOi", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &hashfilter, &self->none_support)) return -1;
	}
	self->name = name;
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	err1(mode_fixup(mode, self->mode));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	if (write_bom) {
		memcpy(self->buf, BOM_STR, 3);
		self->len = 3;
	}
	return 0;
err:
	return -1;
}
#define gzwrite_init_GzWriteBytesLines   gzwrite_init_GzWriteLines
#define gzwrite_init_GzWriteAsciiLines   gzwrite_init_GzWriteLines
#define gzwrite_init_GzWriteUnicodeLines gzwrite_init_GzWriteLines

static int gzwrite_init_GzWriteBlob(PyObject *self_, PyObject *args, PyObject *kwds)
{
	GzWrite *self = (GzWrite *)self_;
	char *name = 0;
	const char *mode = 0;
	PyObject *hashfilter = 0;
	gzwrite_close_(self);
	static char *kwlist[] = {"name", "mode", "hashfilter", "none_support", 0};
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOi", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &hashfilter, &self->none_support)) return -1;
	self->name = name;
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	err1(mode_fixup(mode, self->mode));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

#define gzwrite_init_GzWriteBytes   gzwrite_init_GzWriteBlob
#define gzwrite_init_GzWriteAscii   gzwrite_init_GzWriteBlob
#define gzwrite_init_GzWriteUnicode gzwrite_init_GzWriteBlob

static void gzwrite_dealloc(GzWrite *self)
{
	gzwrite_close_(self);
	PyObject_Del(self);
}

static PyObject *gzwrite_close(GzWrite *self)
{
	if (gzwrite_flush_(self)) return 0;
	if (gzwrite_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *gzwrite_self(GzWrite *self)
{
	if (self->closed) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static PyObject *gzwrite_write_(GzWrite *self, const char *data, Py_ssize_t len)
{
	if (len + self->len > Z) {
		if (gzwrite_flush_(self)) return 0;
	}
	while (len > Z) {
		if (gzwrite(self->fh, data, Z) != Z) {
			PyErr_SetString(PyExc_IOError, "Write failed");
			return 0;
		}
		len -= Z;
		data += Z;
	}
	memcpy(self->buf + self->len, data, len);
	self->len += len;
	Py_RETURN_TRUE;
}

static PyObject *gzwrite_write_GzWrite(GzWrite *self, PyObject *obj)
{
	if (!PyBytes_Check(obj)) {
		PyErr_SetString(PyExc_ValueError, "Only " BYTES_NAME " can be written");
		return 0;
	}
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	return gzwrite_write_(self, data, len);
}

#define WRITE_NONE_SLICE_CHECK do {                                                   	\
	if (!self->none_support) {                                                    	\
		PyErr_SetString(PyExc_ValueError, "Refusing to write None value without none_support=True"); \
		return 0;                                                             	\
	}                                                                             	\
	if (self->spread_None) {                                                      	\
		const unsigned int spread_slice = self->spread_None % self->slices;   	\
		if (actually_write) self->spread_None++;                              	\
		if (spread_slice != self->sliceno) {                                  	\
			Py_RETURN_FALSE;                                              	\
		}                                                                     	\
	} else if (self->sliceno) {                                                   	\
		Py_RETURN_FALSE;                                                      	\
	}                                                                             	\
	if (!actually_write) Py_RETURN_TRUE;                                          	\
} while (0)

#define WRITELINEPROLOGUE(checktype, errname) \
	if (obj == Py_None) {                                                         	\
		WRITE_NONE_SLICE_CHECK;                                               	\
		self->count++;                                                        	\
		return gzwrite_write_(self, "\x00\n", 2);                             	\
	}                                                                             	\
	if (checktype) {                                                              	\
		PyErr_Format(PyExc_TypeError,                                         	\
		             "For your protection, only " errname                     	\
		             " objects are accepted (line %llu)",                     	\
		             (unsigned long long) self->count + 1);                   	\
		return 0;                                                             	\
	}
#define WRITELINEDO(cleanup) \
	if (len == 1 && *data == 0) {                                                 	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value becomes None-marker (line %llu)",                 	\
		             (unsigned long long) self->count + 1);                   	\
		return 0;                                                             	\
	}                                                                             	\
	if (memchr(data, '\n', len)) {                                                	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value must not contain \\n (line %llu)",                	\
		             (unsigned long long) self->count + 1);                   	\
		return 0;                                                             	\
	}                                                                             	\
	if (data[len - 1] == '\r') {                                                  	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value must not end with \\r (line %llu)",               	\
		             (unsigned long long) self->count + 1);                   	\
		return 0;                                                             	\
	}                                                                             	\
	if (self->slices) {                                                           	\
		if (hash(data, len) % self->slices != self->sliceno) {                	\
			cleanup;                                                      	\
			Py_RETURN_FALSE;                                              	\
		}                                                                     	\
	}                                                                             	\
	if (!actually_write) {                                                        	\
		cleanup;                                                              	\
		Py_RETURN_TRUE;                                                       	\
	}                                                                             	\
	PyObject *ret = gzwrite_write_(self, data, len);                              	\
	cleanup;                                                                      	\
	if (!ret) return 0;                                                           	\
	Py_DECREF(ret);                                                               	\
	self->count++;                                                                	\
	return gzwrite_write_(self, "\n", 1);
#define ASCIIVERIFY(cleanup) \
	const unsigned char * const data_ = (unsigned char *)data;                    	\
	for (Py_ssize_t i = 0; i < len; i++) {                                        	\
		if (data_[i] > 127) {                                                 	\
			cleanup;                                                      	\
			if (len < 1000) {                                             	\
				PyErr_Format(PyExc_ValueError,                        	\
				             "Value contains %d at position %ld: %s", 	\
				             data_[i], (long) i, data);               	\
			} else {                                                      	\
				PyErr_Format(PyExc_ValueError,                        	\
				             "Value contains %d at position %ld.",    	\
				             data_[i], (long) i);                     	\
			}                                                             	\
			return 0;                                                     	\
		}                                                                     	\
	}
#define ASCIILINEDO(cleanup) \
	ASCIIVERIFY(cleanup);                                                         	\
	WRITELINEDO(cleanup);
#define ASCIIHASHDO(cleanup) \
	ASCIIVERIFY(cleanup);                                                         	\
	HASHLINEDO(cleanup);

#if PY_MAJOR_VERSION < 3
#  define UNICODELINE(WRITEMACRO) \
	PyObject *strobj = PyUnicode_AsUTF8String(obj);              	\
	if (!strobj) return 0;                                       	\
	const char *data = PyBytes_AS_STRING(strobj);                	\
	const Py_ssize_t len = PyBytes_GET_SIZE(strobj);             	\
	WRITEMACRO(Py_DECREF(strobj));
#else
#  define UNICODELINE(WRITEMACRO) \
	Py_ssize_t len;                                              	\
	const char *data = PyUnicode_AsUTF8AndSize(obj, &len);       	\
	if (!data) return 0;                                         	\
	WRITEMACRO((void)data);
#endif

#define HASHLINEPROLOGUE(checktype, errname) \
	if (obj == Py_None) return PyInt_FromLong(0);                                         	\
	if (checktype) {                                                                      	\
		PyErr_SetString(PyExc_TypeError,                                              	\
		                "For your protection, only " errname " objects are accepted");	\
		return 0;                                                                     	\
	}
#define HASHLINEDO(cleanup) \
	PyObject *res = pyInt_FromU64(hash(data, len));              	\
	cleanup;                                                     	\
	return res;

static PyObject *gzwrite_C_GzWriteBytesLines(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITELINEPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	WRITELINEDO((void)data);
}
static PyObject *gzwrite_hash_GzWriteBytesLines(PyObject *dummy, PyObject *obj)
{
	HASHLINEPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	HASHLINEDO((void)data);
}

static PyObject *gzwrite_C_GzWriteAsciiLines(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITELINEPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	if (PyBytes_Check(obj)) {
		const Py_ssize_t len = PyBytes_GET_SIZE(obj);
		const char *data = PyBytes_AS_STRING(obj);
		ASCIILINEDO((void)data);
	} else { // Must be Unicode
		UNICODELINE(ASCIILINEDO);
	}
}
static PyObject *gzwrite_hash_GzWriteAsciiLines(PyObject *dummy, PyObject *obj)
{
	HASHLINEPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	Py_ssize_t len;
	const char *data;
	if (PyBytes_Check(obj)) {
		len = PyBytes_GET_SIZE(obj);
		data = PyBytes_AS_STRING(obj);
		ASCIIHASHDO((void)data);
	} else { // Must be Unicode
		UNICODELINE(ASCIIHASHDO);
	}
}

static PyObject *gzwrite_C_GzWriteUnicodeLines(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITELINEPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	UNICODELINE(WRITELINEDO);
}
static PyObject *gzwrite_hash_GzWriteUnicodeLines(PyObject *dummy, PyObject *obj)
{
	HASHLINEPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	UNICODELINE(HASHLINEDO);
}

#define MKWLINE(name)                                                                               	\
	static PyObject *gzwrite_write_GzWrite ## name ## Lines(GzWrite *self, PyObject *obj)       	\
	{                                                                                           	\
		return gzwrite_C_GzWrite ## name ## Lines(self, obj, 1);                            	\
	}                                                                                           	\
	static PyObject *gzwrite_hashcheck_GzWrite ## name ## Lines(GzWrite *self, PyObject *obj)   	\
	{                                                                                           	\
		if (!self->slices) {                                                                	\
			PyErr_SetString(PyExc_ValueError, "No hashfilter set");                     	\
			return 0;                                                                   	\
		}                                                                                   	\
		return gzwrite_C_GzWrite ## name ## Lines(self, obj, 0);                            	\
	}
MKWLINE(Bytes);
MKWLINE(Ascii);
MKWLINE(Unicode);


#define WRITEBLOBPROLOGUE(checktype, errname) \
	if (obj == Py_None) {                                                         	\
		WRITE_NONE_SLICE_CHECK;                                               	\
		self->count++;                                                        	\
		return gzwrite_write_(self, "\xff\x00\x00\x00\x00", 5);               	\
	}                                                                             	\
	if (checktype) {                                                              	\
		PyErr_Format(PyExc_TypeError,                                         	\
		             "For your protection, only " errname                     	\
		             " objects are accepted (line %llu)",                     	\
		             (unsigned long long) self->count + 1);                   	\
		return 0;                                                             	\
	}

#define WRITEBLOBDO(cleanup) \
	if (self->slices) {                                                           	\
		if (hash(data, len) % self->slices != self->sliceno) {                	\
			cleanup;                                                      	\
			Py_RETURN_FALSE;                                              	\
		}                                                                     	\
	}                                                                             	\
	if (!actually_write) {                                                        	\
		cleanup;                                                              	\
		Py_RETURN_TRUE;                                                       	\
	}                                                                             	\
	PyObject *ret;                                                                	\
	if (len < 255) {                                                              	\
		uint8_t short_len = len;                                              	\
		ret = gzwrite_write_(self, (char *)&short_len, 1);                    	\
	} else {                                                                      	\
		if (len > 0x7fffffff) {                                               	\
			cleanup;                                                      	\
			PyErr_SetString(PyExc_ValueError, "Value too large");         	\
			return 0;                                                     	\
		}                                                                     	\
		uint32_t long_len = len;                                              	\
		uint8_t lenbuf[5];                                                    	\
		lenbuf[0] = 255;                                                      	\
		memcpy(lenbuf + 1, &long_len, 4);                                     	\
		ret = gzwrite_write_(self, (char *)lenbuf, 5);                        	\
	}                                                                             	\
	if (!ret) {                                                                   	\
		cleanup;                                                              	\
		return 0;                                                             	\
	}                                                                             	\
	Py_DECREF(ret);                                                               	\
	ret = gzwrite_write_(self, data, len);                                        	\
	cleanup;                                                                      	\
	if (!ret) return 0;                                                           	\
	self->count++;                                                                	\
	return ret;

#define ASCIIBLOBDO(cleanup) \
	ASCIIVERIFY(cleanup);                                                         	\
	WRITEBLOBDO(cleanup);

static PyObject *gzwrite_C_GzWriteBytes(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	WRITEBLOBDO((void)data);
}

static PyObject *gzwrite_C_GzWriteAscii(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	if (PyBytes_Check(obj)) {
		const Py_ssize_t len = PyBytes_GET_SIZE(obj);
		const char *data = PyBytes_AS_STRING(obj);
		ASCIIBLOBDO((void)data);
	} else { // Must be Unicode
		// Reusing UNICODELINE is fine
		UNICODELINE(ASCIIBLOBDO);
	}
}

static PyObject *gzwrite_C_GzWriteUnicode(GzWrite *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	// Reusing UNICODELINE is fine
	UNICODELINE(WRITEBLOBDO);
}

// Hashing can reuse the line versions
#define gzwrite_hash_GzWriteBytes   gzwrite_hash_GzWriteBytesLines
#define gzwrite_hash_GzWriteAscii   gzwrite_hash_GzWriteAsciiLines
#define gzwrite_hash_GzWriteUnicode gzwrite_hash_GzWriteUnicodeLines

#define MKWBLOB(name)                                                                               	\
	static PyObject *gzwrite_write_GzWrite ## name (GzWrite *self, PyObject *obj)               	\
	{                                                                                           	\
		return gzwrite_C_GzWrite ## name (self, obj, 1);                                    	\
	}                                                                                           	\
	static PyObject *gzwrite_hashcheck_GzWrite ## name (GzWrite *self, PyObject *obj)           	\
	{                                                                                           	\
		if (!self->slices) {                                                                	\
			PyErr_SetString(PyExc_ValueError, "No hashfilter set");                     	\
			return 0;                                                                   	\
		}                                                                                   	\
		return gzwrite_C_GzWrite ## name (self, obj, 0);                                    	\
	}
MKWBLOB(Bytes);
MKWBLOB(Ascii);
MKWBLOB(Unicode);


static inline uint64_t minmax_value_datetime(uint64_t value) {
	/* My choice to use 2x u32 comes back to bite me. */
	struct { uint32_t i0, i1; } tmp;
	memcpy(&tmp, &value, sizeof(value));
	// ignore .fold, because python does.
	return ((uint64_t)(tmp.i0 & 0xfffffff) << 32) | tmp.i1;
}

#define MK_MINMAX_SET(name, parse)                                                       	\
	static inline void minmax_set_ ## name                                           	\
	(PyObject **ref_obj, PyObject *obj, void *ref_value, void *cmp_value, size_t z)  	\
	{                                                                                	\
		Py_XDECREF(*ref_obj);                                                    	\
		*ref_obj = parse;                                                        	\
		memcpy(ref_value, cmp_value, z);                                         	\
	}
MK_MINMAX_SET(Float64 , PyFloat_FromDouble(*(double *)cmp_value));
MK_MINMAX_SET(Float32 , PyFloat_FromDouble(*(float *)cmp_value));
MK_MINMAX_SET(Int64   , pyInt_FromS64(*(int64_t *)cmp_value));
MK_MINMAX_SET(Int32   , pyInt_FromS32(*(int32_t *)cmp_value));
MK_MINMAX_SET(Bits64  , pyInt_FromU64(*(uint64_t *)cmp_value));
MK_MINMAX_SET(Bits32  , pyInt_FromU32(*(uint32_t *)cmp_value));
MK_MINMAX_SET(Bool    , PyBool_FromLong(*(uint8_t *)cmp_value));
MK_MINMAX_SET(DateTime, unfmt_datetime((*(uint64_t *)cmp_value) >> 32, *(uint64_t *)cmp_value));
MK_MINMAX_SET(Date    , unfmt_date(*(uint32_t *)cmp_value));
MK_MINMAX_SET(Time    , unfmt_time((*(uint64_t *)cmp_value) >> 32, *(uint64_t *)cmp_value));

#define MINMAX_STD(T, v, minmax_set)                                                         	\
	T cmp_value = v;                                                                     	\
	if (!self->min_obj || (cmp_value < self->min_u.as_ ## T)) {                          	\
		minmax_set(&self->min_obj, obj, &self->min_u, &cmp_value, sizeof(cmp_value));	\
	}                                                                                    	\
	if (!self->max_obj || (cmp_value > self->max_u.as_ ## T)) {                          	\
		minmax_set(&self->max_obj, obj, &self->max_u, &cmp_value, sizeof(cmp_value));	\
	}
#define MINMAX_FLOAT(T, v, minmax_set)                                                       	\
	T cmp_value = v;                                                                     	\
	T min_value = self->min_u.as_ ## T;                                                  	\
	T max_value = self->max_u.as_ ## T;                                                  	\
	if (!self->min_obj || (cmp_value < min_value) || isnan(min_value)) {                 	\
		minmax_set(&self->min_obj, obj, &self->min_u, &cmp_value, sizeof(cmp_value));	\
	}                                                                                    	\
	if (!self->max_obj || (cmp_value > max_value) || isnan(max_value)) {                 	\
		minmax_set(&self->max_obj, obj, &self->max_u, &cmp_value, sizeof(cmp_value));	\
	}
#define MINMAX_DUMMY(T, v, minmax_set) /* Nothing */

#define MKWRITER_C(tname, T, HT, conv, withnone, errchk, do_minmax, minmax_value, minmax_set, hash) \
	static int gzwrite_init_ ## tname(PyObject *self_, PyObject *args, PyObject *kwds)	\
	{                                                                                	\
		static char *kwlist[] = {"name", "mode", "default", "hashfilter", "none_support", 0}; \
		GzWrite *self = (GzWrite *)self_;                                        	\
		char *name = 0;                                                          	\
		const char *mode = 0;                                                    	\
		PyObject *default_obj = 0;                                               	\
		PyObject *hashfilter = 0;                                                	\
		gzwrite_close_(self);                                                    	\
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOOi", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &default_obj, &hashfilter, &self->none_support)) return -1; \
		if (!withnone && self->none_support) {                                   	\
			PyErr_Format(PyExc_ValueError, "%s objects don't support None values", self_->ob_type->tp_name); \
			return -1;                                                       	\
		}                                                                        	\
		self->name = name;                                                       	\
		if (default_obj) {                                                       	\
			T value;                                                         	\
			Py_INCREF(default_obj);                                          	\
			self->default_obj = default_obj;                                 	\
			if (withnone && self->none_support && self->default_obj == Py_None) {	\
				memcpy(&value, &noneval_ ## T, sizeof(T));               	\
			} else {                                                         	\
				value = conv(self->default_obj);                         	\
				if (PyErr_Occurred()) goto err;                          	\
				if (withnone && !memcmp(&value, &noneval_ ## T, sizeof(T))) {	\
					PyErr_SetString(PyExc_OverflowError, "Default value becomes None-marker"); \
					goto err;                                        	\
				}                                                        	\
			}                                                                	\
			self->default_value = malloc(sizeof(T));                         	\
			if (!self->default_value) {                                      	\
				PyErr_NoMemory();                                        	\
				goto err;                                                	\
			}                                                                	\
			memcpy(self->default_value, &value, sizeof(T));                  	\
		}                                                                        	\
		err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None)); \
		err1(mode_fixup(mode, self->mode));                                      	\
		self->closed = 0;                                                        	\
		self->count = 0;                                                         	\
		self->len = 0;                                                           	\
		return 0;                                                                	\
err:                                                                                     	\
		return -1;                                                               	\
	}                                                                                	\
	static PyObject *gzwrite_C_ ## tname(GzWrite *self, PyObject *obj, int actually_write)	\
	{                                                                                	\
		if (withnone && obj == Py_None) {                                        	\
is_none:                                                                                 	\
			WRITE_NONE_SLICE_CHECK;                                          	\
			self->count++;                                                   	\
			return gzwrite_write_(self, (char *)&noneval_ ## T, sizeof(T));  	\
		}                                                                        	\
		T value = conv(obj);                                                     	\
		PyObject *pyerr = (errchk ? PyErr_Occurred() : 0);                       	\
		if (withnone && !pyerr &&                                                	\
		    !memcmp(&value, &noneval_ ## T, sizeof(T))                           	\
		   ) {                                                                   	\
			PyErr_SetString(PyExc_OverflowError, "Value becomes None-marker");	\
			pyerr = PyErr_Occurred();                                        	\
		}                                                                        	\
		if (pyerr) {                                                             	\
			if (!self->default_value) return 0;                              	\
			PyErr_Clear();                                                   	\
			if (withnone && self->default_obj == Py_None) goto is_none;      	\
			value = self->default_value->as_ ## T;                           	\
			obj = self->default_obj;                                         	\
		}                                                                        	\
		if (self->slices) {                                                      	\
			const HT h_value = value;                                        	\
			const unsigned int sliceno = hash(&h_value) % self->slices;      	\
			if (sliceno != self->sliceno) Py_RETURN_FALSE;                   	\
		}                                                                        	\
		if (!actually_write) Py_RETURN_TRUE;                                     	\
		do_minmax(T, minmax_value(value), minmax_set)                            	\
		self->count++;                                                           	\
		return gzwrite_write_(self, (char *)&value, sizeof(value));              	\
	}                                                                                	\
	static PyObject *gzwrite_write_ ## tname(GzWrite *self, PyObject *obj)           	\
	{                                                                                	\
		return gzwrite_C_ ## tname(self, obj, 1);                                	\
	}                                                                                	\
	static PyObject *gzwrite_hashcheck_ ## tname(GzWrite *self, PyObject *obj)       	\
	{                                                                                	\
		if (!self->slices) {                                                     	\
			PyErr_SetString(PyExc_ValueError, "No hashfilter set");          	\
			return 0;                                                        	\
		}                                                                        	\
		return gzwrite_C_ ## tname(self, obj, 0);                                	\
	}                                                                                	\
	static PyObject *gzwrite_hash_ ## tname(PyObject *dummy, PyObject *obj)          	\
	{                                                                                	\
		uint64_t h;                                                              	\
		if (withnone && obj == Py_None) {                                        	\
			h = 0;                                                           	\
		} else {                                                                 	\
			const T value = conv(obj);                                       	\
			if (PyErr_Occurred()) return 0;                                  	\
			const HT h_value = value;                                        	\
			h = hash(&h_value);                                              	\
		}                                                                        	\
		return pyInt_FromU64(h);                                                 	\
	}

#define MKWRITER(tname, T, HT, conv, withnone, minmax_value, minmax_set, hash) \
	MKWRITER_C(tname, T, HT, conv, withnone, value == (T)-1, MINMAX_STD, minmax_value, minmax_set, hash)

#if PY_MAJOR_VERSION < 3
// Passing a non-int object to some of the As functions in py2 gives
// SystemError, but we want TypeError.
// Sometimes passing an int to a function that wants a long also breaks.
#  define MKpy2AsFix(T, TN, bitcnt) \
	static T fix_pyLong_As ## TN(PyObject *l)                                        	\
	{                                                                                	\
		T value;                                                                 	\
		if (PyInt_Check(l)) {                                                    	\
			PyObject *ll = PyNumber_Long(l);                                 	\
			if (!ll) return -1; /* "can't" happen */                         	\
			value = pyLong_As ## TN(ll);                                     	\
			Py_DECREF(ll);                                                   	\
		} else {                                                                 	\
			value = pyLong_As ## TN(l);                                      	\
		}                                                                        	\
		if (value == (T)-1 && PyErr_Occurred()) {                                	\
			if (Integer_Check(l)) {                                          	\
				PyErr_SetString(PyExc_OverflowError,                     	\
					"Value doesn't fit in " #bitcnt " bits"          	\
				);                                                       	\
			} else {                                                         	\
				PyErr_Format(PyExc_TypeError,                            	\
					"%s is not an integer type.",                    	\
					l->ob_type->tp_name                              	\
				);                                                       	\
			}                                                                	\
		}                                                                        	\
		return value;                                                            	\
	}

   MKpy2AsFix(uint64_t, U64, 64);
#  undef pyLong_AsU64
#  define pyLong_AsU64 fix_pyLong_AsU64

#  ifdef pyLong_AsS32
     MKpy2AsFix(int32_t, S32, 32);
#    undef pyLong_AsS32
#    define pyLong_AsS32 fix_pyLong_AsS32
#  endif

#  ifdef pyLong_AsU32
     MKpy2AsFix(uint32_t, U32, 32);
#    undef pyLong_AsU32
#    define pyLong_AsU32 fix_pyLong_AsU32
#  endif
#endif

#ifndef pyLong_AsS32
static int32_t pyLong_AsS32(PyObject *l)
{
	int64_t value = pyLong_AsS64(l);
	int32_t real_value = value;
	if (value != real_value) {
		PyErr_SetString(PyExc_OverflowError, "Value doesn't fit in 32 bits");
		return -1;
	}
	return value;
}
#endif

#ifndef pyLong_AsU32
static uint32_t pyLong_AsU32(PyObject *l)
{
	uint64_t value = pyLong_AsU64(l);
	uint32_t real_value = value;
	if (value != real_value) {
		PyErr_SetString(PyExc_OverflowError, "Value doesn't fit in 32 bits");
		return (uint32_t)-1;
	}
	return value;
}
#endif

static uint8_t pyLong_AsBool(PyObject *l)
{
	long value = PyInt_AsLong(l);
	if (value != 0 && value != 1) {
		PyErr_SetString(PyExc_OverflowError, "Value is not 0 or 1");
		return (uint8_t)-1;
	}
	return value;
}

static inline complex32 pyComplex_AsCComplex32(PyObject *obj)
{
	complex64 v64 = PyComplex_AsCComplex(obj);
	complex32 v = { v64.real, v64.imag };
	return v;
}

MKWRITER_C(GzWriteComplex64, complex64, complex64, PyComplex_AsCComplex  , 1, value.real == -1.0, MINMAX_DUMMY, , , hash_complex64);
MKWRITER_C(GzWriteComplex32, complex32, complex32, pyComplex_AsCComplex32, 1, value.real == -1.0, MINMAX_DUMMY, , , hash_complex32);
MKWRITER_C(GzWriteFloat64, double  , double  , PyFloat_AsDouble , 1, value == -1.0, MINMAX_FLOAT, , minmax_set_Float64, hash_double );
MKWRITER_C(GzWriteFloat32, float   , double  , PyFloat_AsDouble , 1, value == -1.0, MINMAX_FLOAT, , minmax_set_Float32, hash_double );
MKWRITER(GzWriteInt64  , int64_t , int64_t , pyLong_AsS64     , 1, , minmax_set_Int64  , hash_integer);
MKWRITER(GzWriteInt32  , int32_t , int64_t , pyLong_AsS32     , 1, , minmax_set_Int32  , hash_integer);
MKWRITER(GzWriteBits64 , uint64_t, uint64_t, pyLong_AsU64     , 0, , minmax_set_Bits64 , hash_integer);
MKWRITER(GzWriteBits32 , uint32_t, uint64_t, pyLong_AsU32     , 0, , minmax_set_Bits32 , hash_integer);
MKWRITER(GzWriteBool   , uint8_t , uint8_t , pyLong_AsBool    , 1, , minmax_set_Bool   , hash_bool   );
static uint64_t fmt_datetime(PyObject *dt)
{
	if (!PyDateTime_Check(dt)) {
		PyErr_SetString(PyExc_ValueError, "datetime object expected");
		return 0;
	}
	const int32_t Y = PyDateTime_GET_YEAR(dt);
	const int32_t m = PyDateTime_GET_MONTH(dt);
	const int32_t d = PyDateTime_GET_DAY(dt);
	const int32_t H = PyDateTime_DATE_GET_HOUR(dt);
	const int32_t M = PyDateTime_DATE_GET_MINUTE(dt);
	const int32_t S = PyDateTime_DATE_GET_SECOND(dt);
	const int32_t u = PyDateTime_DATE_GET_MICROSECOND(dt);
	union { struct { int32_t i0, i1; } i; uint64_t res; } r;
	r.i.i0 = (Y << 14) | (m << 10) | (d << 5) | H;
	r.i.i1 = (M << 26) | (S << 20) | u;
#if PY_VERSION_HEX > 0x03060000
	if (PyDateTime_DATE_GET_FOLD(dt)) r.i.i0 |= 0x10000000;
#endif
	return r.res;
}
static uint32_t fmt_date(PyObject *dt)
{
	if (!PyDate_Check(dt)) {
		PyErr_SetString(PyExc_ValueError, "date object expected");
		return 0;
	}
	const int32_t Y = PyDateTime_GET_YEAR(dt);
	const int32_t m = PyDateTime_GET_MONTH(dt);
	const int32_t d = PyDateTime_GET_DAY(dt);
	return (Y << 9) | (m << 5) | d;
}
static uint64_t fmt_time(PyObject *dt)
{
	if (!PyTime_Check(dt)) {
		PyErr_SetString(PyExc_ValueError, "time object expected");
		return 0;
	}
	const int32_t H = PyDateTime_TIME_GET_HOUR(dt);
	const int32_t M = PyDateTime_TIME_GET_MINUTE(dt);
	const int32_t S = PyDateTime_TIME_GET_SECOND(dt);
	const int32_t u = PyDateTime_TIME_GET_MICROSECOND(dt);
	union { struct { int32_t i0, i1; } i; uint64_t res; } r;
	r.i.i0 = 32277536 | H; // 1970 if read as DateTime
	r.i.i1 = (M << 26) | (S << 20) | u;
#if PY_VERSION_HEX > 0x03060000
	if (PyDateTime_TIME_GET_FOLD(dt)) r.i.i0 |= 0x10000000;
#endif
	return r.res;
}
MKWRITER(GzWriteDateTime, uint64_t, uint64_t, fmt_datetime, 1, minmax_value_datetime, minmax_set_DateTime, hash_datetime);
MKWRITER(GzWriteDate    , uint32_t, uint32_t, fmt_date,     1,                      , minmax_set_Date    , hash_32bits);
MKWRITER(GzWriteTime    , uint64_t, uint64_t, fmt_time,     1, minmax_value_datetime, minmax_set_Time    , hash_datetime);

static int gzwrite_GzWriteNumber_serialize_Long(PyObject *obj, char *buf, const char *msg)
{
	PyErr_Clear();
	const size_t len_bits = _PyLong_NumBits(obj);
	if (len_bits == (size_t)-1 && PyErr_Occurred()) return 1;
	const size_t len_bytes = len_bits / 8 + 1;
	if (len_bytes >= GZNUMBER_MAX_BYTES) {
		PyErr_Format(PyExc_OverflowError,
		             "%s does not fit in %d bytes",
		             msg, GZNUMBER_MAX_BYTES
		            );
		return 1;
	}
	buf[0] = len_bytes;
	unsigned char *ptr = (unsigned char *)buf + 1;
	PyLongObject *lobj = (PyLongObject *)obj;
	return _PyLong_AsByteArray(lobj, ptr, len_bytes, 1, 1) < 0;
}

static int gzwrite_init_GzWriteNumber(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", "default", "hashfilter", "none_support", 0};
	GzWrite *self = (GzWrite *)self_;
	char *name = 0;
	const char *mode = 0;
	PyObject *default_obj = 0;
	PyObject *hashfilter = 0;
	gzwrite_close_(self);
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOOi", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &default_obj, &hashfilter, &self->none_support)) return -1;
	self->name = name;
	if (default_obj) {
		Py_INCREF(default_obj);
		self->default_obj = default_obj;
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(self->default_obj)) {
			PyObject *lobj = PyLong_FromLong(PyInt_AS_LONG(self->default_obj));
			Py_DECREF(self->default_obj);
			self->default_obj = lobj;
		}
#endif
		if (self->default_obj != Py_None || !self->none_support) {
			if (!PyLong_Check(self->default_obj) && !PyFloat_Check(self->default_obj)) {
				PyErr_SetString(PyExc_ValueError, "Bad default value: Only integers/floats accepted");
				goto err;
			}
			if (PyLong_Check(self->default_obj)) {
				char buf[GZNUMBER_MAX_BYTES];
				err1(gzwrite_GzWriteNumber_serialize_Long(self->default_obj, buf, "Bad default value:"));
			}
		}
	}
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	err1(mode_fixup(mode, self->mode));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

static void gzwrite_obj_minmax(GzWrite *self, PyObject *obj)
{
	if (!self->min_obj || (PyFloat_Check(self->min_obj) && isnan(PyFloat_AS_DOUBLE(self->min_obj)))) {
		Py_INCREF(obj);
		Py_XDECREF(self->min_obj);
		self->min_obj = obj;
		Py_INCREF(obj);
		Py_XDECREF(self->max_obj);
		self->max_obj = obj;
		return;
	}
	if (PyObject_RichCompareBool(obj, self->min_obj, Py_LT)) {
		Py_INCREF(obj);
		Py_XDECREF(self->min_obj);
		self->min_obj = obj;
	}
	if (PyObject_RichCompareBool(obj, self->max_obj, Py_GT)) {
		Py_INCREF(obj);
		Py_XDECREF(self->max_obj);
		self->max_obj = obj;
	}
}

static PyObject *gzwrite_C_GzWriteNumber(GzWrite *self, PyObject *obj, int actually_write, int first)
{
	if (obj == Py_None) {
		WRITE_NONE_SLICE_CHECK;
		self->count++;
		return gzwrite_write_(self, "", 1);
	}
	if (PyFloat_Check(obj)) {
		const double value = PyFloat_AS_DOUBLE(obj);
		if (self->slices) {
			const unsigned int sliceno = hash_double(&value) % self->slices;
			if (sliceno != self->sliceno) Py_RETURN_FALSE;
		}
		if (!actually_write) Py_RETURN_TRUE;
		gzwrite_obj_minmax(self, obj);
		char buf[9];
		buf[0] = 1;
		memcpy(buf + 1, &value, 8);
		self->count++;
		return gzwrite_write_(self, buf, 9);
	}
	if (first && !Integer_Check(obj)) {
		if (first && self->default_obj) {
			return gzwrite_C_GzWriteNumber(self, self->default_obj, actually_write, 0);
		}
		PyErr_SetString(PyExc_ValueError, "Only integers/floats accepted");
		return 0;
	}
	const int64_t value = pyLong_AsS64(obj);
	char buf[GZNUMBER_MAX_BYTES];
	if (value != -1 || !PyErr_Occurred()) {
		if (self->slices) {
			const unsigned int sliceno = hash_integer(&value) % self->slices;
			if (sliceno != self->sliceno) Py_RETURN_FALSE;
		}
		if (!actually_write) Py_RETURN_TRUE;
		gzwrite_obj_minmax(self, obj);
		buf[0] = 8;
		memcpy(buf + 1, &value, 8);
		self->count++;
		return gzwrite_write_(self, buf, 9);
	}
	if (gzwrite_GzWriteNumber_serialize_Long(obj, buf, "Value")) {
		if (first && self->default_obj) {
			PyErr_Clear();
			return gzwrite_C_GzWriteNumber(self, self->default_obj, actually_write, 0);
		} else {
			return 0;
		}
	}
	if (self->slices) {
		const unsigned int sliceno = hash(buf + 1, buf[0]) % self->slices;
		if (sliceno != self->sliceno) Py_RETURN_FALSE;
	}
	if (!actually_write) Py_RETURN_TRUE;
	gzwrite_obj_minmax(self, obj);
	self->count++;
	return gzwrite_write_(self, buf, buf[0] + 1);
}
static PyObject *gzwrite_write_GzWriteNumber(GzWrite *self, PyObject *obj)
{
	return gzwrite_C_GzWriteNumber(self, obj, 1, 1);
}
static PyObject *gzwrite_hashcheck_GzWriteNumber(GzWrite *self, PyObject *obj)
{
	if (!self->slices) {
		PyErr_SetString(PyExc_ValueError, "No hashfilter set");
		return 0;
	}
	return gzwrite_C_GzWriteNumber(self, obj, 0, 1);
}
static PyObject *gzwrite_hash_GzWriteNumber(PyObject *dummy, PyObject *obj)
{
	if (obj == Py_None) {
		return PyInt_FromLong(0);
	} else {
		if (PyFloat_Check(obj)) {
			const double value = PyFloat_AS_DOUBLE(obj);
			return pyInt_FromU64(hash_double(&value));
		}
		if (!Integer_Check(obj)) {
			PyErr_SetString(PyExc_ValueError, "Only integers/floats accepted");
			return 0;
		}
		uint64_t h;
		const int64_t value = pyLong_AsS64(obj);
		if (value != -1 || !PyErr_Occurred()) {
			h = hash_integer(&value);
		} else {
			char buf[GZNUMBER_MAX_BYTES];
			if (gzwrite_GzWriteNumber_serialize_Long(obj, buf, "Value")) return 0;
			h = hash(buf + 1, buf[0]);
		}
		return pyInt_FromU64(h);
	}
}

static int gzwrite_init_GzWriteParsedNumber(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", "default", "hashfilter", "none_support", 0};
	PyObject *name = 0;
	PyObject *mode = 0;
	PyObject *default_obj_ = 0;
	PyObject *default_obj = 0;
	PyObject *hashfilter = 0;
	PyObject *none_support = 0;
	PyObject *new_args = 0;
	PyObject *new_kwds = 0;
	int res = -1;
	err1(!PyArg_ParseTupleAndKeywords(args, kwds, "O|OOOO", kwlist, &name, &mode, &default_obj_, &hashfilter, &none_support));
	if (default_obj_) {
		if (default_obj_ == Py_None || PyFloat_Check(default_obj_)) {
			default_obj = default_obj_;
			Py_INCREF(default_obj);
		} else {
			default_obj = PyNumber_Long(default_obj_);
			if (!default_obj) {
				PyErr_Clear();
				default_obj = PyNumber_Float(default_obj_);
			}
			err1(!default_obj);
		}
	}
	new_args = Py_BuildValue("(O)", name);
	new_kwds = PyDict_New();
	err1(!new_args || !new_kwds);
	if (mode) err1(PyDict_SetItemString(new_kwds, "mode", mode));
	if (default_obj) err1(PyDict_SetItemString(new_kwds, "default", default_obj));
	if (hashfilter) err1(PyDict_SetItemString(new_kwds, "hashfilter", hashfilter));
	if (none_support) err1(PyDict_SetItemString(new_kwds, "none_support", none_support));
	res = gzwrite_init_GzWriteNumber(self_, new_args, new_kwds);
err:
	Py_XDECREF(new_kwds);
	Py_XDECREF(new_args);
	Py_XDECREF(default_obj);
	return res;
}

#define MKPARSEDNUMBERWRAPPER(name, selftype) \
	static PyObject *gzwrite_ ## name ## _GzWriteParsedNumber(selftype *self, PyObject *obj)   	\
	{                                                                                          	\
		if (PyFloat_Check(obj) || PyLong_Check(obj) || obj == Py_None) {                   	\
			return gzwrite_ ## name ## _GzWriteNumber(self, obj);                      	\
		}                                                                                  	\
		PyObject *tmp = PyNumber_Int(obj);                                                 	\
		if (!tmp) {                                                                        	\
			PyErr_Clear();                                                             	\
			tmp = PyNumber_Float(obj);                                                 	\
			if (!tmp) {                                                                	\
				/* If there's a default we want that, but we can't check for it here. */\
				PyErr_Clear();                                                     	\
				tmp = obj;                                                         	\
				Py_INCREF(tmp);                                                    	\
			}                                                                          	\
		}                                                                                  	\
		PyObject *res = gzwrite_ ## name ## _GzWriteNumber(self, tmp);                     	\
		Py_DECREF(tmp);                                                                    	\
		return res;                                                                        	\
	}
MKPARSEDNUMBERWRAPPER(write, GzWrite)
MKPARSEDNUMBERWRAPPER(hashcheck, GzWrite)
MKPARSEDNUMBERWRAPPER(hash, PyObject)

#define MKPARSED_C(name, T, HT, inner, conv, withnone, errchk, err_v, do_minmax, minmax_set, hash) \
	static T parse ## name(PyObject *obj)                        	\
	{                                                            	\
		PyObject *parsed = inner(obj);                       	\
		if (!parsed) return err_v;                           	\
		T res = conv(parsed);                                	\
		Py_DECREF(parsed);                                   	\
		return res;                                          	\
	}                                                            	\
	MKWRITER_C(GzWriteParsed ## name, T, HT, parse ## name, withnone, errchk, do_minmax, , minmax_set, hash)
#define MKPARSED(name, T, HT, inner, conv, withnone, minmax_set, hash)	\
	MKPARSED_C(name, T, HT, inner, conv, withnone, value == (T)-1, -1, MINMAX_STD, minmax_set, hash)

static inline PyObject *pyComplex_parse(PyObject *obj)
{
#if PY_MAJOR_VERSION >= 3
	if (PyBytes_Check(obj)) {
		obj = PyUnicode_DecodeUTF8(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj), 0);
		if (!obj) return 0;
	}
#endif
	return PyObject_CallFunctionObjArgs((PyObject *)&PyComplex_Type, obj, 0);
}

static const complex64 complex64_error = { -1.0, 0.0 };
static const complex32 complex32_error = { -1.0, 0.0 };
MKPARSED_C(Complex64, complex64, complex64, pyComplex_parse, PyComplex_AsCComplex  , 1, value.real == -1.0, complex64_error, MINMAX_DUMMY, , hash_double);
MKPARSED_C(Complex32, complex32, complex32, pyComplex_parse, pyComplex_AsCComplex32, 1, value.real == -1.0, complex32_error, MINMAX_DUMMY, , hash_double);
MKPARSED_C(Float64, double  , double  , PyNumber_Float, PyFloat_AsDouble , 1, value == -1.0, -1, MINMAX_FLOAT, minmax_set_Float64, hash_double);
MKPARSED_C(Float32, float   , double  , PyNumber_Float, PyFloat_AsDouble , 1, value == -1.0, -1, MINMAX_FLOAT, minmax_set_Float32, hash_double);
MKPARSED(Int64  , int64_t , int64_t , PyNumber_Int  , pyLong_AsS64     , 1, minmax_set_Int64  , hash_integer);
MKPARSED(Int32  , int32_t , int64_t , PyNumber_Int  , pyLong_AsS32     , 1, minmax_set_Int32  , hash_integer);
MKPARSED(Bits64 , uint64_t, uint64_t, PyNumber_Long , pyLong_AsU64     , 0, minmax_set_Bits64 , hash_integer);
MKPARSED(Bits32 , uint32_t, uint64_t, PyNumber_Int  , pyLong_AsU32     , 0, minmax_set_Bits32 , hash_integer);

static PyMemberDef w_default_members[] = {
	{"name"      , T_STRING   , offsetof(GzWrite, name       ), READONLY},
	{"count"     , T_ULONGLONG, offsetof(GzWrite, count      ), READONLY},
	{"hashfilter", T_OBJECT_EX, offsetof(GzWrite, hashfilter ), READONLY},
	{"min"       , T_OBJECT   , offsetof(GzWrite, min_obj    ), READONLY},
	{"max"       , T_OBJECT   , offsetof(GzWrite, max_obj    ), READONLY},
	{"default"   , T_OBJECT_EX, offsetof(GzWrite, default_obj), READONLY},
	{0}
};

#define MKWTYPE_i(name, methods, members)                            	\
	static PyTypeObject name ## _Type = {                        	\
		PyVarObject_HEAD_INIT(NULL, 0)                       	\
		#name,                          /*tp_name*/          	\
		sizeof(GzWrite),                /*tp_basicsize*/     	\
		0,                              /*tp_itemsize*/      	\
		(destructor)gzwrite_dealloc,    /*tp_dealloc*/       	\
		0,                              /*tp_print*/         	\
		0,                              /*tp_getattr*/       	\
		0,                              /*tp_setattr*/       	\
		0,                              /*tp_compare*/       	\
		0,                              /*tp_repr*/          	\
		0,                              /*tp_as_number*/     	\
		0,                              /*tp_as_sequence*/   	\
		0,                              /*tp_as_mapping*/    	\
		0,                              /*tp_hash*/          	\
		0,                              /*tp_call*/          	\
		0,                              /*tp_str*/           	\
		0,                              /*tp_getattro*/      	\
		0,                              /*tp_setattro*/      	\
		0,                              /*tp_as_buffer*/     	\
		Py_TPFLAGS_DEFAULT,             /*tp_flags*/         	\
		0,                              /*tp_doc*/           	\
		0,                              /*tp_traverse*/      	\
		0,                              /*tp_clear*/         	\
		0,                              /*tp_richcompare*/   	\
		0,                              /*tp_weaklistoffset*/	\
		0,                              /*tp_iter*/          	\
		0,                              /*tp_iternext*/      	\
		methods,                        /*tp_methods*/       	\
		members,                        /*tp_members*/       	\
		0,                              /*tp_getset*/        	\
		0,                              /*tp_base*/          	\
		0,                              /*tp_dict*/          	\
		0,                              /*tp_descr_get*/     	\
		0,                              /*tp_descr_set*/     	\
		0,                              /*tp_dictoffset*/    	\
		gzwrite_init_ ## name,          /*tp_init*/          	\
		PyType_GenericAlloc,            /*tp_alloc*/         	\
		PyType_GenericNew,              /*tp_new*/           	\
		PyObject_Del,                   /*tp_free*/          	\
		0,                              /*tp_is_gc*/         	\
	}
#define MKWTYPE(name)                                                                	\
	static PyMethodDef name ## _methods[] = {                                             	\
		{"__enter__", (PyCFunction)gzwrite_self, METH_NOARGS,  NULL},                 	\
		{"__exit__",  (PyCFunction)gzany_exit, METH_VARARGS, NULL},                   	\
		{"write",     (PyCFunction)gzwrite_write_ ## name, METH_O, NULL},             	\
		{"flush",     (PyCFunction)gzwrite_flush, METH_NOARGS, NULL},                 	\
		{"close",     (PyCFunction)gzwrite_close, METH_NOARGS, NULL},                 	\
		{"hashcheck", (PyCFunction)gzwrite_hashcheck_ ## name, METH_O, NULL},         	\
		{"hash"     , (PyCFunction)gzwrite_hash_## name, METH_STATIC | METH_O, NULL}, 	\
		{0}                                                                           	\
	};                                                                                    	\
	MKWTYPE_i(name, name ## _methods, w_default_members);
static PyMethodDef GzWrite_methods[] = {
	{"__enter__", (PyCFunction)gzwrite_self, METH_NOARGS,  NULL},
	{"__exit__",  (PyCFunction)gzany_exit, METH_VARARGS, NULL},
	{"write",     (PyCFunction)gzwrite_write_GzWrite, METH_O, NULL},
	{"flush",     (PyCFunction)gzwrite_flush, METH_NOARGS, NULL},
	{"close",     (PyCFunction)gzwrite_close, METH_NOARGS, NULL},
	{0}
};
MKWTYPE_i(GzWrite, GzWrite_methods, 0);
MKWTYPE(GzWriteBytes);
MKWTYPE(GzWriteAscii);
MKWTYPE(GzWriteUnicode);
MKWTYPE(GzWriteBytesLines);
MKWTYPE(GzWriteAsciiLines);
MKWTYPE(GzWriteUnicodeLines);
MKWTYPE(GzWriteComplex64);
MKWTYPE(GzWriteComplex32);
MKWTYPE(GzWriteFloat64);
MKWTYPE(GzWriteFloat32);
MKWTYPE(GzWriteNumber);
MKWTYPE(GzWriteInt64);
MKWTYPE(GzWriteInt32);
MKWTYPE(GzWriteBits64);
MKWTYPE(GzWriteBits32);
MKWTYPE(GzWriteBool);
MKWTYPE(GzWriteDateTime);
MKWTYPE(GzWriteDate);
MKWTYPE(GzWriteTime);

MKWTYPE(GzWriteParsedNumber);
MKWTYPE(GzWriteParsedComplex64);
MKWTYPE(GzWriteParsedComplex32);
MKWTYPE(GzWriteParsedFloat64);
MKWTYPE(GzWriteParsedFloat32);
MKWTYPE(GzWriteParsedInt64);
MKWTYPE(GzWriteParsedInt32);
MKWTYPE(GzWriteParsedBits64);
MKWTYPE(GzWriteParsedBits32);

static PyObject *generic_hash(PyObject *dummy, PyObject *obj)
{
	if (obj == Py_None)        return PyInt_FromLong(0);
	if (PyBytes_Check(obj))    return gzwrite_hash_GzWriteBytesLines(0, obj);
	if (PyUnicode_Check(obj))  return gzwrite_hash_GzWriteUnicodeLines(0, obj);
	if (PyFloat_Check(obj))    return gzwrite_hash_GzWriteFloat64(0, obj);
	if (PyBool_Check(obj))     return gzwrite_hash_GzWriteBool(0, obj);
	if (Integer_Check(obj)) {
		return gzwrite_hash_GzWriteNumber(0, obj);
	}
	if (PyDateTime_Check(obj)) return gzwrite_hash_GzWriteDateTime(0, obj);
	if (PyDate_Check(obj))     return gzwrite_hash_GzWriteDate(0, obj);
	if (PyTime_Check(obj))     return gzwrite_hash_GzWriteTime(0, obj);
	if (PyComplex_Check(obj))  return gzwrite_hash_GzWriteComplex64(0, obj);
	PyErr_Format(PyExc_ValueError, "Unknown type %s", obj->ob_type->tp_name);
	return 0;
}

static PyObject *siphash24(PyObject *dummy, PyObject *args)
{
	const uint8_t *v;
	const uint8_t *k = hash_k;
	Py_ssize_t v_len;
	Py_ssize_t k_len = 16;
	if (!PyArg_ParseTuple(args, "s#|s#", &v, &v_len, &k, &k_len)) return 0;
	if (k_len != 16) {
		PyErr_Format(PyExc_ValueError, "Bad k, must be 16 bytes (not %zd)", k_len);
		return 0;
	}
	uint64_t res;
	siphash((uint8_t *)&res, v, v_len, k);
	return pyInt_FromU64(res);
}

static PyMethodDef module_methods[] = {
	{"hash", generic_hash, METH_O, "hash(v) - The hash a writer for type(v) would have used to slice v"},
	{"siphash24", siphash24, METH_VARARGS, "siphash24(v, k=...) - SipHash-2-4 of v, defaults to the same k as the slicing hash"},
	{0}
};

#if PY_MAJOR_VERSION < 3
#  define INITERR
#else
#  define INITERR 0
static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"gzutil",           /*m_name*/
	0,                  /*m_doc*/
	-1,                 /*m_size*/
	module_methods,     /*m_methods*/
	0,                  /*m_reload*/
	0,                  /*m_traverse*/
	0,                  /*m_clear*/
	0,                  /*m_free*/
};
#endif

#define INIT(name) do {                                              	\
	if (PyType_Ready(&name ## _Type) < 0) return INITERR;        	\
	Py_INCREF(&name ## _Type);                                   	\
	PyModule_AddObject(m, #name, (PyObject *) &name ## _Type);   	\
} while (0)

#define VERIFY_FLOATNONE(T) do {                                     	\
	T value;                                                     	\
	memcpy(&value, &noneval_ ## T, sizeof(T));                   	\
	good &= !!isnan(value);                                      	\
} while (0)

__attribute__ ((visibility("default"))) PyMODINIT_FUNC INITFUNC(void)
{
	int good = (sizeof(py64T) == 8);
	good &= (sizeof(int64_t) == 8);
	good &= (sizeof(double) == 8);
	good &= (sizeof(float) == 4);
	good &= (sizeof(long) >= 4);
	if (good) { // only test this if sizes are right at least
		union { int16_t s; uint8_t c[2]; } endian_test;
		endian_test.s = -2;
		if (endian_test.c[0] == 254 && endian_test.c[1] == 255) {
			// little endian two's complement, as expected
		} else if (endian_test.c[0] == 255 && endian_test.c[1] == 254) {
			// big endian, we can work with this.
			memcpy(&noneval_double, &BE_noneval_double, sizeof(noneval_double));
			memcpy(&noneval_float, &BE_noneval_float, sizeof(noneval_float));
			memcpy(&noneval_complex64, &BE_noneval_complex64, sizeof(noneval_complex64));
			memcpy(&noneval_complex32, &BE_noneval_complex32, sizeof(noneval_complex32));
		} else {
			// wat?
			good = 0;
		}
		VERIFY_FLOATNONE(double);
		VERIFY_FLOATNONE(float);
	}
	if (!good) {
		PyErr_SetString(PyExc_OverflowError,
			"This module only works with two's complement "
			"integers, IEEE 754 binary floats and 8 bit bytes."
		);
		return INITERR;
	}
	PyDateTime_IMPORT;
#if PY_MAJOR_VERSION >= 3
	PyObject *m = PyModule_Create(&moduledef);
#else
	PyObject *m = Py_InitModule3("gzutil", module_methods, NULL);
#endif
	if (!m) return INITERR;
	// "Ascii" is Bytes-like in py2, Unicode-like in py3.
	// (Either way you can write both types (with suitable contents) to it.)
#if PY_MAJOR_VERSION >= 3
	GzAsciiLines_Type.tp_base = &GzUnicodeLines_Type;
	GzWriteAsciiLines_Type.tp_base = &GzWriteUnicodeLines_Type;
#else
	GzAsciiLines_Type.tp_base = &GzBytesLines_Type;
	GzWriteAsciiLines_Type.tp_base = &GzWriteBytesLines_Type;
#endif
	INIT(GzBytes);
	INIT(GzUnicode);
	INIT(GzAscii);
	INIT(GzBytesLines);
	INIT(GzUnicodeLines);
	INIT(GzAsciiLines);
	INIT(GzNumber);
	INIT(GzComplex64);
	INIT(GzComplex32);
	INIT(GzFloat64);
	INIT(GzFloat32);
	INIT(GzInt64);
	INIT(GzInt32);
	INIT(GzBits64);
	INIT(GzBits32);
	INIT(GzBool);
	INIT(GzDateTime);
	INIT(GzDate);
	INIT(GzTime);
	INIT(GzWrite);
	INIT(GzWriteBytes);
	INIT(GzWriteUnicode);
	INIT(GzWriteAscii);
	INIT(GzWriteBytesLines);
	INIT(GzWriteUnicodeLines);
	INIT(GzWriteAsciiLines);
	INIT(GzWriteNumber);
	INIT(GzWriteComplex64);
	INIT(GzWriteComplex32);
	INIT(GzWriteFloat64);
	INIT(GzWriteFloat32);
	INIT(GzWriteInt64);
	INIT(GzWriteInt32);
	INIT(GzWriteBits64);
	INIT(GzWriteBits32);
	INIT(GzWriteBool);
	INIT(GzWriteDateTime);
	INIT(GzWriteDate);
	INIT(GzWriteTime);
	INIT(GzWriteParsedNumber);
	INIT(GzWriteParsedComplex64);
	INIT(GzWriteParsedComplex32);
	INIT(GzWriteParsedFloat64);
	INIT(GzWriteParsedFloat32);
	INIT(GzWriteParsedInt64);
	INIT(GzWriteParsedInt32);
	INIT(GzWriteParsedBits64);
	INIT(GzWriteParsedBits32);
	PyObject *c_hash = PyCapsule_New((void *)hash, "gzutil._C_hash", 0);
	if (!c_hash) return INITERR;
	PyModule_AddObject(m, "_C_hash", c_hash);
#if PY_MAJOR_VERSION >= 3
	return m;
#endif
}
