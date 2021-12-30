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
#define NUMBER_MAX_BYTES 127

#define MAX_COMPRESSORS 16

#define err1(v) if (v) goto err

static inline void add_extra_to_exc_msg(const char *extra) {
	if (*extra) {
		PyObject *old_type, *old_value, *old_traceback;
		PyErr_Fetch(&old_type, &old_value, &old_traceback);
#if PY_MAJOR_VERSION < 3
		PyObject *strobj = PyObject_Bytes(old_value);
		if (!strobj) {
			PyErr_Restore(old_type, old_value, old_traceback);
			return;
		}
		const char *strdata = PyBytes_AS_STRING(strobj);
		PyErr_Format(old_type, "%s%s", strdata, extra);
		Py_DECREF(strobj);
#else
		PyErr_Format(old_type, "%S%s", old_value, extra);
#endif
		Py_DECREF(old_type);
		Py_DECREF(old_value);
		Py_XDECREF(old_traceback);
	}
}


typedef struct dsu_compressor {
	int (*read)(void *ctx, char *buf, int *len);
	int (*write)(void *ctx, const char *buf, int len);
	void *(*read_open)(int fd, Py_ssize_t size_hint);
	void *(*write_open)(int fd);
	void (*read_close)(void *ctx);
	int (*write_close)(void *ctx);
} dsu_compressor;

typedef struct dsu_gz_ctx {
	gzFile fh;
} dsu_gz_ctx;

static void *dsu_gz_read_open(int fd, Py_ssize_t size_hint)
{
	dsu_gz_ctx *ctx;
	ctx = malloc(sizeof(*ctx));
	err1(!ctx);
	ctx->fh = gzdopen(fd, "rb");
	err1(!ctx->fh);
	if (size_hint >= 0 && size_hint < 400000) {
		gzbuffer(ctx->fh, 16 * 1024);
	} else {
		gzbuffer(ctx->fh, 64 * 1024);
	}
	if (gzdirect(ctx->fh)) {
		PyErr_SetString(PyExc_IOError, "not gzip compressed");
		goto err;
	}
	return ctx;
err:
	if (ctx) free(ctx);
	return 0;
}

static int dsu_gz_read(void *ctx_, char *buf, int *len)
{
	dsu_gz_ctx *ctx = ctx_;
	err1(!ctx->fh);
	int got = gzread(ctx->fh, buf, *len);
	err1(got == -1);
	*len = got;
	int error = 0;
	if (len == 0) gzerror(ctx->fh, &error);
	return error;
err:
	if (ctx->fh) {
		gzclose(ctx->fh);
		ctx->fh = 0;
	}
	return 1;
}

static void dsu_gz_read_close(void *ctx_)
{
	dsu_gz_ctx *ctx = ctx_;
	if (ctx->fh) gzclose(ctx->fh);
	free(ctx);
}

static void *dsu_gz_write_open(int fd)
{
	dsu_gz_ctx *ctx;
	ctx = malloc(sizeof(*ctx));
	err1(!ctx);
	ctx->fh = gzdopen(fd, "wb");
	err1(!ctx->fh);
	return ctx;
err:
	if (ctx) free(ctx);
	return 0;
}

static int dsu_gz_write_close(void *ctx_)
{
	int res = 1;
	dsu_gz_ctx *ctx = ctx_;
	err1(!ctx->fh);
	res = gzclose(ctx->fh);
err:
	free(ctx);
	return res;
}

static int dsu_gz_write(void *ctx_, const char *buf, int len)
{
	dsu_gz_ctx *ctx = ctx_;
	err1(!ctx->fh);
	int wrote = gzwrite(ctx->fh, buf, len);
	err1(wrote != len);
	return 0;
err:
	if (ctx->fh) {
		gzclose(ctx->fh);
		ctx->fh = 0;
	}
	return 1;
}

static const dsu_compressor dsu_gz = {
	dsu_gz_read,
	dsu_gz_write,
	dsu_gz_read_open,
	dsu_gz_write_open,
	dsu_gz_read_close,
	dsu_gz_write_close,
};


typedef struct read {
	PyObject_HEAD
	char *name;
	PyObject *hashfilter;
	PyObject *callback;
	PY_LONG_LONG want_count;
	PY_LONG_LONG count;
	PY_LONG_LONG break_count;
	PY_LONG_LONG callback_interval;
	PY_LONG_LONG callback_offset;
	uint64_t spread_None;
	void *ctx;
	const dsu_compressor *compressor;
	int error;
	int pos, len;
	unsigned int sliceno;
	unsigned int slices;
	char buf[Z];
} Read;

#define FREE(p) do { PyMem_Free(p); (p) = 0; } while (0)

static int Read_close_(Read *self)
{
	FREE(self->name);
	Py_CLEAR(self->hashfilter);
	self->count = 0;
	self->want_count = -1;
	self->break_count = -1;
	Py_CLEAR(self->callback);
	self->callback_interval = 0;
	self->callback_offset = 0;
	if (self->ctx) {
		self->compressor->read_close(self->ctx);
		self->ctx = 0;
		return 0;
	}
	return 1;
}

#if PY_MAJOR_VERSION < 3
#  define BYTES_NAME      "str"
#  define UNICODE_NAME    "unicode"
#  define EITHER_NAME     "str or unicode"
#  define INITFUNC        init_dsutil
#  define Integer_Check(o) (PyInt_Check(o) || PyLong_Check(o))
#else
#  define BYTES_NAME      "bytes"
#  define UNICODE_NAME    "str"
#  define EITHER_NAME     "str or bytes"
#  define PyInt_FromLong  PyLong_FromLong
#  define PyInt_AsLong    PyLong_AsLong
#  define PyNumber_Int    PyNumber_Long
#  define INITFUNC        PyInit__dsutil
#  define Integer_Check(o) PyLong_Check(o)
#endif

// Stupid forward declarations
static int Read_read_(Read *self, int itemsize);
static PyTypeObject ReadNumber_Type;
static PyTypeObject ReadDateTime_Type;
static PyTypeObject ReadDate_Type;
static PyTypeObject ReadTime_Type;
static PyTypeObject ReadBool_Type;

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
static uint64_t hash_bool(const uint8_t *ptr)
{
	return !!*ptr;
}
static uint64_t hash_uint64(const uint64_t *ptr)
{
	if (!*ptr) return 0;
	return hash(ptr, 8);
}
static uint64_t hash_int64(const int64_t *ptr)
{
	if (!*ptr) return 0;
	return hash(ptr, 8);
}
static uint64_t hash_double(const double *ptr)
{
	int64_t i = *ptr;
	if (i == *ptr) return hash_int64(&i);
	return hash(ptr, sizeof(*ptr));
}
static uint64_t hash_complex64(const complex64 *ptr)
{
	if (ptr->imag == 0.0) return hash_double(&ptr->real);
	return hash(ptr, sizeof(*ptr));
}
static uint64_t hash_complex32(const complex32 *ptr)
{
	complex64 v64;
	v64.real = ptr->real;
	v64.imag = ptr->imag;
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

static PyObject *compression_dict = 0;
static PyObject *compression_names[MAX_COMPRESSORS] = {0};
static const dsu_compressor *compression_funcs[MAX_COMPRESSORS] = {0};

static int parse_compression(PyObject *compression)
{
	if (!compression) return 1; // default to gzip for backwards compatibility
	PyObject *v = PyDict_GetItem(compression_dict, compression);
	if (!v) {
		PyErr_Format(PyExc_ValueError, "Unknown compression %R", compression);
		return -1;
	}
	return PyInt_AsLong(v);
}

static int Read_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	int res = -1;
	Read *self = (Read *)self_;
	char *name = 0;
	int fd = -1;
	PyObject *compression = 0;
	PY_LONG_LONG seek = 0;
	PyObject *hashfilter = 0;
	PyObject *callback = 0;
	PY_LONG_LONG callback_interval = 0;
	PY_LONG_LONG callback_offset = 0;
	Read_close_(self);
	self->error = 0;
	static char *kwlist[] = {
		"name", "compression", "seek", "want_count", "hashfilter",
		"callback", "callback_interval", "callback_offset", "fd", 0
	};
	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "et|OLLOOLLi", kwlist,
		Py_FileSystemDefaultEncoding, &name,
		&compression,
		&seek,
		&self->want_count,
		&hashfilter,
		&callback,
		&callback_interval,
		&callback_offset,
		&fd
	)) return -1;
	int idx = parse_compression(compression);
	if (idx == -1) return -1;
	self->compressor = compression_funcs[idx];
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
	self->ctx = self->compressor->read_open(fd, self->want_count * 4);
	if (!self->ctx) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	fd = -1; // belongs to self->ctx now
	if (self->want_count >= 0) {
		self->break_count = self->want_count;
	}
	if (self->callback_interval > 0) {
		if (self->callback_interval < self->break_count || self->break_count < 0) {
			self->break_count = self->callback_interval;
		}
	}
	self->pos = self->len = 0;
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	res = 0;
err:
	if (fd >= 0) close(fd);
	if (res) {
		Read_close_(self);
		self->error = 1;
	}
	return res;
}

static void Read_dealloc(Read *self)
{
	Read_close_(self);
	PyObject_Del(self);
}

static PyObject *err_closed(void)
{
	PyErr_SetString(PyExc_ValueError, "I/O operation on closed file");
	return 0;
}

static PyObject *Read_close(Read *self)
{
	if (Read_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *Read_self(Read *self)
{
	if (!self->ctx) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static int Read_read_(Read *self, int itemsize)
{
	if (!self->error) {
		self->len = Z;
		if (self->want_count >= 0) {
			PY_LONG_LONG count_left = self->want_count - self->count;
			PY_LONG_LONG candidate = count_left * itemsize + itemsize;
			if (candidate < self->len) self->len = candidate;
		}
		self->error = self->compressor->read(self->ctx, self->buf, &self->len);
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

static inline int do_callback(Read *self)
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
		if (!self->ctx) return err_closed();         	\
		if (self->count == self->break_count) {      	\
			if (self->count == self->want_count) {	\
				return 0;                    	\
			}                                    	\
			if (do_callback(self)) {             	\
				return 0;                    	\
			}                                    	\
		}                                            	\
		if (self->error || self->pos >= self->len) { 	\
			if (Read_read_(self, SIZE_ ## typename)) return 0; \
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

#define MKmkBlob(name, decoder) \
	static inline PyObject *mkblob ## name(Read *self, const char *ptr, int len)     	\
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
	static PyObject *name ## _iternext(Read *self)                                   	\
	{                                                                                	\
		ITERPROLOGUE(typename);                                                  	\
		uint32_t size = ((uint8_t *)self->buf)[self->pos];                       	\
		self->pos++;                                                             	\
		char *ptr = self->buf + self->pos;                                       	\
		uint32_t left_in_buf = self->len - self->pos;                            	\
		if (!left_in_buf && size) {                                              	\
			if (Read_read_(self, SIZE_ ## typename)) goto fferror;           	\
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
				if (Read_read_(self, SIZE_ ## typename)) goto fferror;   	\
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
			int read_len = want_len;                                         	\
			self->error = self->compressor->read(self->ctx, tmp + left_in_buf, &read_len); \
			if (self->error || read_len != want_len) {                       	\
				free(tmp);                                               	\
				goto fferror;                                            	\
			}                                                                	\
			PyObject *res = mkblob ## typename(self, tmp, size);             	\
			free(tmp);                                                       	\
			return res;                                                      	\
		}                                                                        	\
		if (size > left_in_buf) {                                                	\
			memmove(self->buf, ptr, left_in_buf);                            	\
			ptr = self->buf + left_in_buf;                                   	\
			int read_len = Z - left_in_buf;                                  	\
			self->error = self->compressor->read(self->ctx, ptr, &read_len); 	\
			if (self->error || read_len <= 0) goto fferror;                  	\
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
MKBLOBITER(ReadBytes  , Bytes);
MKBLOBITER(ReadAscii  , Ascii);
MKBLOBITER(ReadUnicode, Unicode);


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
	static PyObject * name ## _iternext(Read *self)                      	\
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

MKITER(ReadComplex64, complex64, PyComplex_FromCComplex, hash_complex64, complex64, 1)
MKITER(ReadComplex32, complex32, pyComplex_From32      , hash_complex32, complex32, 1)
MKITER(ReadFloat64  , double   , PyFloat_FromDouble    , hash_double   , double   , 1)
MKITER(ReadFloat32  , float    , PyFloat_FromDouble    , hash_double   , double   , 1)
MKITER(ReadInt64    , int64_t  , pyInt_FromS64         , hash_int64    , int64_t  , 1)
MKITER(ReadInt32    , int32_t  , pyInt_FromS32         , hash_int64    , int64_t  , 1)
MKITER(ReadBits64   , uint64_t , pyInt_FromU64         , hash_uint64   , uint64_t , 0)
MKITER(ReadBits32   , uint32_t , pyInt_FromU32         , hash_uint64   , uint64_t , 0)
MKITER(ReadBool     , uint8_t  , PyBool_FromLong       , hash_bool     , uint8_t  , 1)

static PyObject *ReadNumber_iternext(Read *self)
{
	ITERPROLOGUE(Number);
	int is_float = 0;
	int len = ((uint8_t *)self->buf)[self->pos];
	self->pos++;
	if (!len) HC_RETURN_NONE;
	if (len >= 0x80) {
		int64_t v = (len & 0x7f) - 5;
		HC_CHECK(hash_int64(&v));
		return pyInt_FromS64(v);
	}
	if (len == 1) {
		len = 8;
		is_float = 1;
	}
	if (len >= NUMBER_MAX_BYTES || (len < 8 && len != 2 && len != 4)) {
		PyErr_SetString(PyExc_ValueError, "File format error");
		return 0;
	}
	unsigned char buf[NUMBER_MAX_BYTES];
	const int avail = self->len - self->pos;
	if (avail >= len) {
		memcpy(buf, self->buf + self->pos, len);
		self->pos += len;
	} else {
		memcpy(buf, self->buf + self->pos, avail);
		unsigned char * const ptr = buf + avail;
		const int morelen = len - avail;
		if (Read_read_(self, NUMBER_MAX_BYTES) || morelen > self->len) {
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
	if (len == 2) {
		int16_t v16;
		int64_t v64;
		memcpy(&v16, buf, sizeof(v16));
		v64 = v16;
		HC_CHECK(hash_int64(&v64));
		return pyInt_FromS32(v16);
	}
	if (len == 4) {
		int32_t v32;
		int64_t v64;
		memcpy(&v32, buf, sizeof(v32));
		v64 = v32;
		HC_CHECK(hash_int64(&v64));
		return pyInt_FromS32(v32);
	}
	if (len == 8) {
		int64_t v;
		memcpy(&v, buf, sizeof(v));
		HC_CHECK(hash_int64(&v));
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

static PyObject *ReadDateTime_iternext(Read *self)
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

static PyObject *ReadDate_iternext(Read *self)
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

static PyObject *ReadTime_iternext(Read *self)
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

static PyObject *any_exit(PyObject *self, PyObject *args)
{
	return PyObject_CallMethod(self, "close", NULL);
}

static PyMethodDef Read_methods[] = {
	{"__enter__", (PyCFunction)Read_self , METH_NOARGS , NULL},
	{"__exit__",  (PyCFunction)any_exit  , METH_VARARGS, NULL},
	{"close",     (PyCFunction)Read_close, METH_NOARGS , NULL},
	{NULL, NULL, 0, NULL}
};

#define MKTYPE(name)                                                 	\
	static PyTypeObject name ## _Type = {                        	\
		PyVarObject_HEAD_INIT(NULL, 0)                       	\
		#name,                          /*tp_name          */	\
		sizeof(Read),                   /*tp_basicsize     */	\
		0,                              /*tp_itemsize      */	\
		(destructor)Read_dealloc,       /*tp_dealloc       */	\
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
		(getiterfunc)Read_self,         /*tp_iter          */	\
		(iternextfunc)name ## _iternext,/*tp_iternext      */	\
		Read_methods,                   /*tp_methods       */	\
		r_default_members,              /*tp_members       */	\
		0,                              /*tp_getset        */	\
		0,                              /*tp_base          */	\
		0,                              /*tp_dict          */	\
		0,                              /*tp_descr_get     */	\
		0,                              /*tp_descr_set     */	\
		0,                              /*tp_dictoffset    */	\
		Read_init,                      /*tp_init          */	\
		PyType_GenericAlloc,            /*tp_alloc         */	\
		PyType_GenericNew,              /*tp_new           */	\
		PyObject_Del,                   /*tp_free          */	\
		0,                              /*tp_is_gc         */	\
	}
static PyMemberDef r_default_members[] = {
	{"name"      , T_STRING   , offsetof(Read, name       ), READONLY},
	{"hashfilter", T_OBJECT_EX, offsetof(Read, hashfilter ), READONLY},
	{0}
};
MKTYPE(ReadBytes);
MKTYPE(ReadAscii);
MKTYPE(ReadUnicode);
MKTYPE(ReadNumber);
MKTYPE(ReadComplex64);
MKTYPE(ReadComplex32);
MKTYPE(ReadFloat64);
MKTYPE(ReadFloat32);
MKTYPE(ReadInt64);
MKTYPE(ReadInt32);
MKTYPE(ReadBits64);
MKTYPE(ReadBits32);
MKTYPE(ReadBool);
MKTYPE(ReadDateTime);
MKTYPE(ReadDate);
MKTYPE(ReadTime);


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

typedef struct write {
	PyObject_HEAD
	void *ctx;
	const dsu_compressor *compressor;
	char *name;
	char *error_extra;
	default_u *default_value;
	unsigned PY_LONG_LONG count;
	PyObject *hashfilter;
	PyObject *compression;
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
	char buf[Z];
} Write;

static char * const default_error_extra = "";

static int Write_ensure_open(Write *self)
{
	if (self->ctx) return 0;
	if (self->closed) {
		(void) err_closed();
		return 1;
	}
	int fd = open(self->name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd < 0) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		return 1;
	}
	self->ctx = self->compressor->write_open(fd);
	if (!self->ctx) {
		close(fd);
		PyErr_Format(PyExc_IOError, "failed to init compression for \"%s\"", self->name);
		return 1;
	}
	return 0;
}

static int Write_flush_(Write *self)
{
	if (!self->len) return 0;
	if (Write_ensure_open(self)) return 1;
	const int len = self->len;
	self->len = 0;
	if (self->compressor->write(self->ctx, self->buf, len)) {
		PyErr_SetString(PyExc_IOError, "Write failed");
		return 1;
	}
	return 0;
}

static PyObject *Write_flush(Write *self)
{
	if (Write_ensure_open(self)) return 0;
	if (Write_flush_(self)) return 0;
	Py_RETURN_NONE;
}

static int Write_close_(Write *self)
{
	if (self->default_value) {
		free(self->default_value);
		self->default_value = 0;
	}
	FREE(self->name);
	if (self->error_extra != default_error_extra) FREE(self->error_extra);
	Py_CLEAR(self->hashfilter);
	Py_CLEAR(self->default_obj);
	Py_CLEAR(self->min_obj);
	Py_CLEAR(self->max_obj);
	if (self->closed) return 1;
	if (!self->ctx) return 0;
	int err = Write_flush_(self);
	err |= self->compressor->write_close(self->ctx);
	self->ctx = 0;
	self->closed = 1;
	return err;
}

static int Write_parse_compression(Write *self, PyObject *compression)
{
	int idx = parse_compression(compression);
	if (idx == -1) return 1;
	self->compressor = compression_funcs[idx];
	self->compression = compression_names[idx];
	return 0;
}

static int init_WriteBlob(PyObject *self_, PyObject *args, PyObject *kwds)
{
	Write *self = (Write *)self_;
	PyObject *compression = 0;
	char *name = 0;
	char *error_extra = default_error_extra;
	PyObject *hashfilter = 0;
	Write_close_(self);
	static char *kwlist[] = {
		"name", "compression", "hashfilter",
		"error_extra", "none_support", 0
	};
	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "et|OOeti", kwlist,
		Py_FileSystemDefaultEncoding, &name,
		&compression,
		&hashfilter,
		Py_FileSystemDefaultEncoding, &error_extra,
		&self->none_support
	)) return -1;
	self->name = name;
	self->error_extra = error_extra;
	err1(Write_parse_compression(self, compression));
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

#define init_WriteBytes   init_WriteBlob
#define init_WriteAscii   init_WriteBlob
#define init_WriteUnicode init_WriteBlob

static void Write_dealloc(Write *self)
{
	Write_close_(self);
	PyObject_Del(self);
}

static PyObject *Write_close(Write *self)
{
	if (Write_flush_(self)) return 0;
	if (Write_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *Write_self(Write *self)
{
	if (self->closed) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static PyObject *Write_write_(Write *self, const char *data, Py_ssize_t len)
{
	if (len + self->len > Z) {
		if (Write_flush_(self)) return 0;
	}
	while (len > Z) {
		if (self->compressor->write(self->ctx, data, Z)) {
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

#define WRITE_NONE_SLICE_CHECK do {                                                   	\
	if (!self->none_support) {                                                    	\
		PyErr_Format(PyExc_ValueError,                                        	\
			"Refusing to write None value without none_support=True%s",   	\
			self->error_extra                                             	\
		);                                                                    	\
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

#define ASCIIVERIFY(cleanup, error_extra) \
	const unsigned char * const data_ = (unsigned char *)data;                    	\
	for (Py_ssize_t i = 0; i < len; i++) {                                        	\
		if (data_[i] > 127) {                                                 	\
			cleanup;                                                      	\
			if (len < 1000) {                                             	\
				PyErr_Format(PyExc_ValueError,                        	\
				             "Value contains %d at position %ld%s: %s",	\
				             data_[i], (long) i, error_extra, data);  	\
			} else {                                                      	\
				PyErr_Format(PyExc_ValueError,                        	\
				             "Value contains %d at position %ld%s",   	\
				             data_[i], (long) i, error_extra);        	\
			}                                                             	\
			return 0;                                                     	\
		}                                                                     	\
	}
#define ASCIIHASHDO(cleanup) \
	ASCIIVERIFY(cleanup, "");                                                     	\
	HASHBLOBDO(cleanup);

#if PY_MAJOR_VERSION < 3
#  define UNICODEBLOB(WRITEMACRO) \
	PyObject *strobj = PyUnicode_AsUTF8String(obj);              	\
	if (!strobj) return 0;                                       	\
	const char *data = PyBytes_AS_STRING(strobj);                	\
	const Py_ssize_t len = PyBytes_GET_SIZE(strobj);             	\
	WRITEMACRO(Py_DECREF(strobj));
#else
#  define UNICODEBLOB(WRITEMACRO) \
	Py_ssize_t len;                                              	\
	const char *data = PyUnicode_AsUTF8AndSize(obj, &len);       	\
	if (!data) return 0;                                         	\
	WRITEMACRO((void)data);
#endif

#define HASHBLOBPROLOGUE(checktype, errname) \
	if (obj == Py_None) return PyInt_FromLong(0);                                         	\
	if (checktype) {                                                                      	\
		PyErr_SetString(PyExc_TypeError,                                              	\
		                "For your protection, only " errname " objects are accepted");	\
		return 0;                                                                     	\
	}
#define HASHBLOBDO(cleanup) \
	PyObject *res = pyInt_FromU64(hash(data, len));              	\
	cleanup;                                                     	\
	return res;

static PyObject *hash_WriteBytes(PyObject *dummy, PyObject *obj)
{
	HASHBLOBPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	HASHBLOBDO((void)data);
}

static PyObject *hash_WriteAscii(PyObject *dummy, PyObject *obj)
{
	HASHBLOBPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	Py_ssize_t len;
	const char *data;
	if (PyBytes_Check(obj)) {
		len = PyBytes_GET_SIZE(obj);
		data = PyBytes_AS_STRING(obj);
		ASCIIHASHDO((void)data);
	} else { // Must be Unicode
		UNICODEBLOB(ASCIIHASHDO);
	}
}

static PyObject *hash_WriteUnicode(PyObject *dummy, PyObject *obj)
{
	HASHBLOBPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	UNICODEBLOB(HASHBLOBDO);
}

#define WRITEBLOBPROLOGUE(checktype, errname) \
	if (obj == Py_None) {                                                         	\
		WRITE_NONE_SLICE_CHECK;                                               	\
		self->count++;                                                        	\
		return Write_write_(self, "\xff\x00\x00\x00\x00", 5);                 	\
	}                                                                             	\
	if (checktype) {                                                              	\
		PyErr_Format(PyExc_TypeError,                                         	\
		             "For your protection, only " errname                     	\
		             " objects are accepted%s (line %llu)",                   	\
		             self->error_extra,                                       	\
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
		ret = Write_write_(self, (char *)&short_len, 1);                      	\
	} else {                                                                      	\
		if (len > 0x7fffffff) {                                               	\
			cleanup;                                                      	\
			PyErr_Format(PyExc_ValueError, "Value too large%s", self->error_extra); \
			return 0;                                                     	\
		}                                                                     	\
		uint32_t long_len = len;                                              	\
		uint8_t lenbuf[5];                                                    	\
		lenbuf[0] = 255;                                                      	\
		memcpy(lenbuf + 1, &long_len, 4);                                     	\
		ret = Write_write_(self, (char *)lenbuf, 5);                          	\
	}                                                                             	\
	if (!ret) {                                                                   	\
		cleanup;                                                              	\
		return 0;                                                             	\
	}                                                                             	\
	Py_DECREF(ret);                                                               	\
	ret = Write_write_(self, data, len);                                          	\
	cleanup;                                                                      	\
	if (!ret) return 0;                                                           	\
	self->count++;                                                                	\
	return ret;

#define ASCIIBLOBDO(cleanup) \
	ASCIIVERIFY(cleanup, self->error_extra);                                      	\
	WRITEBLOBDO(cleanup);

static PyObject *C_WriteBytes(Write *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	const char *data = PyBytes_AS_STRING(obj);
	WRITEBLOBDO((void)data);
}

static PyObject *C_WriteAscii(Write *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	if (PyBytes_Check(obj)) {
		const Py_ssize_t len = PyBytes_GET_SIZE(obj);
		const char *data = PyBytes_AS_STRING(obj);
		ASCIIBLOBDO((void)data);
	} else { // Must be Unicode
		UNICODEBLOB(ASCIIBLOBDO);
	}
}

static PyObject *C_WriteUnicode(Write *self, PyObject *obj, int actually_write)
{
	WRITEBLOBPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	UNICODEBLOB(WRITEBLOBDO);
}

#define MKWBLOB(name)                                                                               	\
	static PyObject *write_Write ## name (Write *self, PyObject *obj)                           	\
	{                                                                                           	\
		return C_Write ## name (self, obj, 1);                                              	\
	}                                                                                           	\
	static PyObject *hashcheck_Write ## name (Write *self, PyObject *obj)                       	\
	{                                                                                           	\
		if (!self->slices) {                                                                	\
			PyErr_Format(PyExc_ValueError, "No hashfilter set%s", self->error_extra);   	\
			return 0;                                                                   	\
		}                                                                                   	\
		return C_Write ## name (self, obj, 0);                                              	\
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
	static int init_ ## tname(PyObject *self_, PyObject *args, PyObject *kwds)       	\
	{                                                                                	\
		static char *kwlist[] = {                                                	\
			"name", "compression", "default", "hashfilter",                  	\
			"error_extra", "none_support", 0                                 	\
		};                                                                       	\
		Write *self = (Write *)self_;                                            	\
		char *name = 0;                                                          	\
		char *error_extra = default_error_extra;                                 	\
		PyObject *compression = 0;                                               	\
		PyObject *default_obj = 0;                                               	\
		PyObject *hashfilter = 0;                                                	\
		Write_close_(self);                                                      	\
		if (!PyArg_ParseTupleAndKeywords(                                        	\
			args, kwds, "et|OOOeti", kwlist,                                 	\
			Py_FileSystemDefaultEncoding, &name,                             	\
			&compression,                                                    	\
			&default_obj,                                                    	\
			&hashfilter,                                                     	\
			Py_FileSystemDefaultEncoding, &error_extra,                      	\
			&self->none_support                                              	\
		)) return -1;                                                            	\
		if (!withnone && self->none_support) {                                   	\
			PyErr_Format(PyExc_ValueError, "%s objects don't support None values%s", self_->ob_type->tp_name, error_extra); \
			return -1;                                                       	\
		}                                                                        	\
		self->name = name;                                                       	\
		self->error_extra = error_extra;                                         	\
		err1(Write_parse_compression(self, compression));                        	\
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
					PyErr_Format(PyExc_OverflowError, "Default value becomes None-marker%s", error_extra); \
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
		self->closed = 0;                                                        	\
		self->count = 0;                                                         	\
		self->len = 0;                                                           	\
		return 0;                                                                	\
err:                                                                                     	\
		return -1;                                                               	\
	}                                                                                	\
	static PyObject *C_ ## tname(Write *self, PyObject *obj, int actually_write)     	\
	{                                                                                	\
		if (withnone && obj == Py_None) {                                        	\
is_none:                                                                                 	\
			WRITE_NONE_SLICE_CHECK;                                          	\
			self->count++;                                                   	\
			return Write_write_(self, (char *)&noneval_ ## T, sizeof(T));    	\
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
			if (!self->default_value) {                                      	\
				add_extra_to_exc_msg(self->error_extra);                 	\
				return 0;                                                	\
			}                                                                	\
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
		return Write_write_(self, (char *)&value, sizeof(value));                	\
	}                                                                                	\
	static PyObject *write_ ## tname(Write *self, PyObject *obj)                     	\
	{                                                                                	\
		return C_ ## tname(self, obj, 1);                                        	\
	}                                                                                	\
	static PyObject *hashcheck_ ## tname(Write *self, PyObject *obj)                 	\
	{                                                                                	\
		if (!self->slices) {                                                     	\
			PyErr_Format(PyExc_ValueError, "No hashfilter set%s", self->error_extra); \
			return 0;                                                        	\
		}                                                                        	\
		return C_ ## tname(self, obj, 0);                                        	\
	}                                                                                	\
	static PyObject *hash_ ## tname(PyObject *dummy, PyObject *obj)                  	\
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

MKWRITER_C(WriteComplex64, complex64, complex64, PyComplex_AsCComplex  , 1, value.real == -1.0, MINMAX_DUMMY, ,                   , hash_complex64);
MKWRITER_C(WriteComplex32, complex32, complex32, pyComplex_AsCComplex32, 1, value.real == -1.0, MINMAX_DUMMY, ,                   , hash_complex32);
MKWRITER_C(WriteFloat64  , double   , double   , PyFloat_AsDouble      , 1, value == -1.0     , MINMAX_FLOAT, , minmax_set_Float64, hash_double   );
MKWRITER_C(WriteFloat32  , float    , double   , PyFloat_AsDouble      , 1, value == -1.0     , MINMAX_FLOAT, , minmax_set_Float32, hash_double   );
MKWRITER(WriteInt64      , int64_t  , int64_t  , pyLong_AsS64          , 1,                                   , minmax_set_Int64  , hash_int64    );
MKWRITER(WriteInt32      , int32_t  , int64_t  , pyLong_AsS32          , 1,                                   , minmax_set_Int32  , hash_int64    );
MKWRITER(WriteBits64     , uint64_t , uint64_t , pyLong_AsU64          , 0,                                   , minmax_set_Bits64 , hash_uint64   );
MKWRITER(WriteBits32     , uint32_t , uint64_t , pyLong_AsU32          , 0,                                   , minmax_set_Bits32 , hash_uint64   );
MKWRITER(WriteBool       , uint8_t  , uint8_t  , pyLong_AsBool         , 1,                                   , minmax_set_Bool   , hash_bool     );
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
MKWRITER_C(WriteDateTime, uint64_t, uint64_t, fmt_datetime, 1, !value, MINMAX_STD, minmax_value_datetime, minmax_set_DateTime, hash_datetime);
MKWRITER_C(WriteDate    , uint32_t, uint32_t, fmt_date,     1, !value, MINMAX_STD,                      , minmax_set_Date    , hash_32bits  );
MKWRITER_C(WriteTime    , uint64_t, uint64_t, fmt_time,     1, !value, MINMAX_STD, minmax_value_datetime, minmax_set_Time    , hash_datetime);

static int WriteNumber_serialize_Long(PyObject *obj, char *buf, const char *msg, const char *error_extra)
{
	PyErr_Clear();
	const size_t len_bits = _PyLong_NumBits(obj);
	if (len_bits == (size_t)-1 && PyErr_Occurred()) return 1;
	const size_t len_bytes = len_bits / 8 + 1;
	if (len_bytes >= NUMBER_MAX_BYTES) {
		PyErr_Format(PyExc_OverflowError,
		             "%s does not fit in %d bytes%s",
		             msg, NUMBER_MAX_BYTES, error_extra
		            );
		return 1;
	}
	buf[0] = len_bytes;
	unsigned char *ptr = (unsigned char *)buf + 1;
	PyLongObject *lobj = (PyLongObject *)obj;
	return _PyLong_AsByteArray(lobj, ptr, len_bytes, 1, 1) < 0;
}

static int init_WriteNumber(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {
		"name", "compression", "default", "hashfilter",
		"error_extra", "none_support", 0
	};
	Write *self = (Write *)self_;
	char *name = 0;
	char *error_extra = default_error_extra;
	PyObject *compression = 0;
	PyObject *default_obj = 0;
	PyObject *hashfilter = 0;
	Write_close_(self);
	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "et|OOOeti", kwlist,
		Py_FileSystemDefaultEncoding, &name,
		&compression,
		&default_obj,
		&hashfilter,
		Py_FileSystemDefaultEncoding, &error_extra,
		&self->none_support
	)) return -1;
	self->name = name;
	self->error_extra = error_extra;
	err1(Write_parse_compression(self, compression));
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
				PyErr_Format(PyExc_ValueError, "Bad default value: Only integers/floats accepted%s", error_extra);
				goto err;
			}
			if (PyLong_Check(self->default_obj)) {
				char buf[NUMBER_MAX_BYTES];
				err1(WriteNumber_serialize_Long(self->default_obj, buf, "Bad default value:", error_extra));
			}
		}
	}
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	self->closed = 0;
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

static void Write_obj_minmax(Write *self, PyObject *obj)
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

static PyObject *C_WriteNumber(Write *self, PyObject *obj, int actually_write, int first)
{
	if (obj == Py_None) {
		WRITE_NONE_SLICE_CHECK;
		self->count++;
		return Write_write_(self, "", 1);
	}
	if (PyFloat_Check(obj)) {
		const double value = PyFloat_AS_DOUBLE(obj);
		if (self->slices) {
			const unsigned int sliceno = hash_double(&value) % self->slices;
			if (sliceno != self->sliceno) Py_RETURN_FALSE;
		}
		if (!actually_write) Py_RETURN_TRUE;
		Write_obj_minmax(self, obj);
		char buf[9];
		buf[0] = 1;
		memcpy(buf + 1, &value, 8);
		self->count++;
		return Write_write_(self, buf, 9);
	}
	if (first && !Integer_Check(obj)) {
		if (first && self->default_obj) {
			return C_WriteNumber(self, self->default_obj, actually_write, 0);
		}
		PyErr_Format(PyExc_ValueError, "Only integers/floats accepted%s", self->error_extra);
		return 0;
	}
	const int64_t value = pyLong_AsS64(obj);
	char buf[NUMBER_MAX_BYTES];
	if (value != -1 || !PyErr_Occurred()) {
		if (self->slices) {
			const unsigned int sliceno = hash_int64(&value) % self->slices;
			if (sliceno != self->sliceno) Py_RETURN_FALSE;
		}
		if (!actually_write) Py_RETURN_TRUE;
		Write_obj_minmax(self, obj);
		if (value <= 122 && value >= -5) {
			uint8_t u8 = 0x80 | (value + 5);
			self->count++;
			return Write_write_(self, (char *)&u8, 1);
		}
		if (value <= INT16_MAX && value >= INT16_MIN) {
			buf[0] = 2;
			int16_t value16 = value;
			memcpy(buf + 1, &value16, 2);
			self->count++;
			return Write_write_(self, buf, 3);
		}
		if (value <= INT32_MAX && value >= INT32_MIN) {
			buf[0] = 4;
			int32_t value32 = value;
			memcpy(buf + 1, &value32, 4);
			self->count++;
			return Write_write_(self, buf, 5);
		}
		buf[0] = 8;
		memcpy(buf + 1, &value, 8);
		self->count++;
		return Write_write_(self, buf, 9);
	}
	if (WriteNumber_serialize_Long(obj, buf, "Value", self->error_extra)) {
		if (first && self->default_obj) {
			PyErr_Clear();
			return C_WriteNumber(self, self->default_obj, actually_write, 0);
		} else {
			return 0;
		}
	}
	if (self->slices) {
		const unsigned int sliceno = hash(buf + 1, buf[0]) % self->slices;
		if (sliceno != self->sliceno) Py_RETURN_FALSE;
	}
	if (!actually_write) Py_RETURN_TRUE;
	Write_obj_minmax(self, obj);
	self->count++;
	return Write_write_(self, buf, buf[0] + 1);
}
static PyObject *write_WriteNumber(Write *self, PyObject *obj)
{
	return C_WriteNumber(self, obj, 1, 1);
}
static PyObject *hashcheck_WriteNumber(Write *self, PyObject *obj)
{
	if (!self->slices) {
		PyErr_SetString(PyExc_ValueError, "No hashfilter set");
		return 0;
	}
	return C_WriteNumber(self, obj, 0, 1);
}
static PyObject *hash_WriteNumber(PyObject *dummy, PyObject *obj)
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
			h = hash_int64(&value);
		} else {
			char buf[NUMBER_MAX_BYTES];
			if (WriteNumber_serialize_Long(obj, buf, "Value", "")) return 0;
			h = hash(buf + 1, buf[0]);
		}
		return pyInt_FromU64(h);
	}
}

static int init_WriteParsedNumber(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {
		"name", "compression", "default", "hashfilter",
		"error_extra", "none_support", 0
	};
	PyObject *name = 0;
	PyObject *error_extra = 0;
	PyObject *compression = 0;
	PyObject *default_obj_ = 0;
	PyObject *default_obj = 0;
	PyObject *hashfilter = 0;
	PyObject *none_support = 0;
	PyObject *new_args = 0;
	PyObject *new_kwds = 0;
	int res = -1;
	err1(!PyArg_ParseTupleAndKeywords(
		args, kwds, "O|OOOOO", kwlist,
		&name,
		&compression,
		&default_obj_,
		&hashfilter,
		&error_extra,
		&none_support
	));
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
	if (compression) err1(PyDict_SetItemString(new_kwds, "compression", compression));
	if (default_obj) err1(PyDict_SetItemString(new_kwds, "default", default_obj));
	if (hashfilter) err1(PyDict_SetItemString(new_kwds, "hashfilter", hashfilter));
	if (error_extra) err1(PyDict_SetItemString(new_kwds, "error_extra", error_extra));
	if (none_support) err1(PyDict_SetItemString(new_kwds, "none_support", none_support));
	res = init_WriteNumber(self_, new_args, new_kwds);
err:
	Py_XDECREF(new_kwds);
	Py_XDECREF(new_args);
	Py_XDECREF(default_obj);
	return res;
}

#define MKPARSEDNUMBERWRAPPER(name, selftype) \
	static PyObject *name ## _WriteParsedNumber(selftype *self, PyObject *obj)                 	\
	{                                                                                          	\
		if (PyFloat_Check(obj) || PyLong_Check(obj) || obj == Py_None) {                   	\
			return name ## _WriteNumber(self, obj);                                    	\
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
		PyObject *res = name ## _WriteNumber(self, tmp);                                   	\
		Py_DECREF(tmp);                                                                    	\
		return res;                                                                        	\
	}
MKPARSEDNUMBERWRAPPER(write, Write)
MKPARSEDNUMBERWRAPPER(hashcheck, Write)
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
	MKWRITER_C(WriteParsed ## name, T, HT, parse ## name, withnone, errchk, do_minmax, , minmax_set, hash)
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
MKPARSED_C(Complex64, complex64, complex64, pyComplex_parse, PyComplex_AsCComplex  , 1, value.real == -1.0, complex64_error, MINMAX_DUMMY, , hash_complex64);
MKPARSED_C(Complex32, complex32, complex32, pyComplex_parse, pyComplex_AsCComplex32, 1, value.real == -1.0, complex32_error, MINMAX_DUMMY, , hash_complex32);
MKPARSED_C(Float64, double  , double  , PyNumber_Float, PyFloat_AsDouble , 1, value == -1.0, -1, MINMAX_FLOAT, minmax_set_Float64, hash_double);
MKPARSED_C(Float32, float   , double  , PyNumber_Float, PyFloat_AsDouble , 1, value == -1.0, -1, MINMAX_FLOAT, minmax_set_Float32, hash_double);
MKPARSED(Int64  , int64_t , int64_t , PyNumber_Int  , pyLong_AsS64     , 1, minmax_set_Int64  , hash_int64);
MKPARSED(Int32  , int32_t , int64_t , PyNumber_Int  , pyLong_AsS32     , 1, minmax_set_Int32  , hash_int64);
MKPARSED(Bits64 , uint64_t, uint64_t, PyNumber_Long , pyLong_AsU64     , 0, minmax_set_Bits64 , hash_uint64);
MKPARSED(Bits32 , uint32_t, uint64_t, PyNumber_Int  , pyLong_AsU32     , 0, minmax_set_Bits32 , hash_uint64);

static PyMemberDef w_default_members[] = {
	{"name"      , T_STRING   , offsetof(Write, name       ), READONLY},
	{"count"     , T_ULONGLONG, offsetof(Write, count      ), READONLY},
	{"hashfilter", T_OBJECT_EX, offsetof(Write, hashfilter ), READONLY},
	{"min"       , T_OBJECT   , offsetof(Write, min_obj    ), READONLY},
	{"max"       , T_OBJECT   , offsetof(Write, max_obj    ), READONLY},
	{"default"   , T_OBJECT_EX, offsetof(Write, default_obj), READONLY},
	{"compression",T_OBJECT_EX, offsetof(Write, compression), READONLY},
	{0}
};

#define MKWTYPE_i(name, methods, members)                            	\
	static PyTypeObject name ## _Type = {                        	\
		PyVarObject_HEAD_INIT(NULL, 0)                       	\
		#name,                          /*tp_name*/          	\
		sizeof(Write),                  /*tp_basicsize*/     	\
		0,                              /*tp_itemsize*/      	\
		(destructor)Write_dealloc,      /*tp_dealloc*/       	\
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
		init_ ## name,                  /*tp_init*/          	\
		PyType_GenericAlloc,            /*tp_alloc*/         	\
		PyType_GenericNew,              /*tp_new*/           	\
		PyObject_Del,                   /*tp_free*/          	\
		0,                              /*tp_is_gc*/         	\
	}
#define MKWTYPE(name)                                                                	\
	static PyMethodDef name ## _methods[] = {                                             	\
		{"__enter__", (PyCFunction)Write_self         , METH_NOARGS         , NULL},  	\
		{"__exit__",  (PyCFunction)any_exit           , METH_VARARGS        , NULL},  	\
		{"write",     (PyCFunction)write_ ## name     , METH_O              , NULL},  	\
		{"flush",     (PyCFunction)Write_flush        , METH_NOARGS         , NULL},  	\
		{"close",     (PyCFunction)Write_close        , METH_NOARGS         , NULL},  	\
		{"hashcheck", (PyCFunction)hashcheck_ ## name , METH_O              , NULL},  	\
		{"hash"     , (PyCFunction)hash_## name       , METH_STATIC | METH_O, NULL},  	\
		{0}                                                                           	\
	};                                                                                    	\
	MKWTYPE_i(name, name ## _methods, w_default_members);
MKWTYPE(WriteBytes);
MKWTYPE(WriteAscii);
MKWTYPE(WriteUnicode);
MKWTYPE(WriteComplex64);
MKWTYPE(WriteComplex32);
MKWTYPE(WriteFloat64);
MKWTYPE(WriteFloat32);
MKWTYPE(WriteNumber);
MKWTYPE(WriteInt64);
MKWTYPE(WriteInt32);
MKWTYPE(WriteBits64);
MKWTYPE(WriteBits32);
MKWTYPE(WriteBool);
MKWTYPE(WriteDateTime);
MKWTYPE(WriteDate);
MKWTYPE(WriteTime);

MKWTYPE(WriteParsedNumber);
MKWTYPE(WriteParsedComplex64);
MKWTYPE(WriteParsedComplex32);
MKWTYPE(WriteParsedFloat64);
MKWTYPE(WriteParsedFloat32);
MKWTYPE(WriteParsedInt64);
MKWTYPE(WriteParsedInt32);
MKWTYPE(WriteParsedBits64);
MKWTYPE(WriteParsedBits32);

static PyObject *generic_hash(PyObject *dummy, PyObject *obj)
{
	if (obj == Py_None)        return PyInt_FromLong(0);
	if (PyBytes_Check(obj))    return hash_WriteBytes(0, obj);
	if (PyUnicode_Check(obj))  return hash_WriteUnicode(0, obj);
	if (PyFloat_Check(obj))    return hash_WriteFloat64(0, obj);
	if (PyBool_Check(obj))     return hash_WriteBool(0, obj);
	if (Integer_Check(obj)) {
		return hash_WriteNumber(0, obj);
	}
	if (PyDateTime_Check(obj)) return hash_WriteDateTime(0, obj);
	if (PyDate_Check(obj))     return hash_WriteDate(0, obj);
	if (PyTime_Check(obj))     return hash_WriteTime(0, obj);
	if (PyComplex_Check(obj))  return hash_WriteComplex64(0, obj);
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
	"_dsutil",          /*m_name*/
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
	PyObject *m = Py_InitModule3("_dsutil", module_methods, NULL);
#endif
	if (!m) return INITERR;
	INIT(ReadBytes);
	INIT(ReadUnicode);
	INIT(ReadAscii);
	INIT(ReadNumber);
	INIT(ReadComplex64);
	INIT(ReadComplex32);
	INIT(ReadFloat64);
	INIT(ReadFloat32);
	INIT(ReadInt64);
	INIT(ReadInt32);
	INIT(ReadBits64);
	INIT(ReadBits32);
	INIT(ReadBool);
	INIT(ReadDateTime);
	INIT(ReadDate);
	INIT(ReadTime);
	INIT(WriteBytes);
	INIT(WriteUnicode);
	INIT(WriteAscii);
	INIT(WriteNumber);
	INIT(WriteComplex64);
	INIT(WriteComplex32);
	INIT(WriteFloat64);
	INIT(WriteFloat32);
	INIT(WriteInt64);
	INIT(WriteInt32);
	INIT(WriteBits64);
	INIT(WriteBits32);
	INIT(WriteBool);
	INIT(WriteDateTime);
	INIT(WriteDate);
	INIT(WriteTime);
	INIT(WriteParsedNumber);
	INIT(WriteParsedComplex64);
	INIT(WriteParsedComplex32);
	INIT(WriteParsedFloat64);
	INIT(WriteParsedFloat32);
	INIT(WriteParsedInt64);
	INIT(WriteParsedInt32);
	INIT(WriteParsedBits64);
	INIT(WriteParsedBits32);
	compression_dict = PyDict_New();
	if (!compression_dict) return INITERR;
	compression_funcs[1] = &dsu_gz;
	compression_names[1] = PyUnicode_FromString("gzip");
	if (PyDict_SetItem(compression_dict, compression_names[1], PyInt_FromLong(1))) return INITERR;
#if PY_MAJOR_VERSION >= 3
	return m;
#endif
}
