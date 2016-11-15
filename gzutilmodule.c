#define PY_SSIZE_T_CLEAN 1
#include <Python.h>
#include <bytesobject.h>
#include <datetime.h>
#include <structmember.h>

#include <zlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>


// Must be a multiple of the largest fixed size type
#define Z (128 * 1024)

// Up to +-(2**1007 - 1). Don't increase this.
#define GZNUMBER_MAX_BYTES 127

#define IDSTR_NUMBER   "\xff""Number\0"
#define IDSTR_DATETIME "\xff""DtTm\0\0\0"
#define IDSTR_DATE     "\xff""Date\0\0\0"
#define IDSTR_TIME     "\xff""Time\0\0\0"
#define IDSTR_BOOL     "\xff""Bool\0\0\0"

#define BOM_STR "\xef\xbb\xbf"

#define err1(v) if (v) goto err

typedef struct gzread {
	PyObject_HEAD
	char *name;
	char *encoding;
	char *errors;
	PyObject *(*decodefunc)(const char *, Py_ssize_t, const char *);
	PyObject *hashfilter;
	PY_LONG_LONG max_count;
	PY_LONG_LONG count;
	uint64_t spread_None;
	gzFile fh;
	int error;
	int pos, len;
	int sliceno;
	int slices;
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
	self->max_count = -1;
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

static const uint8_t hash_k[16] = {94, 70, 175, 255, 152, 30, 237, 97, 252, 125, 174, 76, 165, 112, 16, 9};

int siphash(uint8_t *out, const uint8_t *in, uint64_t inlen, const uint8_t *k);
static uint64_t hash(const void *ptr, const uint64_t len)
{
	uint64_t res;
	if (!len) return 0;
	siphash((uint8_t *)&res, ptr, len, hash_k);
	return res;
}
static uint64_t hash_64bits(const void *ptr)
{
	return hash(ptr, 8);
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

static int parse_hashfilter(PyObject *hashfilter, PyObject **r_hashfilter, int *r_sliceno, int *r_slices, uint64_t *r_spread_None)
{
	Py_CLEAR(*r_hashfilter);
	*r_slices = 0;
	*r_sliceno = 0;
	*r_spread_None = 0;
	if (!hashfilter || hashfilter == Py_None) return 0;
	int spread_None = 0;
	if (!PyArg_ParseTuple(hashfilter, "ii|i", r_sliceno, r_slices, &spread_None)) {
		PyErr_Clear();
		PyErr_SetString(PyExc_ValueError, "hashfilter should be a tuple (sliceno, slices) or (sliceno, slices, spread_None)");
		return 1;
	}
	if (*r_sliceno < 0 || *r_slices <= 0 || *r_sliceno >= *r_slices) {
		PyErr_Format(PyExc_ValueError, "Bad hashfilter (%d, %d)", *r_sliceno, *r_slices);
		return 1;
	}
	*r_spread_None = !!spread_None;
	*r_hashfilter = Py_BuildValue("(iiO)", *r_sliceno, *r_slices, spread_None ? Py_True : Py_False);
	return !*r_hashfilter;
}

static int gzread_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	int res = -1;
	GzRead *self = (GzRead *)self_;
	int strip_bom = 0;
	int fd = -1;
	PY_LONG_LONG seek = 0;
	PyObject *hashfilter = 0;
	gzread_close_(self);
	self->error = 0;
	if (self_->ob_type == &GzBytesLines_Type) {
		static char *kwlist[] = {"name", "strip_bom", "seek", "max_count", "hashfilter", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|iLLO", kwlist, Py_FileSystemDefaultEncoding, &self->name, &strip_bom, &seek, &self->max_count, &hashfilter)) return -1;
	} else if (self_->ob_type == &GzUnicodeLines_Type) {
		static char *kwlist[] = {"name", "encoding", "errors", "seek", "max_count", "hashfilter", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|etetLLO", kwlist, Py_FileSystemDefaultEncoding, &self->name, "ascii", &self->encoding, "ascii", &self->errors, &seek, &self->max_count, &hashfilter)) return -1;
	} else {
		static char *kwlist[] = {"name", "seek", "max_count", "hashfilter", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|LLO", kwlist, Py_FileSystemDefaultEncoding, &self->name, &seek, &self->max_count, &hashfilter)) return -1;
	}
	fd = open(self->name, O_RDONLY);
	if (fd < 0) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	if (lseek(fd, seek, 0) != seek) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	self->fh = gzdopen(fd, "rb");
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	gzbuffer(self->fh, (self->max_count < 0 ? 64 : 16) * 1024);
	fd = -1; // belongs to self->fh now
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
		if (self->decodefunc == PyUnicode_DecodeUTF8) strip_bom = 1;
	}
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	gzread_read_(self, 8);
	if (strip_bom) {
		if (self->len >= 3 && !memcmp(self->buf, BOM_STR, 3)) {
			self->pos = 3;
		}
	}
	if (self->len >= 8) {
		int typesig = 0;
		if (self_->ob_type == &GzNumber_Type   && !memcmp(self->buf, IDSTR_NUMBER  , 8)) typesig = 1;
		if (self_->ob_type == &GzDateTime_Type && !memcmp(self->buf, IDSTR_DATETIME, 8)) typesig = 2;
		if (self_->ob_type == &GzDate_Type     && !memcmp(self->buf, IDSTR_DATE    , 8)) typesig = 3;
		if (self_->ob_type == &GzTime_Type     && !memcmp(self->buf, IDSTR_TIME    , 8)) typesig = 4;
		if (self_->ob_type == &GzBool_Type     && !memcmp(self->buf, IDSTR_BOOL    , 8)) typesig = 5;
		if (typesig) self->pos = 8;
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
		if (self->max_count > 0) {
			PY_LONG_LONG count_left = self->max_count - self->count;
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
	if (self->len <= 0) return 1;
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
#define SIZE_double   8
#define SIZE_float    4
#define SIZE_int64_t  8
#define SIZE_int32_t  4
#define SIZE_uint64_t 8
#define SIZE_uint32_t 4
#define SIZE_uint8_t  1

#define ITERPROLOGUE(typename)                               	\
	do {                                                 	\
		if (!self->fh) return err_closed();          	\
		if (self->count == self->max_count) return 0;	\
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
#if PY_MAJOR_VERSION < 3
#  define mkAscii mkBytes
#else
#  define mkAscii mkUnicode
#endif

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

// These are signaling NaNs with extra DEADness in the significand
static const unsigned char noneval_double[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
static const unsigned char noneval_float[4] = {0xde, 0xad, 0x80, 0xff};

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
		const T res = *(T *)(self->buf + self->pos);                 	\
		self->pos += sizeof(T);                                      	\
		if (withnone && !memcmp(&res, &noneval_ ## T, sizeof(T))) {  	\
			HC_RETURN_NONE;                                      	\
		}                                                            	\
		if (self->slices) {                                          	\
			HT v = res;                                          	\
			HC_CHECK(hash(&v));                                  	\
		}                                                            	\
		return conv(res);                                            	\
	}

MKITER(GzFloat64, double  , PyFloat_FromDouble     , hash_double , double  , 1)
MKITER(GzFloat32, float   , PyFloat_FromDouble     , hash_double , double  , 1)
MKITER(GzInt64  , int64_t , PyInt_FromLong         , hash_integer, int64_t , 1)
MKITER(GzInt32  , int32_t , PyInt_FromLong         , hash_integer, int64_t , 1)
MKITER(GzBits64 , uint64_t, PyLong_FromUnsignedLong, hash_integer, uint64_t, 0)
MKITER(GzBits32 , uint32_t, PyLong_FromUnsignedLong, hash_integer, uint64_t, 0)
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
	unsigned char buf[len];
	const int avail = self->len - self->pos;
	if (avail >= len) {
		memcpy(buf, self->buf + self->pos, len);
		self->pos += len;
	} else {
		memcpy(buf, self->buf + self->pos, avail);
		unsigned char * const ptr = buf + avail;
		const int morelen = len - avail;
		if (gzread_read_(self, SIZE_Number) || morelen > self->len) {
			self->error = 1;
			PyErr_SetString(PyExc_ValueError, "File format error");
			return 0;
		}
		memcpy(ptr, self->buf, morelen);
		self->pos = morelen;
	}
	if (is_float) {
		double v = *(double *)buf;
		HC_CHECK(hash_double(&v));
		return PyFloat_FromDouble(v);
	}
	if (len == 8) {
		int64_t v = *(int64_t *)buf;
		HC_CHECK(hash_integer(&v));
		return PyInt_FromLong(v);
	}
	HC_CHECK(hash(buf, len));
	return _PyLong_FromByteArray(buf, len, 1, 1);
}

static inline PyObject *unfmt_datetime(const uint32_t i0, const uint32_t i1)
{
	if (!i0) Py_RETURN_NONE;
	const int Y = i0 >> 14;
	const int m = i0 >> 10 & 0x0f;
	const int d = i0 >> 5 & 0x1f;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
	return PyDateTime_FromDateAndTime(Y, m, d, H, M, S, u);
}

static PyObject *GzDateTime_iternext(GzRead *self)
{
	ITERPROLOGUE(DateTime);
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
	if (!i0) HC_RETURN_NONE;
	HC_CHECK(hash_64bits(self->buf + self->pos - 8));
	return unfmt_datetime(i0, i1);
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
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
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
	return PyTime_FromTime(H, M, S, u);
}

static PyObject *GzTime_iternext(GzRead *self)
{
	ITERPROLOGUE(Time);
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
	if (!i0) HC_RETURN_NONE;
	HC_CHECK(hash_64bits(self->buf + self->pos - 8));
	return unfmt_time(i0, i1);
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
	{"name"    , T_STRING, offsetof(GzRead, name    ), READONLY},
	{"encoding", T_STRING, offsetof(GzRead, encoding), READONLY},
	{"errors"  , T_STRING, offsetof(GzRead, errors  ), READONLY},
	{0}
};
MKTYPE(GzBytesLines, r_default_members);
MKTYPE(GzAsciiLines, r_default_members);
MKTYPE(GzUnicodeLines, r_unicode_members);
MKTYPE(GzNumber, r_default_members);
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


typedef struct gzwrite {
	PyObject_HEAD
	gzFile fh;
	char *name;
	void *default_value;
	unsigned long count;
	PyObject *hashfilter;
	PyObject *default_obj;
	PyObject *min_obj;
	PyObject *max_obj;
	/* These are declared as double (biggest), but stored as whatever */
	double   min_bin;
	double   max_bin;
	uint64_t spread_None;
	int sliceno;
	int slices;
	int len;
	char buf[Z];
} GzWrite;

static int gzwrite_flush_(GzWrite *self)
{
	if (!self->len) return 0;
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
	if (!self->fh) return err_closed();
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
	if (self->fh) {
		int err = gzwrite_flush_(self);
		err |= gzclose(self->fh);
		self->fh = 0;
		return err;
	}
	return 1;
}

static int gzwrite_init_GzWrite(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", 0};
	GzWrite *self = (GzWrite *)self_;
	char mode_buf[3] = {'w', 'b', 0};
	const char *mode = mode_buf;
	gzwrite_close_(self);
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|s", kwlist, Py_FileSystemDefaultEncoding, &self->name, &mode)) return -1;
	if ((mode[0] != 'w' && mode[0] != 'a') || (mode[1] != 'b' && mode[1] != 0)) {
		PyErr_Format(PyExc_IOError, "Bad mode '%s'", mode);
		goto err;
	}
	mode_buf[0] = mode[0]; // always [wa]b
	self->fh = gzopen(self->name, mode_buf);
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

static int gzwrite_init_GzWriteLines(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", "hashfilter", 0};
	GzWrite *self = (GzWrite *)self_;
	char mode_buf[3] = {'w', 'b', 0};
	const char *mode = mode_buf;
	PyObject *hashfilter = 0;
	gzwrite_close_(self);
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sO", kwlist, Py_FileSystemDefaultEncoding, &self->name, &mode, &hashfilter)) return -1;
	if ((mode[0] != 'w' && mode[0] != 'a') || (mode[1] != 'b' && mode[1] != 0)) {
		PyErr_Format(PyExc_IOError, "Bad mode '%s'", mode);
		goto err;
	}
	err1(parse_hashfilter(hashfilter, &self->hashfilter, &self->sliceno, &self->slices, &self->spread_None));
	mode_buf[0] = mode[0]; // always [wa]b
	self->fh = gzopen(self->name, mode_buf);
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	self->count = 0;
	self->len = 0;
	if (mode[0] == 'w' && self_->ob_type == &GzWriteUnicodeLines_Type) {
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
	if (!self->fh) return err_closed();
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
	if (self->spread_None) {                                                      	\
		const int spread_slice = self->spread_None % self->slices;            	\
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
		             " objects are accepted (line %ld)",                      	\
		             self->count + 1);                                        	\
		return 0;                                                             	\
	}
#define WRITELINEDO(cleanup) \
	if (len == 1 && *data == 0) {                                                 	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value becomes None-marker (line %ld)",                  	\
		             self->count + 1);                                        	\
		return 0;                                                             	\
	}                                                                             	\
	if (memchr(data, '\n', len)) {                                                	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value must not contain \\n (line %ld)",                 	\
		             self->count + 1);                                        	\
		return 0;                                                             	\
	}                                                                             	\
	if (data[len - 1] == '\r') {                                                  	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value must not end with \\r (line %ld)",                	\
		             self->count + 1);                                        	\
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
#define ASCIILINEDO(cleanup) \
	const unsigned char * const data_ = (unsigned char *)data;                    	\
	for (int i = 0; i < len; i++) {                                               	\
		if (data_[i] > 127) {                                                 	\
			cleanup;                                                      	\
			PyErr_Format(PyExc_ValueError,                                	\
			             "Value contains %d at position %d (line %ld): %s",	\
			             data_[i], i, self->count + 1, data);             	\
			return 0;                                                     	\
		}                                                                     	\
	}                                                                             	\
	WRITELINEDO(cleanup);
#define ASCIIHASHDO(cleanup) \
	const unsigned char * const data_ = (unsigned char *)data;                    	\
	for (int i = 0; i < len; i++) {                                               	\
		if (data_[i] > 127) {                                                 	\
			cleanup;                                                      	\
			PyErr_Format(PyExc_ValueError,                                	\
			             "Value contains %d at position %d: %s",          	\
			             data_[i], i, data);                              	\
			return 0;                                                     	\
		}                                                                     	\
	}                                                                             	\
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
	PyObject *res = PyLong_FromUnsignedLong(hash(data, len));    	\
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

static inline uint64_t minmax_value_datetime(uint64_t value) {
	/* My choice to use 2x u32 comes back to bite me. */
	struct { uint32_t i0, i1; } tmp;
	memcpy(&tmp, &value, sizeof(value));
	return ((uint64_t)tmp.i0 << 32) | tmp.i1;
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
MK_MINMAX_SET(Int64   , PyInt_FromLong(*(int64_t *)cmp_value));
MK_MINMAX_SET(Int32   , PyInt_FromLong(*(int32_t *)cmp_value));
MK_MINMAX_SET(Bits64  , PyLong_FromUnsignedLong(*(uint64_t *)cmp_value));
MK_MINMAX_SET(Bits32  , PyLong_FromUnsignedLong(*(uint32_t *)cmp_value));
MK_MINMAX_SET(Bool    , PyBool_FromLong(*(uint8_t *)cmp_value));
MK_MINMAX_SET(DateTime, unfmt_datetime((*(uint64_t *)cmp_value) >> 32, *(uint64_t *)cmp_value));
MK_MINMAX_SET(Date    , unfmt_date(*(uint32_t *)cmp_value));
MK_MINMAX_SET(Time    , unfmt_time((*(uint64_t *)cmp_value) >> 32, *(uint64_t *)cmp_value));

#define MKWRITER(tname, T, HT, conv, withnone, minmax_value, minmax_set, hash)           	\
	static int gzwrite_init_ ## tname(PyObject *self_, PyObject *args, PyObject *kwds)	\
	{                                                                                	\
		static char *kwlist[] = {"name", "mode", "default", "hashfilter", 0};    	\
		GzWrite *self = (GzWrite *)self_;                                        	\
		const char *mode = "wb";                                                 	\
		PyObject *hashfilter = 0;                                                	\
		gzwrite_close_(self);                                                    	\
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOO", kwlist, Py_FileSystemDefaultEncoding, &self->name, &mode, &self->default_obj, &hashfilter)) return -1; \
		if (self->default_obj) {                                                 	\
			T value;                                                         	\
			Py_INCREF(self->default_obj);                                    	\
			if (withnone && self->default_obj == Py_None) {                  	\
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
		self->fh = gzopen(self->name, mode);                                     	\
		if (!self->fh) {                                                         	\
			PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);       	\
			goto err;                                                        	\
		}                                                                        	\
		self->count = 0;                                                         	\
		self->len = 0;                                                           	\
		return 0;                                                                	\
err:                                                                                     	\
		return -1;                                                               	\
	}                                                                                	\
	static PyObject *gzwrite_C_ ## tname(GzWrite *self, PyObject *obj, int actually_write)	\
	{                                                                                	\
		if (withnone && obj == Py_None) {                                        	\
			WRITE_NONE_SLICE_CHECK;                                            	\
			self->count++;                                                   	\
			return gzwrite_write_(self, (char *)&noneval_ ## T, sizeof(T));  	\
		}                                                                        	\
		T value = conv(obj);                                                     	\
		if (withnone && !PyErr_Occurred() &&                                     	\
		    !memcmp(&value, &noneval_ ## T, sizeof(T))                           	\
		   ) {                                                                   	\
			PyErr_SetString(PyExc_OverflowError, "Value becomes None-marker");	\
		}                                                                        	\
		if (PyErr_Occurred()) {                                                  	\
			if (!self->default_value) return 0;                              	\
			PyErr_Clear();                                                   	\
			value = *(T *)self->default_value;                               	\
			obj = self->default_obj;                                         	\
		}                                                                        	\
		if (self->slices) {                                                      	\
			const HT h_value = value;                                        	\
			const int sliceno = hash(&h_value) % self->slices;               	\
			if (sliceno != self->sliceno) Py_RETURN_FALSE;                   	\
		}                                                                        	\
		if (!actually_write) Py_RETURN_TRUE;                                     	\
		if (obj && obj != Py_None) {                                             	\
			T cmp_value = minmax_value(value);                               	\
			if (!self->min_obj || (cmp_value < *(T *)&self->min_bin)) {      	\
				minmax_set(&self->min_obj, obj, &self->min_bin, &cmp_value, sizeof(cmp_value));	\
			}                                                                	\
			if (!self->max_obj || (cmp_value > *(T *)&self->max_bin)) {      	\
				minmax_set(&self->max_obj, obj, &self->max_bin, &cmp_value, sizeof(cmp_value));	\
			}                                                                	\
		}                                                                        	\
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
		return PyLong_FromUnsignedLong(h);                                       	\
	}
static uint64_t pylong_asuint64_t(PyObject *l)
{
	uint64_t value = PyLong_AsUnsignedLong(l);
	if (value == (uint64_t)-1 && PyErr_Occurred()) {
		PyErr_SetString(PyExc_OverflowError, "Value doesn't fit in 64 bits");
	}
	return value;
}
static int32_t pylong_asint32_t(PyObject *l)
{
	int64_t value = PyLong_AsLong(l);
	int32_t real_value = value;
	if (value != real_value) {
		PyErr_SetString(PyExc_OverflowError, "Value doesn't fit in 32 bits");
	}
	return value;
}
static uint32_t pylong_asuint32_t(PyObject *l)
{
	uint64_t value = pylong_asuint64_t(l);
	uint32_t real_value = value;
	if (value != real_value) {
		PyErr_SetString(PyExc_OverflowError, "Value doesn't fit in 32 bits");
	}
	return value;
}
static uint8_t pylong_asbool(PyObject *l)
{
	long value = PyLong_AsLong(l);
	if (value != 0 && value != 1) {
		PyErr_SetString(PyExc_OverflowError, "Value is not 0 or 1");
	}
	return value;
}
MKWRITER(GzWriteFloat64, double  , double  , PyFloat_AsDouble , 1, , minmax_set_Float64, hash_double );
MKWRITER(GzWriteFloat32, float   , double  , PyFloat_AsDouble , 1, , minmax_set_Float32, hash_double );
MKWRITER(GzWriteInt64  , int64_t , int64_t , PyLong_AsLong    , 1, , minmax_set_Int64  , hash_integer);
MKWRITER(GzWriteInt32  , int32_t , int64_t , pylong_asint32_t , 1, , minmax_set_Int32  , hash_integer);
MKWRITER(GzWriteBits64 , uint64_t, uint64_t, pylong_asuint64_t, 0, , minmax_set_Bits64 , hash_integer);
MKWRITER(GzWriteBits32 , uint32_t, uint64_t, pylong_asuint32_t, 0, , minmax_set_Bits32 , hash_integer);
MKWRITER(GzWriteBool   , uint8_t , uint8_t , pylong_asbool    , 1, , minmax_set_Bool   , hash_bool   );
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
	return r.res;
}
MKWRITER(GzWriteDateTime, uint64_t, uint64_t, fmt_datetime, 1, minmax_value_datetime, minmax_set_DateTime, hash_64bits);
MKWRITER(GzWriteDate    , uint32_t, uint32_t, fmt_date,     1,                      , minmax_set_Date    , hash_32bits);
MKWRITER(GzWriteTime    , uint64_t, uint64_t, fmt_time,     1, minmax_value_datetime, minmax_set_Time    , hash_64bits);

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
	static char *kwlist[] = {"name", "mode", "default", "hashfilter", 0};
	GzWrite *self = (GzWrite *)self_;
	const char *mode = "wb";
	PyObject *hashfilter = 0;
	gzwrite_close_(self);
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sOO", kwlist, Py_FileSystemDefaultEncoding, &self->name, &mode, &self->default_obj, &hashfilter)) return -1;
	if (self->default_obj) {
		Py_INCREF(self->default_obj);
#if PY_MAJOR_VERSION < 3
		if (PyInt_Check(self->default_obj)) {
			PyObject *lobj = PyLong_FromLong(PyInt_AS_LONG(self->default_obj));
			Py_DECREF(self->default_obj);
			self->default_obj = lobj;
		}
#endif
		if (self->default_obj != Py_None) {
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
	self->fh = gzopen(self->name, mode);
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, self->name);
		goto err;
	}
	self->count = 0;
	self->len = 0;
	return 0;
err:
	return -1;
}

static void gzwrite_obj_minmax(GzWrite *self, PyObject *obj)
{
	if (!self->min_obj || PyObject_RichCompareBool(obj, self->min_obj, Py_LT)) {
		Py_INCREF(obj);
		Py_XDECREF(self->min_obj);
		self->min_obj = obj;
	}
	if (!self->max_obj || PyObject_RichCompareBool(obj, self->max_obj, Py_GT)) {
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
			const int sliceno = hash_double(&value) % self->slices;
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
	const int64_t value = PyLong_AsLong(obj);
	char buf[GZNUMBER_MAX_BYTES];
	if (value != -1 || !PyErr_Occurred()) {
		if (self->slices) {
			const int sliceno = hash_integer(&value) % self->slices;
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
		const int sliceno = hash(buf + 1, buf[0]) % self->slices;
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
			return PyLong_FromUnsignedLong(hash_double(&value));
		}
		if (!Integer_Check(obj)) {
			PyErr_SetString(PyExc_ValueError, "Only integers/floats accepted");
			return 0;
		}
		uint64_t h;
		const int64_t value = PyLong_AsLong(obj);
		if (value != -1 || !PyErr_Occurred()) {
			h = hash_integer(&value);
		} else {
			char buf[GZNUMBER_MAX_BYTES];
			if (gzwrite_GzWriteNumber_serialize_Long(obj, buf, "Value")) return 0;
			h = hash(buf + 1, buf[0]);
		}
		return PyLong_FromUnsignedLong(h);
	}
}

static int gzwrite_init_GzWriteParsedNumber(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", "mode", "default", "hashfilter", 0};
	PyObject *name = 0;
	PyObject *mode = 0;
	PyObject *default_obj = 0;
	PyObject *hashfilter = 0;
	PyObject *new_args = 0;
	PyObject *new_kwds = 0;
	int res = -1;
	err1(!PyArg_ParseTupleAndKeywords(args, kwds, "O|OOO", kwlist, &name, &mode, &default_obj, &hashfilter));
	if (default_obj) {
		if (default_obj == Py_None || PyFloat_Check(default_obj)) {
			Py_INCREF(default_obj);
		} else {
			PyObject *lobj = PyNumber_Long(default_obj);
			if (lobj) {
				default_obj = lobj;
			} else {
				PyErr_Clear();
				default_obj = PyNumber_Float(default_obj);
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

#define MKPARSED(name, T, HT, inner, conv, withnone, minmax_set, hash)	\
	static T parse ## name(PyObject *obj)                        	\
	{                                                            	\
		PyObject *parsed = inner(obj);                       	\
		if (!parsed) return 0;                               	\
		T res = conv(parsed);                                	\
		Py_DECREF(parsed);                                   	\
		return res;                                          	\
	}                                                            	\
	MKWRITER(GzWriteParsed ## name, T, HT, parse ## name, withnone, , minmax_set, hash)
MKPARSED(Float64, double  , double  , PyNumber_Float, PyFloat_AsDouble , 1, minmax_set_Float64, hash_double);
MKPARSED(Float32, float   , double  , PyNumber_Float, PyFloat_AsDouble , 1, minmax_set_Float32, hash_double);
MKPARSED(Int64  , int64_t , int64_t , PyNumber_Int  , PyLong_AsLong    , 1, minmax_set_Int64  , hash_integer);
MKPARSED(Int32  , int32_t , int64_t , PyNumber_Int  , pylong_asint32_t , 1, minmax_set_Int32  , hash_integer);
MKPARSED(Bits64 , uint64_t, uint64_t, PyNumber_Int  , pylong_asuint64_t, 0, minmax_set_Bits64 , hash_integer);
MKPARSED(Bits32 , uint32_t, uint64_t, PyNumber_Int  , pylong_asuint32_t, 0, minmax_set_Bits32 , hash_integer);

static PyMemberDef w_default_members[] = {
	{"name"      , T_STRING   , offsetof(GzWrite, name       ), READONLY},
	{"count"     , T_ULONG    , offsetof(GzWrite, count      ), READONLY},
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
MKWTYPE(GzWriteBytesLines);
MKWTYPE(GzWriteAsciiLines);
MKWTYPE(GzWriteUnicodeLines);
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
	return PyLong_FromUnsignedLong(res);
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

PyMODINIT_FUNC INITFUNC(void)
{
	int good = (sizeof(long) == 8);
	good &= (sizeof(int64_t) == 8);
	good &= (sizeof(double) == 8);
	union { int16_t s; uint8_t c[2]; } endian_test;
	endian_test.s = -2;
	good &= (endian_test.c[0] == 254);
	good &= (endian_test.c[1] == 255);
	if (!good) {
		PyErr_SetString(PyExc_OverflowError,
			"This module only works with twos complement "
			"little endian 64 bit longs (and 8 bit bytes)."
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
	INIT(GzBytesLines);
	INIT(GzUnicodeLines);
	INIT(GzAsciiLines);
	INIT(GzNumber);
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
	INIT(GzWriteBytesLines);
	INIT(GzWriteUnicodeLines);
	INIT(GzWriteAsciiLines);
	INIT(GzWriteNumber);
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
	INIT(GzWriteParsedFloat64);
	INIT(GzWriteParsedFloat32);
	INIT(GzWriteParsedInt64);
	INIT(GzWriteParsedInt32);
	INIT(GzWriteParsedBits64);
	INIT(GzWriteParsedBits32);
	PyObject *c_hash = PyCapsule_New((void *)hash, "gzutil._C_hash", 0);
	if (!c_hash) return INITERR;
	PyModule_AddObject(m, "_C_hash", c_hash);
	PyObject *version = Py_BuildValue("(iii)", 2, 7, 2);
	PyModule_AddObject(m, "version", version);
#if PY_MAJOR_VERSION >= 3
	return m;
#endif
}
