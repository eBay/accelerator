#define PY_SSIZE_T_CLEAN 1
#include <Python.h>
#include <bytesobject.h>
#include <datetime.h>

#include <zlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>


// Must be a multiple of the largest fixed size type
#define Z (128 * 1024)

#define BOM_STR "\xef\xbb\xbf"

#define err1(v) if (v) goto err

typedef struct gzread {
	PyObject_HEAD
	char *errors;
	PyObject *(*decodefunc)(const char *, Py_ssize_t, const char *);
	gzFile fh;
	int pos, len;
	char buf[Z + 1];
} GzRead;


static int gzread_close_(GzRead *self)
{
	PyMem_Free(self->errors);
	self->errors = 0;
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
#else
#  define BYTES_NAME      "bytes"
#  define UNICODE_NAME    "str"
#  define EITHER_NAME     "str or bytes"
#  define PyInt_FromLong  PyLong_FromLong
#  define PyNumber_Int    PyNumber_Long
#  define INITFUNC        PyInit_gzutil
#endif

// Stupid forward declarations
static int gzread_read_(GzRead *self);
static PyTypeObject GzBytesLines_Type;
static PyTypeObject GzAsciiLines_Type;
static PyTypeObject GzUnicodeLines_Type;
static PyTypeObject GzWriteUnicodeLines_Type;

static int gzread_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	int res = -1;
	GzRead *self = (GzRead *)self_;
	char *name = NULL;
	char *encoding = NULL;
	int strip_bom = 0;
	gzread_close_(self);
	if (self_->ob_type == &GzBytesLines_Type) {
		static char *kwlist[] = {"name", "strip_bom", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|i", kwlist, Py_FileSystemDefaultEncoding, &name, &strip_bom)) return -1;
	} else if (self_->ob_type == &GzUnicodeLines_Type) {
		static char *kwlist[] = {"name", "encoding", "errors", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|etet", kwlist, Py_FileSystemDefaultEncoding, &name, "ascii", &encoding, "ascii", &self->errors)) return -1;
	} else {
		static char *kwlist[] = {"name", 0};
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et", kwlist, Py_FileSystemDefaultEncoding, &name)) return -1;
	}
	self->fh = gzopen(name, "rb");
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, name);
		goto err;
	}
	self->pos = self->len = 0;
	if (self_->ob_type == &GzAsciiLines_Type) {
		self->decodefunc = PyUnicode_DecodeASCII;
	}
	if (self_->ob_type == &GzUnicodeLines_Type) {
		if (encoding) {
			PyObject *decoder = PyCodec_Decoder(encoding);
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
				PyErr_Format(PyExc_LookupError, "Unsupported encoding '%s'", encoding);
				goto err;
			}
		} else {
			self->decodefunc = PyUnicode_DecodeUTF8;
		}
		if (self->decodefunc == PyUnicode_DecodeUTF8) strip_bom = 1;
	}
	if (strip_bom) {
		gzread_read_(self);
		if (self->len >= 3 && !memcmp(self->buf, BOM_STR, 3)) {
			self->pos = 3;
		}
	}
	res = 0;
err:
	PyMem_Free(name);
	PyMem_Free(encoding);
	if (res) gzread_close_(self);
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

static int gzread_read_(GzRead *self)
{
	self->len = gzread(self->fh, self->buf, Z);
	if (self->len <= 0) return 1;
	self->buf[self->len] = 0;
	self->pos = 0;
	return 0;
}

#define ITERPROLOGUE                                         	\
	do {                                                 	\
		if (!self->fh) return err_closed();          	\
		if (self->pos >= self->len) {                	\
			if (gzread_read_(self)) return 0;    	\
		}                                            	\
	} while (0)

static inline PyObject *mkBytes(GzRead *self, const char *ptr, int len)
{
	(void) self;
	if (len == 1 && *ptr == 0) {
		Py_RETURN_NONE;
	}
	if (len && ptr[len - 1] == '\r') len--;
	return PyBytes_FromStringAndSize(ptr, len);
}
static inline PyObject *mkUnicode(GzRead *self, const char *ptr, int len)
{
	if (len == 1 && *ptr == 0) {
		Py_RETURN_NONE;
	}
	if (len && ptr[len - 1] == '\r') len--;
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
		ITERPROLOGUE;                                                            	\
		char *ptr = self->buf + self->pos;                                       	\
		char *end = memchr(ptr, '\n', self->len - self->pos);                    	\
		if (!end) {                                                              	\
			int linelen = self->len - self->pos;                             	\
			char line[Z + linelen];                                          	\
			memcpy(line, self->buf + self->pos, linelen);                    	\
			if (gzread_read_(self)) {                                        	\
				return mk ## typename(self, line, linelen);              	\
			}                                                                	\
			end = memchr(self->buf + self->pos, '\n', self->len - self->pos);	\
			if (!end) end = self->buf + self->len;                           	\
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

#define MKITER(name, T, conv, withnone)                                      	\
	static PyObject * name ## _iternext(GzRead *self)                   	\
	{                                                                    	\
		ITERPROLOGUE;                                                	\
		/* Z is a multiple of sizeof(T), so this never overruns. */  	\
		const T res = *(T *)(self->buf + self->pos);                 	\
		self->pos += sizeof(T);                                      	\
		if (withnone && !memcmp(&res, &noneval_ ## T, sizeof(T))) {  	\
			Py_RETURN_NONE;                                      	\
		}                                                            	\
		return conv(res);                                            	\
	}

MKITER(GzFloat64, double  , PyFloat_FromDouble     , 1)
MKITER(GzFloat32, float   , PyFloat_FromDouble     , 1)
MKITER(GzInt64  , int64_t , PyInt_FromLong         , 1)
MKITER(GzInt32  , int32_t , PyInt_FromLong         , 1)
MKITER(GzBits64 , uint64_t, PyLong_FromUnsignedLong, 0)
MKITER(GzBits32 , uint32_t, PyLong_FromUnsignedLong, 0)
MKITER(GzBool   , uint8_t , PyBool_FromLong        , 1)

static PyObject *GzDateTime_iternext(GzRead *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
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

static PyObject *GzDate_iternext(GzRead *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 4, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	self->pos += 4;
	if (!i0) Py_RETURN_NONE;
	const int Y = i0 >> 9;
	const int m = i0 >> 5 & 0x0f;
	const int d = i0 & 0x1f;
	return PyDate_FromDate(Y, m, d);
}

static PyObject *GzTime_iternext(GzRead *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
	if (!i0) Py_RETURN_NONE;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
	return PyTime_FromTime(H, M, S, u);
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

#define MKTYPE(name)                                                 	\
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
		0,                              /*tp_members       */	\
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
MKTYPE(GzBytesLines);
MKTYPE(GzAsciiLines);
MKTYPE(GzUnicodeLines);
MKTYPE(GzFloat64);
MKTYPE(GzFloat32);
MKTYPE(GzInt64);
MKTYPE(GzInt32);
MKTYPE(GzBits64);
MKTYPE(GzBits32);
MKTYPE(GzBool);
MKTYPE(GzDateTime);
MKTYPE(GzDate);
MKTYPE(GzTime);

static PyMethodDef module_methods[] = {
	{NULL, NULL, 0, NULL}
};


typedef struct gzwrite {
	PyObject_HEAD
	gzFile fh;
	void *default_value;
	unsigned long count;
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
	char *name = NULL;
	char mode_buf[3] = {'w', 'b', 0};
	const char *mode = mode_buf;
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|s", kwlist, Py_FileSystemDefaultEncoding, &name, &mode)) return -1;
	if ((mode[0] != 'w' && mode[0] != 'a') || (mode[1] != 'b' && mode[1] != 0)) {
		PyMem_Free(name);
		PyErr_Format(PyExc_IOError, "Bad mode '%s'", mode);
	}
	mode_buf[0] = mode[0]; // always [wa]b
	gzwrite_close_(self);
	self->fh = gzopen(name, mode_buf);
	if (!self->fh) {
		PyErr_SetFromErrnoWithFilename(PyExc_IOError, name);
		PyMem_Free(name);
		return -1;
	}
	PyMem_Free(name);
	self->count = 0;
	self->len = 0;
	if (mode[0] == 'w' && self_->ob_type == &GzWriteUnicodeLines_Type) {
		memcpy(self->buf, BOM_STR, 3);
		self->len = 3;
	}
	return 0;
}
#define gzwrite_init_GzWriteBytesLines   gzwrite_init_GzWrite
#define gzwrite_init_GzWriteAsciiLines   gzwrite_init_GzWrite
#define gzwrite_init_GzWriteUnicodeLines gzwrite_init_GzWrite

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
	Py_RETURN_NONE;
}

static PyObject *gzwrite_write_GzWrite(GzWrite *self, PyObject *args)
{
	const char *data;
	Py_ssize_t len;
	if (!PyArg_ParseTuple(args, "s#", &data, &len)) return 0;
	return gzwrite_write_(self, data, len);
}

#define WRITELINEPROLOGUE(checktype, errname) \
	if (PyTuple_GET_SIZE(args) != 1) {                                            	\
		PyErr_SetString(PyExc_TypeError, "function takes exactly 1 argument");	\
		return 0;                                                             	\
	}                                                                             	\
	PyObject *obj = PyTuple_GET_ITEM(args, 0);                                    	\
	if (obj == Py_None) {                                                         	\
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
	if (len >= Z) {                                                               	\
		cleanup;                                                              	\
		PyErr_Format(PyExc_ValueError,                                        	\
		             "Value is %lld bytes, max %lld allowed (line %ld)",      	\
		             (long long)len, (long long)Z, self->count + 1);          	\
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
	PyObject *ret = gzwrite_write_(self, data, len);                              	\
	cleanup;                                                                      	\
	if (!ret) return 0;                                                           	\
	Py_DECREF(ret);
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

static PyObject *gzwrite_write_GzWriteBytesLines(GzWrite *self, PyObject *args)
{
	WRITELINEPROLOGUE(!PyBytes_Check(obj), BYTES_NAME);
	const Py_ssize_t len = PyBytes_GET_SIZE(obj);
	if (len) {
		const char *data = PyBytes_AS_STRING(obj);
		WRITELINEDO((void)data);
	}
	self->count++;
	return gzwrite_write_(self, "\n", 1);
}

static PyObject *gzwrite_write_GzWriteAsciiLines(GzWrite *self, PyObject *args)
{
	WRITELINEPROLOGUE(!PyBytes_Check(obj) && !PyUnicode_Check(obj), EITHER_NAME);
	if (PyBytes_Check(obj)) {
		const Py_ssize_t len = PyBytes_GET_SIZE(obj);
		if (len) {
			const char *data = PyBytes_AS_STRING(obj);
			ASCIILINEDO((void)data);
		}
	} else if (PyUnicode_GET_SIZE(obj)) { // Must be Unicode
		UNICODELINE(ASCIILINEDO);
	}
	self->count++;
	return gzwrite_write_(self, "\n", 1);
}

static PyObject *gzwrite_write_GzWriteUnicodeLines(GzWrite *self, PyObject *args)
{
	WRITELINEPROLOGUE(!PyUnicode_Check(obj), UNICODE_NAME);
	if (PyUnicode_GET_SIZE(obj)) {
		UNICODELINE(WRITELINEDO);
	}
	self->count++;
	return gzwrite_write_(self, "\n", 1);
}

#define MKWRITER(name, T, conv, withnone) \
	static int gzwrite_init_ ## name(PyObject *self_, PyObject *args, PyObject *kwds)	\
	{                                                                                	\
		static char *kwlist[] = {"name", "mode", "default", 0};                  	\
		GzWrite *self = (GzWrite *)self_;                                        	\
		char *name = NULL;                                                       	\
		const char *mode = "wb";                                                 	\
		PyObject *default_value = NULL;                                          	\
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "et|sO", kwlist, Py_FileSystemDefaultEncoding, &name, &mode, &default_value)) return -1; \
		gzwrite_close_(self);                                                    	\
		if (default_value) {                                                     	\
			T value;                                                         	\
			if (withnone && default_value == Py_None) {                      	\
				memcpy(&value, &noneval_ ## T, sizeof(T));               	\
			} else {                                                         	\
				value = conv(default_value);                             	\
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
		} else {                                                                 	\
			self->default_value = 0;                                         	\
		}                                                                        	\
		self->fh = gzopen(name, mode);                                           	\
		if (!self->fh) {                                                         	\
			PyErr_SetFromErrnoWithFilename(PyExc_IOError, name);             	\
			goto err;                                                        	\
		}                                                                        	\
		PyMem_Free(name);                                                        	\
		self->count = 0;                                                         	\
		self->len = 0;                                                           	\
		return 0;                                                                	\
err:                                                                                     	\
		PyMem_Free(name);                                                        	\
		return -1;                                                               	\
	}                                                                                	\
	static PyObject *gzwrite_write_ ## name(GzWrite *self, PyObject *args)           	\
	{                                                                                	\
		PyObject *obj;                                                           	\
		if (!PyArg_ParseTuple(args, "O", &obj)) return 0;                        	\
		if (withnone && obj == Py_None) {                                        	\
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
		}                                                                        	\
		self->count++;                                                           	\
		return gzwrite_write_(self, (char *)&value, sizeof(value));              	\
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
MKWRITER(GzWriteFloat64, double  , PyFloat_AsDouble , 1);
MKWRITER(GzWriteFloat32, float   , PyFloat_AsDouble , 1);
MKWRITER(GzWriteInt64  , int64_t , PyLong_AsLong    , 1);
MKWRITER(GzWriteInt32  , int32_t , pylong_asint32_t , 1);
MKWRITER(GzWriteBits64 , uint64_t, pylong_asuint64_t, 0);
MKWRITER(GzWriteBits32 , uint32_t, pylong_asuint32_t, 0);
MKWRITER(GzWriteBool   , uint8_t , pylong_asbool    , 1);
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
MKWRITER(GzWriteDateTime, uint64_t, fmt_datetime, 1);
MKWRITER(GzWriteDate    , uint32_t, fmt_date,     1);
MKWRITER(GzWriteTime    , uint64_t, fmt_time,     1);

#define MKPARSED(name, T, inner, conv, withnone)                     	\
	static T parse ## name(PyObject *obj)                        	\
	{                                                            	\
		PyObject *parsed = inner(obj);                       	\
		if (!parsed) return 0;                               	\
		T res = conv(parsed);                                	\
		Py_DECREF(parsed);                                   	\
		return res;                                          	\
	}                                                            	\
	MKWRITER(GzWriteParsed ## name, T , parse ## name, withnone)
MKPARSED(Float64, double  , PyNumber_Float, PyFloat_AsDouble , 1);
MKPARSED(Float32, float   , PyNumber_Float, PyFloat_AsDouble , 1);
MKPARSED(Int64  , int64_t , PyNumber_Int  , PyLong_AsLong    , 1);
MKPARSED(Int32  , int32_t , PyNumber_Int  , pylong_asint32_t , 1);
MKPARSED(Bits64 , uint64_t, PyNumber_Int  , pylong_asuint64_t, 0);
MKPARSED(Bits32 , uint32_t, PyNumber_Int  , pylong_asuint32_t, 0);

#define MKWTYPE(name)                                                                  	\
	static PyMethodDef name ## _methods[] = {                                      	\
		{"__enter__", (PyCFunction)gzwrite_self, METH_NOARGS,  NULL},          	\
		{"__exit__",  (PyCFunction)gzany_exit, METH_VARARGS, NULL},            	\
		{"write",     (PyCFunction)gzwrite_write_ ## name, METH_VARARGS, NULL},	\
		{"flush",     (PyCFunction)gzwrite_flush, METH_NOARGS, NULL},          	\
		{"close",     (PyCFunction)gzwrite_close, METH_NOARGS, NULL},          	\
		{NULL, NULL, 0, NULL}                                                  	\
	};                                                                             	\
	                                                                               	\
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
		name ## _methods,               /*tp_methods*/       	\
		0,                              /*tp_members*/       	\
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
MKWTYPE(GzWrite);
MKWTYPE(GzWriteBytesLines);
MKWTYPE(GzWriteAsciiLines);
MKWTYPE(GzWriteUnicodeLines);
MKWTYPE(GzWriteFloat64);
MKWTYPE(GzWriteFloat32);
MKWTYPE(GzWriteInt64);
MKWTYPE(GzWriteInt32);
MKWTYPE(GzWriteBits64);
MKWTYPE(GzWriteBits32);
MKWTYPE(GzWriteBool);
MKWTYPE(GzWriteDateTime);
MKWTYPE(GzWriteDate);
MKWTYPE(GzWriteTime);

MKWTYPE(GzWriteParsedFloat64);
MKWTYPE(GzWriteParsedFloat32);
MKWTYPE(GzWriteParsedInt64);
MKWTYPE(GzWriteParsedInt32);
MKWTYPE(GzWriteParsedBits64);
MKWTYPE(GzWriteParsedBits32);

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
	INIT(GzAsciiLines);
	INIT(GzUnicodeLines);
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
	INIT(GzWriteAsciiLines);
	INIT(GzWriteUnicodeLines);
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
	INIT(GzWriteParsedFloat64);
	INIT(GzWriteParsedFloat32);
	INIT(GzWriteParsedInt64);
	INIT(GzWriteParsedInt32);
	INIT(GzWriteParsedBits64);
	INIT(GzWriteParsedBits32);
	PyObject *version = Py_BuildValue("(iii)", 2, 0, 4);
	PyModule_AddObject(m, "version", version);
#if PY_MAJOR_VERSION >= 3
	return m;
#endif
}
