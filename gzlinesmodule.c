#include <Python.h>
#include <datetime.h>

#include <zlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>


// Must be a multiple of the largest fixed size type
#define Z (128 * 1024)


typedef struct gzlines {
	PyObject_HEAD
	gzFile fh;
	int pos, len;
	char buf[Z + 1];
} GzLines;


static int gzlines_close_(GzLines *self)
{
	if (self->fh) {
		gzclose(self->fh);
		self->fh = 0;
		return 0;
	}
	return 1;
}

static int gzlines_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", 0};
	GzLines *self = (GzLines *)self_;
	char *name = NULL;
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et", kwlist, Py_FileSystemDefaultEncoding, &name)) return -1;
	gzlines_close_(self);
	self->fh = gzopen(name, "rb");
	if (!self->fh) {
		PyErr_SetString(PyExc_IOError, "Open failed");
		return -1;
	}
	self->pos = self->len = 0;
	return 0;
}

static void gzlines_dealloc(GzLines *self)
{
	gzlines_close_(self);
	PyObject_Del(self);
}

static PyObject *err_closed(void)
{
	PyErr_SetString(PyExc_ValueError, "I/O operation on closed file");
	return 0;
}

static PyObject *gzlines_close(GzLines *self)
{
	if (gzlines_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *gzlines_self(GzLines *self)
{
	if (!self->fh) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static int gzlines_read_(GzLines *self)
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
			if (gzlines_read_(self)) return 0;   	\
		}                                            	\
	} while (0)

static PyObject *GzLines_iternext(GzLines *self)
{
	ITERPROLOGUE;
	char *ptr = self->buf + self->pos;
	char *end = strchr(ptr, '\n');
	if (!end) {
		int linelen = self->len - self->pos;
		char line[Z + linelen];
		memcpy(line, self->buf + self->pos, linelen);
		if (gzlines_read_(self)) {
			return PyString_FromStringAndSize(line, linelen);
		}
		end = strchr(self->buf + self->pos, '\n');
		if (!end) end = self->buf + self->len;
		self->pos = end - self->buf + 1;
		memcpy(line + linelen, self->buf, self->pos - 1);
		return PyString_FromStringAndSize(line, linelen + self->pos - 1);
	}
	int linelen = end - ptr;
	self->pos += linelen + 1;
	return PyString_FromStringAndSize(ptr, linelen);
}

#define MKITER(name, T, conv)                                                	\
	static PyObject * name ## _iternext(GzLines *self)                   	\
	{                                                                    	\
		ITERPROLOGUE;                                                	\
		/* Z is a multiple of sizeof(T), so this never overruns. */  	\
		const T res = *(T *)(self->buf + self->pos);                 	\
		self->pos += sizeof(T);                                      	\
		return conv(res);                                            	\
	}

MKITER(GzFloat64, double  , PyFloat_FromDouble)
MKITER(GzFloat32, float   , PyFloat_FromDouble)
MKITER(GzInt64  , int64_t , PyInt_FromLong)
MKITER(GzInt32  , int32_t , PyInt_FromLong)
MKITER(GzUInt64 , uint64_t, PyLong_FromUnsignedLong)
MKITER(GzUInt32 , uint32_t, PyLong_FromUnsignedLong)
MKITER(GzBool   , uint8_t , PyBool_FromLong)

static PyObject *GzDateTime_iternext(GzLines *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
	const int Y = i0 >> 14;
	const int m = i0 >> 10 & 0x0f;
	const int d = i0 >> 5 & 0x1f;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
	return PyDateTime_FromDateAndTime(Y, m, d, H, M, S, u);
}

static PyObject *GzDate_iternext(GzLines *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 4, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	self->pos += 4;
	const int Y = i0 >> 9;
	const int m = i0 >> 5 & 0x0f;
	const int d = i0 & 0x1f;
	return PyDate_FromDate(Y, m, d);
}

static PyObject *GzTime_iternext(GzLines *self)
{
	ITERPROLOGUE;
	/* Z is a multiple of 8, so this never overruns. */
	const uint32_t i0 = *(uint32_t *)(self->buf + self->pos);
	const uint32_t i1 = *(uint32_t *)(self->buf + self->pos + 4);
	self->pos += 8;
	const int H = i0 & 0x1f;
	const int M = i1 >> 26 & 0x3f;
	const int S = i1 >> 20 & 0x3f;
	const int u = i1 & 0xfffff;
	return PyTime_FromTime(H, M, S, u);
}

static PyObject *gzlines_exit(PyObject *self, PyObject *args)
{
	PyObject *ret = PyObject_CallMethod(self, "close", NULL);
	if (!ret) return 0;
	Py_DECREF(ret);
	Py_RETURN_NONE;
}

static PyMethodDef gzlines_methods[] = {
	{"__enter__", (PyCFunction)gzlines_self, METH_NOARGS,  NULL},
	{"__exit__",  (PyCFunction)gzlines_exit, METH_VARARGS, NULL},
	{"close",     (PyCFunction)gzlines_close, METH_NOARGS,  NULL},
	{NULL, NULL, 0, NULL}
};

#define MKTYPE(name)                                                 	\
	static PyTypeObject name ## _Type = {                        	\
		PyVarObject_HEAD_INIT(NULL, 0)                       	\
		#name,                          /*tp_name          */	\
		sizeof(GzLines),                /*tp_basicsize     */	\
		0,                              /*tp_itemsize      */	\
		(destructor)gzlines_dealloc,    /*tp_dealloc       */	\
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
		(getiterfunc)gzlines_self,      /*tp_iter          */	\
		(iternextfunc)name ## _iternext,/*tp_iternext      */	\
		gzlines_methods,                /*tp_methods       */	\
		0,                              /*tp_members       */	\
		0,                              /*tp_getset        */	\
		0,                              /*tp_base          */	\
		0,                              /*tp_dict          */	\
		0,                              /*tp_descr_get     */	\
		0,                              /*tp_descr_set     */	\
		0,                              /*tp_dictoffset    */	\
		gzlines_init,                   /*tp_init          */	\
		PyType_GenericAlloc,            /*tp_alloc         */	\
		PyType_GenericNew,              /*tp_new           */	\
		PyObject_Del,                   /*tp_free          */	\
		0,                              /*tp_is_gc         */	\
	}
MKTYPE(GzLines);
MKTYPE(GzFloat64);
MKTYPE(GzFloat32);
MKTYPE(GzInt64);
MKTYPE(GzInt32);
MKTYPE(GzUInt64);
MKTYPE(GzUInt32);
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
	int len;
	char buf[Z];
} GzWrite;

static int gzwrite_flush(GzWrite *self)
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

static int gzwrite_close_(GzWrite *self)
{
	if (self->fh) {
		gzwrite_flush(self);
		gzclose(self->fh);
		self->fh = 0;
		return 0;
	}
	return 1;
}

static int gzwrite_init(PyObject *self_, PyObject *args, PyObject *kwds)
{
	static char *kwlist[] = {"name", 0};
	GzWrite *self = (GzWrite *)self_;
	char *name = NULL;
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "et", kwlist, Py_FileSystemDefaultEncoding, &name)) return -1;
	gzwrite_close_(self);
	self->fh = gzopen(name, "wb");
	if (!self->fh) {
		PyErr_SetString(PyExc_IOError, "Open failed");
		return -1;
	}
	self->len = 0;
	return 0;
}

static void gzwrite_dealloc(GzWrite *self)
{
	gzwrite_close_(self);
	PyObject_Del(self);
}

static PyObject *gzwrite_close(GzWrite *self)
{
	if (gzwrite_flush(self)) return 0;
	if (gzwrite_close_(self)) return err_closed();
	Py_RETURN_NONE;
}

static PyObject *gzwrite_self(GzWrite *self)
{
	if (!self->fh) return err_closed();
	Py_INCREF(self);
	return (PyObject *)self;
}

static PyObject *gzwrite_write_(GzWrite *self, const char *data, int len)
{
	if (len + self->len > Z) {
		if (gzwrite_flush(self)) return 0;
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
	int len;
	if (!PyArg_ParseTuple(args, "s#", &data, &len)) return 0;
	return gzwrite_write_(self, data, len);
}

static PyObject *gzwrite_exit(PyObject *self, PyObject *args)
{
	PyObject *ret = PyObject_CallMethod(self, "close", NULL);
	if (!ret) return 0;
	Py_DECREF(ret);
	Py_RETURN_NONE;
}

static PyMethodDef GzWrite_methods[] = {
	{"__enter__", (PyCFunction)gzwrite_self, METH_NOARGS,  NULL},
	{"__exit__",  (PyCFunction)gzwrite_exit, METH_VARARGS, NULL},
	{"write",     (PyCFunction)gzwrite_write_GzWrite, METH_VARARGS, NULL},
	{"close",     (PyCFunction)gzwrite_close, METH_NOARGS,  NULL},
	{NULL, NULL, 0, NULL}
};

static PyTypeObject GzWrite_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"GzWrite",                      /*tp_name*/
	sizeof(GzWrite),                /*tp_basicsize*/
	0,                              /*tp_itemsize*/
	(destructor)gzwrite_dealloc,    /*tp_dealloc*/
	0,                              /*tp_print*/
	0,                              /*tp_getattr*/
	0,                              /*tp_setattr*/
	0,                              /*tp_compare*/
	0,                              /*tp_repr*/
	0,                              /*tp_as_number*/
	0,                              /*tp_as_sequence*/
	0,                              /*tp_as_mapping*/
	0,                              /*tp_hash*/
	0,                              /*tp_call*/
	0,                              /*tp_str*/
	0,                              /*tp_getattro*/
	0,                              /*tp_setattro*/
	0,                              /*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT,             /*tp_flags*/
	0,                              /*tp_doc*/
	0,                              /*tp_traverse*/
	0,                              /*tp_clear*/
	0,                              /*tp_richcompare*/
	0,                              /*tp_weaklistoffset*/
	0,                              /*tp_iter*/
	0,                              /*tp_iternext*/
	GzWrite_methods,                /*tp_methods*/
	0,                              /*tp_members*/
	0,                              /*tp_getset*/
	0,                              /*tp_base*/
	0,                              /*tp_dict*/
	0,                              /*tp_descr_get*/
	0,                              /*tp_descr_set*/
	0,                              /*tp_dictoffset*/
	gzwrite_init,                   /*tp_init*/
	PyType_GenericAlloc,            /*tp_alloc*/
	PyType_GenericNew,              /*tp_new*/
	PyObject_Del,                   /*tp_free*/
	0,                              /*tp_is_gc*/
};



#define INIT(name) do {                                              	\
	if (PyType_Ready(&name ## _Type) < 0) return;                	\
	Py_INCREF(&name ## _Type);                                   	\
	PyModule_AddObject(m, #name, (PyObject *) &name ## _Type);   	\
} while (0)

PyMODINIT_FUNC initgzlines(void)
{
	PyDateTime_IMPORT;
	PyObject *m = Py_InitModule3("gzlines", module_methods, NULL);
	if (!m) return;
	INIT(GzLines);
	INIT(GzFloat64);
	INIT(GzFloat32);
	INIT(GzInt64);
	INIT(GzInt32);
	INIT(GzUInt64);
	INIT(GzUInt32);
	INIT(GzBool);
	INIT(GzDateTime);
	INIT(GzDate);
	INIT(GzTime);
	INIT(GzWrite);
	PyObject *version = Py_BuildValue("(iii)", 1, 3, 0);
	PyModule_AddObject(m, "version", version);
	// old name for compat
	Py_INCREF(&GzLines_Type);
	PyModule_AddObject(m, "gzlines", (PyObject *) &GzLines_Type);
}
