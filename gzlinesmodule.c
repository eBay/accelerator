#include <Python.h>

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

static PyObject *gzlines_iternext(GzLines *self)
{
	if (!self->fh) return err_closed();
	if (self->pos >= self->len) {
		if (gzlines_read_(self)) return 0;
	}
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

static PyObject *GzFloat64_iternext(GzLines *self)
{
	if (!self->fh) return err_closed();
	if (self->pos >= self->len) {
		if (gzlines_read_(self)) return 0;
	}
	// Z is a multiple of 8, so this never overruns.
	const double res = *(double *)(self->buf + self->pos);
	self->pos += 8;
	return PyFloat_FromDouble(res);
}

static PyObject *GzFloat32_iternext(GzLines *self)
{
	if (!self->fh) return err_closed();
	if (self->pos >= self->len) {
		if (gzlines_read_(self)) return 0;
	}
	// Z is a multiple of 4, so this never overruns.
	const float res = *(float *)(self->buf + self->pos);
	self->pos += 4;
	return PyFloat_FromDouble(res);
}

static PyObject *GzInt64_iternext(GzLines *self)
{
	if (!self->fh) return err_closed();
	if (self->pos >= self->len) {
		if (gzlines_read_(self)) return 0;
	}
	// Z is a multiple of 8, so this never overruns.
	const int64_t res = *(int64_t *)(self->buf + self->pos);
	self->pos += 8;
	return PyInt_FromLong(res);
}

static PyObject *GzInt32_iternext(GzLines *self)
{
	if (!self->fh) return err_closed();
	if (self->pos >= self->len) {
		if (gzlines_read_(self)) return 0;
	}
	// Z is a multiple of 4, so this never overruns.
	const int32_t res = *(int32_t *)(self->buf + self->pos);
	self->pos += 4;
	return PyInt_FromLong(res);
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
MKTYPE(gzlines);
MKTYPE(GzFloat64);
MKTYPE(GzFloat32);
MKTYPE(GzInt64);
MKTYPE(GzInt32);

static PyMethodDef module_methods[] = {
	{NULL, NULL, 0, NULL}
};

#define INIT(name) do {                                              	\
	if (PyType_Ready(&name ## _Type) < 0) return;                	\
	Py_INCREF(&name ## _Type);                                   	\
	PyModule_AddObject(m, #name, (PyObject *) &name ## _Type);   	\
} while (0)

PyMODINIT_FUNC initgzlines(void)
{
	PyObject *m = Py_InitModule3("gzlines", module_methods, NULL);
	if (!m) return;
	INIT(gzlines);
	INIT(GzFloat64);
	INIT(GzFloat32);
	INIT(GzInt64);
	INIT(GzInt32);
}
