############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

# This is a separate file from a_dataset_type so setup.py can import
# it and make the _dataset_type module at install time.

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from collections import namedtuple
from functools import partial
import sys
import struct
import codecs
import json

from accelerator.compat import NoneType, iteritems

from . import c_backend_support

__all__ = ('convfuncs', 'typerename', 'typesizes', 'minmaxfuncs',)

def _resolve_datetime(coltype):
	cfunc, fmt = coltype.split(':', 1)
	percent = False
	fmt_a = []
	for c in fmt:
		if percent:
			fmt_a.append('%' + c)
			percent = False
		elif c == '%':
			percent = True
		else:
			fmt_a.append(c)
	if percent:
		raise Exception('Stray %% at end of pattern %r' % (fmt,))
	def split(sep):
		cnt = fmt_a.count(sep)
		if cnt == 1:
			pos = fmt_a.index(sep)
			return fmt_a[:pos], fmt_a[pos + 1:]
		elif cnt:
			raise Exception('Bad pattern %r, only a single %r supported' % (fmt, sep,))
	if '%J' in fmt_a:
		fmt_a, fmt_b = split('%J')
		if any(v[0] == '%' and v != '%%' for v in fmt_a + fmt_b):
			raise Exception('Can only parse %J as the only format specifier. (%r)' % (fmt,))
		assert cfunc.startswith('datetime'), 'Only datetime can use %J'
		cfunc = 'java' + cfunc
		fmt_b = ''.join(fmt_b)
	elif '%f' in fmt_a:
		fmt_a, fmt_b = split('%f')
		fmt_b = ''.join(fmt_b)
	else:
		fmt_b = None
	return cfunc, ''.join(fmt_a), fmt_b

def _resolve_unicode(coltype, strip=False):
	_, fmt = coltype.split(':', 1)
	if '/' in fmt:
		codec, errors = fmt.split('/')
	else:
		codec, errors = fmt, 'strict'
	assert errors in ('strict', 'replace', 'ignore',)
	b''.decode(codec) # trigger error on unknown
	canonical = codecs.lookup(codec).name
	if canonical == codecs.lookup('utf-8').name:
		selected = 'unicode_utf8'
	elif canonical == codecs.lookup('iso-8859-1').name:
		selected = 'unicode_latin1'
	elif canonical == codecs.lookup('ascii').name:
		if errors == 'strict':
			selected = 'ascii_strict'
		else:
			selected = 'unicode_ascii'
	else:
		selected = 'unicode'
	if strip:
		if '_' in selected:
			selected = selected.replace('_', 'strip_', 1)
		else:
			selected += 'strip'
	return selected, codec, errors

_c_conv_bytes_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (*line == 32 || (*line >= 9 && *line <= 13)) {
		line++;
		len--;
	}
	while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
'''

_c_conv_ascii_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (*line == 32 || (*line >= 9 && *line <= 13)) {
		line++;
		len--;
	}
	while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
	char *free_ptr = 0;
	int32_t enc_cnt = 0;
	for (uint32_t i = 0; i < (uint32_t)len; i++) {
		enc_cnt += (%%(enctest)s);
	}
	if (enc_cnt) {
		int64_t elen = (int64_t)len + ((int64_t)enc_cnt * 3);
		err1(elen > 0x7fffffff);
		char *buf = PyMem_Malloc(elen);
		err1(!buf);
		free_ptr = buf;
		int32_t bi = 0;
		for (uint32_t i = 0; i < (uint32_t)len; i++) {
%(conv)s
		}
		ptr = (uint8_t *)buf;
		len = elen;
	}
'''
_c_conv_ascii_encode_template = r'''
			if (%(enctest)s) {
				buf[bi++] = '\\';
				buf[bi++] = '0' + (ptr[i] >> 6);
				buf[bi++] = '0' + ((ptr[i] >> 3) & 7);
				buf[bi++] = '0' + (ptr[i] & 7);
			} else {
				buf[bi++] = ptr[i];
			}
'''
_c_conv_ascii_cleanup = r'''
		if (free_ptr) PyMem_Free(free_ptr);
'''

_c_conv_ascii_strict_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (*line == 32 || (*line >= 9 && *line <= 13)) {
		line++;
		len--;
	}
	while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
	for (uint32_t i = 0; i < (uint32_t)len; i++) {
		if (ptr[i] > 127) {
			ptr = 0;
			break;
		}
	}
'''

_c_null_blob_template = r'''
	int32_t len = g.linelen;
	const uint8_t *ptr = (uint8_t *)line;
'''

_c_conv_unicode_setup = r'''
	PyObject *decoder = PyCodec_Decoder(fmt);
	if (!decoder) {
		PyErr_Format(PyExc_ValueError, "No decoder for '%s'.\n", fmt);
		goto err;
	}
	PyObject *dec_errors = PyUnicode_FromString(fmt_b);
	err1(!dec_errors);
	PyObject *tst_bytes = PyBytes_FromStringAndSize("a", 1);
	if (tst_bytes) {
		PyObject *tst_res = PyObject_CallFunctionObjArgs(decoder, tst_bytes, dec_errors, 0);
		Py_DECREF(tst_bytes);
		if (tst_res) {
			if (PyTuple_Check(tst_res)) {
				if (!PyUnicode_Check(PyTuple_GetItem(tst_res, 0))) {
					PyErr_Format(PyExc_ValueError, "Decoder for '%s' does not produce unicode.\n", fmt);
				}
			}
			Py_DECREF(tst_res);
		}
	}
	err1(PyErr_Occurred());
'''
_c_conv_unicode_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (*line == 32 || (*line >= 9 && *line <= 13)) {
		line++;
		len--;
	}
	while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
#endif
	const uint8_t *ptr = 0;
	PyObject *tmp_bytes = PyBytes_FromStringAndSize(line, len);
	err1(!tmp_bytes);
	PyObject *tmp_res = PyObject_CallFunctionObjArgs(decoder, tmp_bytes, dec_errors, 0);
	Py_DECREF(tmp_bytes);
	if (tmp_res) {
#if PY_MAJOR_VERSION < 3
		PyObject *tmp_utf8bytes = PyUnicode_AsUTF8String(PyTuple_GET_ITEM(tmp_res, 0));
		err1(!tmp_utf8bytes);
		Py_DECREF(tmp_res);
		tmp_res = tmp_utf8bytes;
		ptr = (const uint8_t *)PyBytes_AS_STRING(tmp_utf8bytes);
		Py_ssize_t newlen = PyBytes_GET_SIZE(tmp_utf8bytes);
#else
		PyObject *tmp_uni = PyTuple_GET_ITEM(tmp_res, 0);
		Py_ssize_t newlen;
		ptr = (const uint8_t *)PyUnicode_AsUTF8AndSize(tmp_uni, &newlen);
#endif
		if (newlen > 0x7fffffff) {
			ptr = 0;
		} else {
			len = newlen;
		}
	} else {
		PyErr_Clear();
	}
'''
_c_conv_unicode_cleanup = r'''
		Py_XDECREF(tmp_res);
'''
_c_conv_unicode_specific_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (*line == 32 || (*line >= 9 && *line <= 13)) {
		line++;
		len--;
	}
	while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
#endif
	const uint8_t *ptr = 0;
	PyObject *tmp_res = %(func)s(line, len, fmt_b);
	if (tmp_res) {
#if PY_MAJOR_VERSION < 3
		PyObject *tmp_utf8bytes = PyUnicode_AsUTF8String(tmp_res);
		err1(!tmp_utf8bytes);
		Py_DECREF(tmp_res);
		tmp_res = tmp_utf8bytes;
		ptr = (const uint8_t *)PyBytes_AS_STRING(tmp_utf8bytes);
		Py_ssize_t newlen = PyBytes_GET_SIZE(tmp_utf8bytes);
#else
		Py_ssize_t newlen;
		ptr = (const uint8_t *)PyUnicode_AsUTF8AndSize(tmp_res, &newlen);
#endif
		if (newlen > 0x7fffffff) {
			ptr = 0;
		} else {
			len = newlen;
		}
	} else {
		PyErr_Clear();
	}
'''

_c_conv_date_java_ts_template = r'''
	const char *startptr = line;
	const char *fmtptr = fmt;
	char *endptr;
	while (*fmtptr) {
		if (*fmtptr++ != *startptr++) {
			startptr = "";
			break;
		}
	}
	errno = 0;
	long long ts_j = strtoll(startptr, &endptr, 0);
	if (errno || !endptr || startptr == endptr) {
		ptr = 0;
	} else {
		fmtptr = fmt_b;
		while (*fmtptr) {
			if (*fmtptr++ != *endptr++) {
				ptr = 0;
				break;
			}
		}
		if ((%(whole)d) && ptr) {
			while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		}
		if (((%(whole)d) && *endptr) || !ptr) {
			ptr = 0;
		} else {
			int32_t f = (ts_j %% 1000) * 1000;
			time_t ts_u = ts_j / 1000;
			const int fit = (ts_j / 1000 == ts_u);
			if (f < 0) {
				f += 1000000;
				ts_u--;
			}
			struct tm tm;
			memset(&tm, 0, sizeof(tm));
			if (fit && gmtime_r(&ts_u, &tm)) {
				uint32_t *p = (uint32_t *)ptr;
%(conv)s
			} else {
				ptr = 0;
			}
		}
	}
'''
_c_conv_date_template = r'''
	const char *pres;
	struct tm tm;
	memset(&tm, 0, sizeof(tm));
	tm.tm_year = 70;
	tm.tm_mday = 1;
	if (*fmt) {
		pres = strptime(line, fmt, &tm);
	} else {
		pres = line;
	}
	if (fmt_b) { // There is %%f in the fmt
		if (pres) {
			const char *lres;
			size_t plen = strlen(pres);
			int32_t f;
			if (plen > 6) {
				char tmp[7];
				char *end;
				memcpy(tmp, pres, 6);
				tmp[6] = 0;
				f = strtol(tmp, &end, 10);
				lres = pres + (end - tmp);
			} else {
				char *end;
				f = strtol(pres, &end, 10);
				lres = end;
			}
			// "123000", "123" and "123   " should probably be equivalent.
			for (int digitcnt = lres - pres; digitcnt < 6; digitcnt++) {
				f *= 10;
				if (isspace(*lres)) lres++;
			}
			if (pres == lres || f < 0) {
				pres = 0;
			} else if (*fmt_b) {
				pres = strptime(lres, fmt_b, &tm);
			} else {
				pres = lres;
			}
			if ((%(whole)d) && pres) {
				while (*pres == 32 || (*pres >= 9 && *pres <= 13)) pres++;
			}
			if (pres && ((!%(whole)d) || !*pres)) {
				uint32_t *p = (uint32_t *)ptr;
%(conv)s
			} else {
				ptr = 0;
			}
		} else {
			ptr = 0;
		}
	} else {
		if ((%(whole)d) && pres) {
			while (*pres == 32 || (*pres >= 9 && *pres <= 13)) pres++;
		}
		if (pres && ((!%(whole)d) || !*pres)) {
			uint32_t *p = (uint32_t *)ptr;
			const int32_t f = 0;
%(conv)s
		} else {
			ptr = 0;
		}
	}
'''
_c_conv_datetime = r'''
		if (use_tz) {
			tm.tm_isdst = -1;
			time_t t = mktime(&tm);
			gmtime_r(&t, &tm);
		}
		const uint32_t year = tm.tm_year + 1900;
		const uint32_t mon  = tm.tm_mon + 1;
		const uint32_t mday = tm.tm_mday;
		const uint32_t hour = tm.tm_hour;
		const uint32_t min  = tm.tm_min;
		const uint32_t sec  = tm.tm_sec;
		// Our definition of a valid date is whatever Python will accept.
		// On Python 2 that's unfortunately pretty much anything.
		// We validate the time ourselves because that's easy, but the
		// date part will just be broken on python 2 unless your strptime
		// validates more than most implementations.
		if (
			hour >= 0 && hour < 24 &&
			min >= 0 && min < 60 &&
			sec >= 0 && sec < 60
		) {
			PyObject *o = PyDateTime_FromDateAndTime(year, mon, mday, hour, min, sec, f);
			if (o) {
				Py_DECREF(o);
				p[0] = year << 14 | mon << 10 | mday << 5 | hour;
				p[1] = min << 26 | sec << 20 | f;
			} else {
				PyErr_Clear();
				ptr = 0;
			}
		} else {
			ptr = 0;
		}
'''
_c_conv_date = r'''
		(void) f; // not used for dates, but we don't want the compiler complaining.
		const uint32_t year = tm.tm_year + 1900;
		const uint32_t mon  = tm.tm_mon + 1;
		const uint32_t mday = tm.tm_mday;
		// Our definition of a valid date is whatever Python will accept.
		// On Python 2 that's unfortunately pretty much anything.
		PyObject *o = PyDate_FromDate(year, mon, mday);
		if (o) {
			Py_DECREF(o);
			p[0] = year << 9 | mon << 5 | mday;
		} else {
			PyErr_Clear();
			ptr = 0;
		}
'''
_c_conv_time = r'''
		const uint32_t hour = tm.tm_hour;
		const uint32_t min  = tm.tm_min;
		const uint32_t sec  = tm.tm_sec;
		if (
			hour >= 0 && hour < 24 &&
			min >= 0 && min < 60 &&
			sec >= 0 && sec < 60
		) {
			p[0] = 32277536 | hour; // 1970 if read as datetime
			p[1] = min << 26 | sec << 20 | f;
		} else {
			ptr = 0;
		}
'''

_c_conv_float_template = r'''
		(void) fmt;
		char *endptr;
		%(type)s value = %(func)s(line, &endptr);
#if %(whole)d
		while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		if (*endptr) { // not a valid float
			ptr = 0;
		} else {
#else
		if (1) {
#endif
			// inf and truncated to zero are ok here.
			%(type)s *p = (%(type)s *)ptr;
			*p = value;
		}
'''

_c_conv_int_template = r'''
		(void) fmt;
		errno = 0;
		const char *startptr = line;
		char *endptr;
#if %(unsigned)d
		while (isspace((unsigned char)*startptr)) startptr++;
		if (*startptr == '-') {
			ptr = 0;
		} else {
#endif
		%(rtype)s value = %(func)s(startptr, &endptr, %(base)d);
#if %(whole)d
		while (isspace((unsigned char)*endptr)) endptr++;
		if (*endptr) { // not a valid int
			ptr = 0;
		} else
#endif
		if (errno == ERANGE) { // out of range
			ptr = 0;
		} else {
			%(type)s *p = (%(type)s *)ptr;
			*p = value;
			if (value != *p || ((%(nonemarker)s) && value == %(nonemarker)s)) {
				// Over/underflow (values that don't fit are not ok)
				ptr = 0;
			}
		}
#if %(unsigned)d
		}
#endif
'''

_c_conv_strbool = r'''
		(void) fmt;
		if (!strcasecmp(line, "false")
		    || !strcasecmp(line, "0")
		    || !strcasecmp(line, "f")
		    || !strcasecmp(line, "no")
		    || !strcasecmp(line, "off")
		    || !strcasecmp(line, "nil")
		    || !strcasecmp(line, "null")
		    || !*line
		) {
			*ptr = 0;
		} else {
			*ptr = 1;
		}
'''

_c_conv_floatbool_template = r'''
		(void) fmt;
		char *endptr;
		double value = strtod(line, &endptr);
#if %(whole)d
		while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		if (*endptr) { // not a valid float
			ptr = 0;
		} else {
#else
		if (1) {
#endif
			// inf and truncated to zero are ok here.
			*ptr = !!value;
		}
'''

_c_conv_floatint_exact_template = r'''
		(void) fmt;
		char *endptr;
		double value = strtod(line, &endptr);
#if %(whole)d
		while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		if (*endptr
#else
		if (0
#endif
		            || isnan(value) || value > %(biggest)s || value < %(smallest)s) {
			ptr = 0;
		} else {
			int%(bitsize)d_t *p = (int%(bitsize)d_t *)ptr;
			*p = value;
		}
'''

_c_conv_floatint_saturate_template = r'''
		(void) fmt;
		char *endptr;
		double value = strtod(line, &endptr);
#if %(whole)d
		while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		if (*endptr
#else
		if (0
#endif
		            || isnan(value)) { // not a valid float
			ptr = 0;
		} else {
			int%(bitsize)d_t res;
			if (value <= -INT%(bitsize)d_MAX) {
				res = -INT%(bitsize)d_MAX;
			} else if (value >= INT%(bitsize)d_MAX) {
				res = INT%(bitsize)d_MAX;
			} else {
				res = value;
			}
			int%(bitsize)d_t *p = (int%(bitsize)d_t *)ptr;
			*p = res;
		}
'''

MinMaxTuple = namedtuple('MinMaxTuple', 'setup code')
def _c_minmax_simple(typename, min_const, max_const, check_none):
	d = dict(type=typename, min_const=min_const, max_const=max_const, check_none=check_none)
	setup = r'''
		do {
			%(type)s * const col_min = (%(type)s *)buf_col_min;
			%(type)s * const col_max = (%(type)s *)buf_col_max;
			*col_min = %(max_const)s;
			*col_max = %(min_const)s;
		} while (0)
	''' % d
	code = r'''
		do {
			const %(type)s cand_value = *(const %(type)s *)ptr;
			if (%(check_none)s) { // Some of these need to ignore None-values
				%(type)s * const col_min = (%(type)s *)buf_col_min;
				%(type)s * const col_max = (%(type)s *)buf_col_max;
				if (cand_value < *col_min) *col_min = cand_value;
				if (cand_value > *col_max) *col_max = cand_value;
			}
		} while (0)
	''' % d
	return MinMaxTuple(setup, code,)

_c_minmax_datetime = MinMaxTuple(
	r'''
		do {
			uint32_t * const col_min = (uint32_t *)buf_col_min;
			uint32_t * const col_max = (uint32_t *)buf_col_max;
			col_min[0] = 163836919; col_min[1] = 4021288960; // 9999-12-31 23:59:59
			col_max[0] = 17440;     col_max[1] = 0;          // 0001-01-01 00:00:00
		} while (0)
	''',
	r'''
		do {
			const uint32_t * const cand_p = (const uint32_t *)ptr;
			uint32_t * const col_min = (uint32_t *)buf_col_min;
			uint32_t * const col_max = (uint32_t *)buf_col_max;
			if (cand_p[0]) { // Ignore None-values
				if (cand_p[0] < col_min[0] || (cand_p[0] == col_min[0] && cand_p[1] < col_min[1])) {
					col_min[0] = cand_p[0];
					col_min[1] = cand_p[1];
				}
				if (cand_p[0] > col_max[0] || (cand_p[0] == col_max[0] && cand_p[1] > col_max[1])) {
					col_max[0] = cand_p[0];
					col_max[1] = cand_p[1];
				}
			}
		} while (0)
	''',
)

minmaxfuncs = {
	'float64'  : _c_minmax_simple('double'  , 'DBL_MIN'   , 'DBL_MAX'   , 'memcmp(ptr, noneval_float64, 8)'),
	'float32'  : _c_minmax_simple('float'   , 'FLT_MIN'   , 'FLT_MAX'   , 'memcmp(ptr, noneval_float32, 4)'),
	'int64'    : _c_minmax_simple('int64_t' , '-INT64_MAX', 'INT64_MAX' , 'cand_value != INT64_MIN'),
	'int32'    : _c_minmax_simple('int32_t' , '-INT32_MAX', 'INT32_MAX' , 'cand_value != INT32_MIN'),
	'bits64'   : _c_minmax_simple('uint64_t', '0'         , 'UINT64_MAX', '1'),
	'bits32'   : _c_minmax_simple('uint32_t', '0'         , 'UINT32_MAX', '1'),
	'bool'     : _c_minmax_simple('uint8_t' , '0'         , '1'         , 'cand_value != 255'),
	'datetime' : _c_minmax_datetime,
	'date'     : _c_minmax_simple('uint32_t', '545'       , '5119903'   , 'cand_value'),
	'time'     : _c_minmax_datetime,
}

if len(struct.pack("@L", 0)) == 8:
	strtol_f = 'strtol'
	strtoul_f = 'strtoul'
	long_t = 'long'
	ulong_t = 'unsigned long'
elif len(struct.pack("@q", 0)) == 8:
	strtol_f = 'strtoll'
	strtoul_f = 'strtoull'
	long_t = 'long long'
	ulong_t = 'unsigned long long'
else:
	raise Exception("Unable to find a suitable 64 bit integer type")

if sys.byteorder == 'little':
	noneval_data = r'''
	// These are signaling NaNs with extra DEADness in the significand
	static const unsigned char noneval_float64[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
	static const unsigned char noneval_float32[4] = {0xde, 0xad, 0x80, 0xff};
	'''
elif sys.byteorder == 'big':
	noneval_data = r'''
	// These are signaling NaNs with extra DEADness in the significand
	static const unsigned char noneval_float64[8] = {0xff, 0xf0, 0xde, 0xad, 0xde, 0xad, 0xde, 0xad};
	static const unsigned char noneval_float32[4] = {0xff, 0x80, 0xde, 0xad};
	'''
else:
	raise Exception('Unknown byteorder ' + sys.byteorder)

noneval_data += r'''
// The smallest value is one less than -biggest, so that seems like a good signal value.
static const int64_t noneval_int64 = INT64_MIN;
static const int32_t noneval_int32 = INT32_MIN;

static const uint64_t noneval_datetime = 0;
static const uint64_t noneval_time = 0;
static const uint32_t noneval_date = 0;

static const uint8_t noneval_bool = 255;
'''

def _conv_json(_):
	dec = json.JSONDecoder().decode
	def conv_json(v):
		return dec(v.decode('utf-8'))
	return conv_json

def _conv_complex(t):
	def conv_complex(v):
		return complex(v.decode('utf-8'))
	return conv_complex

ConvTuple = namedtuple('ConvTuple', 'size conv_code_str pyfunc')
# Size is bytes per value, or 0 for variable size.
# If pyfunc is specified it is called with the type string
# and can return either (type, fmt, fmt_b) or a callable for
# doing the conversion. type needs not be the same type that
# was passed in, but the passed type determines the actual
# type in the dataset.
# If conv_code_str and size is set, the destination type must exist in minmaxfuncs.
convfuncs = {
	'complex64'    : ConvTuple(16, None, _conv_complex),
	'complex32'    : ConvTuple(8, None, _conv_complex),
	# no *i-types for complex since we just reuse the python complex constructor.
	'float64'      : ConvTuple(8, _c_conv_float_template % dict(type='double', func='strtod', whole=1), None),
	'float32'      : ConvTuple(4, _c_conv_float_template % dict(type='float', func='strtof', whole=1) , None),
	'float64i'     : ConvTuple(8, _c_conv_float_template % dict(type='double', func='strtod', whole=0), None),
	'float32i'     : ConvTuple(4, _c_conv_float_template % dict(type='float', func='strtof', whole=0) , None),
	'floatint64e'  : ConvTuple(8, _c_conv_floatint_exact_template % dict(bitsize=64, whole=1, biggest='9007199254740992', smallest='-9007199254740992'), None),
	'floatint32e'  : ConvTuple(4, _c_conv_floatint_exact_template % dict(bitsize=32, whole=1, biggest='INT32_MAX', smallest='-INT32_MAX'), None),
	'floatint64s'  : ConvTuple(8, _c_conv_floatint_saturate_template % dict(bitsize=64, whole=1), None),
	'floatint32s'  : ConvTuple(4, _c_conv_floatint_saturate_template % dict(bitsize=32, whole=1), None),
	'floatint64ei' : ConvTuple(8, _c_conv_floatint_exact_template % dict(bitsize=64, whole=0, biggest='9007199254740992', smallest='-9007199254740992'), None),
	'floatint32ei' : ConvTuple(4, _c_conv_floatint_exact_template % dict(bitsize=32, whole=0, biggest='INT32_MAX', smallest='-INT32_MAX'), None),
	'floatint64si' : ConvTuple(8, _c_conv_floatint_saturate_template % dict(bitsize=64, whole=0), None),
	'floatint32si' : ConvTuple(4, _c_conv_floatint_saturate_template % dict(bitsize=32, whole=0), None),
	'int64_0'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=0 , unsigned=0), None),
	'int32_0'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=0 , unsigned=0), None),
	'bits64_0'     : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=1, base=0 , unsigned=1), None),
	'bits32_0'     : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=0 , unsigned=1), None),
	'int64_8'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=8 , unsigned=0), None),
	'int32_8'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=8 , unsigned=0), None),
	'int64_10'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=10, unsigned=0), None),
	'int32_10'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=10, unsigned=0), None),
	'int64_16'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=16, unsigned=0), None),
	'int32_16'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=16, unsigned=0), None),
	'bits64_8'     : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=1, base=8 , unsigned=1), None),
	'bits32_8'     : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=8 , unsigned=1), None),
	'bits64_10'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=1, base=10, unsigned=1), None),
	'bits32_10'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=10, unsigned=1), None),
	'bits64_16'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=1, base=16, unsigned=1), None),
	'bits32_16'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=16, unsigned=1), None),
	'int64_0i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=0 , unsigned=0), None),
	'int32_0i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=0 , unsigned=0), None),
	'bits64_0i'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=0, base=0 , unsigned=1), None),
	'bits32_0i'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=0 , unsigned=1), None),
	'int64_8i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=8 , unsigned=0), None),
	'int32_8i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=8 , unsigned=0), None),
	'int64_10i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=10, unsigned=0), None),
	'int32_10i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=10, unsigned=0), None),
	'int64_16i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=16, unsigned=0), None),
	'int32_16i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=16, unsigned=0), None),
	'bits64_8i'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=0, base=8 , unsigned=1), None),
	'bits32_8i'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=8 , unsigned=1), None),
	'bits64_10i'   : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=0, base=10, unsigned=1), None),
	'bits32_10i'   : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=10, unsigned=1), None),
	'bits64_16i'   : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype=ulong_t        , func=strtoul_f, nonemarker='0'        , whole=0, base=16, unsigned=1), None),
	'bits32_16i'   : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=16, unsigned=1), None),
	'strbool'      : ConvTuple(1, _c_conv_strbool, None),
	'floatbool'    : ConvTuple(1, _c_conv_floatbool_template % dict(whole=1)                   , None),
	'floatbooli'   : ConvTuple(1, _c_conv_floatbool_template % dict(whole=0)                   , None),
	'datetime:*'   : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_datetime,), _resolve_datetime),
	'date:*'       : ConvTuple(4, _c_conv_date_template % dict(whole=1, conv=_c_conv_date,    ), None),
	'time:*'       : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_time,    ), _resolve_datetime),
	'datetimei:*'  : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_datetime,), _resolve_datetime),
	'datei:*'      : ConvTuple(4, _c_conv_date_template % dict(whole=0, conv=_c_conv_date,    ), None),
	'timei:*'      : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_time,    ), _resolve_datetime),
	'bytes'        : ConvTuple(0, _c_conv_bytes_template % dict(strip=0), None),
	'bytesstrip'   : ConvTuple(0, _c_conv_bytes_template % dict(strip=1), None),
	# unicode[strip]:encoding or unicode[strip]:encoding/errorhandling
	# errorhandling can be one of strict (fail, default), replace (with \ufffd) or ignore (remove char)
	'unicode:*'    : ConvTuple(0, [_c_conv_unicode_setup, _c_conv_unicode_template % dict(strip=0), _c_conv_unicode_cleanup], _resolve_unicode),
	'unicodestrip:*':ConvTuple(0, [_c_conv_unicode_setup, _c_conv_unicode_template % dict(strip=1), _c_conv_unicode_cleanup], partial(_resolve_unicode, strip=True)),
	# ascii[strip]:errorhandling, can be replace (default, replace >127 with \ooo),
	# encode (same as replace, plus \ becomes \134) or strict (>127 is an error).
	'ascii'             : ConvTuple(0, None, lambda _: ('ascii_replace', None, None),),
	'asciistrip'        : ConvTuple(0, None, lambda _: ('asciistrip_replace', None, None),),
	'ascii:replace'     : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=0, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127"), _c_conv_ascii_cleanup], None),
	'asciistrip:replace': ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=1, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127"), _c_conv_ascii_cleanup], None),
	'ascii:encode'      : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=0, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127 || ptr[i] == '\\\\'"), _c_conv_ascii_cleanup], None),
	'asciistrip:encode' : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=1, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127 || ptr[i] == '\\\\'"), _c_conv_ascii_cleanup], None),
	'ascii:strict'      : ConvTuple(0, _c_conv_ascii_strict_template % dict(strip=0), None),
	'asciistrip:strict' : ConvTuple(0, _c_conv_ascii_strict_template % dict(strip=1), None),
	# The number type is handled specially, so no code here.
	'number'       : ConvTuple(0, None, None), # integer when possible (up to +-2**1007-1), float otherwise.
	'number:int'   : ConvTuple(0, None, None), # Never float, but accepts int.0 (or int.00 and so on)
	'json'         : ConvTuple(0, None, _conv_json),
}

# These are not made available as valid values in column2type, but they
# can be selected based on the :fmt specified in those values.
# null_* is used when just copying a column with filtering.
hidden_convfuncs = {
	'javadatetime'       : ConvTuple(8, _c_conv_date_java_ts_template % dict(whole=1, conv=_c_conv_datetime,), None),
	'javadatetimei'      : ConvTuple(8, _c_conv_date_java_ts_template % dict(whole=0, conv=_c_conv_datetime,), None),
	'unicode_utf8'       : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeUTF8'), _c_conv_unicode_cleanup], None),
	'unicodestrip_utf8'  : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeUTF8'), _c_conv_unicode_cleanup], None),
	'unicode_latin1'     : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeLatin1'), _c_conv_unicode_cleanup], None),
	'unicodestrip_latin1': ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeLatin1'), _c_conv_unicode_cleanup], None),
	'unicode_ascii'      : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeASCII'), _c_conv_unicode_cleanup], None),
	'unicodestrip_ascii' : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeASCII'), _c_conv_unicode_cleanup], None),
	'null_blob'          : ConvTuple(0, _c_null_blob_template, None),
	'null_1'             : 1,
	'null_4'             : 4,
	'null_8'             : 8,
	'null_number'        : 0,
}

# The actual type produced, when it is not the same as the key in convfuncs
# Note that this is based on the user-specified type, not whatever the
# resolving function returns.
typerename = {
	'strbool'      : 'bool',
	'floatbool'    : 'bool',
	'floatbooli'   : 'bool',
	'int64_0'      : 'int64',
	'int32_0'      : 'int32',
	'int64_8'      : 'int64',
	'int32_8'      : 'int32',
	'int64_10'     : 'int64',
	'int32_10'     : 'int32',
	'int64_16'     : 'int64',
	'int32_16'     : 'int32',
	'bits64_0'     : 'bits64',
	'bits32_0'     : 'bits32',
	'bits64_8'     : 'bits64',
	'bits32_8'     : 'bits32',
	'bits64_10'    : 'bits64',
	'bits32_10'    : 'bits32',
	'bits64_16'    : 'bits64',
	'bits32_16'    : 'bits32',
	'int64_0i'     : 'int64',
	'int32_0i'     : 'int32',
	'bits64_0i'    : 'bits64',
	'bits32_0i'    : 'bits32',
	'int64_8i'     : 'int64',
	'int32_8i'     : 'int32',
	'int64_10i'    : 'int64',
	'int32_10i'    : 'int32',
	'int64_16i'    : 'int64',
	'int32_16i'    : 'int32',
	'bits64_8i'    : 'bits64',
	'bits32_8i'    : 'bits32',
	'bits64_10i'   : 'bits64',
	'bits32_10i'   : 'bits32',
	'bits64_16i'   : 'bits64',
	'bits32_16i'   : 'bits32',
	'floatint64e'  : 'int64',
	'floatint32e'  : 'int32',
	'floatint64s'  : 'int64',
	'floatint32s'  : 'int32',
	'floatint64ei' : 'int64',
	'floatint32ei' : 'int32',
	'floatint64si' : 'int64',
	'floatint32si' : 'int32',
	'float64i'     : 'float64',
	'float32i'     : 'float32',
	'datetimei'    : 'datetime',
	'javadatetime' : 'datetime',
	'javadatetimei': 'datetime',
	'datei'        : 'date',
	'timei'        : 'time',
	'bytesstrip'   : 'bytes',
	'asciistrip'   : 'ascii',
	'unicodestrip' : 'unicode',
	'number:int'   : 'number',
}

# Byte size of each (real) type
typesizes = {typerename.get(key.split(':')[0], key.split(':')[0]): convfuncs[key].size for key in convfuncs}

# Verify that all types have working (well, findable) writers
# and something approaching the right type of data.
def _test():
	from accelerator.gzwrite import typed_writer, _convfuncs
	for key, data in iteritems(convfuncs):
		key = key.split(":")[0]
		typed_writer(typerename.get(key, key))
		assert data.size in (0, 1, 4, 8, 16), (key, data)
		if isinstance(data.conv_code_str, list):
			for v in data.conv_code_str:
				assert isinstance(v, (str, NoneType)), (key, data)
		else:
			assert isinstance(data.conv_code_str, (str, NoneType)), (key, data)
		if data.conv_code_str and data.size:
			assert typerename.get(key, key) in minmaxfuncs
		assert data.pyfunc is None or callable(data.pyfunc), (key, data)
	for key, mm in iteritems(minmaxfuncs):
		for v in mm:
			assert isinstance(v, str), key
	known = set(v for v in _convfuncs if ':' not in v)
	copy_missing = known - set(copy_types)
	copy_extra = set(copy_types) - known
	assert not copy_missing, 'copy_types missing %r' % (copy_missing,)
	assert not copy_extra, 'copy_types contains unexpected %r' % (copy_extra,)


convert_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int res = 1;
	char buf[%(datalen)s];
	char defbuf[%(datalen)s];
	char buf_col_min[%(datalen)s];
	char buf_col_max[%(datalen)s];
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	err1(g_init(&g, in_fns[current_file], offsets[current_file], 1));
	if (!safe_to_skip_write) {
		for (int i = 0; i < slices; i++) {
			outfhs[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfhs[i]);
		}
	}
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		err1(!slicemap);
	}
	if (default_value) {
		err1(default_value_is_None);
		char *ptr = defbuf;
		line = default_value;
		g.linelen = default_len;
		%(convert)s;
		err1(!ptr);
	}
	if (default_value_is_None) {
#if %(noneval_support)d
		memcpy(defbuf, &%(noneval_name)s, sizeof(%(noneval_name)s));
		default_value = ""; // Used as a bool later
#else
		goto err;
#endif
	}
	%(minmax_setup)s;
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		char *ptr = buf;
		%(convert)s;
		if (!ptr) {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
				continue;
			}
			if (!default_value) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.filename, (long long)i - first_line + 1);
				goto err;
			}
			ptr = defbuf;
			default_count[chosen_slice] += 1;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		%(minmax_code)s;
		err1(gzwrite(outfhs[chosen_slice], ptr, %(datalen)s) != %(datalen)s);
	}
	current_file++;
	if (current_file < in_count) {
		g_init(&g, in_fns[current_file], offsets[current_file], 0);
		goto more_infiles;
	}
	gzFile minmaxfh = gzopen(minmax_fn, gzip_mode);
	err1(!minmaxfh);
	res = g.error;
	if (gzwrite(minmaxfh, buf_col_min, %(datalen)s) != %(datalen)s) res = 1;
	if (gzwrite(minmaxfh, buf_col_max, %(datalen)s) != %(datalen)s) res = 1;
	if (gzclose(minmaxfh)) res = 1;
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

convert_number_template = r'''
// Up to +-(2**1007 - 1). Don't increase this.
#define GZNUMBER_MAX_BYTES 127

static inline int convert_number_do(const char *inptr, char * const outptr_, const int allow_float)
{
	unsigned char *outptr = (unsigned char *)outptr_;
	// First remove whitespace at the start
	while (*inptr == 32 || (*inptr >= 9 && *inptr <= 13)) inptr++;
	// Then check length and what symbols we have
	int inlen = 0;
	int hasdot = 0, hasexp = 0, hasletter = 0;
	while (1) {
		const char c = inptr[inlen];
		if (!c) break;
		if (c == decimal_separator) {
			if (hasdot || hasexp) return 0;
			hasdot = 1;
		}
		if (c == 'e' || c == 'E') {
			if (hasexp) return 0;
			hasexp = 1;
		}
		if (c == 'x' || c == 'X' || c == 'p' || c == 'P') {
			// Avoid accepting strange float formats that only some C libs accept.
			// (Things like "0x1.5p+5", which as I'm sure you can see is 42.)
			return 0;
		}
		if (c == 'n' || c == 'N') {
			// Could be 'nan' or 'inf', both of which are ok floats.
			hasletter = 1;
		}
		inlen++;
	}
	// Now remove whitespace at end
	while (inlen && (inptr[inlen - 1] == 32 || (inptr[inlen - 1] >= 9 && inptr[inlen - 1] <= 13))) inlen--;
	// Then remove ending zeroes if there is a decimal dot and no exponent
	if (hasdot && !hasexp) {
		while (inlen && inptr[inlen - 1] == '0') inlen--;
		// And remove the dot if it's the last character.
		if (inlen && inptr[inlen - 1] == decimal_separator) {
			// Woo, it was an int in disguise!
			inlen--;
			hasdot = 0;
		}
	}
	if (!inlen) {
		*outptr = 8;
		memset(outptr + 1, 0, 8);
		return 9;
	}
	if (hasdot || hasexp || hasletter) { // Float
		if (!allow_float) return 0;
		char *end;
		errno = 0;
		const double value = strtod(inptr, &end);
		if (errno || end != inptr + inlen) {
			return 0;
		} else {
			*outptr = 1;
			memcpy(outptr + 1, &value, 8);
			return 9;
		}
	} else {
		char *end;
		errno = 0;
		const int64_t value = %(strtol_f)s(inptr, &end, 10);
		if (errno || end != inptr + inlen) { // big or invalid
			PyObject *s = PyBytes_FromStringAndSize(inptr, inlen);
			if (!s) exit(1); // All is lost
			PyObject *i = PyNumber_Long(s);
			if (!i) PyErr_Clear();
			Py_DECREF(s);
			if (!i) return 0;
			const size_t len_bits = _PyLong_NumBits(i);
			err1(len_bits == (size_t)-1);
			size_t len_bytes = len_bits / 8 + 1;
			err1(len_bytes >= GZNUMBER_MAX_BYTES);
			if (len_bytes < 8) len_bytes = 8; // Could happen for "42L" or similar.
			*outptr = len_bytes;
			err1(_PyLong_AsByteArray((PyLongObject *)i, outptr + 1, len_bytes, 1, 1) < 0);
			Py_DECREF(i);
			return len_bytes + 1;
err:
			Py_DECREF(i);
			return 0;
		} else {
			*outptr = 8;
			memcpy(outptr + 1, &value, 8);
			return 9;
		}
	}
}

%(proto)s
{
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int  res = 1;
	char buf[GZNUMBER_MAX_BYTES];
	char defbuf[GZNUMBER_MAX_BYTES];
	char buf_col_min[GZNUMBER_MAX_BYTES];
	char buf_col_max[GZNUMBER_MAX_BYTES];
	int  deflen = 0;
	int  minlen = 0;
	int  maxlen = 0;
	PyObject *o_col_min = 0;
	PyObject *o_col_max = 0;
	double d_col_min = 0;
	double d_col_max = 0;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	const int allow_float = !fmt;
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	err1(g_init(&g, in_fns[current_file], offsets[current_file], 1));
	if (!safe_to_skip_write) {
		for (int i = 0; i < slices; i++) {
			outfhs[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfhs[i]);
		}
	}
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		err1(!slicemap);
	}
	if (default_value) {
		err1(default_value_is_None);
		deflen = convert_number_do(default_value, defbuf, allow_float);
		err1(!deflen);
	}
	if (default_value_is_None) {
		defbuf[0] = 0;
		deflen = 1;
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		char *ptr = buf;
		int len = convert_number_do(line, ptr, allow_float);
		if (!len) {
			if (record_bad && !deflen) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
				continue;
			}
			if (!deflen) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.filename, (long long)i - first_line + 1);
				goto err;
			}
			ptr = defbuf;
			len = deflen;
			default_count[chosen_slice] += 1;
		}
		// minmax tracking, not done for None-values
		if (len > 1) {
			double d_v = 0;
			PyObject *o_v = 0;
			if (*ptr == 1) { // It's a double
				memcpy(&d_v, ptr + 1, 8);
			} else if (*ptr == 8) { // It's an int64_t
				int64_t tmp;
				memcpy(&tmp, ptr + 1, 8);
				if (tmp <= ((int64_t)1 << 53) && tmp >= -((int64_t)1 << 53)) {
					// Fits in a double without precision loss
					d_v = tmp;
				} else {
					o_v = PyLong_FromLong(tmp);
					err1(!o_v);
				}
			} else { // It's a big number
				o_v = _PyLong_FromByteArray((unsigned char *)ptr + 1, *ptr, 1, 1);
				err1(!o_v);
			}
			if (!o_v && (o_col_min || o_col_max)) {
				o_v = PyFloat_FromDouble(d_v);
				err1(!o_v);
			}

			if (minlen) {
				if (o_v) {
					if (!o_col_min) {
						o_col_min = PyFloat_FromDouble(d_col_min);
					}
					if (!o_col_max) {
						o_col_max = PyFloat_FromDouble(d_col_max);
					}
					if (PyObject_RichCompareBool(o_v, o_col_min, Py_LT)) {
						memcpy(buf_col_min, ptr, len);
						minlen = len;
						Py_INCREF(o_v);
						Py_DECREF(o_col_min);
						o_col_min = o_v;
					}
					if (PyObject_RichCompareBool(o_v, o_col_max, Py_GT)) {
						memcpy(buf_col_max, ptr, len);
						maxlen = len;
						Py_INCREF(o_v);
						Py_DECREF(o_col_max);
						o_col_max = o_v;
					}
					Py_DECREF(o_v);
				} else {
					if (d_v < d_col_min) {
						memcpy(buf_col_min, ptr, len);
						minlen = len;
						d_col_min = d_v;
					}
					if (d_v > d_col_max) {
						memcpy(buf_col_max, ptr, len);
						maxlen = len;
						d_col_max = d_v;
					}
				}
			} else {
				memcpy(buf_col_min, ptr, len);
				memcpy(buf_col_max, ptr, len);
				minlen = maxlen = len;
				d_col_min = d_col_max = d_v;
				o_col_min = o_col_max = o_v;
				if (o_v) Py_INCREF(o_v);
			}
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], ptr, len) != len);
	}
	current_file++;
	if (current_file < in_count) {
		g_init(&g, in_fns[current_file], offsets[current_file], 0);
		goto more_infiles;
	}
	gzFile minmaxfh = gzopen(minmax_fn, gzip_mode);
	err1(!minmaxfh);
	res = g.error;
	if (minlen) {
		if (gzwrite(minmaxfh, buf_col_min, minlen) != minlen) res = 1;
		if (gzwrite(minmaxfh, buf_col_max, maxlen) != maxlen) res = 1;
	} else {
		if (gzwrite(minmaxfh, "\0\0", 2) != 2) res = 1;
	}
	if (gzclose(minmaxfh)) res = 1;
err:
	Py_XDECREF(o_col_min);
	Py_XDECREF(o_col_max);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
	return res;
}
'''

proto_template = 'static int convert_column_%s(const char **in_fns, int in_count, const char **out_fns, const char *gzip_mode, const char *minmax_fn, const char *default_value, uint32_t default_len, int default_value_is_None, const char *fmt, const char *fmt_b, int record_bad, int skip_bad, int badmap_fd, size_t badmap_size, int slices, int slicemap_fd, size_t slicemap_size, uint64_t *bad_count, uint64_t *default_count, off_t *offsets, int64_t *max_counts, int safe_to_skip_write)'

protos = []
funcs = [noneval_data]

proto = proto_template % ('number',)
code = convert_number_template % dict(proto=proto, strtol_f=strtol_f)
protos.append(proto + ';')
funcs.append(code)

convert_blob_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int res = 1;
	uint8_t *defbuf = 0;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	err1(g_init(&g, in_fns[current_file], offsets[current_file], 1));
	if (!safe_to_skip_write) {
		for (int i = 0; i < slices; i++) {
			outfhs[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfhs[i]);
		}
	}
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		err1(!slicemap);
	}
%(setup)s
	if (default_value) {
		err1(default_value_is_None);
		line = default_value;
		g.linelen = default_len;
%(convert)s
		err1(!ptr);
		defbuf = malloc((uint32_t)len + 5);
		err1(!defbuf);
		if (len < 255) {
			defbuf[0] = len;
			memcpy(defbuf + 1, ptr, len);
			default_len = len + 1;
		} else {
			defbuf[0] = 255;
			memcpy(defbuf + 1, &len, 4);
			memcpy(defbuf + 5, ptr, len);
			default_len = (uint32_t)len + 5;
		}
		default_value = (const char *)defbuf;
%(cleanup)s
	}
	if (default_value_is_None) {
		default_value = "\xff\0\0\0\0";
		default_len = 5;
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		if (line == NoneMarker) {
			err1(gzwrite(outfhs[chosen_slice], "\xff\0\0\0\0", 5) != 5);
			continue;
		}
%(convert)s
		if (ptr) {
			if (len > 254) {
				uint8_t lenbuf[5];
				lenbuf[0] = 255;
				memcpy(lenbuf + 1, &len, 4);
				err1(gzwrite(outfhs[chosen_slice], lenbuf, 5) != 5);
			} else {
				uint8_t len8 = len;
				err1(gzwrite(outfhs[chosen_slice], &len8, 1) != 1);
			}
		} else {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
%(cleanup)s
				continue;
			}
			if (!default_value) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.filename, (long long)i - first_line + 1);
				goto err;
			}
			ptr = (const uint8_t *)default_value;
			len = default_len;
			default_count[chosen_slice] += 1;
		}
		err1(gzwrite(outfhs[chosen_slice], ptr, len) != len);
%(cleanup)s
	}
	current_file++;
	if (current_file < in_count) {
		g_init(&g, in_fns[current_file], offsets[current_file], 0);
		goto more_infiles;
	}
	gzFile minmaxfh = gzopen(minmax_fn, gzip_mode);
	err1(!minmaxfh);
	res = g.error;
	if (gzwrite(minmaxfh, "\xff\0\0\0\0\xff\0\0\0\0", 10) != 10) res = 1;
	if (gzclose(minmaxfh)) res = 1;
err:
	if (defbuf) free(defbuf);
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

null_number_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	int res = 1;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	err1(g_init(&g, in_fns[current_file], offsets[current_file], 1));
	if (!safe_to_skip_write) {
		for (int i = 0; i < slices; i++) {
			outfhs[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfhs[i]);
		}
	}
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		err1(!slicemap);
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	unsigned char buf[GZNUMBER_MAX_BYTES];
	for (; i < max_count && !read_fixed(&g, buf, 1); i++) {
		int z = buf[0];
		if (z == 1) z = 8;
		if (z) {
			err1(read_fixed(&g, buf + 1, z));
		}
		z++;
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], buf, z) != z);
	}
	current_file++;
	if (current_file < in_count) {
		g_init(&g, in_fns[current_file], offsets[current_file], 0);
		goto more_infiles;
	}
	res = g.error;
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

null_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	int res = 1;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	err1(g_init(&g, in_fns[current_file], offsets[current_file], 1));
	if (!safe_to_skip_write) {
		for (int i = 0; i < slices; i++) {
			outfhs[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfhs[i]);
		}
	}
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		err1(!slicemap);
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	unsigned char buf[%(size)d];
	for (; i < max_count && !read_fixed(&g, buf, %(size)d); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], buf, %(size)d) != %(size)d);
	}
	current_file++;
	if (current_file < in_count) {
		g_init(&g, in_fns[current_file], offsets[current_file], 0);
		goto more_infiles;
	}
	res = g.error;
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

for name, ct in sorted(list(convfuncs.items()) + list(hidden_convfuncs.items())):
	if isinstance(ct, int):
		proto = proto_template % (name,)
		if ct:
			code = null_template % dict(proto=proto, size=ct,)
		else:
			code = null_number_template % dict(proto=proto,)
	elif not ct.conv_code_str:
		continue
	elif ct.size:
		if ':' in name:
			shortname = name.split(':', 1)[0]
		else:
			shortname = name
		proto = proto_template % (shortname,)
		destname = typerename.get(shortname, shortname)
		mm = minmaxfuncs[destname]
		noneval_support = not destname.startswith('bits')
		noneval_name = 'noneval_' + destname
		code = convert_template % dict(proto=proto, datalen=ct.size, convert=ct.conv_code_str, minmax_setup=mm.setup, minmax_code=mm.code, noneval_support=noneval_support, noneval_name=noneval_name)
	else:
		proto = proto_template % (name.replace(':*', '').replace(':', '_'),)
		args = dict(proto=proto, convert=ct.conv_code_str, setup='', cleanup='')
		if isinstance(ct.conv_code_str, list):
			args['setup'], args['convert'], args['cleanup'] = ct.conv_code_str
		code = convert_blob_template % args
	protos.append(proto + ';')
	funcs.append(code)

copy_types = {typerename.get(k.split(':')[0], k.split(':')[0]): 'null_%d' % (v.size,) if v.size else 'null_blob' for k, v in convfuncs.items()}
copy_types['number'] = 'null_number'
copy_types['pickle'] = 'null_blob'


all_c_functions = r'''
#include <zlib.h>
#include <time.h>
#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include <errno.h>
#include <sys/mman.h>
#include <math.h>
#include <float.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <bytesobject.h>
#include <datetime.h>

#ifndef MAP_NOSYNC
#  define MAP_NOSYNC 0
#endif

#define err1(v) if (v) goto err
#define err2(v, msg) if (v) { err = msg; goto err; }
#define Z (128 * 1024)

typedef struct {
	gzFile fh;
	int len;
	int pos;
	int error;
	uint32_t saved_size;
	int32_t linelen;
	const char *filename;
	char *largetmp;
	char buf[Z + 1];
} g;

static const char NoneMarker[1] = {0};
static char decimal_separator = '.';

static int g_init(g *g, const char *filename, off_t offset, const int first)
{
	if (!first) {
		int e = gzclose(g->fh);
		g->fh = 0;
		if (e || g->error) return 1;
	}
	g->fh = 0;
	g->pos = g->len = 0;
	g->error = 0;
	g->filename = filename;
	if (first) g->largetmp = 0;
	int fd = open(filename, O_RDONLY);
	if (fd < 0) return 1;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g->fh = gzdopen(fd, "rb");
	if (!g->fh) goto errfd;
	return 0;
errfd:
	close(fd);
	return 1;
}

static int g_cleanup(g *g)
{
	if (g->largetmp) free(g->largetmp);
	if (g->fh) return gzclose(g->fh);
	return 0;
}

static int numeric_comma(const char *localename)
{
	decimal_separator = ',';
	if (setlocale(LC_NUMERIC, localename)) {
		return strtod("1,5", 0) != 1.5;
	}
	return 1;
}

static int read_chunk(g *g, int offset)
{
	if (g->error) return 1;
	const int len = gzread(g->fh, g->buf + offset, Z - offset);
	if (len <= 0) {
		(void) gzerror(g->fh, &g->error);
		return 1;
	}
	g->len = offset + len;
	g->buf[g->len] = 0;
	g->pos = 0;
	return 0;
}

static inline const char *read_line(g *g)
{
	if (g->largetmp) {
		free(g->largetmp);
		g->largetmp = 0;
	}
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 0;
	}
	if (!g->pos) {
		uint8_t *uptr = (uint8_t *)g->buf + g->pos;
		g->saved_size = *uptr;
	}
	uint32_t size = g->saved_size;
	g->pos++;
size_again:
	if (size == 255) {
		const int offset = g->len - g->pos;
		if (offset < 4) {
			memmove(g->buf, g->buf + g->pos, offset);
			if (read_chunk(g, offset) || g->len < 4) {
				PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
				g->error = 1;
				return 0;
			}
			goto size_again;
		}
		memcpy(&size, g->buf + g->pos, 4);
		g->pos += 4;
		if (size == 0) {
			if (g->len > g->pos) {
				uint8_t *uptr = (uint8_t *)g->buf + g->pos;
				g->saved_size = *uptr;
			}
			g->linelen = 0;
			return NoneMarker;
		} else if (size < 255 || size > 0x7fffffff) {
			PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
			g->error = 1;
			return 0;
		}
	}
	unsigned int avail = g->len - g->pos;
	if (size > Z) {
		g->largetmp = malloc(size + 1);
		if (!g->largetmp) {
			perror("malloc");
			g->error = 1;
			return 0;
		}
		memcpy(g->largetmp, g->buf + g->pos, avail);
		const int fill_len = size - avail;
		const int read_len = gzread(g->fh, g->largetmp + avail, fill_len);
		if (read_len != fill_len) {
			PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
			g->error = 1;
			return 0;
		}
		g->largetmp[size] = 0;
		g->linelen = size;
		g->pos = g->len;
		return g->largetmp;
	}
	if (avail < size) {
		memmove(g->buf, g->buf + g->pos, avail);
		if (read_chunk(g, avail)) {
			PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
			g->error = 1;
			return 0;
		}
		avail = g->len;
		if (avail < size) {
			PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
			g->error = 1;
			return 0;
		}
	}
	char *res = g->buf + g->pos;
	g->pos += size;
	if (g->len > g->pos) {
		uint8_t *uptr = (uint8_t *)g->buf + g->pos;
		g->saved_size = *uptr;
	}
	res[size] = 0;
	g->linelen = size;
	return res;
}

static inline int read_fixed(g *g, unsigned char *res, int z)
{
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 1;
	}
	int avail = g->len - g->pos;
	if (avail < z) {
		err1(z <= 1); // This can't happen, but some compilers produce warnings without it.
		memcpy(res, g->buf + g->pos, avail);
		res += avail;
		z -= avail;
		if (read_chunk(g, 0) || g->len < z) {
err:
			PyErr_Format(PyExc_IOError, "%s: Format error\n", g->filename);
			g->error = 1;
			return 1;
		}
	}
	memcpy(res, g->buf + g->pos, z);
	g->pos += z;
	return 0;
}

static int use_tz = 0;

static void init(const char *tz)
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyDateTime_IMPORT;
	PyGILState_Release(gstate);
#else
	PyDateTime_IMPORT;
#endif
	if (tz) {
		use_tz = 1;
	} else {
		// strptime parses %s in whatever timezone is set,
		// so set UTC if user does not ask for a timezone.
		tz = "UTC";
	}
	if (setenv("TZ", tz, 1)) exit(1);
	tzset();
}
''' + ''.join(funcs)


extra_c_functions = r'''
static PyObject *py_init(PyObject *dummy, PyObject *o_tz)
{
	const char *tz;
	if (str_or_0(o_tz, &tz)) return 0;
	init(tz);
	Py_RETURN_NONE;
}

static PyObject *py_numeric_comma(PyObject *dummy, PyObject *o_localename)
{
	const char *localename;
	if (str_or_0(o_localename, &localename)) return 0;
	if (numeric_comma(localename)) Py_RETURN_TRUE;
	Py_RETURN_FALSE;
}
'''


c_module_wrapper_template = r'''
static PyObject *py_%s(PyObject *self, PyObject *args)
{
	int good = 0;
	const char *err = 0;
	PyObject *o_in_fns;
	int in_count;
	const char **in_fns = 0;
	PyObject *o_out_fns;
	const char **out_fns = 0;
	const char *gzip_mode;
	const char *minmax_fn;
	PyObject *o_default_value;
	const char *default_value;
	int default_len;
	int default_value_is_None;
	PyObject *o_fmt;
	const char *fmt;
	PyObject *o_fmt_b;
	const char *fmt_b;
	int record_bad;
	int skip_bad;
	int badmap_fd;
	PY_LONG_LONG badmap_size;
	int slices;
	int slicemap_fd;
	PY_LONG_LONG slicemap_size;
	PyObject *o_bad_count;
	uint64_t *bad_count = 0;
	PyObject *o_default_count;
	uint64_t *default_count = 0;
	PyObject *o_offsets;
	off_t *offsets = 0;
	PyObject *o_max_counts;
	int64_t *max_counts = 0;
	int safe_to_skip_write;
	if (!PyArg_ParseTuple(args, "OiOetetOiiOOiiiLiiLOOOOi",
		&o_in_fns,
		&in_count,
		&o_out_fns,
		Py_FileSystemDefaultEncoding, &gzip_mode,
		Py_FileSystemDefaultEncoding, &minmax_fn,
		&o_default_value,
		&default_len,
		&default_value_is_None,
		&o_fmt,
		&o_fmt_b,
		&record_bad,
		&skip_bad,
		&badmap_fd,
		&badmap_size,
		&slices,
		&slicemap_fd,
		&slicemap_size,
		&o_bad_count,
		&o_default_count,
		&o_offsets,
		&o_max_counts,
		&safe_to_skip_write
	)) {
		return 0;
	}
	if (str_or_0(o_default_value, &default_value)) return 0;
	if (str_or_0(o_fmt, &fmt)) return 0;
	if (str_or_0(o_fmt_b, &fmt_b)) return 0;
#define LISTCHK(name, cnt) \
	err2(!PyList_Check(o_ ## name) || PyList_Size(o_ ## name) != cnt, \
		#name " must be list with " #cnt " elements" \
	);
	LISTCHK(bad_count, slices);
	LISTCHK(default_count, slices);
	LISTCHK(in_fns, in_count);
	LISTCHK(offsets, in_count);
	LISTCHK(max_counts, in_count);
	LISTCHK(out_fns, slices);
#undef LISTCHK
	in_fns = malloc(in_count * sizeof(*in_fns));
	err1(!in_fns);
	offsets = malloc(in_count * sizeof(*offsets));
	err1(!offsets);
	max_counts = malloc(in_count * sizeof(*max_counts));
	err1(!max_counts);
	for (int i = 0; i < in_count; i++) {
		in_fns[i] = PyBytes_AS_STRING(PyList_GetItem(o_in_fns, i));
		err1(!in_fns[i]);
		offsets[i] = PyLong_AsLongLong(PyList_GetItem(o_offsets, i));
		err1(PyErr_Occurred());
		max_counts[i] = PyLong_AsLongLong(PyList_GetItem(o_max_counts, i));
		err1(PyErr_Occurred());
	}

	out_fns = malloc(slices * sizeof(*out_fns));
	err1(!out_fns);
	default_count = malloc(slices * 8);
	err1(!default_count);
	bad_count = malloc(slices * 8);
	err1(!bad_count);
	for (int i = 0; i < slices; i++) {
		out_fns[i] = PyBytes_AS_STRING(PyList_GetItem(o_out_fns, i));
		err1(!out_fns[i]);
		default_count[i] = bad_count[i] = 0;
	}

	err1(%s(in_fns, in_count, out_fns, gzip_mode, minmax_fn, default_value, default_len, default_value_is_None, fmt, fmt_b, record_bad, skip_bad, badmap_fd, badmap_size, slices, slicemap_fd, slicemap_size, bad_count, default_count, offsets, max_counts, safe_to_skip_write));
	for (int i = 0; i < slices; i++) {
		err1(PyList_SetItem(o_default_count, i, PyLong_FromUnsignedLongLong(default_count[i])));
		err1(PyList_SetItem(o_bad_count, i, PyLong_FromUnsignedLongLong(bad_count[i])));
	}
	good = 1;
err:
	if (bad_count) free(bad_count);
	if (default_count) free(default_count);
	if (out_fns) free(out_fns);
	if (max_counts) free(max_counts);
	if (offsets) free(offsets);
	if (in_fns) free(in_fns);
	if (good) Py_RETURN_NONE;
	if (err) {
		PyErr_SetString(PyExc_ValueError, err);
	} else if (!PyErr_Occurred()) {
		PyErr_SetString(PyExc_ValueError, "internal error");
		return 0;
	}
	return 0;
}
'''

extra_method_defs = [
	'{"init", py_init, METH_O, 0}',
	'{"numeric_comma", py_numeric_comma, METH_O, 0}',
]

c_module_code, c_module_hash = c_backend_support.make_source('dataset_type', all_c_functions, protos, extra_c_functions, extra_method_defs, c_module_wrapper_template)

def init():
	_test()
	extra_protos = [
		'static int numeric_comma(const char *localename);',
		'static void init(const char *tz);',
	]
	return c_backend_support.init('dataset_type', c_module_hash, protos, extra_protos, all_c_functions)
