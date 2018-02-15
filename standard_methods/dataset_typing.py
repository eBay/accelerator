############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

# Support functions and data relating to typed data.
# Used by methods that convert or filter such data.

from __future__ import division
from __future__ import absolute_import

from types import NoneType
from collections import namedtuple
from functools import partial
import ujson

__all__ = ('convfuncs', 'typerename', 'typesizes', 'minmaxfuncs',)

def _mk_conv_datetime(coltype, fmt):
	import datetime
	strptime = datetime.datetime.strptime
	def conv(v):
		return strptime(v.strip(), fmt)
	return conv

def _mk_conv_time(coltype, fmt):
	import datetime
	strptime = datetime.datetime.strptime
	def conv(v):
		return strptime(v.strip(), fmt).time()
	return conv

def _unsupported_fmt(coltype, fmt):
	raise Exception('Unsupported format "%s" for coltype %s (%%f only works on non-i datetimes)' % (fmt, coltype,))

def _mk_conv_unicode(colname, fmt, strip=False):
	if '/' in fmt:
		codec, errors = fmt.split('/')
	else:
		codec, errors = fmt, 'strict'
	assert errors in ('strict', 'replace', 'ignore',)
	''.decode(codec) # trigger error on unknown
	if strip:
		def conv(s):
			return s.decode(codec, errors).strip()
		return conv
	else:
		def conv(s):
			return s.decode(codec, errors)
		return conv

def _mk_conv_ascii(colname, errors, strip=False):
	assert errors in ('replace', 'encode', 'strict',)
	if errors == 'strict':
		# The writer will enforce the invariant
		if strip:
			return str.strip
		else:
			return str
	else:
		import re
		if errors == 'encode':
			pattern = re.compile(b'[\x80-\xff\\\\]')
		else:
			pattern = re.compile(b'[\x80-\xff]')
		def replace(m):
			return '\\%03o' % ord(m.group(0))
		if strip:
			def conv(s):
				return pattern.sub(replace, s.strip())
			return conv
		else:
			return partial(pattern.sub, replace)

def _conv_number_int(v):
	try:
		return int(v)
	except ValueError:
		i, d = v.strip().split('.')
		if d.strip('0'):
			raise ValueError("number:int got float with non-0 decimals: " + v)
		return int(i)

def _conv_number(v):
	try:
		return _conv_number_int(v)
	except ValueError:
		# "4.2e1" and similar can get here, but you don't
		# get ints from those.
		return float(v)

_c_conv_date_template = r'''
	struct tm tm;
	memset(&tm, 0, sizeof(tm));
	char *pres = strptime(line, fmt, &tm);
	if (%(whole)d && pres) {
		while (*pres == 32 || (*pres >= 9 && *pres <= 13)) pres++;
	}
	if (pres && (!%(whole)d || !*pres)) {
		uint32_t *p = (uint32_t *)ptr;
%(conv)s
	} else {
		ptr = 0;
	}
'''
_c_conv_datetime = r'''
		p[0] = (uint32_t)(tm.tm_year + 1900) << 14 | (uint32_t)(tm.tm_mon + 1) << 10 |
		       (uint32_t)tm.tm_mday << 5 | tm.tm_hour;
		p[1] = (uint32_t)tm.tm_min << 26 | (uint32_t)tm.tm_sec << 20;
'''
_c_conv_date = r'''
		p[0] = (uint32_t)(tm.tm_year + 1900) << 9 | (uint32_t)(tm.tm_mon + 1) << 5 | tm.tm_mday;
'''
_c_conv_time = r'''
		p[0] = 32277536 | tm.tm_hour; // 1970 if read as datetime
		p[1] = (uint32_t)tm.tm_min << 26 | (uint32_t)tm.tm_sec << 20;
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
		char *endptr;
		%(rtype)s value = %(func)s(line, &endptr, %(base)d);
#if %(whole)d
		while (*endptr == 32 || (*endptr >= 9 && *endptr <= 13)) endptr++;
		if (*endptr) { // not a valid int
			ptr = 0;
		} else
#endif
		if (errno == ERANGE) { // out of range
			ptr = 0;
		} else {
			%(type)s *p = (%(type)s *)ptr;
			*p = value;
			if (value != *p || (%(nonemarker)s && value == %(nonemarker)s)) {
				// Over/underflow (values that don't fit are not ok)
				ptr = 0;
			}
		}
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
	'float64'  : _c_minmax_simple('double'  , 'DBL_MIN'   , 'DBL_MAX'   , 'memcmp(ptr, noneval_double, 8)'),
	'float32'  : _c_minmax_simple('float'   , 'FLT_MIN'   , 'FLT_MAX'   , 'memcmp(ptr, noneval_float, 4)'),
	'int64'    : _c_minmax_simple('int64_t' , '-INT64_MAX', 'INT64_MAX' , 'cand_value != INT64_MIN'),
	'int32'    : _c_minmax_simple('int32_t' , '-INT32_MAX', 'INT32_MAX' , 'cand_value != INT32_MIN'),
	'bits64'   : _c_minmax_simple('uint64_t', '0'         , 'UINT64_MAX', '1'),
	'bits32'   : _c_minmax_simple('uint32_t', '0'         , 'UINT32_MAX', '1'),
	'bool'     : _c_minmax_simple('uint8_t' , '0'         , '1'         , 'cand_value != 255'),
	'datetime' : _c_minmax_datetime,
	'date'     : _c_minmax_simple('uint32_t', '545'       , '5119903'   , 'cand_value'),
	'time'     : _c_minmax_datetime,
}

minmax_data = r'''
// These are signaling NaNs with extra DEADness in the significand
static const unsigned char noneval_double[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
static const unsigned char noneval_float[4] = {0xde, 0xad, 0x80, 0xff};
'''

noneval_data = r'''
// These are signaling NaNs with extra DEADness in the significand
static const unsigned char noneval_float64[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
static const unsigned char noneval_float32[4] = {0xde, 0xad, 0x80, 0xff};

// The smallest value is one less than -biggest, so that seems like a good signal value.
static const int64_t noneval_int64 = INT64_MIN;
static const int32_t noneval_int32 = INT32_MIN;

static const uint64_t noneval_datetime = 0;
static const uint64_t noneval_time = 0;
static const uint32_t noneval_date = 0;

static const uint8_t noneval_bool = 255;
'''

ConvTuple = namedtuple('ConvTuple', 'size conv_code_str pyfunc')
# Size is bytes per value, or 0 for newline separated.
# Only one of conv_code_str or pyfunc needs to be specified, except
# the date/time types always use the python_func when the format contains %f.
# If conv_code_str is set, the destination type must exist in minmaxfuncs.
convfuncs = {
	'float64'      : ConvTuple(8, _c_conv_float_template % dict(type='double', func='strtod', whole=1), float),
	'float32'      : ConvTuple(4, _c_conv_float_template % dict(type='float', func='strtof', whole=1) , float),
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
	'int64_0'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=1, base=0 ), int),
	'int32_0'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=0 ), int),
	'bits64_0'     : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=0 ), int),
	'bits32_0'     : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=0 ), int),
	'int64_8'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=1, base=8 ), lambda v: int(v, 8)),
	'int32_8'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=8 ), lambda v: int(v, 8)),
	'int64_10'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=1, base=10), lambda v: int(v, 10)),
	'int32_10'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=10), lambda v: int(v, 10)),
	'int64_16'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=1, base=16), lambda v: int(v, 16)),
	'int32_16'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=16), lambda v: int(v, 16)),
	'bits64_8'     : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=8 ), lambda v: int(v, 8)),
	'bits32_8'     : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=8 ), lambda v: int(v, 8)),
	'bits64_10'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=10), lambda v: int(v, 10)),
	'bits32_10'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=10), lambda v: int(v, 10)),
	'bits64_16'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=16), lambda v: int(v, 16)),
	'bits32_16'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=1, base=16), lambda v: int(v, 16)),
	'int64_0i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=0, base=0 ), None),
	'int32_0i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=0 ), None),
	'bits64_0i'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=0 ), None),
	'bits32_0i'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=0 ), None),
	'int64_8i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=0, base=8 ), None),
	'int32_8i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=8 ), None),
	'int64_10i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=0, base=10), None),
	'int32_10i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=10), None),
	'int64_16i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype='long'         , func='strtol' , nonemarker='INT64_MIN', whole=0, base=16), None),
	'int32_16i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=16), None),
	'bits64_8i'    : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=8 ), None),
	'bits32_8i'    : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=8 ), None),
	'bits64_10i'   : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=10), None),
	'bits32_10i'   : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=10), None),
	'bits64_16i'   : ConvTuple(8, _c_conv_int_template % dict(type='uint64_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=16), None),
	'bits32_16i'   : ConvTuple(4, _c_conv_int_template % dict(type='uint32_t', rtype='unsigned long', func='strtoul', nonemarker='0'        , whole=0, base=16), None),
	'strbool'      : ConvTuple(1, _c_conv_strbool, lambda v: v.lower() not in ('false', '0', 'f', 'no', 'off', 'nil', 'null', '',)),
	'floatbool'    : ConvTuple(1, _c_conv_floatbool_template % dict(whole=1)                   , lambda v: float(v) != 0.0),
	'floatbooli'   : ConvTuple(1, _c_conv_floatbool_template % dict(whole=0)                   , None),
	'datetime:*'   : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_datetime,), _mk_conv_datetime),
	'date:*'       : ConvTuple(4, _c_conv_date_template % dict(whole=1, conv=_c_conv_date,    ), _mk_conv_datetime),
	'time:*'       : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_time,    ), _mk_conv_time),
	'datetimei:*'  : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_datetime,), _unsupported_fmt),
	'datei:*'      : ConvTuple(4, _c_conv_date_template % dict(whole=0, conv=_c_conv_date,    ), _unsupported_fmt),
	'timei:*'      : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_time,    ), _unsupported_fmt),
	'bytes'        : ConvTuple(0, None, str),
	'bytesstrip'   : ConvTuple(0, None, str.strip),
	# unicode[strip]:encoding or unicode[strip]:encoding/errorhandling
	# errorhandling can be one of strict (fail, default), replace (with \ufffd) or ignore (remove char)
	'unicode:*'    : ConvTuple(0, None, _mk_conv_unicode),
	'unicodestrip:*':ConvTuple(0, None, partial(_mk_conv_unicode, strip=True)),
	'ascii'        : ConvTuple(0, None, _mk_conv_ascii(None, 'replace')),
	'asciistrip'   : ConvTuple(0, None, _mk_conv_ascii(None, 'replace', True)),
	# ascii[strip]:errorhandling, can be replace (default, replace >128 with \ooo),
	# encode (same as replace, plus \ becomes \134) or strict (>128 is an error).
	'ascii:*'      : ConvTuple(0, None, _mk_conv_ascii),
	'asciistrip:*' : ConvTuple(0, None, partial(_mk_conv_ascii, strip=True)),
	'number'       : ConvTuple(0, None, _conv_number    ), # integer when possible (up to +-2**1007-1), float otherwise.
	'number:int'   : ConvTuple(0, None, _conv_number_int), # Never float, but accepts int.0 (or int.00 and so on)
	'json'         : ConvTuple(0, None, ujson.loads),
}

# The actual type produced, when it is not the same as the key in convfuncs
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
	from gzwrite import typed_writer
	for key, data in convfuncs.iteritems():
		key = key.split(":")[0]
		typed_writer(typerename.get(key, key))
		assert data.size in (0, 1, 4, 8,), (key, data)
		assert isinstance(data.conv_code_str, (str, NoneType)), (key, data)
		if data.conv_code_str:
			assert typerename.get(key, key) in minmaxfuncs
		assert data.pyfunc is None or callable(data.pyfunc), (key, data)
	for key, mm in minmaxfuncs.iteritems():
		for v in mm:
			assert isinstance(v, str), key
_test()
