############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2019 Carl Drougge                       #
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

from __future__ import division
from __future__ import absolute_import

import cffi
from resource import getpagesize
from os import unlink, symlink
from mmap import mmap, PROT_READ

from accelerator.compat import NoneType, unicode, imap, iteritems, itervalues, PY2

from accelerator.extras import OptionEnum, json_save, DotDict
from accelerator.gzwrite import typed_writer
from accelerator.dataset import DatasetWriter
from accelerator.report import report
from accelerator.sourcedata import type2iter
from . import dataset_typing

depend_extra = (dataset_typing,)

description = r'''
Convert one or more columns in a dataset from bytes/ascii/unicode to any type.
'''

# Without filter_bad the method fails when a value fails to convert and
# doesn't have a default. With filter_bad the value is filtered out
# together with all other values on the same line.
#
# With filter_bad a new dataset is produced - columns not specified in
# column2type become inaccessible.
#
# If you need to preserve unconverted columns with filter_bad, specify them
# as converted to bytes.

TYPENAME = OptionEnum(dataset_typing.convfuncs.keys())

options = {
	'column2type'               : {'COLNAME': TYPENAME},
	'defaults'                  : {}, # {'COLNAME': value}, unspecified -> method fails on unconvertible unless filter_bad
	'rename'                    : {}, # {'OLDNAME': 'NEWNAME'} doesn't shadow OLDNAME.
	'caption'                   : 'typed dataset',
	'discard_untyped'           : bool, # Make unconverted columns inaccessible ("new" dataset)
	'filter_bad'                : False, # Implies discard_untyped
	'numeric_comma'             : False, # floats as "3,14"
}

datasets = ('source', 'previous',)

byteslike_types = ('bytes', 'ascii', 'unicode',)

ffi = cffi.FFI()
convert_template = r'''
%(proto)s
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	g g;
	gzFile outfh;
	const char *line;
	int res = 1;
	char buf[%(datalen)s];
	char defbuf[%(datalen)s];
	char buf_col_min[%(datalen)s];
	char buf_col_max[%(datalen)s];
	char *badmap = 0;
	int fd = open(in_fn, O_RDONLY);
	if (fd < 0) goto errfd;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g_init(&g, backing_format, in_fn);
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	outfh = gzopen(out_fn, "wb");
	err1(!outfh);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
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
	if (max_count < 0) max_count = INT64_MAX;
	for (int i = 0; (line = read_line(&g)) && i < max_count; i++) {
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			*bad_count += 1;
			continue;
		}
		char *ptr = buf;
		%(convert)s;
		if (!ptr) {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				*bad_count += 1;
				continue;
			}
			if (!default_value) {
				fprintf(stderr, "\n    Failed to convert \"%%s\" from %%s line %%d\n\n", line, in_fn, i + 1);
				goto err;
			}
			ptr = defbuf;
			*default_count += 1;
		}
		%(minmax_code)s;
		err1(gzwrite(outfh, ptr, %(datalen)s) != %(datalen)s);
	}
	gzFile minmaxfh = gzopen(minmax_fn, "wb");
	err1(!minmaxfh);
	res = g.error;
	if (gzwrite(minmaxfh, buf_col_min, %(datalen)s) != %(datalen)s) res = 1;
	if (gzwrite(minmaxfh, buf_col_max, %(datalen)s) != %(datalen)s) res = 1;
	if (gzclose(minmaxfh)) res = 1;
err:
	if (g_cleanup(&g)) res = 1;
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
	PyGILState_Release(gstate);
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
	gzFile outfh;
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
	const int allow_float = !fmt;
	PyGILState_STATE gstate = PyGILState_Ensure();
	int fd = open(in_fn, O_RDONLY);
	if (fd < 0) goto errfd;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g_init(&g, backing_format, in_fn);
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	outfh = gzopen(out_fn, "wb");
	err1(!outfh);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
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
	if (max_count < 0) max_count = INT64_MAX;
	for (int i = 0; (line = read_line(&g)) && i < max_count; i++) {
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			*bad_count += 1;
			continue;
		}
		char *ptr = buf;
		int len = convert_number_do(line, ptr, allow_float);
		if (!len) {
			if (record_bad && !deflen) {
				badmap[i / 8] |= 1 << (i %% 8);
				*bad_count += 1;
				continue;
			}
			if (!deflen) {
				fprintf(stderr, "\n    Failed to convert \"%%s\" from %%s line %%d\n\n", line, in_fn, i + 1);
				goto err;
			}
			ptr = defbuf;
			len = deflen;
			*default_count += 1;
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
		err1(gzwrite(outfh, ptr, len) != len);
	}
	gzFile minmaxfh = gzopen(minmax_fn, "wb");
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
	PyGILState_Release(gstate);
	if (g_cleanup(&g)) res = 1;
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
	return res;
}
'''

proto_template = 'int convert_column_%s(const char *in_fn, const char *out_fn, const char *minmax_fn, const char *default_value, uint32_t default_len, int default_value_is_None, const char *fmt, const char *fmt_b, int record_bad, int skip_bad, int badmap_fd, size_t badmap_size, uint64_t *bad_count, uint64_t *default_count, off_t offset, int64_t max_count, int backing_format)'

protos = []
funcs = [dataset_typing.noneval_data]

proto = proto_template % ('number',)
code = convert_number_template % dict(proto=proto, strtol_f=dataset_typing.strtol_f)
protos.append(proto + ';')
funcs.append(code)

convert_blob_template = r'''
%(proto)s
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	g g;
	gzFile outfh;
	const char *line;
	int res = 1;
	uint8_t *defbuf = 0;
	char *badmap = 0;
	int fd = open(in_fn, O_RDONLY);
	if (fd < 0) goto errfd;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g_init(&g, backing_format, in_fn);
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	outfh = gzopen(out_fn, "wb");
	err1(!outfh);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
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
	if (max_count < 0) max_count = INT64_MAX;
	for (int i = 0; (line = read_line(&g)) && i < max_count; i++) {
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			*bad_count += 1;
			continue;
		}
		if (line == NoneMarker) {
			err1(gzwrite(outfh, "\xff\0\0\0\0", 5) != 5);
			continue;
		}
%(convert)s
		if (ptr) {
			if (len > 254) {
				uint8_t lenbuf[5];
				lenbuf[0] = 255;
				memcpy(lenbuf + 1, &len, 4);
				err1(gzwrite(outfh, lenbuf, 5) != 5);
			} else {
				uint8_t len8 = len;
				err1(gzwrite(outfh, &len8, 1) != 1);
			}
		} else {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				*bad_count += 1;
%(cleanup)s
				continue;
			}
			if (!default_value) {
				fprintf(stderr, "\n    Failed to convert \"%%s\" from %%s line %%d\n\n", line, in_fn, i + 1);
				goto err;
			}
			ptr = (const uint8_t *)default_value;
			len = default_len;
			*default_count += 1;
		}
		err1(gzwrite(outfh, ptr, len) != len);
%(cleanup)s
	}
	gzFile minmaxfh = gzopen(minmax_fn, "wb");
	err1(!minmaxfh);
	res = g.error;
	if (gzwrite(minmaxfh, "\xff\0\0\0\0\xff\0\0\0\0", 10) != 10) res = 1;
	if (gzclose(minmaxfh)) res = 1;
err:
	if (defbuf) free(defbuf);
	if (g_cleanup(&g)) res = 1;
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
	PyGILState_Release(gstate);
	return res;
}
'''

for name, ct in sorted(list(dataset_typing.convfuncs.items()) + list(dataset_typing.hidden_convfuncs.items())):
	if not ct.conv_code_str:
		continue
	if ct.size:
		if ':' in name:
			shortname = name.split(':', 1)[0]
		else:
			shortname = name
		proto = proto_template % (shortname,)
		destname = dataset_typing.typerename.get(shortname, shortname)
		mm = dataset_typing.minmaxfuncs[destname]
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

protos.append('int numeric_comma(const char *localename);')
protos.append('void init(void);')

# cffi apparently doesn't know about off_t.
# "typedef int... off_t" isn't allowed with verify,
# so I guess we'll just assume that ssize_t is the same as off_t.
ffi.cdef('typedef ssize_t off_t;')
ffi.cdef(''.join(protos))
backend = ffi.verify(r'''
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
#define Z (128 * 1024)

typedef struct {
	gzFile fh;
	int len;
	int pos;
	int error;
	int backing_format;
	uint32_t saved_size;
	int32_t linelen;
	const char *filename;
	char *largetmp;
	char buf[Z + 1];
} g;

static const char NoneMarker[1] = {0};
static char decimal_separator = '.';

static void g_init(g *g, int backing_format, const char *filename)
{
	g->fh = 0;
	g->pos = g->len = 0;
	g->error = 0;
	g->backing_format = backing_format;
	g->filename = filename;
	g->largetmp = 0;
}

static int g_cleanup(g *g)
{
	if (g->largetmp) free(g->largetmp);
	return gzclose(g->fh);
}

int numeric_comma(const char *localename)
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

static inline const char *read_line_v2(g *g)
{
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 0;
	}
	char *ptr = g->buf + g->pos;
	char *end = memchr(ptr, '\n', g->len - g->pos);
	if (!end) {
		const int32_t linelen = g->len - g->pos;
		memmove(g->buf, g->buf + g->pos, linelen);
		if (read_chunk(g, linelen)) { // if eof
			g->pos = g->len;
			g->buf[linelen] = 0;
			g->linelen = linelen;
			return linelen ? g->buf : 0;
		}
		ptr = g->buf;
		end = memchr(ptr, '\n', g->len);
		if (!end) { // very long line - can't deal
			fprintf(stderr, "%s: Line too long, bailing out\n", g->filename);
			g->error = 1;
			return 0;
		}
	}
	int32_t linelen = end - ptr;
	g->pos += linelen + 1;
	if (linelen == 1 && *ptr == 0) {
		g->linelen = 0;
		return NoneMarker;
	}
	ptr[linelen] = 0;
	if (linelen && ptr[linelen - 1] == '\r') ptr[--linelen] = 0;
	g->linelen = linelen;
	return ptr;
}

static inline const char *read_line_v3(g *g)
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
				fprintf(stderr, "%s: Format error\n", g->filename);
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
			fprintf(stderr, "%s: Format error\n", g->filename);
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
			fprintf(stderr, "%s: Format error\n", g->filename);
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
			fprintf(stderr, "%s: Format error\n", g->filename);
			g->error = 1;
			return 0;
		}
		avail = g->len;
		if (avail < size) {
			fprintf(stderr, "%s: Format error\n", g->filename);
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

static inline const char *read_line(g *g)
{
	if (g->backing_format == 2) return read_line_v2(g);
	return read_line_v3(g);
}

void init(void)
{
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyDateTime_IMPORT;
	PyGILState_Release(gstate);
	// strptime parses %s in whatever timezone is set, so set UTC.
	if (setenv("TZ", "UTC", 1)) exit(1);
	tzset();
}
''' + ''.join(funcs), libraries=['z'], extra_compile_args=['-std=c99'])

def prepare():
	backend.init()
	d = datasets.source
	columns = {}
	for colname, coltype in iteritems(options.column2type):
		assert d.columns[colname].type in byteslike_types, colname
		coltype = coltype.split(':', 1)[0]
		columns[options.rename.get(colname, colname)] = dataset_typing.typerename.get(coltype, coltype)
	if options.filter_bad or options.discard_untyped:
		assert options.discard_untyped is not False, "Can't keep untyped when filtering bad"
		parent = None
	else:
		parent = datasets.source
	return DatasetWriter(
		columns=columns,
		caption=options.caption,
		hashlabel=options.rename.get(d.hashlabel, d.hashlabel),
		hashlabel_override=True,
		parent=parent,
		previous=datasets.previous,
		meta_only=True,
	)

def analysis(sliceno):
	if options.numeric_comma:
		try_locales = [
			'da_DK', 'nb_NO', 'nn_NO', 'sv_SE', 'fi_FI',
			'en_ZA', 'es_ES', 'es_MX', 'fr_FR', 'ru_RU',
			'de_DE', 'nl_NL', 'it_IT',
		]
		for localename in try_locales:
			localename = localename.encode('ascii')
			if not backend.numeric_comma(localename):
				break
			if not backend.numeric_comma(localename + b'.UTF-8'):
				break
		else:
			raise Exception("Failed to enable numeric_comma, please install at least one of the following locales: " + " ".join(try_locales))
	if options.filter_bad:
		badmap_fh = open('badmap%d' % (sliceno,), 'w+b')
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, True)
		if sum(itervalues(bad_count)):
			final_bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, False)
			final_bad_count = max(itervalues(final_bad_count))
		else:
			final_bad_count = 0
		badmap_fh.close()
	else:
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, None, False)
		final_bad_count = 0
	for src, dst in link_candidates:
		symlink(src, dst)
	return bad_count, final_bad_count, default_count, minmax

# make any unicode args bytes, for cffi calls.
def bytesargs(*a):
	return [v.encode('utf-8') if isinstance(v, unicode) else ffi.NULL if v is None else v for v in a]

# In python3 indexing into bytes gives integers (b'a'[0] == 97),
# this gives the same behaviour on python2. (For use with mmap.)
class IntegerBytesWrapper(object):
	def __init__(self, inner):
		self.inner = inner
	def close(self):
		self.inner.close()
	def __getitem__(self, key):
		return ord(self.inner[key])
	def __setitem__(self, key, value):
		self.inner[key] = chr(value)

def analysis_lap(sliceno, badmap_fh, first_lap):
	known_line_count = 0
	badmap_size = 0
	badmap_fd = -1
	res_bad_count = {}
	res_default_count = {}
	res_minmax = {}
	link_candidates = []
	if first_lap:
		record_bad = options.filter_bad
		skip_bad = 0
	else:
		record_bad = 0
		skip_bad = options.filter_bad
	minmax_fn = 'minmax%d' % (sliceno,)
	dw = DatasetWriter()
	for colname, coltype in iteritems(options.column2type):
		out_fn = dw.column_filename(options.rename.get(colname, colname))
		fmt = fmt_b = None
		if coltype in dataset_typing.convfuncs:
			shorttype = coltype
			_, cfunc, pyfunc = dataset_typing.convfuncs[coltype]
		else:
			shorttype, fmt = coltype.split(':', 1)
			_, cfunc, pyfunc = dataset_typing.convfuncs[shorttype + ':*']
		if cfunc:
			cfunc = shorttype.replace(':', '_')
		if pyfunc:
			tmp = pyfunc(coltype)
			if callable(tmp):
				pyfunc = tmp
				cfunc = None
			else:
				pyfunc = None
				cfunc, fmt, fmt_b = tmp
		if coltype == 'number':
			cfunc = 'number'
		elif coltype == 'number:int':
			coltype = 'number'
			cfunc = 'number'
			fmt = "int"
		assert cfunc or pyfunc, coltype + " didn't have cfunc or pyfunc"
		coltype = shorttype
		d = datasets.source
		assert d.columns[colname].type in byteslike_types, colname
		if options.filter_bad:
			line_count = d.lines[sliceno]
			if known_line_count:
				assert line_count == known_line_count, (colname, line_count, known_line_count)
			else:
				known_line_count = line_count
				pagesize = getpagesize()
				badmap_size = (line_count // 8 // pagesize + 1) * pagesize
				badmap_fh.truncate(badmap_size)
				badmap_fd = badmap_fh.fileno()
		if d.columns[colname].backing_type.startswith('_v2_'):
			backing_format = 2
		else:
			backing_format = 3
		in_fn = d.column_filename(colname, sliceno)
		if d.columns[colname].offsets:
			offset = d.columns[colname].offsets[sliceno]
			max_count = d.lines[sliceno]
		else:
			offset = 0
			max_count = -1
		if cfunc:
			default_value = options.defaults.get(colname, ffi.NULL)
			default_len = 0
			if default_value is None:
				default_value = ffi.NULL
				default_value_is_None = True
			else:
				default_value_is_None = False
				if default_value != ffi.NULL:
					if isinstance(default_value, unicode):
						default_value = default_value.encode("utf-8")
					default_len = len(default_value)
			bad_count = ffi.new('uint64_t [1]', [0])
			default_count = ffi.new('uint64_t [1]', [0])
			c = getattr(backend, 'convert_column_' + cfunc)
			res = c(*bytesargs(in_fn, out_fn, minmax_fn, default_value, default_len, default_value_is_None, fmt, fmt_b, record_bad, skip_bad, badmap_fd, badmap_size, bad_count, default_count, offset, max_count, backing_format))
			assert not res, 'Failed to convert ' + colname
			res_bad_count[colname] = bad_count[0]
			res_default_count[colname] = default_count[0]
			coltype = coltype.split(':', 1)[0]
			with type2iter[dataset_typing.typerename.get(coltype, coltype)](minmax_fn) as it:
				res_minmax[colname] = list(it)
			unlink(minmax_fn)
		else:
			# python func
			nodefault = object()
			if colname in options.defaults:
				default_value = options.defaults[colname]
				if default_value is not None:
					if isinstance(default_value, unicode):
						default_value = default_value.encode('utf-8')
					default_value = pyfunc(default_value)
			else:
				default_value = nodefault
			if options.filter_bad:
				badmap = mmap(badmap_fd, badmap_size)
				if PY2:
					badmap = IntegerBytesWrapper(badmap)
			bad_count = 0
			default_count = 0
			dont_minmax_types = {'bytes', 'ascii', 'unicode', 'json'}
			real_coltype = dataset_typing.typerename.get(coltype, coltype)
			do_minmax = real_coltype not in dont_minmax_types
			with typed_writer(real_coltype)(out_fn) as fh:
				col_min = col_max = None
				for ix, v in enumerate(d._column_iterator(sliceno, colname, _type='bytes' if backing_format == 3 else '_v2_bytes')):
					if skip_bad:
						if badmap[ix // 8] & (1 << (ix % 8)):
							bad_count += 1
							continue
					try:
						v = pyfunc(v)
					except ValueError:
						if default_value is not nodefault:
							v = default_value
							default_count += 1
						elif record_bad:
							bad_count += 1
							bv = badmap[ix // 8]
							badmap[ix // 8] = bv | (1 << (ix % 8))
							continue
						else:
							raise Exception("Invalid value %r with no default in %s" % (v, colname,))
					if do_minmax and not isinstance(v, NoneType):
						if col_min is None:
							col_min = col_max = v
						if v < col_min: col_min = v
						if v > col_max: col_max = v
					fh.write(v)
			if options.filter_bad:
				badmap.close()
			res_bad_count[colname] = bad_count
			res_default_count[colname] = default_count
			res_minmax[colname] = [col_min, col_max]
	return res_bad_count, res_default_count, res_minmax, link_candidates

def synthesis(params, analysis_res, prepare_res):
	r = report()
	res = DotDict()
	d = datasets.source
	analysis_res = list(analysis_res)
	if options.filter_bad:
		num_lines_per_split = [num - data[1] for num, data in zip(d.lines, analysis_res)]
		res.bad_line_count_per_slice = [data[1] for data in analysis_res]
		res.bad_line_count_total = sum(res.bad_line_count_per_slice)
		r.println('Slice   Bad line count')
		for sliceno, cnt in enumerate(res.bad_line_count_per_slice):
			r.println('%5d   %d' % (sliceno, cnt,))
		r.println('total   %d' % (res.bad_line_count_total,))
		r.line()
		r.println('Slice   Bad line number')
		reported_count = 0
		for sliceno, data in enumerate(analysis_res):
			fn = 'badmap%d' % (sliceno,)
			if data[1] and reported_count < 32:
				with open(fn, 'rb') as fh:
					badmap = mmap(fh.fileno(), 0, prot=PROT_READ)
					for ix, v in enumerate(imap(ord, badmap)):
						if v:
							for jx in range(8):
								if v & (1 << jx):
									r.println('%5d   %d' % (sliceno, ix * 8 + jx,))
									reported_count += 1
									if reported_count >= 32: break
							if reported_count >= 32: break
					badmap.close()
			unlink(fn)
		if reported_count >= 32:
			r.println('...')
		r.line()
		res.bad_line_count_per_column = {}
		r.println('Bad line count   Column')
		for colname in sorted(analysis_res[0][0]):
			cnt = sum(data[0][colname] for data in analysis_res)
			r.println('%14d   %s' % (cnt, colname,))
			res.bad_line_count_per_column[colname] = cnt
		r.line()
	else:
		num_lines_per_split = d.lines
	dw = prepare_res
	for sliceno, count in enumerate(num_lines_per_split):
		dw.set_lines(sliceno, count)
	if options.defaults:
		r.println('Defaulted values')
		res.defaulted_per_slice = {}
		res.defaulted_total = {}
		for colname in sorted(options.defaults):
			r.println('    %s:' % (colname,))
			r.println('        Slice   Defaulted line count')
			res.defaulted_per_slice[colname] = [data[2][colname] for data in analysis_res]
			res.defaulted_total[colname] = sum(res.defaulted_per_slice[colname])
			for sliceno, cnt in enumerate(res.defaulted_per_slice[colname]):
				r.println('        %5d   %d' % (sliceno, cnt,))
			r.println('        total   %d' % (res.defaulted_total[colname],))
		r.line()
	for sliceno, data in enumerate(analysis_res):
		dw.set_minmax(sliceno, data[3])
	d = dw.finish()
	res.good_line_count_per_slice = num_lines_per_split
	res.good_line_count_total = sum(num_lines_per_split)
	r.line()
	r.println('Total of %d lines converted' % (res.good_line_count_total,))
	r.close()
	json_save(res)
