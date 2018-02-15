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

# Convert one or more columns in a dataset to a binary type.

from __future__ import division
from __future__ import absolute_import

import cffi
from resource import getpagesize
from os import unlink, symlink
from mmap import mmap, PROT_READ
from itertools import imap
from types import NoneType

from extras import OptionEnum, json_save, DotDict
from gzwrite import typed_writer
from dataset import DatasetWriter
from report import report
from . import dataset_typing
from sourcedata import type2iter

depend_extra = (dataset_typing,)

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

ffi = cffi.FFI()
convert_template = r'''
%(proto)s
{
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
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	g.pos = g.len = 0;
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
	res = 0;
	if (gzwrite(minmaxfh, buf_col_min, %(datalen)s) != %(datalen)s) res = 1;
	if (gzwrite(minmaxfh, buf_col_max, %(datalen)s) != %(datalen)s) res = 1;
	if (gzclose(minmaxfh)) res = 1;
err:
	gzclose(g.fh);
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
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
	int hasdot = 0, hasexp = 0;
	while (1) {
		const char c = inptr[inlen];
		if (!c) break;
		if (c == '.' || c == ',') {
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
		inlen++;
	}
	// Now remove whitespace at end
	while (inlen && (inptr[inlen - 1] == 32 || (inptr[inlen - 1] >= 9 && inptr[inlen - 1] <= 13))) inlen--;
	// Then remove ending zeroes if there is a decimal dot and no exponent
	if (hasdot && !hasexp) {
		while (inlen && inptr[inlen - 1] == '0') inlen--;
		// And remove the dot if it's the last character.
		if (inlen && inptr[inlen - 1] == '.') {
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
	if (hasdot || hasexp) { // Float
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
		const int64_t value = strtol(inptr, &end, 10);
		if (errno || end != inptr + inlen) { // big or invalid
			PyObject *s = PyString_FromStringAndSize(inptr, inlen);
			if (!s) exit(1); // All is lost
			PyObject *i = PyNumber_Long(s);
			if (!i) PyErr_Clear();
			Py_DECREF(s);
			if (!i) return 0;
			const size_t len_bits = _PyLong_NumBits(i);
			err1(len_bits == (size_t)-1);
			const size_t len_bytes = len_bits / 8 + 1;
			err1(len_bytes >= GZNUMBER_MAX_BYTES);
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
	double d_col_min;
	double d_col_max;
	char *badmap = 0;
	const int allow_float = !fmt;
	PyGILState_STATE gstate = PyGILState_Ensure();
	int fd = open(in_fn, O_RDONLY);
	if (fd < 0) goto errfd;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	g.pos = g.len = 0;
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
	res = 0;
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
	if (g.fh) gzclose(g.fh);
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
	return res;
}
'''

proto_template = 'int convert_column_%s(const char *in_fn, const char *out_fn, const char *minmax_fn, const char *default_value, int default_value_is_None, const char *fmt, int record_bad, int skip_bad, int badmap_fd, size_t badmap_size, uint64_t *bad_count, uint64_t *default_count, size_t offset, int64_t max_count)'

protos = []
funcs = [dataset_typing.minmax_data, dataset_typing.noneval_data]

proto = proto_template % ('number',)
code = convert_number_template % dict(proto=proto,)
protos.append(proto + ';')
funcs.append(code)

for name, ct in dataset_typing.convfuncs.iteritems():
	if not ct.conv_code_str:
		continue
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
	protos.append(proto + ';')
	funcs.append(code)

filter_string_template = r'''
int %(name)s(const char *in_fn, const char *out_fn, int badmap_fd, size_t badmap_size, size_t offset, int64_t max_count)
{
	g g;
	gzFile outfh;
	const char *line;
	int res = 1;
	char *badmap = 0;
	int fd = open(in_fn, O_RDONLY);
	if (fd < 0) goto errfd;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g.fh = gzdopen(fd, "rb");
	if (!g.fh) goto errfd;
	fd = -1;
	g.pos = g.len = 0;
	outfh = gzopen(out_fn, "wb");
	err1(!outfh);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		err1(!badmap);
	}
	if (max_count < 0) max_count = INT64_MAX;
	for (int i = 0; (line = read_line(&g)) && i < max_count; i++) {
		if (badmap && badmap[i / 8] & (1 << (i %% 8))) {
			continue;
		}
		// Yes this could be more efficient, but filter_strings isn't enough of a priority.
%(conv)s
		err1(gzwrite(outfh, line, len) != len);
		err1(gzwrite(outfh, "\n", 1) != 1);
	}
	res = 0;
err:
	gzclose(g.fh);
	if (outfh && gzclose(outfh)) res = 1;
	if (badmap) munmap(badmap, badmap_size);
errfd:
	if (fd >= 0) close(fd);
	return res;
}
'''
protos.append('int filter_strings(const char *in_fn, const char *out_fn, int badmap_fd, size_t badmap_size, size_t offset, int64_t max_count);')
protos.append('int filter_stringstrip(const char *in_fn, const char *out_fn, int badmap_fd, size_t badmap_size, size_t offset, int64_t max_count);')
protos.append('int numeric_comma(void);')
funcs.append(filter_string_template % dict(name='filter_strings', conv=r'''
		int len = strlen(line);
'''))
funcs.append(filter_string_template % dict(name='filter_stringstrip', conv=r'''
		while (*line == 32 || (*line >= 9 && *line <= 13)) line++;
		int len = strlen(line);
		while (len && (line[len - 1] == 32 || (line[len - 1] >= 9 && line[len - 1] <= 13))) len--;
'''))

ffi.cdef(''.join(protos))
backend = ffi.verify(r'''
#include <zlib.h>
#include <time.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <sys/mman.h>
#include <math.h>
#include <float.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>

#ifndef MAP_NOSYNC
#  define MAP_NOSYNC 0
#endif

#define err1(v) if (v) goto err
#define Z (128 * 1024)

typedef struct {
	gzFile fh;
	int len;
	int pos;
	char buf[Z + 1];
} g;

int numeric_comma(void)
{
	return !setlocale(LC_NUMERIC, "sv_SE.UTF-8");
}

static int read_chunk(g *g, int offset)
{
	const int len = gzread(g->fh, g->buf + offset, Z - offset);
	if (len <= 0) return 1;
	g->len = offset + len;
	g->buf[g->len] = 0;
	g->pos = 0;
	return 0;
}

static char *read_line(g *g)
{
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 0;
	}
	char *ptr = g->buf + g->pos;
	char *end = strchr(ptr, '\n');
	if (!end) {
		const int linelen = g->len - g->pos;
		memmove(g->buf, g->buf + g->pos, linelen);
		if (read_chunk(g, linelen)) { // if eof
			g->pos = g->len;
			g->buf[linelen] = 0;
			return linelen ? g->buf : 0;
		}
		ptr = g->buf;
		end = strchr(ptr, '\n');
		if (!end) end = ptr + g->len; // very long line - split it
	}
	const int linelen = end - ptr;
	g->pos += linelen + 1;
	ptr[linelen] = 0;
	if (linelen && ptr[linelen - 1] == '\r') ptr[linelen - 1] = 0;
	return ptr;
}
''' + ''.join(funcs), libraries=['z'], extra_compile_args=['-std=c99'])

def prepare():
	d = datasets.source
	columns = {}
	for colname, coltype in options.column2type.iteritems():
		assert d.columns[colname].type == 'bytes', colname
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
		if backend.numeric_comma():
			raise Exception("Failed to enable numeric_comma")
	if options.filter_bad:
		badmap_fh = open('badmap%d' % (sliceno,), 'w+b')
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, True)
		if sum(bad_count.itervalues()):
			final_bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, False)
			final_bad_count = max(final_bad_count.itervalues())
		else:
			final_bad_count = 0
		badmap_fh.close()
	else:
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, None, False)
		final_bad_count = 0
	for src, dst in link_candidates:
		symlink(src, dst)
	return bad_count, final_bad_count, default_count, minmax

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
	for colname, coltype in options.column2type.iteritems():
		out_fn = dw.column_filename(options.rename.get(colname, colname)).encode('ascii')
		if ':' in coltype and not coltype.startswith('number:'):
			coltype, fmt = coltype.split(':', 1)
			_, cfunc, pyfunc = dataset_typing.convfuncs[coltype + ':*']
			if '%f' in fmt:
				# needs to fall back to python version
				cfunc = None
			if not cfunc:
				pyfunc = pyfunc(coltype, fmt)
		else:
			_, cfunc, pyfunc = dataset_typing.convfuncs[coltype]
			fmt = ffi.NULL
		d = datasets.source
		assert d.columns[colname].type in ('bytes', 'string',), colname
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
		in_fn = d.column_filename(colname, sliceno).encode('ascii')
		if d.columns[colname].offsets:
			offset = d.columns[colname].offsets[sliceno]
			max_count = d.lines[sliceno]
		else:
			offset = 0
			max_count = -1
		if coltype == 'number':
			cfunc = True
		if coltype == 'number:int':
			coltype = 'number'
			cfunc = True
			fmt = "int"
		if cfunc:
			default_value = options.defaults.get(colname, ffi.NULL)
			if default_value is None:
				default_value = ffi.NULL
				default_value_is_None = True
			else:
				default_value_is_None = False
			bad_count = ffi.new('uint64_t [1]', [0])
			default_count = ffi.new('uint64_t [1]', [0])
			c = getattr(backend, 'convert_column_' + coltype)
			res = c(in_fn, out_fn, minmax_fn, default_value, default_value_is_None, fmt, record_bad, skip_bad, badmap_fd, badmap_size, bad_count, default_count, offset, max_count)
			assert not res, 'Failed to convert ' + colname
			res_bad_count[colname] = bad_count[0]
			res_default_count[colname] = default_count[0]
			with type2iter[dataset_typing.typerename.get(coltype, coltype)](minmax_fn) as it:
				res_minmax[colname] = list(it)
			unlink(minmax_fn)
		elif pyfunc is str:
			# We skip it the first time around, and link it from
			# the source dataset if there were no bad lines.
			# (That happens at the end of analysis.)
			# We can't do that if the file is not slice-specific though.
			if skip_bad or '%s' not in d.column_filename(colname, '%s'):
				res = backend.filter_strings(in_fn, out_fn, badmap_fd, badmap_size, offset, max_count);
				assert not res, 'Failed to convert ' + colname
			else:
				link_candidates.append((in_fn, out_fn,))
			res_bad_count[colname] = 0
			res_default_count[colname] = 0
		elif pyfunc is str.strip:
			res = backend.filter_stringstrip(in_fn, out_fn, badmap_fd, badmap_size, offset, max_count);
			assert not res, 'Failed to convert ' + colname
			res_bad_count[colname] = 0
			res_default_count[colname] = 0
		else:
			# python func
			nodefault = object()
			if colname in options.defaults:
				if options.defaults[colname] is None:
					default_value = None
				else:
					default_value = pyfunc(options.defaults[colname])
			else:
				default_value = nodefault
			if options.filter_bad:
				badmap = mmap(badmap_fd, badmap_size)
			bad_count = 0
			default_count = 0
			with typed_writer(dataset_typing.typerename.get(coltype, coltype))(out_fn) as fh:
				col_min = col_max = None
				for ix, v in enumerate(d.iterate(sliceno, colname)):
					if skip_bad:
						if ord(badmap[ix // 8]) & (1 << (ix % 8)):
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
							bv = ord(badmap[ix // 8])
							badmap[ix // 8] = chr(bv | (1 << (ix % 8)))
							continue
						else:
							raise Exception("Invalid value %r with no default in %s" % (v, colname,))
					if not isinstance(v, (NoneType, str, unicode,)):
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
	columns = {}
	for colname, coltype in options.column2type.iteritems():
		coltype = coltype.split(':', 1)[0]
		columns[options.rename.get(colname, colname)] = dataset_typing.typerename.get(coltype, coltype)
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
