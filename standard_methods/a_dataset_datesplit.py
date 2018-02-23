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

from __future__ import division
from __future__ import absolute_import

description = r'''
Take a (chain of) dataset(s) and split it based on a date(time) column.

Regrettably does not work with Number columns, nor with list columns.

Tries to be intelligent about whether it re-reads old datasets or saves the
spilled data from them (for fast re-reading, when there isn't much of it).

Intended to be used in a continually trailing chain from some source with
less than perfect date splitting.

You can extract spilled data by not specifying a source, optionally into
several datasets by incrementing split_date, or all at once by not setting
any split_date.

Moving discard_before_date around won't bring any data back from previous
jobs, even if those datasets are re-read.

If you use discard_before_date you can get a look at what was discarded
through the dataset_datesplit_discarded method.
'''

import cffi
from os.path import exists
from datetime import datetime, date, time
from os import unlink

from extras import OptionString, json_save, job_params, DotDict
from dataset import Dataset, DatasetWriter
from . import dataset_typing
from sourcedata import type2iter
import blob


options = {
	'date_column'               : OptionString,
	'split_date'                : datetime,
	'discard_before_date'       : datetime,
	'caption'                   : 'spilled dataset',
	'hard_spill'                : False, # always write spill - never re-read older sources.
}

datasets = ('source', 'previous', )

depend_extra = (dataset_typing,)


minmax_type2idx = {}
funcs = []
for ix, (name, mm,) in enumerate(sorted(dataset_typing.minmaxfuncs.iteritems())):
	minmax_type2idx[name] = ix
	funcs.append(r'''
		static void minmax_setup%(ix)d(char *buf_col_min, char *buf_col_max)
		{
			%(setup)s;
		}
		static void minmax_code%(ix)d(const char *ptr, char *buf_col_min, char *buf_col_max)
		{
			%(code)s;
		}
	''' % dict(ix=ix, setup=mm.setup, code=mm.code,))
minmax_code = dataset_typing.minmax_data + ''.join(funcs) + r'''
	static void (*minmax_setup[])(char *, char *) = {
		''' + ','.join('minmax_setup%d' % (i,) for i in range(len(minmax_type2idx))) + r'''
	};
	static void (*minmax_code[])(const char *, char *, char *) = {
		''' + ','.join('minmax_code%d' % (i,) for i in range(len(minmax_type2idx))) + r'''
	};
'''

ffi = cffi.FFI()
ffi.cdef(r'''
int filter(const int count, const char *in_files[], const size_t offsets[], const char *out_files[], const char *minmax_files[], const int sizes[], uint64_t counters[4], const uint32_t dates[6], const int minmax_typeidx[], const int64_t line_count);
''')
backend = ffi.verify(r'''
#include <zlib.h>
#include <string.h>
#include <float.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define err1(v) if (v) goto err
#define err2(v, msg) if (v) { error_msg = msg; goto err; }
#define Z (128 * 1024)

typedef struct {
	gzFile fh;
	int len;
	int pos;
	char buf[Z + 1];
} g;

static int read_chunk(g *g, int offset)
{
	const int len = gzread(g->fh, g->buf + offset, Z - offset);
	if (len <= 0) return 1;
	g->len = offset + len;
	g->buf[g->len] = 0;
	g->pos = 0;
	return 0;
}

static const char *read_line(g *g, int *len)
{
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 0;
	}
	char *ptr = g->buf + g->pos;
	char *end = memchr(ptr, '\n', g->len - g->pos);
	if (!end) {
		const int linelen = g->len - g->pos;
		memmove(g->buf, g->buf + g->pos, linelen);
		if (read_chunk(g, linelen)) { // if eof
			g->pos = g->len;
			*len = linelen + 1;
			return linelen ? g->buf : 0;
		}
		ptr = g->buf;
		end = memchr(ptr, '\n', g->len - g->pos);
		if (!end) end = ptr + g->len; // very long line - split it
	}
	const int linelen = end - ptr;
	g->pos += linelen + 1;
	*len = linelen + 1;
	return ptr;
}

''' + minmax_code + r'''

/*
	in_files[0] is the date column.
	sizes[] is the value size for columns, 0 for line based.
	out_files is first count entries for class 1, then count entries
	for class 2, and finally for class 3. Note that class 0 is absent.
	Any value in out_files can be 0 to discard that data.
	dates[0, 1] is the previous split date (or [0, 0] the first time).
	dates[2, 3] is the too old date (or [0, 0] if everything is valid).
	dates[4, 5] is is the split date (or [~0, ~0] to include everything).
	minmax_typeidx[] selects a minmax implementation for each column.
	(see minmax_setup and minmax_code)
*/

int filter(const int count, const char *in_files[static count], const size_t offsets[static count], const char *out_files[], const char *minmax_files[static count], const int sizes[static count], uint64_t counters[static 4], const uint32_t dates[static 6], const int minmax_typeidx[static count], const int64_t line_count)
{
	g in_fh[count];
	gzFile out_fh[4][count];
	char buf_col_min[4][count * 8];
	char buf_col_max[4][count * 8];
	const char *error_msg = "internal error";
	int res = 1;
	int fd = -1;
	memset(in_fh, 0, count * sizeof(g));
	memset(out_fh, 0, 4 * count * sizeof(gzFile));
	err1(sizes[0] != 4 && sizes[0] != 8); // sanity check (it must be a date type)
	for (int i = 0; i < count; i++) {
		fd = open(in_files[i], O_RDONLY);
		err2(fd < 0, in_files[i]);
		err2(lseek(fd, offsets[i], 0) != offsets[i], in_files[i]);
		in_fh[i].fh = gzdopen(fd, "rb");
		err2(!in_fh[i].fh, in_files[i]);
		fd = -1;
		for (int c = 0; c < 3; c++) {
			const char * const fn = out_files[count * c + i];
			if (fn) {
				out_fh[c + 1][i] = gzopen(fn, "ab");
				err2(!out_fh[c + 1][i], fn);
			}
		}
		if (sizes[i]) {
			for (int c = 1; c < 4; c++) {
				minmax_setup[minmax_typeidx[i]](buf_col_min[c] + 8 * i, buf_col_max[c] + 8 * i);
			}
		}
	}

/*
 * Each line can be classified as one of four things:
 * 0. Already extracted (or too old) in a previous datesplit.
 * 1. Too old. (Arrived too late.)
 * 2. Valid.
 * 3. Too new. (Spilled.)
 *
 * 0 is ignored. 1 is minmaxed but otherwise ignored. 2 gets included in
 * the new dataset. 3 is minmaxed and otherwise ignored unless we won't be
 * iterating this dataset again, in that case it is spilled.
 *
 * This is controlled by setting class to one of those numbers and then
 * writing to an fh in out_fh[class] (if set).
 *
 * What is saved where is thus controlled only by out_files[] and dates[].
 *
 * Anything except class 0 can be saved by setting the right options.
 */

	for (int64_t line_num = 0; line_num < line_count; line_num++) {
		char buf[8];
		const uint32_t * const u32p = (uint32_t *)buf;
		enum { CLS_ALREADY_HANDLED, CLS_TOO_OLD, CLS_VALID, CLS_TOO_NEW } class;
		err2(gzread(in_fh[0].fh, buf, sizes[0]) != sizes[0], "read");
		if (u32p[0] < dates[0] || (sizes[0] == 8 && u32p[0] == dates[0] && u32p[1] < dates[1])) {
			class = CLS_ALREADY_HANDLED;
		} else if (u32p[0] < dates[2] || (sizes[0] == 8 && u32p[0] == dates[2] && u32p[1] < dates[3])) {
			class = CLS_TOO_OLD;
		} else if (u32p[0] < dates[4] || (sizes[0] == 8 && u32p[0] == dates[4] && u32p[1] < dates[5])) {
			class = CLS_VALID;
		} else {
			class = CLS_TOO_NEW;
		}
		counters[class]++;
#define WRITE_VAL(i) do { \
	if (class) { /* Ignore CLS_ALREADY_HANDLED */ \
		minmax_code[minmax_typeidx[i]](buf, buf_col_min[class] + 8 * i, buf_col_max[class] + 8 * i); \
		if (out_fh[class][i]) { \
			err2(gzwrite(out_fh[class][i], buf, sizes[i]) != sizes[i], "write"); \
		} \
	} \
} while(0)
		WRITE_VAL(0);
		for (int i = 1; i < count; i++) {
			if (sizes[i]) {
				err2(gzread(in_fh[i].fh, buf, sizes[i]) != sizes[i], "read");
				WRITE_VAL(i);
			} else {
				const char *ptr;
				int len;
				ptr = read_line(&in_fh[i], &len);
				err2(!ptr, "read");
				if (out_fh[class][i]) {
					err2(gzwrite(out_fh[class][i], ptr, len) != len, "write");
				}
			}
		}
	}
	res = 0;
	for (int i = 0; i < count; i++) {
		if (sizes[i] && minmax_files[i]) {
			gzFile fh = gzopen(minmax_files[i], "wb");
			if (fh) {
				for (int c = 1; c < 4; c++) {
					if (gzwrite(fh, buf_col_min[c] + 8 * i, sizes[i]) != sizes[i]) res = 1;
				}
				for (int c = 1; c < 4; c++) {
					if (gzwrite(fh, buf_col_max[c] + 8 * i, sizes[i]) != sizes[i]) res = 1;
				}
				if (gzclose(fh)) res = 1;
			} else {
				res = 1;
			}
			err2(res, "write");
		}
	}
err:
	if (fd >= 0) close(fd);
	for (int i = 0; i < count; i++) {
		if (in_fh[i].fh && gzclose(in_fh[i].fh)) res = 1;
		for (int c = 1; c < 4; c++) {
			if (out_fh[c][i] && gzclose(out_fh[c][i])) res = 1;
		}
	}
	if (res) fprintf(stderr, "c backend error: %s", error_msg);
	return res;
}
''', libraries=['z'], extra_compile_args=['-std=c99'])


def real_prepare(d, previous, options):
	column_types = {n: c.type for n, c in d.columns.items()}
	column_sizes = []
	column_names = list(column_types)
	column_names.remove(options.date_column)
	column_names.insert(0, options.date_column)
	minmax_typeidx = []
	for colname in column_names:
		typ = column_types[colname]
		column_sizes.append(dataset_typing.typesizes[typ])
		minmax_typeidx.append(minmax_type2idx.get(typ, -1))
	minmax_typeidx = ffi.new('int []', minmax_typeidx)
	kw = dict(
		columns=column_types,
		hashlabel=d.hashlabel,
		caption=options.caption,
		previous=previous,
		meta_only=True,
	)
	dw = DatasetWriter(**kw)
	dw_spill = DatasetWriter(name='SPILL', **kw)
	return dw, dw_spill, column_names, column_sizes, column_types, minmax_typeidx

def prepare():
	return real_prepare(datasets.source or datasets.previous, datasets.previous, options)

def empty_spilldata(spill_ds='default'):
	return DotDict(
		version     = 1,
		counter     = 0,
		spill_ds    = spill_ds,
		last_time   = False,
		seen_before = False,
	)

def process_one(sliceno, options, source, prepare_res, data=None, save_discard=False):
	# Future improvement: Look at the old minmax to determine if we will get anything from reading this data
	dw, dw_spill, column_names, column_sizes, column_types, minmax_typeidx = prepare_res
	if data:
		assert data.version == 1
		data.seen_before = True
	else:
		data = empty_spilldata()
	d = Dataset(source, data.spill_ds)
	in_files = []
	out_files = []
	offsets = []
	if not save_discard:
		out_files += [ffi.NULL] * len(column_names) # don't save "too old" lines
	minmax_files = []
	minmax_d = {}
	for colname in column_names:
		out_fn = dw.column_filename(colname, sliceno).encode('ascii')
		in_fn = d.column_filename(colname, sliceno).encode('ascii')
		offset = d.columns[colname].offsets[sliceno] if d.columns[colname].offsets else 0
		in_files.append(ffi.new('char []', in_fn))
		out_files.append(ffi.new('char []', out_fn))
		offsets.append(offset)
		minmax_fn = out_fn + '_minmax'
		minmax_files.append(ffi.new('char []', minmax_fn))
		minmax_d[colname] = minmax_fn
	if save_discard:
		out_files += [ffi.NULL] * len(column_names) # don't save "good" lines (save discard instead)
	date_coltype = column_types[options.date_column]
	def date2cfmt(dt):
		if date_coltype == 'datetime':
			date0 = (dt.year << 14) | (dt.month << 10) | (dt.day << 5) | dt.hour
			date1 = (dt.minute << 26) | (dt.second << 20) | dt.microsecond
		elif date_coltype == 'date':
			date0 = (dt.year << 9) | (dt.month << 5) | dt.day
			date1 = 0
		elif date_coltype == 'time':
			date0 = 32277536 | dt.hour
			date1 = (dt.minute << 26) | (dt.second << 20) | dt.microsecond
		else:
			raise Exception('Bad date_coltype type: ' + date_coltype)
		return date0, date1
	dates = [0, 0, 0, 0, 0xffffffff, 0xffffffff]
	stats = DotDict()
	if data.seen_before:
		dates[0:2] = date2cfmt(data.get('process_date', datetime.min))
	if (data.last_time or options.hard_spill) and not save_discard:
		for colname in column_names:
			out_fn = dw_spill.column_filename(colname, sliceno).encode('ascii')
			out_files.append(ffi.new('char []', out_fn))
		stats.virtual_spill = False
	else:
		# We still have to make sure the files exist, or we end up
		# with a broken dataset if only some slices wanted to spill.
		for colname in column_names:
			open(dw_spill.column_filename(colname, sliceno), 'ab').close()
		out_files += [ffi.NULL] * len(column_names)
		stats.virtual_spill = True
	# We are done reading `data` - update it for next iteration
	del data.seen_before
	data.process_date = datetime.min
	if options.discard_before_date:
		if options.split_date:
			assert options.discard_before_date < options.split_date
		dates[2:3] = date2cfmt(options.discard_before_date)
		data.process_date = options.discard_before_date
	if options.split_date:
		dates[4:6] = date2cfmt(options.split_date)
		data.process_date = max(data.process_date, options.split_date)
	counters = ffi.new('uint64_t [4]') # one for each class-enum
	res = backend.filter(len(in_files), in_files, offsets, out_files, minmax_files, column_sizes, counters, dates, minmax_typeidx, d.lines[sliceno])
	assert not res, "cffi converter returned error on data from " + source
	stats.version = 0
	stats.counters = list(counters)
	stats.minmax = {}
	for colname, fn in minmax_d.iteritems():
		if exists(fn):
			with type2iter[column_types[colname]](fn) as it:
				stats.minmax[colname] = list(it)
			unlink(fn)
	# If there is at most 2% left, spill it next time.
	# Or if there is at most 10% left and we have read it at least 8 times.
	# Or if there is at most 20% left and we have read it at least 16 times.
	# A reasonable balance between re-reading and re-writing, one hopes.
	data.counter += 1
	total_lines = sum(counters)
	data.last_time = (counters[3] <= total_lines / 50 or
		(data.counter >= 8 and counters[3] <= total_lines / 10) or
		(data.counter >= 16 and counters[3] <= total_lines / 5)
	)
	# If no lines were spilled we will not need this dataset again,
	# nor if we wrote the spill in this dataset.
	if not counters[3] or not stats.virtual_spill:
		data = None
	return data, stats

def analysis(sliceno, params, prepare_res):
	spilldata = {}
	stats = {}
	we_have_spill = False
	if datasets.previous:
		prev_spilldata = blob.load('spilldata', jobid=datasets.previous, sliceno=sliceno)
		for source, data in prev_spilldata:
			spilldata[source], stats[source] = process_one(sliceno, options, source, prepare_res, data)
			we_have_spill |= not stats[source].virtual_spill
	if datasets.source:
		prev_params = job_params(datasets.previous, default_empty=True)
		for source in datasets.source.chain(stop_ds=prev_params.datasets.source):
			spilldata[source], stats[source] = process_one(sliceno, options, source, prepare_res)
			we_have_spill |= not stats[source].virtual_spill
	spilldata = [(k, v) for k, v in spilldata.iteritems() if v]
	if we_have_spill:
		spilldata.append((params.jobid, empty_spilldata('SPILL')))
	blob.save(spilldata, 'spilldata', sliceno=sliceno, temp=False)
	blob.save(stats, 'stats', sliceno=sliceno, temp=False)
	return we_have_spill

def real_synthesis(params, options, datasets, minmax_index, prepare_res, we_have_spill, save_discard=False):
	stats = DotDict(
		included_lines          = [0] * params.slices,
		discarded_lines         = [0] * params.slices,
		spilled_lines           = [0] * params.slices,
		virtually_spilled_lines = [0] * params.slices,
		split_date              = str(options.split_date) if options.split_date else None,
		discard_before_date     = str(options.discard_before_date) if options.discard_before_date else None,
	)
	minmax_per_slice = [{} for _ in range(params.slices)]
	def update_stats(data):
		for item in data.itervalues():
			stats.included_lines[sliceno] += item.counters[2]
			stats.discarded_lines[sliceno] += item.counters[1]
			if item.virtual_spill:
				stats.virtually_spilled_lines[sliceno] += item.counters[3]
			else:
				stats.spilled_lines[sliceno] += item.counters[3]
			update_minmax(minmax_per_slice[sliceno], item.minmax)
	def update_minmax(dest, src):
		for name, lst0 in src.iteritems():
			lst1 = dest.get(name, lst0)
			mins = map(min, zip(lst0[:3], lst1[:3]))
			maxs = map(max, zip(lst0[3:], lst1[3:]))
			dest[name] = mins + maxs
	for sliceno in range(params.slices):
		update_stats(blob.load('stats', sliceno=sliceno))
	minmax = {}
	for item in minmax_per_slice:
		update_minmax(minmax, item)
	def minmax_select(offset, stringify=False):
		d = {}
		for k, v in minmax.iteritems():
			mn = v[offset]
			mx = v[3 + offset]
			if mn <= mx:
				if stringify and isinstance(mn, (date, time,)):
					d[k] = [str(mn), str(mx)]
				else:
					d[k] = [mn, mx]
		return d
	dw, dw_spill = prepare_res[:2]
	dw.set_minmax(None, minmax_select(minmax_index))
	dw_spill.set_minmax(None, minmax_select(2))
	if save_discard:
		included_lines = stats.discarded_lines
	else:
		included_lines = stats.included_lines
	for sliceno in range(params.slices):
		dw.set_lines(sliceno, included_lines[sliceno])
		dw_spill.set_lines(sliceno, stats.spilled_lines[sliceno])
	if not we_have_spill:
		dw_spill.discard()
	stats.minmax_discarded = minmax_select(0, True)
	stats.minmax           = minmax_select(1, True)
	stats.minmax_spilled   = minmax_select(2, True)
	json_save(stats)

def synthesis(params, prepare_res, analysis_res):
	real_synthesis(params, options, datasets, 1, prepare_res, sum(analysis_res))
