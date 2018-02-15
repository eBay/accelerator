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
CSV file to dataset.

Read a CSV file, with any single character separator, with or without quotes.
Labels from first line or specified in options.
'''


import os
import cffi

import report
from extras import OptionString
import blob
import gzutil
from dataset import DatasetWriter


options = {
	'filename'                  : OptionString,
	'separator'                 : ',',
	'labelsonfirstline'         : True,
	'labels'                    : [], # Mandatory if not labelsonfirstline, always sets labels if set.
	'hashlabel'                 : None,
	'quote_support'             : False, # 'foo',"bar" style CSV
	'rename'                    : {},    # Labels to replace (if they are in the file) (happens first)
	'discard'                   : set(), # Labels to not include (if they are in the file)
	'allow_bad'                 : False, # Still succeed if some lines have too few/many fields.
}

datasets = ('previous', )


ffi = cffi.FFI()
ffi.cdef('''
int import_slice(const char *fn, const int slices, const int sliceno, const int skip_line, const int field_count, const char *out_fns[], const char separator, int hash_idx, uint64_t *res_num, const int quote_support);
''')
backend = ffi.verify(r'''
#include <zlib.h>
#include <stdlib.h>

#define err1(v) if (v) goto err
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

#define HANDLE_UNQUOTED \
	do { \
		char *end = strchr(line, separator); \
		if (end) { \
			if (i == field_count - 1) goto bad; \
			*end = '\n'; \
			field_len[i] = end - line; \
		} else { \
			if (i != field_count - 1) goto bad; \
			field_len[i] = strlen(line); \
			line[field_len[i]] = '\n'; \
		} \
		field[i] = line; \
		line = end + 1; \
	} while (0)

int import_slice(const char *fn, const int slices, const int sliceno, const int skip_line, const int field_count, const char *out_fns[], const char separator, int hash_idx, uint64_t *res_num, const int quote_support)
{
	int res = 1;
	g g;
	g.fh = gzopen(fn, "rb");
	if (!g.fh) return 1;
	g.pos = g.len = 0;
	char *line;
	if (skip_line) read_line(&g);
	gzFile outfh[field_count];
	for (int i = 0; i < field_count; i++) {
		outfh[i] = 0;
	}
	PyGILState_STATE gstate = PyGILState_Ensure();
	uint64_t (*hash)(const void *ptr, const uint64_t len) = PyCapsule_Import("gzutil._C_hash", 0);
	err1(!hash);
	for (int i = 0; i < field_count; i++) {
		if (out_fns[i]) {
			outfh[i] = gzopen(out_fns[i], "ab");
			err1(!outfh[i]);
		}
	}
	long lineno = -1;
	while ((line = read_line(&g))) {
		lineno++;
		if (hash_idx == -1) {
			if (lineno % slices != sliceno) continue;
		}
		char *field[field_count];
		int field_len[field_count];
		if (quote_support) {
			for (int i = 0; i < field_count; i++) {
				if (*line == '"' || *line == '\'') {
					const char q = *line;
					char *end = line + 1;
					while (*end) {
						end = strchr(end, q);
						if (!end       // Broken quoting. How regrettable.
						    || !end[1] // EOL
						) {
							if (end) end++; // pretend separator
							break;
						}
						if (end[1] == separator) {
							end++;
							break;
						}
						// we'll just assume it was a quote, because what else to do?
						end += 2;
					}
					int len;
					char *ptr = line;
					if (end) {
						len = end - line;
						line = end;
						if (*line) {
							if (i == field_count - 1) goto bad; // too many fields
							line++;
						} else {
							if (i != field_count - 1) goto bad; // too few fields
						}
					} else {
						if (i != field_count - 1) goto bad;
						len = strlen(line);
					}
					// Field is still quoted, now we remove that.
					len--; // separator
					if (ptr[len] == q) len--; // end quote, if it's there
					ptr++; // start quote
					for (int j = 0; j < len - 1; j++) { // collapse doubles
						if (ptr[j] == q && ptr[j + 1] == q) {
							memmove(ptr + j + 1, ptr + j + 2, len - j - 2);
							j++;
							len--;
						}
					}
					ptr[len] = '\n';
					field[i] = ptr;
					field_len[i] = len;
				} else {
					HANDLE_UNQUOTED;
				}
			}
		} else {
			for (int i = 0; i < field_count; i++) {
				HANDLE_UNQUOTED;
			}
		}
		if (hash_idx != -1) {
			int h = hash(field[hash_idx], field_len[hash_idx]) % slices;
			if (h != sliceno) continue;
		}
		for (int i = 0; i < field_count; i++) {
			if (outfh[i]) {
				const int len = field_len[i] + 1;
				err1(gzwrite(outfh[i], field[i], len) != len);
			}
		}
		res_num[1]++;
		continue;
bad:
		if (!res_num[0]) {
			printf("Line %ld bad (further bad lines in slice %d not reported)\n", lineno, sliceno);
		}
		res_num[0]++;
	}
	res = 0;
err:
	for (int i = 0; i < field_count; i++) {
		if (outfh[i] && gzclose(outfh[i])) res = 1;
	}
	gzclose(g.fh);
	PyGILState_Release(gstate);
	return res;
}
''', libraries=['z'], extra_compile_args=['-std=c99'])

def prepare(SOURCE_DIRECTORY):
	separator = options.separator
	assert len(separator) == 1
	filename  = os.path.join(SOURCE_DIRECTORY, options.filename)
	orig_filename = filename

	if filename.lower().endswith('.zip'):
		from zipfile import ZipFile
		filename = 'extracted'
		with ZipFile(orig_filename, 'r') as z:
			infos = z.infolist()
			assert len(infos) == 1, 'There is only support for ZIP files with exactly one member.'
			# Wouldn't it be nice if ZipFile.extract let me choose the filename?
			with open(filename, 'wb') as ofh:
				zfh = z.open(infos[0])
				while True:
					data = zfh.read(1024 * 1024)
					if not data:
						break
					ofh.write(data)

	if options.labelsonfirstline:
		with gzutil.GzBytesLines(filename, strip_bom=True) as fh:
			labels_str = next(fh).decode('ascii', 'replace').encode('ascii', 'replace') # garbage -> '?'
		if options.quote_support:
			labels = []
			sep = options.separator
			while labels_str is not None:
				if labels_str.startswith(('"', "'",)):
					q = labels_str[0]
					pos = 1
					while pos + 1 < len(labels_str):
						pos = labels_str.find(q, pos)
						if pos == -1: # all is lost
							pos = len(labels_str) - 1
						if pos + 1 == len(labels_str): # eol
							break
						if labels_str[pos + 1] == sep:
							break
						# we'll just assume it was a quote, because what else to do?
						labels_str = labels_str[:pos] + labels_str[pos + 1:]
						pos += 1
					labels.append(labels_str[1:pos])
					if len(labels_str) > pos + 1:
						labels_str = labels_str[pos + 2:]
					else:
						labels_str = None
				else:
					if sep in labels_str:
						field, labels_str = labels_str.split(sep, 1)
					else:
						field, labels_str = labels_str, None
					labels.append(field)
		else:
			labels = labels_str.split(options.separator)
	labels = options.labels or labels # only from file if not specified in options
	assert labels, "No labels"
	labels = [options.rename.get(x, x) for x in labels]
	assert '' not in labels, "Empty label for column %d" % (labels.index(''),)
	assert len(labels) == len(set(labels)), "Duplicate labels: %r" % (labels,)

	dw = DatasetWriter(
		columns={n: 'bytes' for n in labels},
		filename=orig_filename,
		hashlabel=options.hashlabel,
		caption='csvimport of ' + orig_filename,
		previous=datasets.previous,
		meta_only=True,
	)

	return separator, filename, orig_filename, labels, dw,


def analysis(sliceno, prepare_res, params):
	""" reading complete file, writing to this slice only"""

	separator, filename, _, labels, dw = prepare_res

	if options.hashlabel:
		hash_ix = labels.index(options.hashlabel)
	else:
		hash_ix = -1

	copied_lines = 0
	n_labels = len(labels)

	res_num = ffi.new('uint64_t [2]')
	res_num[0] = 0 # broken_lines
	res_num[1] = copied_lines
	out_fns = [ffi.NULL if l in options.discard else ffi.new('char []', dw.column_filename(l).encode('ascii')) for l in labels]
	err = backend.import_slice(filename, params.slices, sliceno, options.labelsonfirstline, n_labels, out_fns, separator, hash_ix, res_num, options.quote_support)
	assert not err, "c import_slice returned error"

	res = dict(
		num_broken_lines = res_num[0],
		num_lines        = res_num[1],
	)
	return res



def synthesis(prepare_res, analysis_res, params):
	from math import sqrt

	separator, filename, orig_filename, labels, dw = prepare_res
	labels = [n for n in labels if n not in options.discard]

	if filename != orig_filename:
		os.unlink(filename)

	# aggregate typing and statistics
	res = {}
	res['num_broken_lines'] = 0
	res['num_lines'] = 0
	res['lines_per_slice'] = []
	for sliceno, tmp in enumerate(analysis_res):
		res['num_broken_lines'] += tmp['num_broken_lines']
		res['num_lines']        += tmp['num_lines']
		res['lines_per_slice'].append(tmp['num_lines'])
		dw.set_lines(sliceno, tmp['num_lines'])

	blob.save(res, 'import')

	# write report
	r = report.report()
	if not res['num_lines']:
		r.println('No lines read - empty file!')
		r.close()
		return
	r.println('Number of rows read\n')
	r.println('  slice                            lines')
	for sliceno, nlines in enumerate(res['lines_per_slice']):
		if res['num_lines']:
			r.println('    %2d                         %9d  (%6.2f%%)' %(sliceno, nlines, 100*nlines/res['num_lines']))
		else:
			r.println('    %2d                         %9d           ' %(sliceno, nlines, 100*nlines/res['num_lines']))
	r.println('  total                        %9d' %(res['num_lines'],))
	stdev = sqrt(sum((x-res['num_lines']/params.slices)**2 for x in res['lines_per_slice'])/params.slices)
	r.println('\n  hash stdev                   %9d  (%6.2f%%)' % (stdev, round(100*stdev/res['num_lines'])))
	r.line()

	r.println('Number of columns              %9d' % len(labels,))
	r.close()

	if res['num_broken_lines'] and not options.allow_bad:
		raise Exception('%d bad lines without options.allow_bad' % (res['num_broken_lines'],))
