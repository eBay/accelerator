############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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

# This is a separate file from a_csvimport so setup.py can import
# it and make the _csvimport module at install time.

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from . import c_backend_support

all_c_functions = r'''
#include <zlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/types.h>
#include <signal.h>

#define err1(v) if (v) { if (errno) perror("ERROR"); fprintf(stderr, "ERROR on %s line %d\n", __FILE__, __LINE__); goto err; }
#define BIG_Z (1024 * 1024 * 16 - 64)
#define SMALL_Z (1024 * 64)

// OS X has no pthread_barrier support, so we get to do this instead.
static struct {
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	unsigned int count1;
	unsigned int count2;
} barrier;

static void barrier_wait(void)
{
	pthread_mutex_lock(&barrier.mutex);
	if (++barrier.count1 == 2) {
		barrier.count2++;
		barrier.count1 = 0;
		pthread_cond_signal(&barrier.cond);
	} else {
		unsigned int count2 = barrier.count2;
		do {
			pthread_cond_wait(&barrier.cond, &barrier.mutex);
		} while (barrier.count2 == count2);
	}
	pthread_mutex_unlock(&barrier.mutex);
}

static char *bufs[3] = {0};
volatile int32_t buf_lens[2];
static gzFile read_fh;

static int writeall(const int fd, const void * const buf, const size_t count)
{
	size_t written_so_far = 0;
	const char * const ptr = buf;
	while (written_so_far < count) {
		ssize_t this_time = write(fd, ptr + written_so_far, count - written_so_far);
		if (this_time < 1) return 1;
		written_so_far += this_time;
	}
	return 0;
}

static void *readgz_thread(void *args)
{
	int i = 0;
	while (1) {
		const int32_t len = gzread(read_fh, bufs[i], BIG_Z);
		buf_lens[i] = len;
		if (len <= 0) {
			int e = Z_OK;
			const char *msg = gzerror(read_fh, &e);
			if (e == Z_OK) {
				barrier_wait();
				return 0;
			} else if (e == Z_ERRNO) {
				perror("readgz_thread");
			} else {
				fprintf(stderr, "zlib: %s\n", msg);
			}
			fflush(stderr);
			kill(getpid(), 9);
			return 0;
		}
		barrier_wait();
		i = !i;
	}
}

static char *read_line(const int lf_char, int32_t *r_len)
{
	static int i = 1;
	static int32_t len = 0;
	static int32_t pos = 0;
	int32_t overflow_len = 0;
	if (len == -1) {
		*r_len = 0;
		return bufs[2];
	}
again:
	if (pos == len) {
		i = !i;
		barrier_wait();
		len = buf_lens[i];
		if (len == 0) {
			len = -1;
			*r_len = overflow_len;
			return bufs[2];
		}
		pos = 0;
	}
	char *ptr = bufs[i] + pos;
	char *lf = memchr(ptr, lf_char, len - pos);
	if (!lf) {
		if (overflow_len) {
			fprintf(stderr, "Cannot handle lines longer than %d bytes\n", BIG_Z);
			goto err;
		}
		overflow_len = len - pos;
		memcpy(bufs[2], ptr, overflow_len);
		pos = len;
		goto again;
	}
	int32_t line_len = lf - ptr + 1;
	pos += line_len;
	*r_len = line_len + overflow_len;
	if (overflow_len) {
		if (*r_len > BIG_Z) {
			fprintf(stderr, "Cannot handle lines longer than %d bytes\n", BIG_Z);
			goto err;
		}
		memcpy(bufs[2] + overflow_len, ptr, line_len);
		overflow_len = 0;
		return bufs[2];
	} else {
		return ptr;
	}
err:
	*r_len = -1;
	len = -1;
	return 0;
}

#define SLICEBUF_Z 4000
#define SLICEBUF_THRESH (SLICEBUF_Z / 2 - 10)

#define FLUSH_WRITES(i) do { \
	if (slicebuf_lens[i]) { \
		err1(writeall(outfds[i], slicebufs[i], slicebuf_lens[i])); \
		slicebuf_lens[i] = 0; \
	} \
} while(0)

// smallest int32
#define LABELS_DONE_MARKER -2147483648

static int reader(const char *fn, const int slices, uint64_t skip_lines, const int skip_empty_lines, const int outfds[], int labels_fd, int status_fd, const int comment_char, const int lf_char)
{
	int res = 1;
	int sliceno = 0;
	pthread_t thread;
	read_fh = 0;
	char *slicebufs[slices];
	int32_t slicebuf_lens[slices];
	const int rl_lf_char = (lf_char == 256 ? '\n' : lf_char);
	uint64_t linecnt = 0;
	uint64_t comments_before_labels = 0;
	uint64_t comments_capacity = 0;
	char **comments = 0;
	int32_t *comment_lens = 0;

	for (int i = 0; i < slices; i++) {
		slicebufs[i] = 0;
		slicebuf_lens[i] = 0;
	}
	for (int i = 0; i < slices; i++) {
		slicebufs[i] = malloc(SLICEBUF_Z);
		err1(!slicebufs[i]);
	}
	read_fh = gzopen(fn, "rb");
	err1(!read_fh);
	err1(gzbuffer(read_fh, SMALL_Z));
	barrier.count1 = barrier.count2 = 0;
	err1(pthread_mutex_init(&barrier.mutex, 0));
	err1(pthread_cond_init(&barrier.cond, 0));
	// 3 because we need one as scratchpad when spanning a buffer boundary
	for (int i = 0; i < 3; i++) {
		bufs[i] = malloc(BIG_Z + 16);
		err1(!bufs[i]);
		bufs[i] = bufs[i] + 16;
	}
	err1(pthread_create(&thread, 0, readgz_thread, 0));
	while (1) {
		int32_t len;
		int32_t claim_len;
		char *ptr = read_line(rl_lf_char, &len);
		if (!len) break;
		if (!ptr) goto err;
		if ((++linecnt % 1000000) == 0) {
			// failure here only breaks status updating, so we don't care.
			ssize_t ignore = write(status_fd, &linecnt, 8);
			(void) ignore;
		}
		if (lf_char == 256) {
			if (ptr[len - 1] == '\n') {
				len--;
				if (len && ptr[len - 1] == '\r') {
					len--;
				}
			}
		} else if (ptr[len - 1] == lf_char) {
			len--;
		}
		if (skip_lines || *ptr == comment_char || (skip_empty_lines && len == 0)) {
			if (skip_lines) skip_lines--;
			claim_len = -len - 1;
		} else {
			claim_len = len;
		}
		if (labels_fd == -1) {
			if (len > SLICEBUF_THRESH) {
				FLUSH_WRITES(sliceno);
				memcpy(ptr - 4, &claim_len, 4);
				err1(writeall(outfds[sliceno], ptr - 4, len + 4));
			} else {
				if (slicebuf_lens[sliceno] + len + 4 > SLICEBUF_Z) {
					FLUSH_WRITES(sliceno);
				}
				char *sptr = slicebufs[sliceno] + slicebuf_lens[sliceno];
				memcpy(sptr, &claim_len, 4);
				memcpy(sptr + 4, ptr, len);
				slicebuf_lens[sliceno] += len + 4;
			}
			sliceno = (sliceno + 1) % slices;
		} else if (claim_len < 0) {
			// No writers yet, so trying to write to the outfd might block forever.
			const int32_t tmp_len = len + 4;
			char *tmp = malloc(tmp_len);
			err1(!tmp);
			memcpy(tmp, &claim_len, 4);
			memcpy(tmp + 4, ptr, len);
			if (comments_before_labels == comments_capacity) {
				comments_capacity = (comments_capacity + 10) * 2;
				comments = realloc(comments, comments_capacity * sizeof(*comments));
				err1(!comments);
				comment_lens = realloc(comment_lens, comments_capacity * sizeof(*comment_lens));
				err1(!comment_lens);
			}
			comments[comments_before_labels] = tmp;
			comment_lens[comments_before_labels] = tmp_len;
			comments_before_labels++;
		} else {
			// Only happens once anyway, so no need to optimize
			err1(writeall(labels_fd, &len, 4));
			err1(writeall(labels_fd, ptr, len));
			close(labels_fd);
			labels_fd = -1;
			// Presumably there are not all that many of these, so one write each it is.
			if (comments_before_labels) {
				for (uint64_t i = 0; i < comments_before_labels; i++) {
					err1(writeall(outfds[sliceno], comments[i], comment_lens[i]));
					sliceno = (sliceno + 1) % slices;
					free(comments[i]);
				}
				free(comments);
				free(comment_lens);
			}
			// Let all slices know the labels are done so they can add 1 to lineno.
			const int32_t labels_done_marker = LABELS_DONE_MARKER;
			for (int i = 0; i < slices; i++) {
				memcpy(slicebufs[i], &labels_done_marker, 4);
				slicebuf_lens[i] = 4;
			}
		}
	}
	for (int i = 0; i < slices; i++) {
		FLUSH_WRITES(i);
	}
	res = 0;
err:
	if (res && errno) perror("reader");
	if (labels_fd != -1) close(labels_fd);
	for (int i = 0; i < slices; i++) {
		if (slicebufs[i]) free(slicebufs[i]);
	}
	// leave some things not cleaned up to avoid problems in readgz_thread
	return res;
}

static inline int field_write(gzFile fh, char *ptr, const int32_t len)
{
	if (len < 255) {
		// callers make sure there is room for one byte before ptr
		uint8_t *uptr = (uint8_t *)ptr - 1;
		*uptr = len;
		return (gzwrite(fh, uptr, len + 1) != len + 1);
	} else {
		uint8_t lenbuf[5];
		lenbuf[0] = 255;
		memcpy(lenbuf + 1, &len, 4);
		if (gzwrite(fh, &lenbuf, 5) != 5) return 1;
		return (gzwrite(fh, ptr, len) != len);
	}
}

typedef struct {
	uint32_t pos;
	uint32_t avail;
	char padding_for_small_size_before_buf;
	char buf[BIG_Z];
} readbuf;

static inline int bufread(const int fd, readbuf *buf, const uint32_t len, int *r_eof, char **r_ptr)
{
	if (len > buf->avail) {
		if (buf->avail == 0) {
			buf->pos = 0;
		} else if (buf->avail < 64 || len > sizeof(buf->buf) - buf->pos) {
			memmove(buf->buf, buf->buf + buf->pos, buf->avail);
			buf->pos = 0;
		}
		uint32_t readpos = buf->pos + buf->avail;
		while (buf->avail < len) {
			const ssize_t this_time = read(fd, buf->buf + readpos, sizeof(buf->buf) - readpos);
			if (this_time == 0) *r_eof = 1;
			if (this_time < 1) return 1;
			buf->avail += this_time;
			readpos += this_time;
		}
	}
	*r_ptr = buf->buf + buf->pos;
	buf->pos += len;
	buf->avail -= len;
	return 0;
}

static int import_slice(const int fd, const int sliceno, const int slices, int field_count, const char *out_fns[], const char *gzip_mode, const int separator, uint64_t *r_num, const int quote_char, const int lf_char, const int allow_bad, const int allow_extra_empty)
{
	FILE * const badline_report_fh = (allow_bad ? stdout : stderr);
	int badline_reported = 0;
	int res = 1;
	uint64_t num = 0;
	readbuf *buf = 0;
	char *qbuf = 0;
	const int parsing_labels = (field_count == -1);
	const int real_field_count = (parsing_labels ? 1 : field_count);
	const int full_field_count = (parsing_labels ? 1 : real_field_count + 5);
	gzFile outfh[full_field_count];
	char *field_ptrs[real_field_count];
	int32_t field_lens[real_field_count];
	const int save_lineno = !!out_fns[real_field_count + 4];
	for (int i = 0; i < full_field_count; i++) {
		outfh[i] = 0;
	}
	buf = malloc(sizeof(*buf));
	err1(!buf);
	errno = 0;
	buf->pos = buf->avail = 0;
	if (quote_char < 257) {
		// For storing unquoted fields (extra room for a short length)
		qbuf = malloc(BIG_Z + 1);
		err1(!qbuf);
		qbuf++; // Room for a short length before
	}
	int eof = 0;
	int32_t len;
	uint64_t lineno = sliceno + 1;
	int field;
	int skip_line = 0;
	char *bufptr;
	// Do this first so we can skip opening output files if we are empty.
	if (bufread(fd, buf, 4, &eof, &bufptr)) {
		if (eof) goto done;
		goto err;
	}
	for (int i = 0; i < full_field_count; i++) {
		if (out_fns[i]) {
			outfh[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfh[i]);
		}
	}
	goto buf_prefilled;
keep_going:
	while (1) {
		if (bufread(fd, buf, 4, &eof, &bufptr)) {
			if (eof) break;
			goto err;
		}
buf_prefilled:
		memcpy(&len, bufptr, 4);
		if (len < 0) {
			if (len == LABELS_DONE_MARKER) {
				// labels are done, so we are now offset one line
				lineno++;
				continue;
			}
			len = -(len + 1);
			skip_line = 1;
		}
		err1(bufread(fd, buf, len, &eof, &bufptr));
		if (skip_line) {
			err1(gzwrite(outfh[real_field_count + 2], &lineno, 8) != 8);
			err1(field_write(outfh[real_field_count + 3], bufptr, len));
			r_num[2]++;
			skip_line = 0;
			lineno += slices;
			continue;
		}
		int32_t pos = 0;
		int32_t qpos = 0;
		field = 0;
		while (pos < len) {
			int last = 0;
			char *sep;
			const int quote = bufptr[pos];
#define IS_QUOTE (quote == quote_char || (quote_char == 256 && (quote == '"' || quote == '\'')))
			if (IS_QUOTE) {
				char *ptr = bufptr + pos + 1;
				char *qptr = 0;
				const char * const buf_end = bufptr + len;
				field_ptrs[field] = ptr;
				field_lens[field] = 0;
				char *candidate;
				while (1) {
					candidate = memchr(ptr, quote, buf_end - ptr);
					if (!candidate) goto bad_line;
					if (candidate == buf_end - 1 || candidate[1] == separator) {
						if (candidate == buf_end - 1) last = 1;
						if (qptr) {
							const int32_t partlen = candidate - ptr;
							memcpy(qptr, ptr, partlen);
							field_lens[field] += partlen;
							qpos += field_lens[field] + 1;
						} else {
							field_lens[field] = candidate - (bufptr + pos) - 1;
						}
						break;
					} else if (candidate[1] == quote) {
						const int32_t partlen = candidate - ptr + 1;
						if (qptr) {
							field_lens[field] += partlen;
						} else {
							qptr = qbuf + qpos;
							field_ptrs[field] = qptr;
							field_lens[field] = partlen;
						}
						memcpy(qptr, ptr, partlen);
						qptr += partlen;
						ptr = candidate + 2;
						if (ptr >= buf_end) goto bad_line;
					} else {
						goto bad_line;
					}
				}
				pos = candidate - bufptr + 2;
			} else {
				field_ptrs[field] = bufptr + pos;
				sep = memchr(bufptr + pos, separator, len - pos);
				if (sep) {
					field_lens[field] = sep - (bufptr + pos);
				} else {
					field_lens[field] = len - pos;
					last = 1;
				}
				pos += field_lens[field] + 1;
			}
			if (parsing_labels) {
				err1(field_write(outfh[field], field_ptrs[field], field_lens[field]));
			} else {
				field++;
				if (last) {
					if (field != real_field_count) {
						if (!r_num[1]) {
							fprintf(badline_report_fh, "Not enough fields on line %llu", (unsigned long long)lineno);
						}
						badline_reported = 1;
						goto bad_line;
					}
				} else if (field == real_field_count) {
					if (allow_extra_empty) while (pos < len) {
						if (bufptr[pos] == separator) {
							pos++;
						} else {
							const int quote = bufptr[pos];
							if (pos + 2 <= len && IS_QUOTE) {
								if (bufptr[pos + 1] == quote && (pos + 2 == len || bufptr[pos + 2] == separator)) {
									pos += 3;
								} else {
									break;
								}
							} else {
								break;
							}
						}
					}
					if (!allow_extra_empty || pos < len) {
						if (!r_num[1]) {
							fprintf(badline_report_fh, "Too many fields on line %llu", (unsigned long long)lineno);
						}
						badline_reported = 1;
						goto bad_line;
					}
				}
			}
		}
		if (!parsing_labels) {
			if (field == real_field_count - 1) {
				// The last field was empty (we can't reach here if it was totally missing)
				field_lens[field] = 0;
				field_ptrs[field] = bufptr + len;
				field++;
			}
			if (field != real_field_count) goto bad_line; // Happens if the line is empty
			for (field = 0; field < real_field_count; field++) {
				if (outfh[field]) {
					err1(field_write(outfh[field], field_ptrs[field], field_lens[field]));
				}
			}
			if (save_lineno) {
				err1(gzwrite(outfh[real_field_count + 4], &lineno, 8) != 8);
			}
		}
		num++;
		lineno += slices;
	}
done:
	*r_num = num;
	res = 0;
err:
	if (res && errno) perror("import_slice");
	for (int i = 0; i < full_field_count; i++) {
		if (outfh[i] && gzclose(outfh[i])) res = 1;
	}
	return res;
bad_line:
	if (!r_num[1]) {
		if (!badline_reported) {
			fprintf(badline_report_fh, "Line %llu bad", (unsigned long long)lineno);
		}
		if (allow_bad) {
			fprintf(badline_report_fh, " (further bad lines in slice %d not reported)", sliceno);
		}
		fprintf(badline_report_fh, "\n");
		fflush(stderr);
	}
	r_num[1]++;
	if (allow_bad) {
		if (outfh[real_field_count]) {
			err1(gzwrite(outfh[real_field_count], &lineno, 8) != 8);
			err1(field_write(outfh[real_field_count + 1], bufptr, len));
		}
		lineno += slices;
		goto keep_going;
	} else {
		goto err;
	}
}

// This is easier than using a type of known signedness above.
static int char2int(const char c)
{
	return c;
}
'''


extra_c_functions = r'''
static PyObject *py_reader(PyObject *self, PyObject *args)
{
	int fail = 1;
	const char *fn;
	int slices;
	PY_LONG_LONG skip_lines;
	int skip_empty_lines;
	PyObject *o_outfds;
	int *outfds = 0;
	int labels_fd;
	int status_fd;
	int comment_char;
	int lf_char;
	if (!PyArg_ParseTuple(args, "etiLiOiiii",
		Py_FileSystemDefaultEncoding, &fn,
		&slices,
		&skip_lines,
		&skip_empty_lines,
		&o_outfds,
		&labels_fd,
		&status_fd,
		&comment_char,
		&lf_char
	)) {
		return 0;
	}
	err1(!PyList_Check(o_outfds));
	Py_ssize_t cnt = PyList_Size(o_outfds);
	outfds = malloc(sizeof(int) * cnt);
	err1(!outfds);
	for (Py_ssize_t i = 0; i < cnt; i++) {
		PyObject *tmp = PyList_GET_ITEM(o_outfds, i);
		outfds[i] = PyLong_AsLong(tmp);
		if (PyErr_Occurred()) {
			free(outfds);
			return 0;
		}
	}
	fail = reader(fn, slices, skip_lines, skip_empty_lines, outfds, labels_fd, status_fd, comment_char, lf_char);
	fflush(stderr);
err:
	if (outfds) free(outfds);
	if (fail) Py_RETURN_TRUE;
	Py_RETURN_FALSE;
}

static PyObject *py_import_slice(PyObject *self, PyObject *args)
{
	int fail = 1;
	int fd;
	int sliceno;
	int slices;
	int field_count;
	PyObject *o_out_fns;
	const char **out_fns = 0;
	const char *gzip_mode;
	int separator;
	PyObject *o_r_num;
	uint64_t r_num[3] = {0, 0, 0};
	int quote_char;
	int lf_char;
	int allow_bad;
	int allow_extra_empty;
	if (!PyArg_ParseTuple(args, "iiiiOetiOiiii",
		&fd,
		&sliceno,
		&slices,
		&field_count,
		&o_out_fns,
		Py_FileSystemDefaultEncoding, &gzip_mode,
		&separator,
		&o_r_num,
		&quote_char,
		&lf_char,
		&allow_bad,
		&allow_extra_empty
	)) {
		return 0;
	}
	err1(!PyList_Check(o_out_fns));
	err1(!PyList_Check(o_r_num));
	err1(PyList_Size(o_r_num) != 3);
	Py_ssize_t cnt = PyList_Size(o_out_fns);
	out_fns = malloc(sizeof(char *) * cnt);
	err1(!out_fns);
	for (Py_ssize_t i = 0; i < cnt; i++) {
		PyObject *tmp = PyList_GET_ITEM(o_out_fns, i);
		if (str_or_0(tmp, &out_fns[i])) {
			free(out_fns);
			return 0;
		}
	}
	err1(import_slice(fd, sliceno, slices, field_count, out_fns, gzip_mode, separator, r_num, quote_char, lf_char, allow_bad, allow_extra_empty));
	for (int i = 0; i < 3; i++) {
		err1(PyList_SetItem(o_r_num, i, PyLong_FromUnsignedLongLong(r_num[i])));
	}
	fail = 0;
err:
	if (out_fns) free(out_fns);
	if (fail) Py_RETURN_TRUE;
	Py_RETURN_FALSE;
}

static PyObject *py_char2int(PyObject *dummy, PyObject *o_charstr)
{
	const char *charstr;
	if (str_or_0(o_charstr, &charstr)) return 0;
	if (!charstr) Py_RETURN_NONE;
	return PyLong_FromLong(char2int(*charstr));
}
'''

extra_method_defs = [
	'{"reader", py_reader, METH_VARARGS, 0}',
	'{"import_slice", py_import_slice, METH_VARARGS, 0}',
	'{"char2int", py_char2int, METH_O, 0}',
]


c_module_code, c_module_hash = c_backend_support.make_source('csvimport', all_c_functions, [], extra_c_functions, extra_method_defs, None)

def init():
	protos = [
		'static int reader(const char *fn, const int slices, uint64_t skip_lines, const int skip_empty_lines, const int outfds[], int labels_fd, int status_fd, const int comment_char, const int lf_char);',
		'static int import_slice(const int fd, const int sliceno, const int slices, const int field_count, const char *out_fns[], const char *gzip_mode, const int separator, uint64_t *r_num, const int quote_char, const int lf_char, const int allow_bad, const int allow_extra_empty);',
		'static int char2int(const char c);',
	]
	return c_backend_support.init('csvimport', c_module_hash, [], protos, all_c_functions)
