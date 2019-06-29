############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
CSV file to dataset.

Read a CSV file (can be gziped), with any single iso-8859-1 character
separator (including \0) or no separator at all (always one field per line)
with or without quotes. Any single iso-8859-1 character or both \n and \r\n
(default) as newline. Labels from first line or specified in options.

If allow_bad is set also creates a "bad" dataset containing lineno and data
from bad lines.

If comment or skip_lines is set also creates a "skipped" dataset containing
lineno and data from skipped lines.
'''


import os
import cffi
from multiprocessing import Process
from threading import Thread
import struct
import locale

from extras import OptionString, DotDict
from dataset import DatasetWriter
from sourcedata import typed_reader
from compat import setproctitle, uni
import blob
from report import Report


options = dict(
	filename          = OptionString,
	separator         = ',',   # Single iso-8859-1 character or empty for a single field.
	comment           = '',    # Single iso-8859-1 character or empty, lines beginning with this character are ignored.
	newline           = '',    # Empty means \n or \r\n, or you can specify any single iso-8859-1 character.
	quotes            = '',    # Empty or False means no quotes, True means both ' and ", any other character means itself.
	labelsonfirstline = True,
	labels            = [],    # Mandatory if not labelsonfirstline, always sets labels if set.
	rename            = {},    # Labels to replace (if they are in the file) (happens first)
	discard           = set(), # Labels to not include (if they are in the file)
	allow_bad         = False, # Still succeed if some lines have too few/many fields or bad quotes
	                           # creates a "bad" dataset containing lineno and data from the bad lines.
	skip_lines        = 0,     # skip this many lines at the start of the file.
	compression       = 6,     # gzip level
)

datasets = ('previous', )


ffi = cffi.FFI()
ffi.cdef('''
int reader(const char *fn, const int slices, uint64_t skip_lines, const int outfds[], int labels_fd, int status_fd, const int comment_char, const int lf_char);
int import_slice(const int fd, const int sliceno, const int slices, const int field_count, const char *out_fns[], const char *gzip_mode, const int separator, uint64_t *r_num, const int quote_char, const int lf_char, const int allow_bad);
int char2int(const char c);
''')

backend = ffi.verify(r'''
#include <zlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/types.h>
#include <signal.h>

#define err1(v) if (v) { perror("ERROR"); printf("ERROR! %s %d\n", __FILE__, __LINE__); goto err; }
#define BIG_Z (1024 * 1024 * 16 - 64)
#define SMALL_Z (1024 * 64)

static pthread_barrier_t barrier;
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
		err1(len < 0);
		buf_lens[i] = len;
		pthread_barrier_wait(&barrier);
		if (len == 0) return 0;
		i = !i;
	}
err:
	perror("readgz_thread");
	kill(getpid(), 9);
	return 0;
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
		pthread_barrier_wait(&barrier);
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
			printf("Cannot handle lines longer than %d bytes\n", BIG_Z);
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
			printf("Cannot handle lines longer than %d bytes\n", BIG_Z);
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

int reader(const char *fn, const int slices, uint64_t skip_lines, const int outfds[], int labels_fd, int status_fd, const int comment_char, const int lf_char)
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
	err1(pthread_barrier_init(&barrier, 0, 2));
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
		err1(!ptr);
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
		if (skip_lines || *ptr == comment_char) {
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
	if (res) perror("reader");
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

int import_slice(const int fd, const int sliceno, const int slices, int field_count, const char *out_fns[], const char *gzip_mode, const int separator, uint64_t *r_num, const int quote_char, const int lf_char, const int allow_bad)
{
	int res = 1;
	uint64_t num = 0;
	readbuf *buf = 0;
	char *qbuf = 0;
	const int parsing_labels = (field_count == -1);
	const int real_field_count = (parsing_labels ? 1 : field_count);
	const int full_field_count = (parsing_labels ? 1 : real_field_count + 4);
	gzFile outfh[full_field_count];
	char *field_ptrs[real_field_count];
	int32_t field_lens[real_field_count];
	for (int i = 0; i < full_field_count; i++) {
		outfh[i] = 0;
	}
	buf = malloc(sizeof(*buf));
	err1(!buf);
	buf->pos = buf->avail = 0;
	if (quote_char < 257) {
		// For storing unquoted fields (extra room for a short length)
		qbuf = malloc(BIG_Z + 1);
		err1(!qbuf);
		qbuf++; // Room for a short length before
	}
	for (int i = 0; i < full_field_count; i++) {
		if (out_fns[i]) {
			outfh[i] = gzopen(out_fns[i], gzip_mode);
			err1(!outfh[i]);
		}
	}
	int eof = 0;
	int32_t len;
	uint64_t lineno = sliceno + 1;
	int field;
	int skip_line = 0;
	char *bufptr;
keep_going:
	while (1) {
		if (bufread(fd, buf, 4, &eof, &bufptr)) {
			if (eof) break;
			goto err;
		}
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
			if (quote == quote_char || (quote_char == 256 && (quote == '"' || quote == '\''))) {
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
							printf("Not enough fields on line %llu\n", (unsigned long long)lineno);
						}
						goto bad_line;
					}
				} else {
					if (field == real_field_count) {
						if (!r_num[1]) {
							printf("Too many fields on line %llu\n", (unsigned long long)lineno);
						}
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
		}
		num++;
		lineno += slices;
	}
	*r_num = num;
	res = 0;
err:
	if (res) perror("import_slice");
	for (int i = 0; i < full_field_count; i++) {
		if (outfh[i] && gzclose(outfh[i])) res = 1;
	}
	return res;
bad_line:
	if (!r_num[1]) {
		printf("Line %llu bad (further bad lines in slice %d not reported)\n", (unsigned long long)lineno, sliceno);
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
int char2int(const char c)
{
	return c;
}
''', libraries=[str('z')], extra_compile_args=[str('-std=c99')])

def reader_status(status_fd, update):
	# try to get nicer number formating
	try:
		locale.resetlocale(locale.LC_NUMERIC)
	except Exception:
		pass
	count = 0
	while True:
		update('{0:n} lines read'.format(count))
		data = os.read(status_fd, 8)
		if not data:
			break
		count = struct.unpack("=Q", data)[0]

def reader_process(params, filename, write_fds, labels_fd, success_fd, status_fd, comment_char, lf_char):
	# Terrible hack - try to close FDs we didn't want in this process.
	# (This is important, if the main process dies this won't be
	# detected if we still have these open.)
	keep_fds = set(write_fds)
	keep_fds.add(labels_fd)
	keep_fds.add(success_fd)
	keep_fds.add(status_fd)
	# a few extra to be safe.
	for fd in range(3, max(keep_fds) + 32):
		if fd not in keep_fds:
			try:
				os.close(fd)
			except OSError:
				pass
	setproctitle("reader")
	res = backend.reader(filename.encode("ascii"), params.slices, options.skip_lines, write_fds, labels_fd, status_fd, comment_char, lf_char)
	os.write(success_fd, b"\x01" if res else b"\0")
	os.close(success_fd)

def char2int(name, empty_value, specials="empty"):
	char = options.get(name)
	if not char:
		return empty_value
	msg = "%s must be a single iso-8859-1 character (or %s)" % (name, specials,)
	if isinstance(char, bytes):
		char = uni(char)
	try:
		char = char.encode("iso-8859-1")
	except UnicodeEncodeError:
		raise Exception(msg)
	assert len(char) == 1, msg
	return backend.char2int(char)

def prepare(SOURCE_DIRECTORY, params):
	# use 256 as a marker value, because that's not a possible char value (assuming 8 bit chars)
	lf_char = char2int("newline", 256)
	# separator uses lf_char or \n as the empty value, because memchr might mishandle 256.
	separator = char2int("separator", 10 if lf_char == 256 else lf_char)
	comment_char = char2int("comment", 256)
	if options.quotes == 'True':
		quote_char = 256
	elif options.quotes == 'False':
		quote_char = 257
	else:
		quote_char = char2int("quotes", 257, "True/False/empty")
	filename = os.path.join(SOURCE_DIRECTORY, options.filename)
	orig_filename = filename
	assert 1 <= options.compression <= 9

	fds = [os.pipe() for _ in range(params.slices)]
	read_fds = [t[0] for t in fds]
	write_fds = [t[1] for t in fds]

	if options.labelsonfirstline:
		labels_rfd, labels_wfd = os.pipe()
	else:
		labels_wfd = -1
	success_rfd, success_wfd = os.pipe()
	status_rfd, status_wfd = os.pipe()

	p = Process(target=reader_process, name="reader", args=(params, filename, write_fds, labels_wfd, success_wfd, status_wfd, comment_char, lf_char))
	p.start()
	for fd in write_fds:
		os.close(fd)
	os.close(success_wfd)
	os.close(status_wfd)

	if options.labelsonfirstline:
		os.close(labels_wfd)
		# re-use import logic
		out_fns = [ffi.new('char []', "labels".encode("ascii"))]
		r_num = ffi.new('uint64_t [3]')
		res = backend.import_slice(labels_rfd, -1, -1, -1, out_fns, b"wb1", separator, r_num, quote_char, lf_char, 0)
		os.close(labels_rfd)
		assert res == 0, "c backend failed in label parsing"
		with typed_reader("bytes")("labels") as fh:
			labels_from_file = [lab.decode("utf-8", "backslashreplace") for lab in fh]
		os.unlink("labels")
	else:
		labels_from_file = None

	labels = options.labels or labels_from_file
	assert labels, "No labels"
	labels = [options.rename.get(x, x) for x in labels]
	assert '' not in labels, "Empty label for column %d" % (labels.index(''),)
	assert len(labels) == len(set(labels)), "Duplicate labels: %r" % (labels,)

	dw = DatasetWriter(
		columns={n: 'bytes' for n in labels if n not in options.discard},
		filename=orig_filename,
		caption='csvimport of ' + orig_filename,
		previous=datasets.previous,
		meta_only=True,
	)

	if options.allow_bad:
		bad_dw = DatasetWriter(
			name="bad",
			columns=dict(lineno="int64", data="bytes"),
			caption='bad lines from csvimport of ' + orig_filename,
			meta_only=True,
		)
	else:
		bad_dw = None

	if options.comment or options.skip_lines:
		skipped_dw = DatasetWriter(
			name="skipped",
			columns=dict(lineno="int64", data="bytes"),
			caption='skipped lines from csvimport of ' + orig_filename,
			meta_only=True,
		)
	else:
		skipped_dw = None

	return separator, quote_char, lf_char, filename, orig_filename, labels, dw, bad_dw, skipped_dw, read_fds, success_rfd, status_rfd,

def analysis(sliceno, params, prepare_res, update_top_status):
	separator, quote_char, lf_char, filename, _, labels, dw, bad_dw, skipped_dw, fds, _, status_fd, = prepare_res
	if sliceno == 0:
		t = Thread(
			target=reader_status,
			args=(status_fd, update_top_status),
			name='reader status',
		)
		t.daemon = True
		t.start()
	else:
		os.close(status_fd)
	# Close the FDs for all other slices.
	# Not techically necessary, but it feels like a good idea.
	for ix, fd in enumerate(fds):
		if ix != sliceno:
			os.close(fd)
	out_fns = []
	for label in labels:
		if label in options.discard:
			out_fns.append(ffi.NULL)
		else:
			fn = dw.column_filename(label)
			out_fns.append(ffi.new('char []', fn.encode("ascii")))
	for extra_dw in (bad_dw, skipped_dw):
		if extra_dw:
			for n in ("lineno", "data"):
				fn = extra_dw.column_filename(n)
				out_fns.append(ffi.new('char []', fn.encode("ascii")))
		else:
			out_fns.append(ffi.NULL)
			out_fns.append(ffi.NULL)
	r_num = ffi.new('uint64_t [3]') # [good_count, bad_count, comment_count]
	gzip_mode = b"wb%d" % (options.compression,)
	res = backend.import_slice(fds[sliceno], sliceno, params.slices, len(labels), out_fns, gzip_mode, separator, r_num, quote_char, lf_char, options.allow_bad)
	assert res == 0, "c backend failed in slice %d" % (sliceno,)
	os.close(fds[sliceno])
	return list(r_num)

def synthesis(params, prepare_res, analysis_res):
	separator, _, _, filename, _, labels, dw, bad_dw, skipped_dw, fds, success_fd, _, = prepare_res
	# Analysis may have gotten a perfectly legitimate EOF if something
	# went wrong in the reader process, so we need to check that all
	# went well.
	try:
		reader_res = os.read(success_fd, 1)
	except OSError:
		reader_res = None
	if reader_res != b"\0":
		raise Exception("Reader process failed")
	good_counts = []
	bad_counts = []
	skipped_counts = []
	for sliceno, (good_count, bad_count, skipped_count) in enumerate(analysis_res):
		dw.set_lines(sliceno, good_count)
		if bad_dw:
			bad_dw.set_lines(sliceno, bad_count)
		if skipped_dw:
			skipped_dw.set_lines(sliceno, skipped_count)
		good_counts.append(good_count)
		bad_counts.append(bad_count)
		skipped_counts.append(skipped_count)
	res = DotDict(
		num_lines=sum(good_counts),
		lines_per_slice=good_counts,
		num_broken_lines=sum(bad_counts),
		broken_lines_per_slice=bad_counts,
		num_skipped_lines=sum(skipped_counts),
		skipped_lines_per_slice=skipped_counts,
	)
	blob.save(res, 'import')
	write_report(res, params, labels)

def write_report(res, params, labels):
	with Report() as r:
		divider = (res.num_lines + res.num_broken_lines + res.num_skipped_lines) or 1
		r.println("Number of rows read\n")
		r.write("  slice           lines")
		if res.num_broken_lines:
			r.write("               broken")
		if res.num_skipped_lines:
			r.write("              skipped")
		r.write("\n")
		for sliceno, (good_cnt, bad_cnt, skipped_cnt) in enumerate(zip(res.lines_per_slice, res.broken_lines_per_slice, res.skipped_lines_per_slice)):
			r.write("  %5d       %9d  (%6.2f%%)" % (sliceno, good_cnt, 100 * good_cnt / divider,))
			if res.num_broken_lines:
				r.write(" %9d  (%6.2f%%)" % (bad_cnt, 100 * bad_cnt / divider,))
			if res.num_skipped_lines:
				r.write(" %9d  (%6.2f%%)" % (skipped_cnt, 100 * skipped_cnt / divider,))
			r.write("\n")
		r.write("  total       %9d" % (res.num_lines,))
		if res.num_broken_lines or res.num_skipped_lines:
			r.write("  (%6.2f%%)" % (100 * res.num_lines / divider,))
		if res.num_broken_lines:
			r.write(" %9d  (%6.2f%%)" % (res.num_broken_lines, 100 * res.num_broken_lines / divider,))
		if res.num_skipped_lines:
			r.write(" %9d  (%6.2f%%)" % (res.num_skipped_lines, 100 * res.num_skipped_lines / divider,))
		r.write("\n")
		r.line()
		r.println('Number of columns %5d' % len(labels,))
