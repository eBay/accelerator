############################################################################
#                                                                          #
# Copyright (c) 2019-2021 Carl Drougge                                     #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

If comment, skip_lines or skip_empty_lines is set also creates a "skipped"
dataset containing lineno and data from skipped lines.

If you want lineno for good lines too set lineno_label.

There is no support for multi-line quoted fields. (But if you control the
writing side try using something like \x1e or \0 instead of newline.)
'''


import os
from multiprocessing import Process
from threading import Thread
import struct
import locale

from accelerator import OptionString, DotDict
from accelerator.sourcedata import typed_reader
from accelerator.compat import setproctitle, uni
from . import csvimport

depend_extra = (csvimport,)

options = dict(
	filename          = OptionString,
	separator         = ',',   # Single iso-8859-1 character or empty for a single field.
	comment           = '',    # Single iso-8859-1 character or empty, lines beginning with this character are ignored.
	newline           = '',    # Empty means \n or \r\n, or you can specify any single iso-8859-1 character.
	quotes            = '',    # Empty or False means no quotes, True means both ' and ", any other character means itself.
	labelsonfirstline = True,
	labels            = [],    # Mandatory if not labelsonfirstline, always sets labels if set.
	strip_labels      = False, # Do .strip() on all labels (happens before rename).
	rename            = {},    # Labels to replace (if they are in the file) (happens before discard).
	discard           = set(), # Labels to not include (if they are in the file)
	lineno_label      = "",    # Label of column to store line number in (not stored if empty).
	allow_bad         = False, # Still succeed if some lines have too few/many fields or bad quotes
	                           # creates a "bad" dataset containing lineno and data from the bad lines.
	allow_extra_empty = False, # Still consider a line good if it has extra empty fields at the end.
	skip_lines        = 0,     # skip this many lines at the start of the file.
	skip_empty_lines  = False, # ignore empty lines
	compression       = 6,     # gzip level
)

datasets = ('previous', )


cstuff = csvimport.init()

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

def reader_process(slices, filename, write_fds, labels_fd, success_fd, status_fd, comment_char, lf_char):
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
	os.dup2(success_fd, 2) # reader writes errors to stderr
	os.close(success_fd)
	success_fd = 2
	res = cstuff.backend.reader(filename.encode("utf-8"), slices, options.skip_lines, options.skip_empty_lines, write_fds, labels_fd, status_fd, comment_char, lf_char)
	if not res:
		os.write(success_fd, b"\0")
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
	return cstuff.backend.char2int(char)

def import_slice(fallback_msg, fd, sliceno, slices, field_count, out_fns, gzip_mode, separator, r_num, quote_char, lf_char, allow_bad, allow_extra_empty):
	fn = "import.success.%d" % (sliceno,)
	fh = open(fn, "wb+")
	real_stderr = os.dup(2)
	try:
		os.dup2(fh.fileno(), 2)
		res = cstuff.backend.import_slice(*cstuff.bytesargs(fd, sliceno, slices, field_count, out_fns, gzip_mode, separator, r_num, quote_char, lf_char, allow_bad, allow_extra_empty))
		os.dup2(real_stderr, 2)
		fh.seek(0)
		msg = fh.read().decode("utf-8", "replace")
		if msg or res:
			raise Exception(msg.strip() or fallback_msg)
	finally:
		os.dup2(real_stderr, 2)
		os.close(real_stderr)
		fh.close()
		os.unlink(fn)

def prepare(job, slices):
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
	filename = os.path.join(job.input_directory, options.filename)
	orig_filename = filename
	assert 1 <= options.compression <= 9

	# To get a more useful error if the file doesn't exist or similar
	open(filename, 'rb').close()

	fds = [os.pipe() for _ in range(slices)]
	read_fds = [t[0] for t in fds]
	write_fds = [t[1] for t in fds]

	if options.labelsonfirstline:
		labels_rfd, labels_wfd = os.pipe()
	else:
		labels_wfd = -1
	success_fh = open("reader.success", "wb+")
	status_rfd, status_wfd = os.pipe()

	p = Process(target=reader_process, name="reader", args=(slices, filename, write_fds, labels_wfd, success_fh.fileno(), status_wfd, comment_char, lf_char))
	p.start()
	for fd in write_fds:
		os.close(fd)
	os.close(status_wfd)

	if options.labelsonfirstline:
		os.close(labels_wfd)
		# re-use import logic
		out_fns = ["labels"]
		r_num = cstuff.mk_uint64(3)
		open("labels", "wb").close()
		try:
			import_slice("c backend failed in label parsing", labels_rfd, -1, -1, -1, out_fns, b"wb1", separator, r_num, quote_char, lf_char, 0, 0)
		finally:
			os.close(labels_rfd)
		with typed_reader("bytes")("labels") as fh:
			labels_from_file = [lab.decode("utf-8", "backslashreplace") for lab in fh]
		os.unlink("labels")
	else:
		labels_from_file = None

	labels = options.labels or labels_from_file
	if options.allow_extra_empty:
		while labels and labels[-1] == '':
			labels.pop()
	assert labels, "No labels"
	if options.strip_labels:
		labels = [x.strip() for x in labels]
	labels = [options.rename.get(x, x) for x in labels]
	assert '' not in labels, "Empty label for column %d" % (labels.index(''),)
	assert len(labels) == len(set(labels)), "Duplicate labels: %r" % (labels,)

	dw = job.datasetwriter(
		columns={n: 'bytes' for n in labels if n not in options.discard},
		filename=orig_filename,
		caption='csvimport of ' + orig_filename,
		previous=datasets.previous,
		meta_only=True,
	)
	if options.lineno_label:
		dw.add(options.lineno_label, "int64")

	def dsprevious(name):
		if datasets.previous and datasets.previous.name == 'default':
			from accelerator.error import NoSuchDatasetError
			try:
				return datasets.previous.job.dataset(name)
			except NoSuchDatasetError:
				return None
		return None

	if options.allow_bad:
		bad_dw = job.datasetwriter(
			name="bad",
			filename=orig_filename,
			columns=dict(lineno="int64", data="bytes"),
			caption='bad lines from csvimport of ' + orig_filename,
			previous=dsprevious('bad'),
			meta_only=True,
		)
	else:
		bad_dw = None

	if options.comment or options.skip_lines or options.skip_empty_lines:
		skipped_dw = job.datasetwriter(
			name="skipped",
			filename=orig_filename,
			columns=dict(lineno="int64", data="bytes"),
			caption='skipped lines from csvimport of ' + orig_filename,
			previous=dsprevious('skipped'),
			meta_only=True,
		)
	else:
		skipped_dw = None

	return separator, quote_char, lf_char, filename, orig_filename, labels, dw, bad_dw, skipped_dw, read_fds, success_fh, status_rfd,

def analysis(sliceno, slices, prepare_res, update_top_status):
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
			out_fns.append(cstuff.NULL)
		else:
			out_fns.append(dw.column_filename(label))
	for extra_dw in (bad_dw, skipped_dw):
		if extra_dw:
			for n in ("lineno", "data"):
				out_fns.append(extra_dw.column_filename(n))
		else:
			out_fns.append(cstuff.NULL)
			out_fns.append(cstuff.NULL)
	if options.lineno_label:
		out_fns.append(dw.column_filename(options.lineno_label))
	else:
		out_fns.append(cstuff.NULL)
	r_num = cstuff.mk_uint64(3) # [good_count, bad_count, comment_count]
	gzip_mode = b"wb%d" % (options.compression,)
	try:
		import_slice("c backend failed in slice %d" % (sliceno,), fds[sliceno], sliceno, slices, len(labels), out_fns, gzip_mode, separator, r_num, quote_char, lf_char, options.allow_bad, options.allow_extra_empty)
	finally:
		os.close(fds[sliceno])
	return list(r_num)

def synthesis(prepare_res, analysis_res):
	separator, _, _, filename, _, labels, dw, bad_dw, skipped_dw, fds, success_fh, _, = prepare_res
	# Analysis may have gotten a perfectly legitimate EOF if something
	# went wrong in the reader process, so we need to check that all
	# went well.
	reader_res = []
	try:
		success_fh.seek(0)
		reader_res = success_fh.read()
	except OSError:
		pass
	if reader_res != b"\0":
		reader_res = reader_res.decode("utf-8", "replace").strip("\r\n \t\0")
		raise Exception(reader_res or "Reader process failed")
	success_fh.close()
	os.unlink("reader.success")
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
	return DotDict(
		num_lines=sum(good_counts),
		lines_per_slice=good_counts,
		num_broken_lines=sum(bad_counts),
		broken_lines_per_slice=bad_counts,
		num_skipped_lines=sum(skipped_counts),
		skipped_lines_per_slice=skipped_counts,
	)
