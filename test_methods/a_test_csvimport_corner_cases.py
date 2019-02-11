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
Verify various corner cases in csvimport.
'''

import subjobs
from dispatch import JobError
from extras import resolve_jobid_filename
from dataset import Dataset
from compat import PY3

def openx(filename):
	return open(filename, "xb" if PY3 else "wbx")

def check_array(params, lines, filename, bad_lines=(), **options):
	d = {}
	with openx(filename) as fh:
		for ix, data in enumerate(bad_lines, 1):
			ix = str(ix).encode("ascii")
			fh.write(ix + b"," + data + b"," + data + b"\n")
		for ix, data in enumerate(lines, len(bad_lines) + 1):
			if isinstance(data, tuple):
				data, d[ix] = data
			else:
				d[ix] = data
			ix = str(ix).encode("ascii")
			fh.write(ix + b"," + data + b"," + data + b"\n")
	options.update(
		filename=resolve_jobid_filename(params.jobid, filename),
		allow_bad=bool(bad_lines),
		labelsonfirstline=False,
		labels=["ix", "0", "1"],
	)
	verify_ds(options, d, filename)

def verify_ds(options, d, filename):
	jid = subjobs.build("csvimport", options=options)
	# Order varies depending on slice count, so we use a dict {ix: data}
	for ix, a, b in Dataset(jid).iterate(None, ["ix", "0", "1"]):
		try:
			ix = int(ix)
		except ValueError:
			# We have a few non-numeric ones
			pass
		assert ix in d, "Bad index %r in %r (%s)" % (ix, filename, jid)
		assert a == b == d[ix], "Wrong data for line %r in %r (%s)" % (ix, filename, jid,)
		del d[ix]
	assert not d, "Not all lines returned from %r (%s), %r missing" % (filename, jid, set(d.keys()),)

def require_failure(name, options):
	try:
		subjobs.build("csvimport", options=options)
	except JobError:
		return
	raise Exception("File with %s was imported without error." % (name,))

def check_bad_file(params, name, data):
	filename = name + ".txt"
	with openx(filename) as fh:
		fh.write(data)
	options=dict(
		filename=resolve_jobid_filename(params.jobid, filename),
	)
	require_failure(name, options)

def check_good_file(params, name, data, d, **options):
	filename = name + ".txt"
	with openx(filename) as fh:
		fh.write(data)
	options.update(
		filename=resolve_jobid_filename(params.jobid, filename),
	)
	verify_ds(options, d, filename)

def synthesis(params):
	check_good_file(params, "mixed line endings", b"ix,0,1\r\n1,a,a\n2,b,b\r\n3,c,c", {1: b"a", 2: b"b", 3: b"c"})
	check_good_file(params, "ignored quotes", b"ix,0,1\n1,'a,'a\n2,'b','b'\n3,\"c\",\"c\"\n4,d',d'\n", {1: b"'a", 2: b"'b'", 3: b'"c"', 4: b"d'"})
	check_good_file(params, "ignored quotes and extra fields", b"ix,0,1\n1,\"a,\"a\n2,'b,c',d\n3,d\",d\"\n", {1: b'"a', 3: b'd"'}, allow_bad=True)
	check_good_file(params, "spaces and quotes", b"ix,0,1\none,a,a\ntwo, b, b\n three,c,c\n4,\"d\"\"\",d\"\n5, 'e',\" 'e'\"\n", {b"one": b"a", b"two": b" b", b" three": b"c", 4: b'd"', 5: b" 'e'"}, quote_support=True)
	check_good_file(params, "empty fields", b"ix,0,1\n1,,''\n2,,\n3,'',\n4,\"\",", {1: b"", 2: b"", 3: b"", 4: b""}, quote_support=True)
	check_good_file(params, "renamed fields", b"0,1,2\n0,foo,foo", {0: b"foo"}, rename={"0": "ix", "2": "0"})
	check_good_file(params, "discarded field", b"ix,0,no,1\n0,yes,no,yes\n1,a,'foo,bar',a", {0: b"yes", 1: b"a"}, quote_support=True, discard={"no"})
# Should ignore the lines with bad quotes, but currently does not.
#	check_good_file(params, "bad quotes", b"""ix,0,1\n1,a,a\n2,"b,"b\n\n3,'c'c','c'c'\n4,"d",'d'\n""", {1: b"a", 4: b"d"}, quote_support=True, allow_bad=True)
	bad_lines = [
		b"bad,bad",
		b",",
		b"bad,",
		b",bad",
		b"',',",
# These should be considered bad, but currently are not.
#		b"'lo there broken line",
#		b"'nope\"",
#		b"'bad quotes''",
#		b'"bad quote " inside"',
#		b'"more bad quotes """ inside"',
	]
	good_lines = [
		(b"'good, good'", b"good, good"),
		(b'"also good, yeah!"', b"also good, yeah!"),
		(b"'single quote''s inside'", b"single quote's inside"),
		(b"'single quote at end: '''", b"single quote at end: '"),
		(b'"""double quotes around"""', b'"double quotes around"'),
		(b'"double quote at end: """', b'double quote at end: "'),
		(b'" I\'m special "', b" I'm special "),
		b"I'm not",
		b" unquoted but with spaces around ",
		(b"','", b","),
# These should work, but currently do not.
#		b"\xff\x00\x08\x00",
#		(b"'lot''s of ''quotes'' around here: '''''''' '", b"lot's of 'quotes' around here: '''' ")
	]
	check_array(params, good_lines, "strange values.txt", bad_lines, quote_support=True)
	# The lines will be 2 * length + 3 bytes (plus lf)
	long_lines = [b"a" * length for length in (64 * 1024 - 2, 999, 999, 1999, 3000, 65000)]
	check_array(params, long_lines, "long lines.txt")
	check_bad_file(params, "extra field", b"foo,bar\nwith,extra,field\nok,here\n")
	check_bad_file(params, "missing field", b"foo,bar\nmissing\nok,here\n")
	check_bad_file(params, "no valid lines", b"foo\nc,\n")
