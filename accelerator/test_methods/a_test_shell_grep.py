############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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
Test the "ax grep" shell command. This isn't testing complex regexes,
but rather the various output options and data types.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

from subprocess import check_output
import datetime
import os
import json

from accelerator.compat import PY2, PY3, izip_longest
from accelerator.dsutil import _convfuncs

def grep_text(args, want, sep='\t', encoding='utf-8', unordered=False):
	if not unordered:
		args = ['--ordered'] + args
	cmd = options.command_prefix + ['grep'] + args
	res = check_output(cmd)
	res = res.split(b'\n')[:-1]
	if len(want) != len(res):
		raise Exception('%r gave %d lines, wanted %d.\n%r' % (cmd, len(res), len(want), res,))
	if encoding:
		res = [el.decode(encoding, 'replace') for el in res]
	typ = type(sep)
	want = [sep.join(typ(el) for el in l) for l in want]
	for lineno, (want, got) in enumerate(zip(want, res), 1):
		if want != got:
			raise Exception('%r gave wrong result on line %d:\nWant: %r\nGot:  %r' % (cmd, lineno, want, got,))

def frame(pre, a, post='\x1b[39m'):
	return [pre + el + post for el in a]

def grep_json(args, want):
	cmd = options.command_prefix + ['grep', '--ordered', '--format=json'] + args
	res = check_output(cmd)
	res = res.decode('utf-8', 'surrogatepass')
	res = res.split('\n')[:-1]
	if len(want) != len(res):
		raise Exception('%r gave %d lines, wanted %d.\n%r' % (cmd, len(res), len(want), res,))
	for lineno, (want, got) in enumerate(zip(want, res), 1):
		try:
			got = json.loads(got)
		except Exception as e:
			raise Exception('%r made bad json %r on line %d: %s' % (cmd, got, lineno, e,))
		if want != got:
			raise Exception('%r gave wrong result on line %d:\nWant: %r\nGot:  %r' % (cmd, lineno, want, got,))

if PY2:
	def mk_bytes(low, high):
		return b''.join(chr(c) for c in range(low, high))
else:
	def mk_bytes(low, high):
		return bytes(range(low, high))

# looks like 'bar' when matching but 'foo' when printing
# intended to catch if objects are evaluated too many times
class TricksyObject:
	def __init__(self):
		self.counter = 0
	def __str__(self):
		self.counter += 1
		if self.counter == 1:
			return 'bar'
		elif self.counter == 2:
			return 'foo'
		else:
			return 'oops'

def synthesis(job, slices):
	used_types = set()
	def mk_ds(name, types, *lines, **kw):
		dw = job.datasetwriter(name=name, **kw)
		for t in types:
			if isinstance(t, tuple):
				t, name = t
			else:
				name = t
			dw.add(name, t)
			used_types.add(t)
		for sliceno, line in izip_longest(range(slices), lines):
			if sliceno is not None:
				dw.set_slice(sliceno)
			if line:
				dw.write(*line)
		return dw.finish()
	HDR_HI = '\x1b[34m'
	COMMA_HI = '\x1b[36;4m,\x1b[24;39m'
	TAB_HI = '\x1b[36;4m\t\x1b[24;39m'
	os.unsetenv('NO_COLOR')
	os.unsetenv('CLICOLOR')
	os.unsetenv('CLICOLOR_FORCE')

	# start with testing basic output, chaining, column selection and headers.
	a = mk_ds('a', ['int32', 'int64'], [100, 200], [101, 201])
	b = mk_ds('b', ['int32'], [1000], [1001], previous=a)
	grep_text(['', a], [[100, 200], [101, 201]])
	grep_json(['', a], [{'int32': 100, 'int64': 200}, {'int32': 101, 'int64': 201}])
	grep_text(['-S', '', a], [[0, 100, 200], [1, 101, 201]])
	grep_text(['', b + '~'], [[100, 200], [101, 201]]) # verify ds parsing happens
	grep_json(['-S', '-L', '-D', '-c', '', b], [
		{'dataset': a, 'sliceno': 0, 'lineno': 0, 'data': {'int32': 100, 'int64': 200}},
		{'dataset': a, 'sliceno': 1, 'lineno': 0, 'data': {'int32': 101, 'int64': 201}},
		{'dataset': b, 'sliceno': 0, 'lineno': 0, 'data': {'int32': 1000}},
		{'dataset': b, 'sliceno': 1, 'lineno': 0, 'data': {'int32': 1001}},
	])
	grep_text(['-t', '2', '', a], [[100, '"200"'], [101, '"201"']], sep='2') # stupid separator leads to escaping
	grep_text(['-t', '2', '-f', 'raw', '', a], [[100, 200], [101, 201]], sep='2') # but not in the raw format

	# try some colour
	grep_text(['--colour', '-t', ',', '-D', '-S', '-H', '', a], [frame(HDR_HI, ['[DATASET]', '[SLICE]', 'int32', 'int64']), [a, 0, 100, 200], [a, 1, 101, 201]], sep=COMMA_HI)
	os.putenv('CLICOLOR_FORCE', '1')
	grep_text(['-t', ',', '-L', '-S', '-H', '', a], [frame(HDR_HI, ['[SLICE]', '[LINE]', 'int32', 'int64']), [0, 0, 100, 200], [1, 0, 101, 201]], sep=COMMA_HI)
	grep_text(['-t', ',', '-L', '-S', '-H', '--colour=never', '', a], [['[SLICE]', '[LINE]', 'int32', 'int64'], [0, 0, 100, 200], [1, 0, 101, 201]], sep=',')
	grep_text(['-t', ',', '-D', '-H', '', b], [frame(HDR_HI, ['[DATASET]', 'int32']), [b, 1000], [b, 1001]], sep=COMMA_HI)
	grep_text(['-t', ',', '-D', '-H', '-c', '', b], [frame(HDR_HI, ['[DATASET]', 'int32', 'int64']), [a, 100, 200], [a, 101, 201], frame(HDR_HI, ['[DATASET]', 'int32']), [b, 1000], [b, 1001]], sep=COMMA_HI)
	grep_json(['-s', '0', '', a], [{'int32': 100, 'int64': 200}]) # no colour in json
	grep_text(['-s', '0', '', b, a], [['1000'], ['100', '200']], sep=TAB_HI)
	grep_text(['--color=never', '0', b], [[1000], [1001]])
	os.unsetenv('CLICOLOR_FORCE')
	os.putenv('NO_COLOR', '')
	grep_text(['--colour', 'always', '0', b], [['1\x1b[31m0\x1b[39m\x1b[31m0\x1b[39m\x1b[31m0\x1b[39m'], ['1\x1b[31m0\x1b[39m\x1b[31m0\x1b[39m1']], sep=TAB_HI)
	os.unsetenv('NO_COLOR')
	if PY3: # no pickle type on PY2
		pickle = mk_ds('pickle', ['pickle'], [TricksyObject()], [''], [{'foo'}])
		grep_text(['', pickle], [['foo'], [''], ["{'foo'}"]])
		grep_text(['.', pickle], [['foo'], ["{'foo'}"]])
		grep_text(['bar', pickle], [['foo']])
		bytespickle = mk_ds('bytespickle', ['pickle'], [b'\xf0'], [b'\t'])
		# pickles are str()d, not special cased like bytes columns
		grep_text(['-f', 'raw', 'xf0', bytespickle], [["b'\\xf0'"]])
		grep_json(['', bytespickle], [{'pickle': "b'\\xf0'"}, {'pickle': "b'\\t'"}])

	# check all possible byte values in all output formats
	allbytes = mk_ds('allbytes', ['ascii', 'bytes'],
		['control chars', mk_bytes(0, 32)],
		['printable', mk_bytes(32, 128)],
		['not ascii', mk_bytes(128, 256)],
	)
	if PY2:
		not_ascii = '\ufffd'.encode('utf-8') * 128
	else:
		not_ascii = mk_bytes(128, 256)
	grep_text(
		['--format=raw', '', allbytes],
		[
			[b'control chars', mk_bytes(0, 10)],
			[mk_bytes(11, 32)], # we end up with an extra line because the control chars have a newline
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', not_ascii],
		],
		encoding=None,
		sep=b'\t',
	)
	if PY3:
		not_ascii = not_ascii.decode('utf-8', 'surrogateescape').encode('utf-8', 'surrogatepass')
	grep_text(
		['', allbytes],
		[
			[b'control chars', b'"' + mk_bytes(0, 10) + b'\\n' + mk_bytes(11, 32) + b'"'],
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', not_ascii],
		],
		encoding=None,
		sep=b'\t',
	)
	grep_json(['', allbytes], [
		{'ascii': 'control chars', 'bytes': mk_bytes(0, 32).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
		{'ascii': 'printable', 'bytes': mk_bytes(32, 128).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
		{'ascii': 'not ascii', 'bytes': mk_bytes(128, 256).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
	])

	# header printing should happen between datasets only when columns change,
	# and must wait for all slices for each switch.
	# to make this predictable without -O, only one slice is used per column set.
	columns = [
		('int32', 'int64',),
		('int64', 'int32',), # not actually a change
		('int32', 'number',),
		('int32',),
		('int32',),
		('int32',),
		('int64',),
	]
	values_every_time = range(10)
	previous = None
	previous_cols = []
	slice = 0
	for ds_ix, cols in enumerate(columns):
		dw = job.datasetwriter(name='header test %d' % (ds_ix,), previous=previous, allow_missing_slices=True)
		for col in cols:
			dw.add(col, col)
		if sorted(cols) != previous_cols:
			# columns changed, so switch slice to make failure more likely
			previous_cols = sorted(cols)
			slice = (slice + 1) % slices
		dw.set_slice(slice)
		for value in values_every_time:
			args = (value,) * len(cols)
			dw.write(*args)
		previous = dw.finish()
	grep_text(
		['-H', '-c', '', previous],
			[['int32', 'int64']] +
			[(v, v,) for v in values_every_time] +
			[(v, v,) for v in values_every_time] +
			[['int32', 'number']] +
			[(v, v,) for v in values_every_time] +
			[['int32']] +
			[(v,) for v in values_every_time] +
			[(v,) for v in values_every_time] +
			[(v,) for v in values_every_time] +
			[['int64']] +
			[(v,) for v in values_every_time],
		unordered=True,
	)

	# more escaping
	escapy = mk_ds('escapy',
		[('ascii', 'spaced name'), ('unicode', 'tabbed\tname')],
		['comma', 'foo,bar'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new\nline'],
		['doublequote start', '"foo'],
		['doublequote inside', 'f"oo'],
		['doublequote end', 'foo"'],
		['singlequote start', "'foo"],
		['singlequote inside', "f'oo"],
		['singlequote end', "foo'"],
	)
	grep_text(['-H', '', escapy], [
		['spaced name', '"tabbed\tname"'],
		['comma', 'foo,bar'],
		['tab', '"foo\tbar"'],
		['newline', 'a brand new\\nline'],
		['doublequote start', '"""foo"'],
		['doublequote inside', 'f"oo'],
		['doublequote end', '"foo"""'],
		['singlequote start', "\"'foo\""],
		['singlequote inside', "f'oo"],
		['singlequote end', "\"foo'\""],
	])
	grep_text(['-H', '-f', 'raw', '', escapy], [
		['spaced name', 'tabbed\tname'],
		['comma', 'foo,bar'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new'],
		['line'], # newline is not escaped
		['doublequote start', '"foo'],
		['doublequote inside', 'f"oo'],
		['doublequote end', 'foo"'],
		['singlequote start', "'foo"],
		['singlequote inside', "f'oo"],
		['singlequote end', "foo'"],
	])
	grep_text(['-H', '-t', ',', '(bar|newline|end)', escapy], [
		['spaced name', 'tabbed\tname'],
		['comma', '"foo,bar"'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new\\nline'],
		['doublequote end', '"foo"""'],
		['singlequote end', "\"foo'\""],
	], sep=',')
	grep_text(['-H', '-t', ' ', '(tab|inside)', escapy], [
		['"spaced name"', 'tabbed\tname'],
		['tab', 'foo\tbar'],
		['"doublequote inside"', 'f"oo'],
		['"singlequote inside"', "f'oo"],
	], sep=' ')
	grep_json(['', escapy], [
		{'spaced name': 'comma', 'tabbed\tname': 'foo,bar'},
		{'spaced name': 'tab', 'tabbed\tname': 'foo\tbar'},
		{'spaced name': 'newline', 'tabbed\tname': 'a brand new\nline'},
		{'spaced name': 'doublequote start', 'tabbed\tname': '"foo'},
		{'spaced name': 'doublequote inside', 'tabbed\tname': 'f"oo'},
		{'spaced name': 'doublequote end', 'tabbed\tname': 'foo"'},
		{'spaced name': 'singlequote start', 'tabbed\tname': "'foo"},
		{'spaced name': 'singlequote inside', 'tabbed\tname': "f'oo"},
		{'spaced name': 'singlequote end', 'tabbed\tname': "foo'"},
	])

	alltypes = mk_ds('alltypes',
		[
			'ascii',
			'bits32',
			'bits64',
			'bool',
			'bytes',
			'complex32',
			'complex64',
			'date',
			'datetime',
			'float32',
			'float64',
			'json',
			'number',
			'time',
			'unicode',
		], [
			'foo',
			11111,
			99999,
			True,
			b'\xff\x00octets',
			1+2j,
			1.5-0.5j,
			datetime.date(2021, 9, 20),
			datetime.datetime(2021, 9, 20, 1, 2, 3),
			0.125,
			1e42,
			[1, 2, 3, {'FOO': 'BAR'}, None],
			-2,
			datetime.time(4, 5, 6),
			'codepoints\x00\xe4',
		], [
			'',
			0,
			0,
			False,
			b'',
			0j,
			0j,
			datetime.date(1, 1, 1),
			datetime.datetime(1, 1, 1, 1, 1, 1),
			0.0,
			0.0,
			'json',
			0,
			datetime.time(1, 1, 1),
			'',
		],
	)
	grep_text(['json', alltypes], [['', 0, 0, 'False', '', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 'json', 0, '01:01:01', '']])
	grep_text(['-g', 'json', 'foo', alltypes], [])
	grep_text(['-g', 'bytes', 'tet', alltypes, 'ascii', 'unicode'], [['foo', 'codepoints\x00\xe4']])
	grep_text(['-g', 'bytes', '\\x00', alltypes, 'bool'], [['True']])
	if PY3:
		# python2 doesn't really handle non-utf8 bytes
		grep_text(['-g', 'bytes', '\\udcff', alltypes, 'bool'], [['True']])
	grep_text(['--format=raw', '-g', 'json', '-i', 'foo', alltypes], [[b'foo', b'11111', b'99999', b'True', b'\xff\x00octets' if PY3 else b'\xef\xbf\xbd\x00octets', b'(1+2j)', b'(1.5-0.5j)', b'2021-09-20', b'2021-09-20 01:02:03', b'0.125', b'1e+42', b"[1, 2, 3, {'FOO': 'BAR'}, None]" if PY3 else b"[1, 2, 3, {u'FOO': u'BAR'}, None]", b'-2', b'04:05:06', b'codepoints\x00\xc3\xa4']], sep=b'\t', encoding=None)
	grep_json([':05:', alltypes, 'bool', 'time', 'unicode', 'bytes'], [{'bool': True, 'time': '04:05:06', 'unicode': 'codepoints\x00\xe4', 'bytes': '\udcff\x00octets' if PY3 else '\ufffd\x00octets'}])

	columns = [
		'ascii',
		'bits32',
		'bits64',
		'bytes',
		'complex32',
		'complex64',
		'date',
		'datetime',
		'float32',
		'float64',
		'int32',
		'int64',
		'json',
		'number',
		'time',
		'unicode',
	]
	d = mk_ds('d', columns,
		['42', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 42, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 42, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'42', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 42+0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 42+0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(42, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(42, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 42.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 42.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 42, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 42, '', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '42', 0, datetime.time(1, 1, 1), '',],
		['', 0, 0, b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 42, datetime.time(1, 1, 1), '',],
		['a', 0, 0, b'b', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 42), '',],
		['B', 0, 0, b'A', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '42',],
	)
	want = [
		['42', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 42, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 42, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '42', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 42+0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 42+0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0042-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0042-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 42.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 42.0, 0, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 42, 0, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 42, '', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '42', 0, '01:01:01', ''],
		['', 0, 0, '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 42, '01:01:01', ''],
		['a', 0, 0, 'b', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:42', ''],
		['B', 0, 0, 'A', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', '42'],
	]
	grep_text(['42', d], want)
	def json_fixup(line):
		line = list(line)
		for key in ('complex32', 'complex64',):
			value = line[columns.index(key)]
			line[columns.index(key)] = [value.real, value.imag]
		return line
	want_json = [dict(zip(columns, json_fixup(line))) for line in want]
	grep_json(['42', d], want_json)
	grep_json(['-D', '42', d], [{'dataset': d, 'data': data} for data in want_json])
	grep_text(['-i', 'a', d], [
		['a', 0, 0, 'b', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 0, 0, '', 0, '01:01:42', ''],
		['B', 0, 0, 'A', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 0, 0, '', 0, '01:01:01', '42'],
	])
	grep_text(['-i', 'a', d, 'unicode', 'ascii'], [['', 'a']])
	grep_text(['-i', '-g', 'bytes', 'a', d, 'unicode', 'ascii'], [['42', 'B']])
	grep_json(['-g', 'bits32', '-g', 'ascii', '-D', '-L', '-S', '42', d], [
		{'dataset': d, 'sliceno': 0, 'lineno': 0, 'data': want_json[0]},
		{'dataset': d, 'sliceno': 1, 'lineno': 0, 'data': want_json[1]},
	])
	all_types = {n for n in _convfuncs if not n.startswith('parsed:')}
	if PY2:
		all_types.remove('pickle')
	assert used_types == all_types, 'Missing/extra column types: %r %r' % (all_types - used_types, used_types - all_types,)
