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

description = r'''
Verify that lazy_quotes quotes the expected values in csvexport with
various quotes and separators.
'''

from accelerator import subjobs
from accelerator.compat import PY3

def test(job, prefix, none_support):
	expect = [[], [], []]
	def write(sliceno, a, b, c):
		w(a, b, c)
		if isinstance(c, bytes):
			if PY3:
				c = c.decode('utf-8', 'backslashreplace')
		else:
			c = repr(c)
		expect[sliceno].append((str(a), repr(b), c,))
	dw = job.datasetwriter(name=prefix + 'a')
	dw.add('a', 'ascii', none_support=none_support)
	dw.add('b', 'int32', none_support=none_support)
	dw.add('c', 'float64', none_support=none_support)
	w = dw.get_split_write()
	write(0, 'hello', 42, float('-inf'))
	write(1, "'world'", -1, 5e-33)
	if none_support:
		write(2, None, None, None)
	a = dw.finish()
	dw = job.datasetwriter(name=prefix + 'b', previous=a)
	dw.add('a', 'bool', none_support=none_support)
	dw.add('b', 'number', none_support=none_support)
	dw.add('c', 'bytes', none_support=none_support)
	w = dw.get_split_write()
	write(0, False, float('nan'), b"'ascii'")
	write(1, True, 1.2e+22, b'\xff')
	if none_support:
		write(2, None, None, None)
	b = dw.finish()
	expect = [('a', 'b', 'c',)] + expect[0] + expect[1] + expect[2] + [()]
	for lazy_quotes in (False, True,):
		for q in ('', '42', '-', 'f', 'hello',):
			for sep in ('+', 'a', '',):
				verify(b, lazy_quotes, q, sep, expect)
	if none_support:
		expect = [none2nah(line) for line in expect]
		verify(b, True, '"', 'ah', expect, none_as='nah')
		verify(b, True, "'", '"', expect, none_as='nah')
		verify(b, True, 'a', ',', expect, none_as='nah')

def verify(source, lazy_quotes, q, sep, expect, **kw):
	j = subjobs.build(
		'csvexport',
		chain_source=True,
		source=source,
		lazy_quotes=lazy_quotes,
		quote_fields=q,
		separator=sep,
		**kw
	)
	with j.open('result.csv', 'r' if PY3 else 'rb') as fh:
		got = fh.read()
	if lazy_quotes and sep:
		quote_func = make_lazy(sep, q)
	else:
		quote_func = lambda v: q + v.replace(q, q + q) + q
	want = '\n'.join(sep.join(map(quote_func, line)) for line in expect)
	if want != got:
		print('Unhappy with %s:' % (j.filename('result.csv'),))
		print()
		print('Expected:')
		print(want)
		print('Got:')
		print(got)
		raise Exception('csvexport failed with quote_fields=%r, separator=%r, lazy_quotes=%r' % (q, sep, lazy_quotes,))

def make_lazy(sep, q):
	if q == '"':
		def quote(v):
			if v.startswith(q) or v.endswith(q) or sep in v or v.startswith("'") or v.endswith("'"):
				return q + v.replace(q, q + q) + q
			else:
				return v
	else:
		def quote(v):
			if v.startswith(q) or v.endswith(q) or sep in v:
				return q + v.replace(q, q + q) + q
			else:
				return v
	return quote

def none2nah(line):
	return ['nah' if v == 'None' else v for v in line]

def synthesis(job):
	test(job, '', False)
	test(job, 'None ', True)
