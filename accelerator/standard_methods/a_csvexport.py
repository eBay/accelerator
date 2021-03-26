############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2021 Carl Drougge                       #
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

description = r'''Dataset (or chain) to CSV file.'''

from shutil import copyfileobj
from os import unlink
from os.path import exists
from contextlib import contextmanager
from json import JSONEncoder
from functools import partial
from itertools import chain
import gzip

from accelerator.compat import PY3, PY2, izip, imap, long
from accelerator import status


options = dict(
	filename          = 'result.csv', # .gz or with .gz. in it for compressed mode
	separator         = ',',
	line_separator    = '\n',
	labelsonfirstline = True,
	chain_source      = False, # everything in source is replaced by datasetchain(self, stop=from previous)
	quote_fields      = '', # can be any string, but use '"' or "'"
	lazy_quotes       = True, # only quote field if value needs quoting
	none_as           = None, # A string or {label: string} to use for None-values. Default 'None' ('null' for json).
	labels            = [], # empty means all labels in (first) dataset
	sliced            = False, # one output file per slice, you can put %02d or similar in filename (or get filename.%d)
	compression       = 6,     # gzip level
)

datasets = (['source'],) # normally just one, but you can specify several

jobs = ('previous',)

@contextmanager
def writer(fh):
	with fh:
		write = fh.write
		line_sep = options.line_separator
		def wrapped_write(s):
			write(s)
			write(line_sep)
		yield wrapped_write

format = dict(
	ascii=None,
	float32=repr,
	float64=repr,
	json=JSONEncoder(sort_keys=True, ensure_ascii=True, check_circular=False).encode,
)

if PY3:
	enc = str
	format['bytes'] = lambda s: s.decode('utf-8', errors='backslashreplace')
	format['number'] = repr
	format['unicode'] = None
else:
	enc = lambda s: s.encode('utf-8')
	format['bytes'] = None
	format['number'] = lambda n: str(n) if isinstance(n, long) else repr(n)
	format['unicode'] = lambda s: s.encode('utf-8')

def csvexport(sliceno, filename, labelsonfirstline):
	d = datasets.source[0]
	if not options.labels:
		options.labels = sorted(d.columns)
	if options.chain_source:
		if jobs.previous:
			prev_source = jobs.previous.params.datasets.source
			assert len(datasets.source) == len(prev_source)
		else:
			prev_source = [None] * len(datasets.source)
		lst = []
		for src, stop in zip(datasets.source, prev_source):
			lst.extend(src.chain(stop_ds=stop))
		datasets.source = lst
	if options.filename.lower().endswith('.gz') or '.gz.' in options.filename.lower():
		open_func = partial(gzip.open, compresslevel=options.compression)
	else:
		open_func = open
	if PY2:
		open_func = partial(open_func, mode='wb')
	else:
		open_func = partial(open_func, mode='xt', encoding='utf-8')
	if options.none_as:
		if isinstance(options.none_as, dict):
			bad_none = set(options.none_as) - set(options.labels)
			assert not bad_none, 'Unknown labels in none_as: %r' % (bad_none,)
		else:
			assert isinstance(options.none_as, str), "What did you pass as none_as?"
	def resolve_none(label, col):
		d = options.none_as or {}
		if col.type in ('json', 'pickle',):
			if isinstance(options.none_as, str):
				return options.none_as
			return d.get(label)
		elif col.none_support:
			if isinstance(options.none_as, str):
				return options.none_as
			return d.get(label, 'None')
	q = options.quote_fields
	qq = q + q
	sep = options.separator
	def quote_always(v):
		return q + v.replace(q, qq) + q
	if q in '"\'':
		# special case so both quotes will quote the other
		def quote_if_needed(v):
			if v and (v[0] in '"\'' or v[-1] in '"\'' or sep in v):
				return q + v.replace(q, qq) + q
			else:
				return v
	else:
		def quote_if_needed(v):
			if v.startswith(q) or v.endswith(q) or sep in v:
				return q + v.replace(q, qq) + q
			else:
				return v
	if not q:
		quote_func = str
	elif options.lazy_quotes and sep: # always quote if no separator
		quote_func = quote_if_needed
	else:
		quote_func = quote_always
	def needs_quoting(typ):
		if not q:
			return False
		if not options.lazy_quotes:
			return True
		# maybe we can skip quoting because values that need quoting are impossible?
		if typ in ('int32', 'int64', 'bits32', 'bits64',):
			possible = '0123456789-'
		elif typ in ('float32', 'float64', 'number',):
			possible = '0123456789-+einfa.'
		else:
			possible = False
		if possible:
			q_s = set(q)
			sep_s = set(sep)
			possible_s = set(possible)
			if q_s - possible_s and sep_s - possible_s:
				return False
		return True
	def column_iterator(d, label, first):
		col = d.columns[label]
		f = format.get(col.type, str)
		it = d.iterate(sliceno, label, status_reporting=first)
		none_as = resolve_none(label, col)
		if none_as is not None:
			none_as = quote_func(none_as)
			if needs_quoting(col.type):
				if f:
					it = (none_as if v is None else quote_func(f(v)) for v in it)
				else:
					it = (none_as if v is None else quote_func(v) for v in it)
			else:
				if f:
					it = (none_as if v is None else f(v) for v in it)
				else:
					it = (none_as if v is None else v for v in it)
		elif f:
			if needs_quoting(col.type):
				it = (quote_func(f(v)) for v in it)
			else:
				it = imap(f, it)
		elif needs_quoting(col.type):
			it = imap(quote_func, it)
		return it
	def outer_iterator(label, first):
		return chain.from_iterable(column_iterator(d, label, first) for d in datasets.source)
	iters = []
	first = True
	for label in options.labels:
		iters.append(outer_iterator(label, first))
		first = False
	it = izip(*iters)
	with writer(open_func(filename)) as write:
		if labelsonfirstline:
			write(enc(sep.join(map(quote_func, options.labels))))
		for data in it:
			write(sep.join(data))

def analysis(sliceno, job):
	if options.sliced:
		if '%' in options.filename:
			filename = options.filename % (sliceno,)
		else:
			filename = '%s.%d' % (options.filename, sliceno,)
		csvexport(sliceno, filename, options.labelsonfirstline)
		job.register_file(filename)
	else:
		labelsonfirstline = (sliceno == 0 and options.labelsonfirstline)
		csvexport(sliceno, str(sliceno), labelsonfirstline)

def synthesis(job, slices):
	if not options.sliced:
		def msg(sliceno):
			return "Assembling %s (%d/%d)" % (options.filename, sliceno + 1, slices,)
		with status(msg(0)) as update:
			with job.open(options.filename, "wb") as outfh:
				for sliceno in range(slices):
					filename = str(sliceno)
					if exists(filename):
						update(msg(sliceno))
						with open(filename, "rb") as infh:
							copyfileobj(infh, outfh)
						unlink(filename)
