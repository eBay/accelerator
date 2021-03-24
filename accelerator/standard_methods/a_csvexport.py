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
	if isinstance(options.none_as, str):
		default_none = options.none_as
		none_dict = {}
		none_set = True
	else:
		default_none = 'None'
		none_dict = options.none_as or {}
		none_set = bool(none_dict)
		bad_none = set(none_dict) - set(options.labels)
		assert not bad_none, 'Unknown labels in none_as: %r' % (bad_none,)
	def column_iterator(d, label, first):
		col = d.columns[label]
		f = format.get(col.type, str)
		it = d.iterate(sliceno, label, status_reporting=first)
		if col.none_support and (none_set or col.type != 'json'):
			none_as = none_dict.get(label, default_none)
			if f:
				it = (none_as if v is None else f(v) for v in it)
			else:
				it = (none_as if v is None else v for v in it)
		elif f:
			it = imap(f, it)
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
		q = options.quote_fields
		sep = options.separator
		if q:
			qq = q + q
			if labelsonfirstline:
				write(enc(sep.join(q + n.replace(q, qq) + q for n in options.labels)))
			for data in it:
				write(sep.join(q + n.replace(q, qq) + q for n in data))
		else:
			if labelsonfirstline:
				write(enc(sep.join(options.labels)))
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
