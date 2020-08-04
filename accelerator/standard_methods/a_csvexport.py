############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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
from contextlib import contextmanager
from json import JSONEncoder

from accelerator.compat import PY3, PY2, izip, imap
from accelerator.gzutil import GzWriteUnicodeLines, GzWriteBytesLines
from accelerator import OptionString, status


options = dict(
	filename          = OptionString, # .csv or .gz
	separator         = ',', # Single iso-8859-1 character
	labelsonfirstline = True,
	chain_source      = False, # everything in source is replaced by datasetchain(self, stop=from previous)
	quote_fields      = '', # can be ' or "
	labels            = [], # empty means all labels in (first) dataset
	sliced            = False, # one output file per slice, put %02d or similar in filename
)

datasets = (['source'],) # normally just one, but you can specify several

jobs = ('previous',)

@contextmanager
def mkwrite_gz(filename):
	w = GzWriteUnicodeLines if PY3 else GzWriteBytesLines
	with w(filename) as fh:
		yield fh.write

@contextmanager
def mkwrite_uncompressed(filename):
	if PY2:
		fh = open(filename, 'wb')
	else:
		fh = open(filename, 'w', encoding='utf-8')
	with fh:
		def write(s):
			fh.write(s + '\n')
		yield write

if PY3:
	enc = str
else:
	enc = lambda s: s.encode('utf-8')

def nonefix_u(s):
	return u'None' if s is None else s
def nonefix_b(s):
	return b'None' if s is None else s

def csvexport(sliceno, filename, labelsonfirstline):
	assert len(options.separator) == 1
	assert options.quote_fields in ('', "'", '"',)
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
	if filename.lower().endswith('.gz'):
		mkwrite = mkwrite_gz
	elif filename.lower().endswith('.csv'):
		mkwrite = mkwrite_uncompressed
	else:
		raise Exception("Filename should end with .gz for compressed or .csv for uncompressed")
	iters = []
	first = True
	dumps = JSONEncoder(
		sort_keys=True,
		ensure_ascii=True,
		check_circular=False,
	).encode
	for label in options.labels:
		it = d.iterate_list(sliceno, label, datasets.source, status_reporting=first)
		first = False
		t = d.columns[label].type
		if d.columns[label].none_support:
			if t == 'bytes' or (PY2 and t == 'ascii'):
				it = imap(nonefix_b, it)
			elif t in ('ascii', 'unicode',):
				it = imap(nonefix_u, it)
		if t == 'unicode' and PY2:
			it = imap(enc, it)
		elif t == 'bytes' and PY3:
			it = imap(lambda s: s.decode('utf-8', errors='backslashreplace'), it)
		elif t in ('float32', 'float64',):
			it = imap(repr, it)
		elif t == 'number':
			if PY2:
				it = imap(lambda n: str(n) if isinstance(n, long) else repr(n), it)
			else:
				it = imap(repr, it)
		elif t == 'json':
			it = imap(dumps, it)
		elif t not in ('unicode', 'ascii', 'bytes'):
			it = imap(str, it)
		iters.append(it)
	it = izip(*iters)
	with mkwrite(filename) as write:
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
		csvexport(sliceno, options.filename % (sliceno,), options.labelsonfirstline)
		job.register_file(options.filename % (sliceno,))
	else:
		labelsonfirstline = (sliceno == 0 and options.labelsonfirstline)
		filename = '%d.gz' if options.filename.lower().endswith('.gz') else '%d.csv'
		csvexport(sliceno, filename % (sliceno,), labelsonfirstline)

def synthesis(job, slices):
	if not options.sliced:
		filename = '%d.gz' if options.filename.lower().endswith('.gz') else '%d.csv'
		with job.open(options.filename, "wb") as outfh:
			for sliceno in range(slices):
				with status("Assembling %s (%d/%d)" % (options.filename, sliceno, slices)):
					with open(filename % sliceno, "rb") as infh:
						copyfileobj(infh, outfh)
					unlink(filename % sliceno)
