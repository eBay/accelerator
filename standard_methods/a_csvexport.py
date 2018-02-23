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

from itertools import izip, imap
from shutil import copyfileobj
from os import unlink
from ujson import dumps

from extras import OptionString, job_params
from gzwrite import GzWrite
from status import status

options = dict(
	filename          = OptionString, # .csv or .gz
	separator         = ',',
	labelsonfirstline = True,
	chain_source      = False, # everything in source is replaced by datasetchain(self, stop=from previous)
	quote_fields      = '', # can be ' or "
	labels            = [], # empty means all labels in (first) dataset
	sliced            = False, # one output file per slice, put %02d or similar in filename
)

datasets = (['source'],) # normally just one, but you can specify several

jobids = ('previous',)

def csvexport(sliceno, filename, labelsonfirstline):
	assert len(options.separator) == 1
	assert options.quote_fields in ('', "'", '"',)
	d = datasets.source[0]
	if not options.labels:
		options.labels = sorted(d.columns)
	if options.chain_source:
		if jobids.previous:
			prev_source = job_params(jobids.previous).datasets.source
			assert len(datasets.source) == len(prev_source)
		else:
			prev_source = [None] * len(datasets.source)
		lst = []
		for src, stop in zip(datasets.source, prev_source):
			lst.extend(src.chain(stop_ds=stop))
		datasets.source = lst
	if filename.lower().endswith('.gz'):
		mkwrite = GzWrite
	elif filename.lower().endswith('.csv'):
		def mkwrite(filename):
			return open(filename, "wb")
	else:
		raise Exception("Filename should end with .gz for compressed or .csv for uncompressed")
	iters = []
	first = True
	for label in options.labels:
		it = d.iterate_list(sliceno, label, datasets.source, status_reporting=first)
		first = False
		t = d.columns[label].type
		if t == 'unicode':
			it = imap(lambda s: s.encode('utf-8'), it)
		elif t in ('float32', 'float64', 'number'):
			it = imap(repr, it)
		elif t == 'json':
			it = imap(dumps, it)
		elif t not in ('ascii', 'bytes'):
			it = imap(str, it)
		iters.append(it)
	it = izip(*iters)
	with mkwrite(filename) as fh:
		q = options.quote_fields
		sep = options.separator
		if q:
			qq = q + q
			if labelsonfirstline:
				fh.write((sep.join(q + n.replace(q, qq) + q for n in options.labels) + '\n').encode('utf-8'))
			for data in it:
				fh.write(sep.join(q + n.replace(q, qq) + q for n in data) + '\n')
		else:
			if labelsonfirstline:
				fh.write((sep.join(options.labels) + '\n').encode('utf-8'))
			for data in it:
				fh.write(sep.join(data) + '\n')

def analysis(sliceno):
	if options.sliced:
		csvexport(sliceno, options.filename % (sliceno,), options.labelsonfirstline)
	else:
		labelsonfirstline = (sliceno == 0 and options.labelsonfirstline)
		filename = '%d.gz' if options.filename.lower().endswith('.gz') else '%d.csv'
		csvexport(sliceno, filename % (sliceno,), labelsonfirstline)

def synthesis(params):
	if not options.sliced:
		filename = '%d.gz' if options.filename.lower().endswith('.gz') else '%d.csv'
		with open(options.filename, "wb") as outfh:
			for sliceno in range(params.slices):
				with status("Assembling %s (%d/%d)" % (options.filename, sliceno, params.slices)):
					with open(filename % sliceno, "rb") as infh:
						copyfileobj(infh, outfh)
					unlink(filename % sliceno)
