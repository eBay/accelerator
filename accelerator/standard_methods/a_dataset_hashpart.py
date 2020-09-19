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

description = r'''
Rewrite a dataset (or chain to previous) with new hashlabel.
'''

from shutil import copyfileobj
from os.path import exists

from accelerator import OptionString

options = {
	'hashlabel'                 : OptionString,
	'caption'                   : '"%(caption)s" hashed on %(hashlabel)s',
	'length'                    : -1, # Go back at most this many datasets. You almost always want -1 (which goes until previous.source)
	'as_chain'                  : False, # one dataset per slice (avoids rewriting at the end)
}

datasets = ('source', 'previous',)

def prepare(job, slices):
	d = datasets.source
	caption = options.caption % dict(caption=d.caption, hashlabel=options.hashlabel)
	chain = d.chain(stop_ds={datasets.previous: 'source'}, length=options.length)
	if len(chain) == 1:
		filename = d.filename
	else:
		filename = None
	dws = []
	previous = datasets.previous
	if options.as_chain:
		# The last slice that actually has data in it becomes 'default'.
		# Or slice 0 if the whole source is empty (we must produce a ds).
		default_sliceno = 0
		for sliceno in range(slices - 1, -1, -1):
			if chain.lines(sliceno):
				default_sliceno = sliceno
				break
	else:
		default_sliceno = None
	for sliceno in range(slices):
		if sliceno == default_sliceno or chain.lines(sliceno):
			if sliceno == default_sliceno:
				name = "default"
			else:
				name = str(sliceno)
			dw = job.datasetwriter(
				caption="%s (slice %d)" % (caption, sliceno),
				hashlabel=options.hashlabel,
				filename=filename,
				previous=previous,
				name=name,
				for_single_slice=sliceno,
			)
			previous = dw
		else:
			dw = None
		dws.append(dw)
	names = []
	cols = {}
	for n, c in d.columns.items():
		# names has to be in the same order as the add calls
		# so the iterator returns the same order the writer expects.
		names.append(n)
		cols[n] = (c.type, chain.none_support(n))
		for dw in dws:
			if dw:
				dw.add(n, c.type, none_support=cols[n][1])
	return dws, names, caption, filename, cols

def analysis(sliceno, prepare_res):
	dws, names = prepare_res[:2]
	if not dws[sliceno]:
		return
	it = datasets.source.iterate_chain(
		sliceno,
		names,
		stop_ds={datasets.previous: 'source'},
		length=options.length,
	)
	write = dws[sliceno].get_split_write_list()
	for values in it:
		write(values)

def synthesis(prepare_res, job, slices):
	if not options.as_chain:
		# If we don't want a chain we abuse our knowledge of dataset internals
		# to avoid recompressing. Don't do this stuff yourself.
		dws, names, caption, filename, cols = prepare_res
		merged_dw = job.datasetwriter(
			caption=caption,
			hashlabel=options.hashlabel,
			filename=filename,
			previous=datasets.previous,
			meta_only=True,
			columns=cols,
		)
		dws = list(filter(None, dws))
		for sliceno in range(slices):
			merged_dw.set_lines(sliceno, sum(dw._lens[sliceno] for dw in dws))
			for dwno, dw in enumerate(dws):
				merged_dw.set_minmax((sliceno, dwno), dw._minmax[sliceno])
			for n in names:
				fn = merged_dw.column_filename(n, sliceno=sliceno)
				with open(fn, "wb") as out_fh:
					for dw in dws:
						fn = dw.column_filename(n, sliceno=sliceno)
						if exists(fn):
							with open(fn, "rb") as in_fh:
								copyfileobj(in_fh, out_fh)
		for dw in dws:
			dw.discard()
