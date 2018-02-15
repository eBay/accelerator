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

description = r'''
Like dataset_type, but can guess the type of columns (number or ascii)
It does not look at previous, only the current dataset is considered.
'''

from dataset import Dataset
from subjobs import build

options = {
	'column2type'               : {}, # {'COLNAME': 'type'}, for columns where you don't want autodetection
	'defaults'                  : {}, # {'COLNAME': value}, unspecified -> method fails on unconvertible unless filter_bad
	'rename'                    : {}, # {'OLDNAME': 'NEWNAME'} doesn't shadow OLDNAME.
	'exclude'                   : set(), # columns you don't want to touch. Still visible unless discard_untyped.
	'include'                   : set(), # columns you do want to guess the type of.
	                                     # at most one of exclude and include can be specified.
	'caption'                   : 'autotyped dataset',
	'discard_untyped'           : bool,  # Make unconverted columns inaccessible ("new" dataset)
	'filter_bad'                : False, # Implies discard_untyped, only applies to manually typed columns.
	'numeric_comma'             : False, # floats as "3,14"
}

datasets = ('source', 'previous',)

def prepare():
	assert (not options.exclude) or (not options.include), "Specify at most one of exclude and include"
	cols = set(datasets.source.columns)
	def chk(name):
		unknown = set(options.get(name, ())) - cols
		assert not unknown, "Unknown columns in name: %s" % (name, ", ".join(unknown),)
	chk("include")
	chk("column2type")
	chk("defaults")
	return cols - options.exclude - set(options.column2type)

def analysis(sliceno, prepare_res):
	badnesses = {}
	d = datasets.source
	for colname in prepare_res:
		badness = 0
		for v in d.iterate(sliceno, colname):
			if badness < 1:
				try:
					int(v, 10)
				except ValueError:
					badness = 1
			try:
				if options.numeric_comma:
					v = v.replace(".", "nope").replace(",", ".")
				float(v)
			except ValueError:
				badness = 2
				break
		badnesses[colname] = badness
	return badnesses

def synthesis(analysis_res, params):
	badnesses = next(analysis_res)
	for tmp in analysis_res:
		badnesses = {k: max(badnesses[k], tmp[k]) for k in tmp}
	badness2type = {
		0: "number", # this used to be int64_10
		1: "number", # and this used to be float64
		2: "ascii:encode",
	}
	types = {k: badness2type[v] for k, v in badnesses.iteritems()}
	types.update(options.column2type)
	sub_opts = dict(
		column2type     = types,
		defaults        = options.defaults,
		rename          = options.rename,
		caption         = options.caption,
		discard_untyped = options.discard_untyped,
		filter_bad      = options.filter_bad,
		numeric_comma   = options.numeric_comma,
	)
	jid = build("dataset_type", options=sub_opts, datasets=datasets)
	Dataset(jid).link_to_here()
