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

from collections import defaultdict, Counter

from extras import JobWithFile, OptionString
import blob

options = {
	'key_filter'  : JobWithFile, # a set of keys to keep (or nothing)
	'value_filter': JobWithFile, # a set of values to keep (or nothing)
	'key_column'  : OptionString,
	'value_column': OptionString,
}
datasets = ('source',)
jobids = ('previous',)


def prepare(jobids):
	key_filter   = blob.load(options.key_filter, default=set()),
	value_filter = blob.load(options.value_filter, default=set())
	return key_filter, value_filter

def analysis(sliceno, prepare_res):
	key_filter, value_filter = prepare_res
	d = blob.load(jobid=jobids.previous, sliceno=sliceno, default=defaultdict(set))
	if options.key_filter:
		d = {k: v for k, v in d.iteritems() if k in key_filter}
	iterator = datasets.source.iterate_chain(
		sliceno,
		(options.key_column, options.value_column,),
		stop_ds={jobids.previous: 'source'},
	)
	# These break out into four versions for shorter runtime
	if options.value_filter:
		# Remove anything that's not in the filter
		for k, v in d.items():
			v = v & value_filter
			if v:
				d[k] = v
			else:
				del d[k]
		# This lets us reuse the same str object for the same value (smaller pickles)
		value_filter = {v: v for v in value_filter}
		if options.key_filter:
			for k, v in iterator:
				if k in key_filter and v in value_filter:
					d[k].add(value_filter[v])
		else:
			for k, v in iterator:
				if v in value_filter:
					d[k].add(value_filter[v])
	else:
		reuse = {}
		if options.key_filter:
			for k, v in iterator:
				if k in key_filter:
					d[k].add(reuse.setdefault(v, v))
		else:
			for k, v in iterator:
				d[k].add(reuse.setdefault(v, v))
	blob.save(d, sliceno=sliceno, temp=False)
	blob.save(set(d), 'keyset', sliceno=sliceno, temp=False)
	blob.save(Counter(len(v) for v in d.itervalues()), 'setsizehist', sliceno=sliceno, temp=False)

def synthesis(params):
	setsizehist = Counter()
	for sliceno in range(params.slices):
		setsizehist.update(blob.load('setsizehist', sliceno=sliceno))
	blob.save(setsizehist, 'setsizehist')
