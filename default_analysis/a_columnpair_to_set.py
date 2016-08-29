from __future__ import division

from collections import defaultdict, Counter

from chaining import jobchain_prev, iterate_datasetchain
from extras import pickle_load, pickle_save, job_params, JobWithFile, OptionString

options = {
	'key_filter'  : JobWithFile, # a set of keys to keep (or nothing)
	'value_filter': JobWithFile, # a set of values to keep (or nothing)
	'key_column'  : OptionString,
	'value_column': OptionString,
}
datasets = ('src',)
jobids = ('previous',)


def prepare(jobids):
	key_filter   = pickle_load(options.key_filter, default=set()),
	value_filter = pickle_load(options.value_filter, default=set())
	return key_filter, value_filter

def analysis(sliceno, prepare_res):
	key_filter, value_filter = prepare_res
	prev = jobchain_prev()
	d = pickle_load(jobid=prev, sliceno=sliceno, default=defaultdict(set))
	if options.key_filter:
		d = {k: v for k, v in d.iteritems() if k in key_filter}
	iterator = iterate_datasetchain(
		sliceno,
		(options.key_column, options.value_column,),
		tip_jobid=datasets.src,
		stop_jobid=job_params(jobids.previous, default_empty=True).datasets.src,
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
					d[k].add(reuse.setdefault(k, k))
		else:
			for k, v in iterator:
				d[k].add(reuse.setdefault(k, k))
	pickle_save(d, sliceno=sliceno)
	pickle_save(set(d), 'keyset', sliceno=sliceno)
	pickle_save(Counter(len(v) for v in d.itervalues()), 'setsizehist', sliceno=sliceno)

def synthesis(params):
	setsizehist = Counter()
	for sliceno in range(params.slices):
		setsizehist.update(pickle_load('setsizehist', sliceno=sliceno))
	pickle_save(setsizehist, 'setsizehist')
