from __future__ import division

description = r'''
Rewrite a dataset (or chain to previous) with new hashlabel.
'''

from extras import OptionString, job_params
from dataset import DatasetWriter

options = {
	'hashlabel'                 : OptionString,
	'caption'                   : '"%(caption)s" hashed on %(hashlabel)s',
	'length'                    : -1, # Go back at most this many datasets. You almost always want -1 (which goes until previous.source)
}

datasets = ('source', 'previous',)

def prepare():
	d = datasets.source
	caption = options.caption % dict(caption=d.caption, hashlabel=options.hashlabel)
	prev_p = job_params(datasets.previous, default_empty=True)
	prev_source = prev_p.datasets.source
	if len(d.chain(stop_jobid=prev_source, length=options.length)) == 1:
		filename = d.filename
	else:
		filename = None
	dw = DatasetWriter(
		caption=caption,
		hashlabel=options.hashlabel,
		filename=filename,
		previous=datasets.previous,
	)
	names = []
	for n, c in d.columns.items():
		# names has to be in the same order as the add calls
		# so the iterator returns the same order the writer expects.
		names.append(n)
		dw.add(n, c.type)
	return dw, names, prev_source

def analysis(sliceno, prepare_res):
	dw, names, prev_source = prepare_res
	it = datasets.source.iterate_chain(
		sliceno,
		names,
		stop_jobid=prev_source,
		length=options.length,
		hashlabel=options.hashlabel,
	)
	write = dw.write_list
	for values in it:
		write(values)
