from __future__ import division

description = r'''
Rewrite a dataset (or chain to previous) with new hashlabel.
'''

from extras import OptionString, job_params
from dataset import DatasetWriter
from chaining import jobchain

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
	if len(jobchain(tip_jobid=datasets.source, stop_jobid=prev_source, length=options.length)) == 1:
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
	return dw, d.iterate_chain(None, names, stop_jobid=prev_source, length=options.length)

def analysis(sliceno, prepare_res):
	dw, it = prepare_res
	write = dw.write_list
	for values in it:
		write(values)
