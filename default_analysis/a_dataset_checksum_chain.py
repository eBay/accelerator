from __future__ import division

description = r'''
Take a chain of datasets and make a checksum of one or more columns.
See dataset_checksum.description for more information.

datasets.source is mandatory, datasets.stop is optional.
options.chain_length defaults to -1.

Sort does not sort across datasets.
'''

from chaining import jobchain
from subjobs import build
from extras import DotDict, pickle_load

options = dict(
	chain_length = -1,
	columns      = set(),
	sort         = True,
)

datasets = ('source', 'stop',)

def synthesis():
	sum = 0
	jobs = jobchain(length=options.chain_length, tip_jobid=datasets.source, stop_jobid=datasets.stop)
	for src in jobs:
		jid = build('dataset_checksum', options=dict(columns=options.columns, sort=options.sort), datasets=dict(source=src))
		data = pickle_load(jobid=jid)
		sum ^= data.sum
	print "Total: %016x" % (sum,)
	return DotDict(sum=sum, columns=data.columns, sort=options.sort, sources=jobs)
