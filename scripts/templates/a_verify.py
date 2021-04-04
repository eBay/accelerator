from math import isnan
from datetime import datetime
import sys

import accelerator
from accelerator.test_methods import test_data

options = dict(n=int, now=datetime)
jobs = ('source',)

nanmarker = object()
def nanfix(values):
	def fix(v):
		if isinstance(v, float) and isnan(v):
			return nanmarker
		else:
			return v
	return list(map(fix, values))

def prepare():
	data = jobs.source.load()
	assert data['now'] == options.now
	if data['py_version'] == 2:
		assert data['blaa'] == u'bl\xe5'.encode('utf-8')
	else:
		assert data['blaa'] == u'bl\xe5'

def analysis(sliceno):
	good = test_data.sort_data_for_slice(sliceno)
	for lineno, got in enumerate(jobs.source.dataset().iterate(sliceno)):
		want = next(good)
		assert nanfix(want) == nanfix(got), "Wanted:\n%r\nbut got:\n%r\non line %d in slice %d of %s" % (want, got, lineno, sliceno, jobs.source)
	left_over = len(list(good))
	assert left_over == 0, "Slice %d of %s missing %d lines" % (sliceno, jobs.source, left_over,)
	if jobs.source.load()['py_version'] > 2 and sys.version_info[0] > 2:
		assert list(jobs.source.dataset('pickle').iterate(sliceno, 'p')) == [{'sliceno': sliceno}]

def synthesis(job):
	p = jobs.source.params
	assert p.versions.accelerator == accelerator.__version__
	with job.open_input('proj/accelerator.conf') as fh:
		for line in fh:
			if line.startswith('interpreters: p%d ' % (options.n,)):
				path = line.split(' ', 2)[2].strip()[1:-1]
				break
		else:
			raise Exception('Failed to find interpreter #%d in accelerator.conf' % (options.n,))
	assert p.versions.python_path == path
