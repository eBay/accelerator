from collections import Counter

description = "Dataset: Iterate and aggregate a column"

datasets = ('source',)


def analysis(sliceno):
	v = Counter()
	for s, n in datasets.source.iterate(sliceno, ['String', 'Int']):
		v[s] += n
	return v


def synthesis(analysis_res):
	v = analysis_res.merge_auto()  # Don't merge or sort huge datasets, though!
	for item in sorted(v.items()):
		print('print some stuff', item)
	return v
