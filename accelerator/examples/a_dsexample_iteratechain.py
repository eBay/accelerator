datasets = ('source',)

description = "Iterate and aggregate data from a chained dataset."


def analysis(sliceno):
	# this is equivalent to just
	# return list(datasets.source.iterate_chain(sliceno, ['Int', 'String']))
	# but written out so you can see what it does.
	v = []
	for n, s in datasets.source.iterate_chain(sliceno, ['Int', 'String']):
		v.append((n, s))
	return v


def synthesis(analysis_res):
	v = analysis_res.merge_auto()
	for item in v:
		print(item)
	return v
