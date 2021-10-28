description = "Dataset: Append a column to an existing dataset."

datasets = ('source',)


def prepare(job):
	dw = job.datasetwriter(parent=datasets.source)
	dw.add('Floatcolumn', 'float64')
	return dw


def analysis(prepare_res, sliceno):
	dw = prepare_res
	for n in datasets.source.iterate(sliceno, 'Numbercolumn'):
		dw.write(n * 3.14)
