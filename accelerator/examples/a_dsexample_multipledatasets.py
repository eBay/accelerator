def prepare(job):
	dw1 = job.datasetwriter(name='first')
	dw2 = job.datasetwriter(name='second')
	dw3 = job.datasetwriter(name='third')
	dw1.add('col1', 'int64')
	dw2.add('col1', 'json')
	dw3.add('col1', 'number')
	dw3.add('col2', 'ascii')
	dw3.add('col3', 'bool')
	return dw1, dw2, dw3


def analysis(sliceno, prepare_res):
	dw1, dw2, dw3 = prepare_res
	dw1.write(sliceno)
	dw2.write({'sliceno': sliceno})
	dw3.write(sliceno, str(sliceno), sliceno % 2 == 0)
