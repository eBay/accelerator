from datetime import datetime
import sys

from accelerator.test_methods import test_data

options = dict (unicode_string=u'', time=datetime)

def prepare(job):
	if sys.version_info[0] > 2:
		dw_pickle = job.datasetwriter(name='pickle', columns={'p': 'pickle'})
	else:
		dw_pickle = None
	job.save({
		u'py_version': sys.version_info[0],
		u'blaa': options.unicode_string,
		u'now': options.time,
	})
	return dw_pickle, job.datasetwriter(columns=test_data.columns)

def analysis(sliceno, prepare_res):
	dw_pickle, dw = prepare_res
	for values in test_data.sort_data_for_slice(sliceno):
		dw.write_list(values)
	if dw_pickle:
		dw_pickle.write({'sliceno': sliceno})
