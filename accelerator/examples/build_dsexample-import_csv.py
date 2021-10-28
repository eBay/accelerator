from os.path import dirname, join
from .printer import prt

description = "Dataset: Import, type, sort, and hash partition a tabular file."


# file is stored in same directory as this python file
filename = join(dirname(__file__), 'data.csv')


def main(urd):
	prt.source(__file__)
	prt()
	prt('Import, type, sort, and hash partition the file "data.csv":')
	prt()

	imp = urd.build('csvimport', filename=filename)
	imp = urd.build(
		'dataset_type',
		source=imp,
		column2type=dict(
			Date='date:%Y-%m-%d',
			String='unicode:utf-8',
			Int='number',
			Float='float64',
		),
	)
	imp = urd.build('dataset_sort', source=imp, sort_columns='Date')
	imp = urd.build('dataset_hashpart', source=imp, hashlabel='String')

	job = urd.build('dsexample_aggandmergedata', source=imp)

	prt()
	prt('Now you can do')
	prt.command('ax ds', imp)
	prt('to show info on the default dataset in the dataset_hashpart job.  Note that')
	prt('the latest built dataset_hashpart dataset can also be specified like this')
	prt.command('ax ds dataset_hashpart')

	prt()
	prt('To see the output (to stdout) from the "dsexample_aggandmergedata" job, do')
	prt.command('ax job -O', job)
	prt('And, to see information about the job itself, do')
	prt.command('ax job', job)

	prt()
	prt('To see the contents of the dataset, type')
	prt.command('ax cat -H', imp)
	prt('or, for example')
	prt.command('ax cat -f json', imp)

	prt()
	prt('Please check options using "ax ds --help" etc.')
