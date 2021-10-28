from os.path import dirname, join
from .printer import prt

description = "Tutorial: Import, type, sort, and hash partition a file."


# File is stored in same directory as this python file.
# In a real project, set "input directory" in config file instead!
filename = join(dirname(__file__), 'data.csv')


def main(urd):
	prt.source(__file__)
	prt()
	prt('Import a CSV-file.  Type, sort, and hash-partition the imported dataset.')
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

	prt()
	prt('Note how the output from a build()-call is used as input')
	prt('to the next in order to pass data and/or parameters and create')
	prt('dependencies.')

	prt()
	with prt.header('VISUALISING A JOB\'S DEPENDENCIES'):
		prt('View the job\'s metadata using')
		prt.command('ax job %s' % (imp,))
		prt('We can see that the dataset "%s" is input to this job.' % (imp.params.datasets.source,))

	prt()
	with prt.header('REFERENCES TO ALL JOBS ARE STORED IN THE "urd.joblist" OBJECT:'):
		prt.output(urd.joblist.pretty)
	prt('All jobs are stored in "urd.joblist" so that they can be easily')
	prt('fetched at a later time.  This will be used in the next step.')

	prt()
	with prt.header('HASH PARTITIONED DATASET'):
		prt('''
			Hash partitioning is a very efficient way to prepare
			a dataset for parallel processing (see manual).

			Take a look at the dataset created by dataset_hashpart:
		''')
		prt.command('ax ds %s' % (imp,))
		prt('''
			The asterisk on the row corresponding to the "String" column
			indicates that the dataset is hash partitioned based the values
			in this column.  It is possible to use the "-s" option to
			"ax cat" to print data from individual slices.)

			We can also see how many rows there are in each slice by typing
		''')
		prt.command('ax ds -s %s' % (imp,))
