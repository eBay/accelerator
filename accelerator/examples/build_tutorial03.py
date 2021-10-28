from os.path import dirname, join
from .printer import prt

description = "Tutorial: Import several files into a dataset chain"

filenames = ('data.csv', 'data2.csv', 'data3.csv')
# Again, files are stored in same directory as this python file.
# In a real project, set "input directory" in config file instead!
filenames = (join(dirname(__file__), x) for x in filenames)


def main(urd):
	prt.source(__file__)
	prt()
	prt('A loop importing three files, creating a chained dataset of all data')
	prt()

	for filename in filenames:
		prt.plain(filename)
		imp = urd.build(
			'csvimport',
			filename=filename,
			previous=urd.joblist.get('csvimport'),
		)
		imp = urd.build(
			'dataset_type',
			source=imp,
			previous=urd.joblist.get('dataset_type'),
			column2type=dict(
				Date='date:%Y-%m-%d',
				String='unicode:utf-8',
				Int='number',
				Float='float64',
			),
		)
		imp = urd.build(
			'dataset_sort',
			source=imp,
			sort_columns='Date',
			previous=urd.joblist.get('dataset_sort'),
		)
		imp = urd.build(
			'dataset_hashpart',
			source=imp,
			hashlabel='String',
			previous=urd.joblist.get('dataset_hashpart'),
		)

	prt()
	prt('''
		Each job has a dataset input parameter called previous.  We
		assign this input parameter to (a reference) of the job that
		processed the previous file in the list.  This creates a
		dataset chain.

		The point of chaining is that it allows datasets to grow in
		number of rows, with linear time complexity, without changing
		any existing processing state.  This means that we can add
		new rows to a dataset, but we always have access to all previous
		imports of the data.  Nothing is modified, data is just appended.
	''')

	prt()
	with prt.header('INSPECTING DATASET CHAINS'):
		prt('  The flags -s, -S, and -c to "ax ds" are useful when looking')
		prt('  at chained datasets')
		prt.command('ax ds -s %s' % (imp,))
		prt.command('ax ds -S %s' % (imp,))
		prt.command('ax ds -c %s' % (imp,))
		prt('As always, see info and more options using')
		prt.command('ax ds --help')

	prt()
	with prt.header('PRINTING A DATASET CHAIN'):
		prt('This is a small example, so we can print all data using')
		prt.command('ax cat -c %s' % (imp,))

	prt()
	with prt.header('GREPPING'):
		prt('''
			Here's an example of grepping the string "bb" in the
			"String" column, but show "Date", "Float", and "Int"
			columns only.
		''')
		prt.command('ax grep bb -c -g String %s Date Float Int' % (imp,))
