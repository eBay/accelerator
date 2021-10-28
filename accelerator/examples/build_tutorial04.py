from os.path import dirname, join
from .printer import prt

description = "Tutorial: Iterate over a chain of datasets."


filenames = ('data.csv', 'data2.csv', 'data3.csv')
# Again, files are stored in same directory as this python file.
# In a real project, set "input directory" in config file instead!
filenames = (join(dirname(__file__), x) for x in filenames)


def main(urd):
	prt.source(__file__)
	prt()
	with prt.header('Iterate over a dataset chain.'):
		prt('''
			We've copied the import loop from "build_tutorial03.py" in
			order to get a reference to the chained dataset we've created.

			(Soon, we'll show how to do this in a much more elegant way!)
		''')
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

	with prt():
		prt('''
			The variable "imp" now hold a reference to the last built method,
			which happens to be dataset_hashpart of the data from "data3.csv".
			Because of the chaining, all imported data is available through
			this reference.
		''')

		prt()
		prt('Now, let\'s iterate over all imported data')
		prt()
		job = urd.build('dsexample_iteratechain', source=imp)
		prt()
		prt('...and the result is (something printed by the job to stdout)')
		prt.output(job.output())
		prt()
		prt('Try')
		prt.command('ax job -O', job)
		prt('to print this output from the %s job to the terminal.' % (job.method,))
