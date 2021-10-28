from os.path import dirname, join
from accelerator.colour import bold
from .printer import prt

description = "Tutorial: Import a file."


# The file is stored in the same directory as this python file.
# In a real project, set "input directory" in the config file instead!
filename = join(dirname(__file__), 'data.csv')


def main(urd):
	prt.source(__file__)
	prt()
	prt('Import a CSV-file:')
	prt()

	imp = urd.build('csvimport', filename=filename)

	prt()
	with prt.header('HOW TO GET INFORMATION ABOUT JOBS AND DATASETS'):
		prt('To show info on the imported dataset, type')
		prt.command('ax ds', imp)
		prt('To show info on the job that created the dataset, type')
		prt.command('ax job', imp)
		prt('To show the contents of the dataset (with header), type')
		prt.command('ax cat -H', imp)
		prt('Or perhaps in a json format:')
		prt.command('ax cat -f json -D -S -L', imp)

	prt()
	with prt.header('ON DATASET NAMING AND DEFAULT NAME'):
		prt('''
			As long as we do not assign names to datasets, they are
			referenced by the default dataset name, which is "default".
			If we are working on the default dataset, we can use the
			reference to the job that created the dataset as a reference
			to the default dataset inside the job.  I.e.
		''')
		prt.command('ax ds %s' % (imp,))
		prt('is equivalent to')
		prt.command('ax ds %s/default' % (imp,))
		prt('which is the formally correct way to refer to the dataset.')

	prt()
	with prt.header('USING METHOD NAMES INSTEAD OF JOB NAMES.'):
		prt('It is also possible to write for example')
		prt.command('ax job csvimport')
		prt('to get information on the ' + bold('last created') + ' csvimport job.')

	prt()
	with prt.header('GETTING HELP'):
		prt.command('ax <command> --help')
		prt('or just')
		prt.command('ax --help')
