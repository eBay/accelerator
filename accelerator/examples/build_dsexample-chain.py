from os.path import dirname, join
from .printer import prt

description = "Dataset: Create a chained dataset."

# Files are stored in same directory as this python file,
# see comment below.
path = dirname(__file__)


def main(urd):
	prt.source(__file__)
	prt()
	prt('Create a chain of datasets using csvimport.')
	imp = None
	for filename in ('data.csv', 'data2.csv', 'data3.csv'):
		# Ideally, you'd set "input_directory" to the location of the
		# input files in "accelerator.conf" to avoid an absolute path
		# in the input filename.
		filename = join(path, filename)
		imp = urd.build('csvimport', filename=filename, previous=imp)

	prt()
	prt('Try this to investigate the chain.')
	prt.command('ax ds -c -S', imp)

	prt()
	prt('To go back in chain and investigate datasets, try')
	prt.command('ax ds %s' % (imp,))
	prt.command('ax ds %s~' % (imp,))
	prt.command('ax ds %s~~' % (imp,))
	prt('Note that ~~ can also be written ~2 etc.')

	prt()
	prt('This method will iterate over the whole chain.')
	job = urd.build('dsexample_iteratechain', source=imp)

	prt()
	prt('To see its output, try')
	prt.command('ax job -O', job)
