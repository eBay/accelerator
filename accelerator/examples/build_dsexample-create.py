from .printer import prt

description = "Dataset: Create a dataset from scratch."


def main(urd):
	prt.source(__file__)
	prt()
	prt('Build a job that creates a dataset from scratch.')
	job = urd.build('dsexample_createdataset')

	prt()
	prt('Check contents of dataset using')
	prt.command('ax cat -H', job)

	prt()
	prt('More info about the dataset')
	prt.command('ax ds -s', job)

	prt()
	prt('For convenience, the jobid can be used as a reference to the default')
	prt('dataset in a job.  The full name is "%s/default".' % (job,))
