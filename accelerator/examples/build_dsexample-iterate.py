from .printer import prt

description = "Dataset: Iterate"

def main(urd):
	prt.source(__file__)
	prt()
	prt('Run a method that creates a dataset.')
	job = urd.build('dsexample_createdataset')

	prt()
	prt('Iterate and merge the number column of the dataset.')
	job = urd.build('dsexample_iterateandmerge', source=job)
	prt()
	prt("The job's return value is:", job.load())
