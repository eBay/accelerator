from .printer import prt

description = "Dataset: Append columns to an existing dataset."


def main(urd):
	prt.source(__file__)
	prt()
	prt('Create a dataset.')
	job = urd.build('dsexample_createdataset')

	prt()
	prt('Append a new columnd to the dataset.')
	job = urd.build('dsexample_appendcolumn', source=job)

	prt()
	prt('Try')
	prt.command('ax ds', job)
	prt('The "parent" row shows that this dataset includes columns')
	prt('from a parent dataset.')
	prt()
	prt('To see the location of the different columns, try')
	prt.command('ax ds -w', job)
