from .printer import prt

description = "Dataset: Several datasets in a job - print some info per dataset"


def main(urd):
	prt.source(__file__)
	prt()
	prt('This job creates three datasets')
	job = urd.build('dsexample_multipledatasets')

	prt()
	prt('Print dataset names, columns and types')
	for ds in job.datasets:
		prt.plain(ds, sum(ds.lines), {name: ds.columns[name].type for name in ds.columns})

	prt()
	prt('To see all datasets in a job, try')
	prt.command('ax ds -l', job)

	prt()
	prt('Print contents of a specific dataset like this')
	prt.command('ax cat -H %s/third' % (job,))
