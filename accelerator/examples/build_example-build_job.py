from .printer import prt

description = "Jobs: Build a job, load its returned value."


def main(urd):
	prt.source(__file__)
	prt()
	prt('Run method "example_returnhelloworld"')
	urd.build('example_returnhelloworld')

	prt()
	prt('Run method "example_returnhelloworld" again')
	job = urd.build('example_returnhelloworld')
	prt('Note that is is linked, and not re-executed.')

	prt()
	prt('Load and print the job\'s return value')
	result = job.load()
	prt.output(result)

	prt()
	prt('Try these commands:')
	prt.command('ax job', job)
	prt.command('ls', job.path)
