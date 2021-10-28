from .printer import prt

description = "Jobs: prepare, analysis, and synthesis + return values"


def main(urd):
	prt.source(__file__)
	prt()
	prt('Run a job that returns (and prints) something in all of')
	prt('prepare, analysis, and synthesis.')
	job = urd.build('example_returninprepanasyn')

	prt()
	prt('Job output:')
	prt.output(job.output())

	prt()
	prt('Job return value:')
	prt.output(job.load())
