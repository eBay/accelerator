from datetime import date
from .printer import prt

description = "Job options."


def main(urd):
	prt.source(__file__)
	prt()
	prt.header('Examples options')
	prt('Run a job that accepts a variety of options:')
	job = urd.build(
		'example_options',
		day1=date(2021, 10, 14),
		day2=date(2021, 10, 14),
		something=3.14,
		text='hello',
		number=42,
		seq=[1,2,3,4],
		cplx=dict(a=dict(b='c')),
	)
	prt('The job will print its options to stdout.')
	prt()
	prt('See the options as the job sees them by running')
	prt.command('ax job -O %s' % (job,))
	prt('The "ax job" command will also show the job\'s options')
	prt.command('ax job %s' % (job,))
	prt('See everything using')
	prt.command('ax job -o %s' % (job,))

	prt()
	prt.header('Short and long version (see code)')
	prt('Note that')
	urd.build('example_options', text='world')
	prt('is the shorter version of')
	urd.build('example_options', options=dict(text='world'))
	prt('which can be used to avoid name clashes between jobs,')
	prt('options, and datasets.')
