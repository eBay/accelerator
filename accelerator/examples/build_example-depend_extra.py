from os.path import dirname, join
from .printer import prt

description = "Jobs: depend_extra howto"


def main(urd):
	prt.source(__file__)
	prt()
	prt('The following method shows how to do "depend_extra"')
	job = urd.build('example_dependextra')

	prt()
	prt('The job will be re-built if there are changes to')
	prt('"mydependency.txt" or "mydependency.py.')

	prt()
	prt('source code is here:')
	prt.plain(join(dirname(__file__), 'a_%s.py' % (job.method,)))
