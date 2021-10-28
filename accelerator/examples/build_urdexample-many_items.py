from datetime import date, timedelta
from json import dumps
from .printer import prt

description = 'More Urd, several timestamps.'


def main(urd):
	prt.source(__file__)
	# Truncate list to timestamp 0 every time we run this.  If we do
	# not, code changes may lead to inconsistensies that will raise
	# errors.  Per design.  See manual.
	listname = 'example-many'
	urd.truncate(listname, 0)

	prt('Start by creating ten chained jobs.  Each job is associated')
	prt('with a unique Urd-item in the "%s" Urd list.' % (listname,))
	starttime = date(2021, 8, 1)
	for days in range(10):
		timestamp = starttime + timedelta(days=days)
		urd.begin(listname, timestamp, caption='caption_' + str(days))
		previous = urd.latest('listname').joblist.get(-1)
		urd.build('example_dummyjob', timestamp=timestamp, previous=previous)
		urd.finish(listname)

	prt()
	prt('Here\'s a list of all entries in the list "%s"' % (listname,))
	for ts in urd.since(listname, 0):
		prt.output(ts, '"%s"' % (urd.peek(listname, ts).caption,))
	prt('This is available in the shell using')
	prt.command('ax urd %s/' % (listname,))

	prt()
	prt('Here\'s the session for timestamp 2021-08-06')
	prt.plain(dumps(urd.peek(listname, '2021-08-06'), indent=4))
	prt('Note the "deps" section.  The job that is built is fed with a')
	prt('"previous" input job reference.  This reference is provided by Urd')
	prt('using the "urd.latest()" function.')

	prt()
	prt('To print this in a more human friendly format on the command line, try')
	prt.command('    ax urd %s/2021-08-06' % (listname,))
