from .printer import prt

description = 'Basic Urd database useage'


def main(urd):
	prt.source(__file__)
	# Truncate list to timestamp 0 every time we run this.  If we do
	# not, code changes may lead to inconsistensies that will raise
	# errors.  Per design.  See manual.
	listname = 'example-basic'
	timestamp = '2021-07-01'
	urd.truncate(listname, 0)

	prt()
	prt('Create and Urd item containing a joblist of one job.')
	prt('The name of the Urd list is "%s".' % (listname,))
	urd.begin(listname, timestamp, caption='you may assign a caption here')
	urd.build('example_returnhelloworld')
	urd.finish(listname)

	prt()
	prt('Here\'s a list of all available Urd-lists')
	for item in urd.list():
		prt.output(item)

	prt()
	prt('The command')
	prt.command('ax urd')
	prt('# will do the same thing on the command line.')

	prt()
	prt('Here\'s a list of all entries in the list "%s"' % (listname,))
	for ts in urd.since(listname, 0):
		prt.output(ts)

	prt()
	prt('The command')
	prt.command('ax urd %s/since/0' % (listname,))
	prt('or')
	prt.command('ax urd %s/' % (listname,))
	prt('will do the same thing on the command line.')

	prt()
	prt('To see a specific entry, try')
	prt.command('ax urd %s/%s' % (listname, timestamp))

	prt()
	prt('To see information about the a specific job in that Urd session')
	prt.command('ax job :%s/%s:example1' % (listname, timestamp))

	prt()
	prt('To see information about the last job in that Urd session')
	prt.command('ax job :%s/%s:-1' % (listname, timestamp))
	prt.command('ax job :%s/%s:' % (listname, timestamp))
	prt('Note that the commands defaults to the _last_ item')

	prt()
	prt('To see information about the last job in the last Urd session')
	prt.command('ax job :%s:' % (listname,))
	prt('Note that the commands defaults to the _last_ item')

	prt()
	prt('This is particularly useful for dataset, where we can write for')
	prt('example "ax cat :import:"  to print the dataset in the last job')
	prt('in the last Urd-item in the "import" list.')
