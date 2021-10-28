from os.path import dirname, join
from .printer import prt

description = "Tutorial: Import a set of files and persistently store all references and dependencies."


filenames = ('data.csv', 'data2.csv', 'data3.csv')
# Again, files are stored in same directory as this python file.
# In a real project, set "input directory" in config file instead!
filenames = (join(dirname(__file__), x) for x in filenames)


def main(urd):
	prt.source(__file__)
	prt()
	prt('Import a list of files and store references in the persistent')
	prt('Urd database for later use.')
	prt()
	key = 'example_tutorial05'
	urd.truncate(key, 0)
	for ts, filename in enumerate(filenames, 1):
		prt.plain(filename, ts)
		urd.begin(key, ts, caption=filename)
		latest = urd.latest(key).joblist
		imp = urd.build(
			'csvimport',
			filename=filename,
			previous=latest.get('csvimport'),
		)
		imp = urd.build(
			'dataset_type',
			source=imp,
			previous=latest.get('dataset_type'),
			column2type=dict(
				Date='date:%Y-%m-%d',
				String='unicode:utf-8',
				Int='number',
				Float='float64',
			),
		)
		imp = urd.build(
			'dataset_sort',
			source=imp,
			sort_columns='Date',
			previous=latest.get('dataset_sort'),
		)
		imp = urd.build(
			'dataset_hashpart',
			source=imp,
			hashlabel='String',
			previous=latest.get('dataset_hashpart'),
		)
		urd.finish(key)

	prt()
	prt('''
		Now, references to everything that has been built, including
		job dependencies, is stored in the Urd server using the.
		key "<user>/%s".
	''' % (key,))

	prt()
	with prt.header('View all Urd lists.'):
		prt('To view all Urd "lists", type')
		prt.command('ax urd')
		prt('Each key is composed by <user>/<listname>:')
		for item in urd.list():
			prt.output(item)

	prt()
	with prt.header('Inspecting all sessions in a list.'):
		prt('All sessions are timestamped.  To see all timestamps')
		prt('and captions in "%s" since timestamp zero, do' % (key,))
		prt.command('ax urd %s/since/0' % (key,))
		prt('or equivalently')
		prt.command('ax urd %s/' % (key,))
		prt('the output from this command looks something like this (try it)')
		for ts in urd.since(key, 0):
			prt.output(ts, urd.peek(key, ts).caption)

	prt()
	with prt.header('Inspecting individual Urd items.'):
		prt('We can look at an individual Urd-item like this')
		prt.command('ax urd %s/3' % (key,))
		prt('which corresponds to the list in key "%s" at timestamp 3.' % (key,))
		prt('(Timestamps can be dates, datetimes, integers, or tuples')
		prt('of date/datetimes and integers.)')

	prt()
	prt('''
		The data is imported into jobs in a workdir, and references
		to these jobs are recorded in the Urd persistent transaction-log
		database.
	''')
