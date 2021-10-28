from os.path import dirname, join
from .printer import prt

description = "Jobs: equivalent_hashes howto"


def main(urd):
	prt.source(__file__)
	prt()
	prt('This example shows how to create an "equivalent hash", i.e.')
	prt('how to modify a method without causing a build script to')
	prt('re-build it.')
	job = urd.build('example_equivalenthashes')

	thehash = job.params.hash
	prt()
	prt('Take the hash from the built job:')
	prt.output('"%s"' % (thehash,))
	prt('and add it to the method like this')
	prt.output('"equivalent_hashes = {\'whatever\': (\'%s\',)}"' % (thehash,))

	prt()
	prt('Also do some other change to the method while at it to make it different.')
	prt('Re-run and the daemon output will be something like')
	prt.output('''
		==========================================================================================
		WARNING: a_example_equivalenthashes has equivalent_hashes, but missing verifier <verifier>
		==========================================================================================
	''')
	prt('Now, let <verifier> replace the string \'whatever\' in the method.')
	prt('Done, the new version of the method is now considered equivalent')
	prt('to the old one.')
	prt()
	prt('Method source file is here')
	prt.plain(join(dirname(__file__), 'a_%s.py' % (job.method,)))
