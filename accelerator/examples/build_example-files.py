from .printer import prt

description = "Jobs: sliced files, stdout and stderror, load any file from a job, jobwithfile"


def main(urd):
	prt.source(__file__)
	prt()
	prt('This job writes sliced files in "myfile1", and a single file "myfile2".')
	job1 = urd.build('example_writeslicedfile')

	prt()
	prt('These are the files created by / located in job %s.' % (job1,))
	prt.output(job1.files())

	prt()
	prt('This job reads the sliced and the single file and prints its')
	prt('contents to stdout')
	job2 = urd.build(
		'example_readjobwithfile',
		firstfile=job1.withfile('myfile1', sliced=True),
		secondfile=job1.withfile('myfile2'),
	)

	prt()
	prt('Read and print stored stdout from %s synthesis' % (job2,))
	prt.output(job2.output('synthesis'))
	prt()
	prt('Read and print stored stdout from %s everything' % (job2,))
	prt.output(job2.output())
	prt()
	prt('Read and print stored stdout from %s analysis process 2' % (job2,))
	prt.output(job2.output(2))

	prt()
	prt('We can also use job.open() to get a file handle and do any')
	prt('kind of file operations.')
	with job1.open('myfile2', 'rb') as fh:
		import pickle
		prt.output(pickle.load(fh))

	prt('To see all files stored in a job, try')
	prt.command('ax job %s' % (job1,))
