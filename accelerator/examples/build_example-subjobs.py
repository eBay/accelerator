from accelerator import Job
from .printer import prt

description = "Jobs: Subjob"


def main(urd):
	prt.source(__file__)
	prt()
	prt('This job will execute a subjob')
	job = urd.build('example_buildsubjob')

	prt()
	prt('Print subjob info')
	for sjob in job.post.subjobs:
		prt.plain('subjob', dict(job=sjob, method=Job(sjob).method))
	prt()
	prt('You can see subjobs in the output from')
	prt.command('ax job', job)
