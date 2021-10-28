from .printer import prt

description = "Jobs: Build and link two jobs, load and print return values"


def main(urd):
	prt.source(__file__)
	prt()
	prt('Run methods "example_returnhelloworld" + "example_returnconcatinput"')
	job1 = urd.build('example_returnhelloworld')
	job2 = urd.build('example_returnconcatinput', first=job1)  # "first" is an item in the "jobs" list in a_example2.py

	prt()
	prt('Return value from "example_returnhelloworld"')
	prt.output(job1.load())
	prt()
	prt('Return value from "example_returnconcatinput"')
	prt.output(job2.load())
