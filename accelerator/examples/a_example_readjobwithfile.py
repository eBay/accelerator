from accelerator.job import JobWithFile

description = 'JobWithFile, read normal and sliced files'

options = dict(firstfile=JobWithFile, secondfile=JobWithFile)


def analysis(sliceno):
	print(options.firstfile.load(sliceno=sliceno))


def synthesis():
	print(options.secondfile.load())
