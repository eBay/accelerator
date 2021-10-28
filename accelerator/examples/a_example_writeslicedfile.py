description = "Write a sliced file in analysis, one file in synthesis, and return some data."


def analysis(sliceno, job):
	# save one file per analysis process...
	filename = 'myfile1'
	data = 'This is job %s analysis slice %d.' % (job, sliceno,)
	# We need "temp=False" for the files written in analysis
	# to be kept permanently.
	# If not set, the files will be erased at job completion.
	job.save(data, filename, sliceno=sliceno, temp=False)


def synthesis(job):
	# ...and one file in the synthesis process...
	filename = 'myfile2'
	data = 'this is job %s synthesis' % (job,)
	job.save(data, filename)

	# ...and let's return some data too.
	returndata = 'this is job %s return value' % (job,)
	return returndata
