description = "Read result from input job, add it to itself and return it."

# "jobs" is a list (or tuple) of jobs assigned by the build script.
# Entries that are left unassigned are set to None.
jobs = ('first',)


def synthesis():
	# All items in the jobs variable will be populated by Job objects
	# when executed.
	# The Job.load() function is used to load the job's return value.
	res = jobs.first.load()
	return res + res
