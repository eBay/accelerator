description = 'return "Hello, World!"'


def synthesis():
	# The synthesis-function is run once. The return value is easy to
	# load into another job or build script without the need to
	# specify a filename.  (So we use the job that created the data as
	# reference, never again an arbitrary filename.)
	return 'Hello, World!'
