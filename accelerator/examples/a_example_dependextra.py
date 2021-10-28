from . import mydependency

description = 'depend_extra'

# This method will be re-executed if any of
#    the file "mydependencyfile.txt" or
#    the module "mydependency"
# is changed.
depend_extra = ('mydependencyfile.txt', mydependency)


def synthesis():
	pass
