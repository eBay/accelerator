import os


# WORKSPACES should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKSPACES = {}


def put_workspaces(workspaces_dict):
	global WORKSPACES
	WORKSPACES = workspaces_dict


class Jobid:
	def __init__(self, jobid):
		self.wspace, tmp = jobid.split('-')
		self.major, self.minor = (int(x) for x in tmp.split('_'))
		self.number = self.major*1000+self.minor


def globexpression(name):
	return '%s-[0-9]*_*' % name


def create(name, major, minor):
	return name + '-%d_%d' % (major, minor)


def get_workspace_name(jobid):
	return str(jobid).split('-',1)[0]


def get_path(jobid):
	return WORKSPACES[get_workspace_name(jobid)]


def resolve_jobid_filename(jobid, filename):
	"""
	Used by extras, dataset, and sourcedata to find
	full path of filename based on lookup from
	jobid -> workspace
	"""
	if jobid:
		jobid = str(jobid)
		path = get_path(jobid)
		return os.path.join(path, jobid, filename)
	else:
		return filename
