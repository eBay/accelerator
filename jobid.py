import os
import re


# WORKSPACES should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKSPACES = {}


def put_workspaces(workspaces_dict):
	global WORKSPACES
	WORKSPACES = workspaces_dict


class Jobid:
	def __init__(self, jobid):
		self.wspace, tmp = jobid.rsplit('-', 1)
		self.number = int(tmp)


def dirnamematcher(name):
	return re.compile(re.escape(name) + r'-[0-9]+$').match


def create(name, number):
	return '%s-%d' % (name, number,)


def get_workspace_name(jobid):
	return jobid.rsplit('-', 1)[0]


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
