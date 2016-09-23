from __future__ import print_function
from __future__ import division

# This used to contain all the dataset chaining stuff, but that now lives in
# the dataset module. Usually that's what you want, but sometimes you want
# to chain by job arguments, and that's still here.

from compat import str_types

from extras import job_params


def jobchain_prev(tip_jobid=None):
	"""Wrap jobchain for a common use of jobchain: find previous jobid
	to this one."""

	# +bool because tip is included if set
	jobid = jobchain(length=1 + bool(tip_jobid), tip_jobid=tip_jobid)
	if jobid:
		return jobid[0]
	else:
		return ''


def jobchain(length=-1, reverse=False, tip_jobid=None, stop_jobid=None):
	"""Look backwards over "previous" (jobid or dataset) from tip_jobid
	(default current job) and return length (or all) latest jobids
	(includes tip only if explicitly specified)
	Return up to but not including stop_jobid.
	stop_jobid can be a {job: optname} dict, resolving dataset/jobid "optname" from job"""

	def x2opt(jobid, optname="previous"):
		params = job_params(jobid)
		return params.jobids.get(optname) or params.datasets.get(optname)

	if not stop_jobid:
		stop_jobid = ()
	elif isinstance(stop_jobid, str_types):
		stop_jobid = (stop_jobid,)
	elif isinstance(stop_jobid, dict):
		stuff = stop_jobid.items()
		stop_jobid = set()
		for parent, var in stuff:
			stop_jobid.add(x2opt(parent, var))
	assert isinstance(stop_jobid, (list, tuple, set,)), "stop_jobid must be str, dict or set-ish"

	jobid = tip_jobid
	if tip_jobid:
		l_jobid = [tip_jobid]
		length -= 1
	else:
		l_jobid = []
	while length:
		jobid = x2opt(jobid)
		if not jobid:
			break
		if jobid in stop_jobid:
			break
		l_jobid.append(jobid)
		length -= 1
	if not reverse:
		l_jobid.reverse()
	return l_jobid
