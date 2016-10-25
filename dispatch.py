from __future__ import print_function
from __future__ import division

import os
import sys
import time
import json
from functools import partial
from signal import SIGTERM, SIGKILL

from compat import PY3

from status_messaging import statmsg
from status import children
from extras import job_params, json_encode

class JobError(Exception):
	def __init__(self, jobid, method, status):
		Exception.__init__(self, "Failed to build %s (%s)" % (jobid, method,))
		self.jobid = jobid
		self.method = method
		self.status = status

	def format_msg(self):
		res = ["%s (%s):" % (self.jobid, self.method,)]
		for component, msg in self.status.items():
			res.append("  %s:" % (component,))
			res.append("   %s" % (msg.replace("\n", "\n    "),))
		return "\n".join(res)

valid_fds = []
def update_valid_fds():
	# Collect all valid fds, so we can close them in job processes
	global valid_fds
	valid_fds = []
	from fcntl import fcntl, F_GETFD
	from resource import getrlimit, RLIMIT_NOFILE
	for fd in range(3, getrlimit(RLIMIT_NOFILE)[0]):
		try:
			fcntl(fd, F_GETFD)
			valid_fds.append(fd)
		except Exception:
			pass

def run(cmd, close_in_child, keep_in_child, with_pgrp=True):
	child = os.fork()
	if child:
		return child
	if with_pgrp:
		os.setpgrp() # this pgrp is killed if the job fails
	for fd in close_in_child:
		os.close(fd)
	status_fd = int(os.getenv('BD_STATUS_FD'))
	keep_in_child = set(keep_in_child)
	keep_in_child.add(status_fd)
	for fd in valid_fds:
		# Apparently sometimes one of them has gone away.
		# That's a little worrying, so try to protect our stuff (and ignore errors).
		try:
			if fd not in keep_in_child:
				os.close(fd)
		except OSError:
			pass
	# unreadable stdin - less risk of stuck jobs
	devnull = os.open('/dev/null', os.O_RDONLY)
	os.dup2(devnull, 0)
	os.close(devnull)
	if PY3:
		keep_in_child.update([1, 2])
		for fd in keep_in_child:
			os.set_inheritable(fd, True)
	os.execv(cmd[0], cmd)
	os._exit()

def launch_common(name, workdir, jobid, config, Methods, active_workspaces, slices, debug, daemon_url, subjob_cookie, parent_pid):
	starttime = time.time()
	wstr = ','.join(x[0] + ':' + x[1] for x in active_workspaces.items())
	method = job_params(jobid).method
	if subjob_cookie:
		print_prefix = ''
	else:
		print_prefix = '    '
	print('%s| %s [%s]  %-20s|' % (print_prefix, jobid, method, name))
	statmsg('| %s [%s]  %-20s|' % (jobid, method, name))
	prof_r, prof_w = os.pipe()
	try:
		cmd = [sys.executable, 'launch.py',
			'--workdir=' + workdir,
			'--slices=%d' % (slices,),
			'--jobid=' + jobid,
			'--' + name,
			'--prof_fd=%d' % (prof_w,),
			'--result_directory=' + config.get('result_directory', ''),
			'--common_directory=' + config.get('common_directory', ''),
			'--source_directory=' + config.get('source_directory', ''),
			'--wstr=' + wstr,
			'--daemon_url=' + daemon_url,
			'--subjob_cookie=' + (subjob_cookie or ''),
			'--parent_pid=%d' % (parent_pid,),
		]
		if debug:
			cmd.append('--debug')
		child = run(cmd, [prof_r], [prof_w])
		# There's a race where if we get interrupted right after fork this is not recorded
		# (the launched job could continue running)
		children.add(child)
		os.close(prof_w)
		prof_w = None
		prof = []
		# We have closed prof_w. When child exits we get eof.
		while True:
			data = os.read(prof_r, 4096)
			if not data:
				break
			prof.append(data)
		try:
			status, data = json.loads(b''.join(prof).decode('utf-8'))
		except Exception:
			status = {'launcher': '[invalid data] (probably killed)'}
		if status:
			os.killpg(child, SIGTERM) # give it a chance to exit gracefully
			msg = json_encode(status, as_str=True)
			print('%s| %s [%s]  failed!    (%5.1fs) |' % (print_prefix, jobid, method, time.time() -  starttime))
			statmsg('| %s [%s]  failed!             |' % (jobid, method))
			statmsg(msg)
			time.sleep(1) # give it a little time to do whatever cleanup it feels the need to do
		# There is a race where stuff on the status socket has not arrived when
		# the sending process exits. This is basically benign, but let's give
		# it a chance to arrive to cut down on confusing warnings.
		time.sleep(0.05)
		os.killpg(child, SIGKILL) # this should normally be a no-op, but in case it left anything.
		children.remove(child)
		os.waitpid(child, 0) # won't block (we just killed it, plus it had probably already exited)
		if status:
			raise JobError(jobid, method, status)
		print('%s| %s [%s]  completed. (%5.1fs) |' % (print_prefix, jobid, method, time.time() -  starttime))
		statmsg('| %s [%s]  completed.          |' % (jobid, method))
		return data
	finally:
		for fd in (prof_r, prof_w,):
			try:
				os.close(fd)
			except Exception:
				pass

launch_all = partial(launch_common, 'all')
launch_analysis = partial(launch_common, 'analysis')
launch_synthesis = partial(launch_common, 'synthesis')
