############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2018-2021 Carl Drougge                       #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#  http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
#                                                                          #
############################################################################

from __future__ import print_function
from __future__ import division

import sys
import socket
import traceback
import signal
import os
import resource
import time
from stat import S_ISSOCK
from threading import Thread, Lock as TLock, Lock as JLock
from multiprocessing import Process
from string import ascii_letters
import random
import atexit

from accelerator.compat import unicode, ArgumentParser

from accelerator.web import ThreadedHTTPServer, ThreadedUnixHTTPServer, BaseWebHandler

from accelerator import autoflush
from accelerator import control
from accelerator.extras import json_encode, json_decode, DotDict
from accelerator.build import JobError
from accelerator.job import Job
from accelerator.statmsg import statmsg_sink, children, print_status_stacks, status_stacks_export
from accelerator import iowrapper, board, g, __version__ as ax_version



DEBUG_WRITE_JSON = False


def gen_cookie(size=16):
	return ''.join(random.choice(ascii_letters) for _ in range(size))

# This contains cookie: {lock, last_error, last_time, workdir}
# for all jobs, main jobs have cookie None.
job_tracking = {None: DotDict(lock=JLock(), last_error=None, last_time=0, workdir=None)}


# This needs .ctrl to work. It is set from main()
class XtdHandler(BaseWebHandler):
	server_version = "scx/0.1"
	unicode_args = True
	DEBUG =  not True

	def log_message(self, format, *args):
		return

	def do_response(self, code, content_type, body):
		hdrs = [('Accelerator-Version', ax_version)]
		BaseWebHandler.do_response(self, code, content_type, body, hdrs)

	def encode_body(self, body):
		if isinstance(body, bytes):
			return body
		if isinstance(body, unicode):
			return body.encode('utf-8')
		return json_encode(body)

	def handle_req(self, path, args):
		if self.DEBUG:  print("@server.py:  Handle_req, path = \"%s\", args = %s" %( path, args ), file=sys.stderr)
		try:
			self._handle_req( path, args )
		except Exception:
			traceback.print_exc()
			self.do_response(500, "text/plain", "ERROR")

	def _handle_req(self, path, args):
		if path[0] == 'status':
			data = job_tracking.get(args.get('subjob_cookie') or None)
			if not data:
				self.do_response(400, 'text/plain', 'bad subjob_cookie!\n' )
				return
			timeout = min(float(args.get('timeout', 0)), 128)
			status = DotDict(idle=data.lock.acquire(False))
			deadline = time.time() + timeout
			while not status.idle and time.time() < deadline:
				time.sleep(0.1)
				status.idle = data.lock.acquire(False)
			if status.idle:
				if data.last_error:
					status.last_error_time = data.last_error[0]
				status.last_time = data.last_time
				data.lock.release()
			elif path == ['status', 'full']:
				status.status_stacks, status.current = status_stacks_export()
			status.report_t = time.time()
			self.do_response(200, "text/json", status)
			return

		elif path==['last_error']:
			data = job_tracking.get(args.get('subjob_cookie') or None)
			if not data:
				self.do_response(400, 'text/plain', 'bad subjob_cookie!\n' )
				return
			status = DotDict()
			if data.last_error:
				status.time = data.last_error[0]
				status.last_error = data.last_error[1]
			self.do_response(200, "text/json", status)
			return

		elif path==['list_workdirs']:
			ws = {k: v.path for k, v in self.ctrl.list_workdirs().items()}
			self.do_response(200, "text/json", ws)

		elif path[0]=='workdir':
			self.do_response(200, "text/json", self.ctrl.DataBase.db_by_workdir[path[1]])

		elif path==['config']:
			self.do_response(200, "text/json", self.ctrl.config)

		elif path==['update_methods']:
			self.do_response(200, "text/json", self.ctrl.update_methods())

		elif path==['methods']:
			""" return a json with everything the Method object knows about the methods """
			self.do_response(200, "text/json", self.ctrl.get_methods())

		elif path[0]=='method_info':
			method = path[1]
			self.do_response(200, "text/json", self.ctrl.method_info(method))

		elif path[0]=='workspace_info':
			self.do_response(200, 'text/json', self.ctrl.get_workspace_details())

		elif path[0] == 'abort':
			tokill = list(children)
			print('Force abort', tokill)
			for child in tokill:
				os.killpg(child, signal.SIGKILL)
			self.do_response(200, 'text/json', {'killed': len(tokill)})

		elif path[0] == 'method2job':
			method, num = path[1:]
			jobs = self.ctrl.DataBase.db_by_method.get(method, ())
			start_ix = 0
			start_from = args.get('start_from')
			if start_from:
				for start_ix, job in enumerate(jobs):
					if job.id == start_from:
						break
				else:
					start_ix = None
			if start_ix is None:
				res = {'error': '%s is not a current %s job' % (start_from, method,)}
			else:
				num = int(num) + start_ix
				if not jobs:
					res = {'error': 'no current jobs with method %s available' % (method,)}
				elif num + start_ix >= len(jobs):
					res = {'error': 'tried to go %d jobs back from %s, but only %d earlier (current) jobs available' % (num, jobs[start_ix].id, len(jobs) - start_ix,)}
				else:
					res = {'id': jobs[num + start_ix].id}
			self.do_response(200, 'text/json', res)

		elif path[0] == 'job_is_current':
			job = Job(path[1])
			job = self.ctrl.DataBase.db_by_workdir[job.workdir].get(job)
			self.do_response(200, 'text/json', bool(job and job['current']))

		elif path==['submit']:
			if self.ctrl.broken:
				self.do_response(500, "text/json", {'broken': self.ctrl.broken, 'error': 'Broken methods: ' + ', '.join(sorted(m.split('.')[-1][2:] for m in self.ctrl.broken))})
			elif 'json' in args:
				if DEBUG_WRITE_JSON:
					with open('DEBUG_WRITE.json', 'wb') as fh:
						fh.write(args['json'])
				setup = json_decode(args['json'])
				data = job_tracking.get(setup.get('subjob_cookie') or None)
				if not data:
					self.do_response(403, 'text/plain', 'bad subjob_cookie!\n' )
					return
				if len(job_tracking) - 1 > 5: # max five levels
					print('Too deep subjob nesting!')
					self.do_response(403, 'text/plain', 'Too deep subjob nesting')
					return
				if data.lock.acquire(False):
					still_locked = True
					respond_after = True
					try:
						if self.DEBUG:  print('@server.py:  Got the lock!', file=sys.stderr)
						workdir = setup.get('workdir', data.workdir)
						jobidv, job_res = self.ctrl.initialise_jobs(setup, workdir)
						job_res['done'] = False
						if jobidv:
							error = []
							tlock = TLock()
							link2job = {j['link']: j for j in job_res['jobs'].values()}
							def run(jobidv, tlock):
								for jobid in jobidv:
									passed_cookie = None
									# This is not a race - all higher locks are locked too.
									while passed_cookie in job_tracking:
										passed_cookie = gen_cookie()
									job_tracking[passed_cookie] = DotDict(
										lock=JLock(),
										last_error=None,
										last_time=0,
										workdir=workdir,
									)
									try:
										self.ctrl.run_job(jobid, subjob_cookie=passed_cookie, parent_pid=setup.get('parent_pid', 0))
										# update database since a new jobid was just created
										job = self.ctrl.add_single_jobid(jobid)
										with tlock:
											link2job[jobid]['make'] = 'DONE'
											link2job[jobid]['total_time'] = job.total
									except JobError as e:
										error.append([e.jobid, e.method, e.status])
										with tlock:
											link2job[jobid]['make'] = 'FAIL'
										return
									finally:
										del job_tracking[passed_cookie]
								# everything was built ok, update symlink
								try:
									dn = self.ctrl.workspaces[workdir].path
									ln = os.path.join(dn, workdir + "-LATEST_")
									try:
										os.unlink(ln)
									except OSError:
										pass
									os.symlink(jobid, ln)
									os.rename(ln, os.path.join(dn, workdir + "-LATEST"))
								except OSError:
									traceback.print_exc()
							t = Thread(target=run, name="job runner", args=(jobidv, tlock,))
							t.daemon = True
							t.start()
							t.join(2) # give job two seconds to complete
							with tlock:
								for j in link2job.values():
									if j['make'] in (True, 'FAIL',):
										respond_after = False
										job_res_json = json_encode(job_res)
										break
							if not respond_after: # not all jobs are done yet, give partial response
								self.do_response(200, "text/json", job_res_json)
							t.join() # wait until actually complete
							del tlock
							del t
							# verify that all jobs got built.
							total_time = 0
							for j in link2job.values():
								jobid = j['link']
								if j['make'] == True:
									# Well, crap.
									error.append([jobid, "unknown", {"INTERNAL": "Not built"}])
									print("INTERNAL ERROR IN JOB BUILDING!", file=sys.stderr)
								total_time += j.get('total_time', 0)
							if error:
								data.last_error = (time.time(), error)
							data.last_time = total_time
					except Exception as e:
						if respond_after:
							data.lock.release()
							still_locked = False
							self.do_response(500, "text/json", {'error': str(e)})
						raise
					finally:
						if still_locked:
							data.lock.release()
					if respond_after:
						job_res['done'] = True
						self.do_response(200, "text/json", job_res)
					if self.DEBUG:  print("@server.py:  Process releases lock!", file=sys.stderr) # note: has already done http response
				else:
					self.do_response(503, 'text/plain', 'Busy doing work for you...\n')
			else:
				self.do_response(400, 'text/plain', 'Missing json input!\n' )
		else:
			self.do_response(404, 'text/plain', 'Unknown path\n' )
			return


def exitfunction(*a):
	if a != (DeadlyThread,): # if not called from a DeadlyThread
		signal.signal(signal.SIGTERM, signal.SIG_IGN)
		signal.signal(signal.SIGINT, signal.SIG_IGN)
	print()
	print('The deathening! %d %s' % (os.getpid(), children,))
	print()
	for child in children:
		try:
			os.killpg(child, signal.SIGKILL)
		except Exception:
			pass
	time.sleep(0.16) # give iowrapper a chance to output our last words
	os.killpg(os.getpgid(0), signal.SIGKILL)
	os._exit(1) # we really should be dead already

# A Thread that kills the server if it exits
class DeadlyThread(Thread):
	def run(self):
		try:
			Thread.run(self)
		except Exception:
			traceback.print_exc()
		finally:
			print("Thread %r died. That's bad." % (self.name,))
			exitfunction(DeadlyThread)


def check_socket(fn):
	dn = os.path.dirname(fn)
	try:
		os.mkdir(dn, 0o750)
	except OSError:
		pass
	try:
		s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		try:
			s.connect(fn)
		finally:
			s.close()
	except socket.error:
		try:
			assert S_ISSOCK(os.lstat(fn).st_mode), fn + " exists as non-socket"
			os.unlink(fn)
		except OSError:
			pass
		return
	raise Exception("Socket %s already listening" % (fn,))

def siginfo(sig, frame):
	print_status_stacks()

def main(argv, config):
	g.running = 'server'

	parser = ArgumentParser(prog=argv.pop(0))
	parser.add_argument('--debug', action='store_true')
	options = parser.parse_args(argv)

	# all forks belong to the same happy family
	try:
		os.setpgrp()
	except OSError:
		print("Failed to create process group - there is probably already one (daemontools).", file=sys.stderr)

	# Set a low (but not too low) open file limit to make
	# dispatch.update_valid_fds faster.
	# The runners will set the highest limit they can
	# before actually running any methods.
	r1, r2 = resource.getrlimit(resource.RLIMIT_NOFILE)
	r1 = min(r1, r2, 1024)
	resource.setrlimit(resource.RLIMIT_NOFILE, (r1, r2))

	# Start the board in a separate process so it can't interfere.
	# Even if it dies we don't care.
	try:
		if not isinstance(config.board_listen, tuple):
			# Don't bother if something is already listening.
			check_socket(config.board_listen)
		Process(target=board.run, args=(config,), name='board').start()
	except Exception:
		pass

	iowrapper.main()

	# setup statmsg sink and tell address using ENV
	statmsg_rd, statmsg_wr = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
	os.environ['BD_STATUS_FD'] = str(statmsg_wr.fileno())
	def buf_up(fh, opt):
		sock = socket.fromfd(fh.fileno(), socket.AF_UNIX, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, opt, 256 * 1024)
		# does not close fh, because fromfd dups the fd (but not the underlying socket)
		sock.close()
	buf_up(statmsg_wr, socket.SO_SNDBUF)
	buf_up(statmsg_rd, socket.SO_RCVBUF)

	t = DeadlyThread(target=statmsg_sink, args=(statmsg_rd,), name="statmsg sink")
	t.daemon = True
	t.start()

	# do all main-stuff, i.e. run server
	sys.stdout = autoflush.AutoFlush(sys.stdout)
	sys.stderr = autoflush.AutoFlush(sys.stderr)
	atexit.register(exitfunction)
	signal.signal(signal.SIGTERM, exitfunction)
	signal.signal(signal.SIGINT, exitfunction)

	signal.signal(signal.SIGUSR1, siginfo)
	signal.siginterrupt(signal.SIGUSR1, False)
	if hasattr(signal, 'SIGINFO'):
		signal.signal(signal.SIGINFO, siginfo)
		signal.siginterrupt(signal.SIGINFO, False)

	if isinstance(config.listen, tuple):
		server = ThreadedHTTPServer(config.listen, XtdHandler)
	else:
		check_socket(config.listen)
		# We want the socket to be world writeable, protect it with dir permissions.
		u = os.umask(0)
		server = ThreadedUnixHTTPServer(config.listen, XtdHandler)
		os.umask(u)

	if config.get('urd_listen') == 'local':
		from accelerator import urd
		t = DeadlyThread(target=urd.main, args=(['urd', '--quiet', '--allow-passwordless'], config), name='urd')
		t.daemon = True
		t.start()

	ctrl = control.Main(config, options, config.url)
	print()
	ctrl.print_workdirs()
	print()

	XtdHandler.ctrl = ctrl
	job_tracking[None].workdir = ctrl.target_workdir

	for n in ("project_directory", "result_directory", "input_directory", "urd_listen"):
		if n == "urd_listen":
			dispn = "urd"
		else:
			dispn = n.replace("_", " ")
		print("%17s: %s" % (dispn, config.get(n),))
	print()

	print("Serving on %s\n" % (config.listen,), file=sys.stderr)
	server.serve_forever()
