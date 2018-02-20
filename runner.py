############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

# This runs once per python version the daemon supports methods for.
# On reload, it is killed and started again.
# When launching a method, it forks and calls the method (without any exec).
# Also contains the function that starts these (new_runners) and the dict
# of running ones {version: Runner}

from __future__ import print_function
from __future__ import division

from importlib import import_module
from types import ModuleType
from traceback import print_exc
import hashlib
import os
import sys
import re
import socket
import signal
import struct
import json
import io
import tarfile
import resource
import gc
from threading import Thread, Lock

from compat import PY2, PY3, iteritems, itervalues, pickle, Queue, QueueFull, str_types

from extras import DotDict
import dispatch

archives = {}

def load_methods(data):
	res_warnings = []
	res_failed = []
	res_hashes = {}
	res_params = {}
	for package, key in data:
		modname = '%s.a_%s' % (package, key)
		try:
			mod = import_module(modname)
			filename = mod.__file__
			if filename[-4:] in ('.pyc', '.pyo',):
				filename = filename[:-1]
			prefix = os.path.dirname(filename) + '/'
			with open(filename, 'rb') as fh:
				src = fh.read()
			tar_fh = io.BytesIO()
			tar_o = tarfile.open(mode='w:gz', fileobj=tar_fh, compresslevel=1)
			tar_o.add(filename, arcname='a_%s.py' % (key,))
			h = hashlib.sha1(src)
			hash = int(h.hexdigest(), 16)
			likely_deps = set()
			for k in dir(mod):
				v = getattr(mod, k)
				if isinstance(v, ModuleType):
					dep = getattr(v, '__file__', '')
					if dep.startswith(prefix):
						dep = os.path.basename(dep)
						if dep[-4:] in ('.pyc', '.pyo',):
							dep = dep[:-1]
						likely_deps.add(dep)
			hash_extra = 0
			for dep in getattr(mod, 'depend_extra', ()):
				if isinstance(dep, ModuleType):
					dep = dep.__file__
					if dep[-4:] in ('.pyc', '.pyo',):
						dep = dep[:-1]
				if isinstance(dep, str_types):
					dep = str(dep) # might be unicode on py2
					if not dep.startswith('/'):
						dep = prefix + dep
					with open(dep, 'rb') as fh:
						hash_extra ^= int(hashlib.sha1(fh.read()).hexdigest(), 16)
					bn = os.path.basename(dep)
					likely_deps.discard(bn)
					tar_o.add(dep, arcname=bn)
				else:
					raise Exception('Bad depend_extra in %s.a_%s: %r' % (package, key, dep,))
			for dep in likely_deps:
				res_warnings.append('%s.a_%s should probably depend_extra on %s' % (package, key, dep[:-3],))
			res_hashes[key] = ("%040x" % (hash ^ hash_extra,),)
			res_params[key] = params = DotDict()
			for name, default in (('options', {},), ('datasets', (),), ('jobids', (),),):
				params[name] = getattr(mod, name, default)
			equivalent_hashes = getattr(mod, 'equivalent_hashes', ())
			if equivalent_hashes:
				assert isinstance(equivalent_hashes, dict), 'Read the docs about equivalent_hashes'
				assert len(equivalent_hashes) == 1, 'Read the docs about equivalent_hashes'
				k, v = next(iteritems(equivalent_hashes))
				assert isinstance(k, str), 'Read the docs about equivalent_hashes'
				assert isinstance(v, tuple), 'Read the docs about equivalent_hashes'
				for v in v:
					assert isinstance(v, str), 'Read the docs about equivalent_hashes'
				start = src.index(b'equivalent_hashes')
				end   = src.index(b'}', start)
				h = hashlib.sha1(src[:start])
				h.update(src[end:])
				verifier = "%040x" % (int(h.hexdigest(), 16) ^ hash_extra,)
				if verifier in equivalent_hashes:
					res_hashes[key] += equivalent_hashes[verifier]
				else:
					res_warnings.append('%s.a_%s has equivalent_hashes, but missing verifier %s' % (package, key, verifier,))
			tar_o.close()
			tar_fh.seek(0)
			archives[key] = tar_fh.read()
		except Exception:
			print_exc()
			res_failed.append(modname)
			continue
	return res_warnings, res_failed, res_hashes, res_params

def launch_start(data):
	from launch import run
	if PY2:
		data = {k: v.encode('utf-8') if isinstance(v, unicode) else v for k, v in data.items()}
	prof_r, prof_w = os.pipe()
	# Disable the GC here, leaving it disabled in the child (the method).
	# The idea is that most methods do not actually benefit from the GC, but
	# they may well be significantly slowed by it.
	# Additionally, as seen in https://bugs.python.org/issue31558
	# the GC sometimes causes considerable extra COW after fork.
	# (If prepare_res is something GC-tracked.)
	# Once gc.freeze is available we will probably want to call that before
	# splitting the analysis processes if the method has re-enabled gc.
	try:
		gc.disable()
		child = os.fork()
		if not child: # we are the child
			try:
				os.setpgrp() # this pgrp is killed if the job fails
				os.close(prof_r)
				status_fd = int(os.getenv('BD_STATUS_FD'))
				keep = [prof_w, status_fd]
				dispatch.close_fds(keep)
				data['prof_fd'] = prof_w
				run(**data)
			finally:
				os._exit(0)
	except Exception:
		os.close(prof_r)
		raise
	finally:
		os.close(prof_w)
		gc.enable()
	return child, prof_r

def respond(cookie, data):
	res = pickle.dumps((cookie, data), 2)
	header = struct.pack('<cI', op, len(res))
	with sock_lock:
		sock.sendall(header + res)

def launch_finish(cookie, data):
	status = {'launcher': '[invalid data] (probably killed)'}
	result = None
	try:
		child, prof_r, workdir, jobid, method = data
		arc_name = os.path.join(workdir, jobid, 'method.tar.gz')
		with open(arc_name, 'wb') as fh:
			fh.write(archives[method])
		# We have closed prof_w. When child exits we get eof.
		prof = []
		while True:
			data = os.read(prof_r, 4096)
			if not data:
				break
			prof.append(data)
		try:
			status, result = json.loads(b''.join(prof).decode('utf-8'))
		except Exception:
			pass
	finally:
		os.close(prof_r)
		respond(cookie, (status, result))

# because a .recvall method is clearly too much to hope for
# (MSG_WAITALL doesn't really sound like the same thing to me)
def recvall(sock, z, fatal=False):
	data = []
	while z:
		tmp = sock.recv(z)
		if not tmp:
			if fatal:
				sys.exit(0)
			return
		data.append(tmp)
		z -= len(tmp)
	return b''.join(data)

class Runner(object):
	def __init__(self, pid, sock):
		self.pid = pid
		self.sock = sock
		self.cookie = 0
		self._waiters = {}
		self._lock = Lock()
		self._thread = Thread(
			target=self._receiver,
			name="%d receiver" % (pid,),
		)
		self._thread.daemon = True
		self._thread.start()

	# runs on it's own thread (in the daemon), one per Runner object
	def _receiver(self):
		while True:
			try:
				op, length = struct.unpack('<cI', recvall(self.sock, 5))
				data = recvall(self.sock, length)
				cookie, data = pickle.loads(data)
				q = self._waiters.pop(cookie)
				q.put(data)
			except Exception:
				break
		# All is lost, unblock anyone waiting
		for q in itervalues(self._waiters):
			try:
				q.put(None, block=False)
			except QueueFull:
				pass

	def kill(self):
		try:
			self.sock.close()
		except Exception:
			pass
		try:
			os.kill(self.pid, signal.SIGKILL)
		except Exception:
			pass
		try:
			os.waitpid(self.pid, 0)
		except Exception:
			pass

	def _waiter(self, cookie):
		q = Queue(1)
		self._waiters[cookie] = q
		return q.get

	# this is called from request threads, so needs locking to avoid races
	def _do(self, op, data):
		with self._lock:
			cookie = self.cookie
			self.cookie += 1
			# have to register waiter before we send packet (to avoid a race)
			waiter = self._waiter(cookie)
			data = pickle.dumps((cookie, data), 2)
			header = struct.pack('<cI', op, len(data))
			self.sock.sendall(header + data)
		# must wait without the lock, otherwise all this threading gets us nothing.
		res = waiter()
		if res is None:
			raise Exception("Runner exited unexpectedly.")
		return res

	def load_methods(self, data):
		return self._do(b'm', data)

	def launch_start(self, data):
		return self._do(b's', data)

	def launch_finish(self, child, prof_r, workdir, jobid, method):
		return self._do(b'f', (child, prof_r, workdir, jobid, method))

runners = {}
def new_runners(config):
	from dispatch import run
	if 'py' in runners:
		del runners['py']
	for runner in itervalues(runners):
		runner.kill()
	runners.clear()
	py_v = 'py3' if PY3 else 'py2'
	todo = {py_v: sys.executable}
	for k, v in iteritems(config):
		if re.match(r"py\d+$", k):
			todo[k] = v
	for k, py_exe in iteritems(todo):
		sock_p, sock_c = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
		cmd = [py_exe, './runner.py', str(sock_c.fileno())]
		pid = run(cmd, [sock_p.fileno()], [sock_c.fileno()], False)
		sock_c.close()
		runners[k] = Runner(pid=pid, sock=sock_p)
	runners['py'] = runners[py_v]
	return runners

if __name__ == "__main__":
	from autoflush import AutoFlush

	sys.stdout = AutoFlush(sys.stdout)
	sys.stderr = AutoFlush(sys.stderr)
	sock = socket.fromfd(int(sys.argv[1]), socket.AF_UNIX, socket.SOCK_STREAM)
	sock_lock = Lock()
	dispatch.update_valid_fds()

	# Set the highest open file limit we can.
	# At least OS X seems to like claiming no limit as max without
	# allowing that to be set, so let's do some retrying.
	r1, r2 = resource.getrlimit(resource.RLIMIT_NOFILE)
	limits = [500000, 100000, 50000, 10000, 5000, 1000, r1, r2]
	limits = sorted((v for v in limits if r1 <= v <= r2), reverse=True)
	for try_limit in limits:
		try:
			resource.setrlimit(resource.RLIMIT_NOFILE, (try_limit, r2))
			break
		except ValueError:
			pass
	r1, r2 = resource.getrlimit(resource.RLIMIT_NOFILE)
	if r1 < r2:
		print("WARNING: Failed to raise RLIMIT_NOFILE to %d. Set to %d." % (r2, r1,))
	if r1 < 5000:
		print("WARNING: RLIMIT_NOFILE is %d, that's not much." % (r1,))

	# sys.path needs to contain .. (the project dir), put it after accelerator
	sys.path.insert(1, os.path.dirname(sys.path[0]))

	while True:
		op, length = struct.unpack('<cI', recvall(sock, 5, True))
		data = recvall(sock, length, True)
		cookie, data = pickle.loads(data)
		if op == b'm':
			res = load_methods(data)
			respond(cookie, res)
		elif op == b's':
			res = launch_start(data)
			respond(cookie, res)
		elif op == b'f':
			# waits until job is done, so must run on a separate thread
			Thread(
				target=launch_finish,
				args=(cookie, data,),
				name=data[3], # jobid
			).start()
