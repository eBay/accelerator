############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

# This runs once per python version the server supports methods for.
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
import socket
import signal
import struct
import json
import io
import tarfile
import resource
import gc
import re
from threading import Thread, Lock

archives = {}

def mod2filename(mod):
	if isinstance(mod, ModuleType):
		filename = getattr(mod, '__file__', None)
		if filename and filename[-4:] in ('.pyc', '.pyo',):
			filename = filename[:-1]
		return filename
	else:
		return mod

def get_mod(modname):
	mod = import_module(modname)
	filename = mod2filename(mod)
	prefix = os.path.dirname(filename) + '/'
	return mod, filename, prefix

def path_prefix(paths):
	prefix = os.path.commonprefix(paths)
	# commonprefix works per character
	prefix = prefix.rsplit('/', 1)[0] + '/'
	return prefix

class MsgException(Exception):
	pass

def check_picklable(desc, value):
	from accelerator.compat import pickle
	try:
		pickle.dumps(value)
		return
	except Exception as e:
		msg = str(e)
	raise MsgException('Unpicklable %s: %s' % (desc, msg,))

def load_methods(all_packages, data):
	from accelerator.compat import str_types, iteritems
	from accelerator.extras import DotDict, OptionEnum, OptionEnumValue
	from accelerator.extras import RequiredOption, OptionDefault
	from accelerator import __version__ as ax_version
	res_warnings = []
	res_failed = []
	res_hashes = {}
	res_params = {}
	res_descriptions = {}
	def tar_add(name, data):
		assert name.startswith(dep_prefix)
		info = tarfile.TarInfo()
		info.name = name[len(dep_prefix):]
		info.size = len(data)
		tar_o.addfile(info, io.BytesIO(data))
	all_prefixes = set()
	# This is only used for informational warnings, so failure is not a
	# disaster. And failure is somewhat likely when using several runners
	# using the same-ish python version. (Maybe only on python 2.)
	# There appears to be a race where one python reads an incomplete
	# .pyc file written by the other.
	for package in all_packages:
		try:
			all_prefixes.add(get_mod(package)[2])
		except Exception:
			pass
	for package, key in data:
		modname = '%s.a_%s' % (package, key)
		try:
			mod, mod_filename, prefix = get_mod(modname)
			depend_extra = []
			for dep in getattr(mod, 'depend_extra', ()):
				dep = mod2filename(dep)
				if isinstance(dep, str_types):
					dep = str(dep) # might be unicode on py2
					if not dep.startswith('/'):
						dep = prefix + dep
					depend_extra.append(dep)
				else:
					raise MsgException('Bad depend_extra: %r' % (dep,))
			dep_prefix = os.path.commonprefix(depend_extra + [mod_filename])
			# commonprefix works per character (and commonpath is v3.5+)
			dep_prefix = dep_prefix.rsplit('/', 1)[0] + '/'
			with open(mod_filename, 'rb') as fh:
				src = fh.read()
			tar_fh = io.BytesIO()
			tar_o = tarfile.open(mode='w:gz', fileobj=tar_fh, compresslevel=1)
			tar_add(mod_filename, src)
			h = hashlib.sha1(src)
			hash = int(h.hexdigest(), 16)
			likely_deps = set()
			dep_names = {}
			for k in dir(mod):
				v = getattr(mod, k)
				if isinstance(v, ModuleType):
					filename = mod2filename(v)
					if filename:
						for cand_prefix in all_prefixes:
							if filename.startswith(cand_prefix):
								likely_deps.add(filename)
								dep_names[filename] = v.__name__
								break
			hash_extra = 0
			for dep in depend_extra:
				with open(dep, 'rb') as fh:
					data = fh.read()
				hash_extra ^= int(hashlib.sha1(data).hexdigest(), 16)
				tar_add(dep, data)
			for dep in (likely_deps - set(depend_extra)):
				res_warnings.append('%s.a_%s should probably depend_extra on %s' % (package, key, dep_names[dep],))
			res_hashes[key] = ("%040x" % (hash ^ hash_extra,),)
			res_params[key] = params = DotDict()
			# It would have been nice to be able to use ast.get_source_segment
			def find_source(name):
				bname = name.encode('ascii')
				try:
					start = src.find(b'\n' + bname + b' ')
					if start == -1:
						start = src.index(b'\n' + bname + b'=')
					def find_end(startchar, endchar):
						pos = src.index(startchar, start)
						nest = 0
						in_comment = False
						for pos, c in enumerate(src[pos:], pos):
							if in_comment:
								if c == b'\n'[0]:
									in_comment = False
							elif c == b'#'[0]:
								in_comment = True
							elif c == startchar:
								nest += 1
							elif c == endchar:
								nest -= 1
								if nest == 0:
									return pos
					end = None
					for c in src[start:]:
						if c == b'{'[0]:
							end = find_end(b'{'[0], b'}'[0])
							break
						elif c == b'('[0]:
							end = find_end(b'('[0], b')'[0])
							break
						elif c == b'['[0]:
							end = find_end(b'['[0], b']'[0])
							break
					if not end:
						print('Failed to figure out where %s is in %s' % (name, key,))
						end = start
					return slice(start, end)
				except Exception:
					return slice(0, 0)
			res_descriptions[key] = {
				'text': getattr(mod, 'description', '').strip(),
				'interpreter': sys.executable,
				'interpreter_version': sys.version,
				'accelerator_version': ax_version,
			}
			def fmtopt(v):
				if isinstance(v, type):
					return v.__name__
				elif isinstance(v, dict):
					return '{%s}' % (', '.join('%s: %s' % (fmtopt(k), fmtopt(v)) for k, v in v.items()),)
				elif isinstance(v, list):
					return '[%s]' % (', '.join(fmtopt(v) for v in v),)
				elif isinstance(v, OptionEnum):
					return '{%s}' % (', '.join(sorted(map(str, v._valid))),)
				elif isinstance(v, OptionEnumValue):
					return '%r {%s}' % (v, ', '.join(sorted(map(str, v._valid))),)
				elif isinstance(v, RequiredOption):
					return 'RequiredOption(%s%s)' % (fmtopt(v.value), ', none_ok=True' if v.none_ok else '',)
				elif isinstance(v, OptionDefault):
					if v.default is None:
						return 'OptionDefault(%s)' % (fmtopt(v.value),)
					return 'OptionDefault(%s, default=%s)' % (fmtopt(v.value), fmtopt(v.default),)
				else:
					return repr(v)
			for name, default in (('options', {},), ('datasets', (),), ('jobs', (),),):
				params[name] = d = getattr(mod, name, default)
				if d:
					items = {v[0] if isinstance(v, list) else v for v in params[name]}
					if isinstance(d, dict):
						res_descriptions[key][name] = items = {v: [fmtopt(d[v])] for v in items}
					else:
						res_descriptions[key][name] = items = {v: [] for v in items}
					src_part = src[find_source(name)].decode('utf-8', 'backslashreplace')
					item = None
					spill = []
					prev_item = None
					item_indent = 0
					for orig_line in src_part.split('\n'):
						line = orig_line.strip()
						if not line:
							continue
						itempart = line
						if line.startswith('['):
							itempart = line.split(']')[0][1:]
						if itempart.startswith("'"):
							item = itempart[1:itempart.index("'", 1)]
						elif itempart.startswith('"'):
							item = itempart[1:itempart.index('"', 1)]
						elif not line.startswith('#'):
							item = line.split()[0].split('=')[0]
							if item.startswith('[') and item.endswith(']'):
								item = item[1:-1]
						if item != prev_item:
							if spill and item in items:
								items[item].extend(spill)
							prev_item = item
							item_indent = len(re.match(r'^\s*', orig_line).group(0).expandtabs())
							spill = []
						if '#' in line:
							indent, value = orig_line.split('#', 1)
							value = value.strip()
							if value:
								indent = len(indent.expandtabs())
								if item in items and indent > item_indent:
									items[item].append(value)
								else:
									spill.append(value)
					if spill and item in items:
						items[item].extend(spill)
			for name in ('datasets', 'jobs',):
				d = res_descriptions[key].get(name)
				for item in getattr(mod, name, ()):
					if isinstance(item, list):
						d['[%s]' % (item[0],)] = d.pop(item[0])
			equivalent_hashes = getattr(mod, 'equivalent_hashes', ())
			if equivalent_hashes:
				try:
					assert isinstance(equivalent_hashes, dict)
					assert len(equivalent_hashes) == 1
					k, v = next(iteritems(equivalent_hashes))
					assert isinstance(k, str_types)
					if isinstance(v, str_types):
						v = (v,)
					assert isinstance(v, tuple)
					for vv in v:
						assert isinstance(vv, str_types)
						assert len(vv) == 40
				except AssertionError:
					raise MsgException('Read the docs about equivalent_hashes')
				if src.startswith(b'equivalent_hashes '):
					start = 0
				else:
					start = src.index(b'\nequivalent_hashes ') + 1
					assert start > 0, 'Failed to find equivalent_hashes in ' + mod_filename
				end   = src.index(b'}', start)
				h = hashlib.sha1(src[:start])
				h.update(src[end:])
				verifier = "%040x" % (int(h.hexdigest(), 16) ^ hash_extra,)
				if verifier == k:
					res_hashes[key] += v
				else:
					res_warnings.append('%s.a_%s has equivalent_hashes, but missing verifier %s' % (package, key, verifier,))
			tar_o.close()
			tar_fh.seek(0)
			archives[key] = tar_fh.read()
			check_picklable('options/datasets/jobs', res_params[key])
			check_picklable('description', res_descriptions[key])
		except Exception as e:
			if isinstance(e, MsgException):
				print('%s: %s' % (modname, str(e),))
			else:
				print_exc()
			res_failed.append(modname)
			for d in res_hashes, res_params, res_descriptions:
				d.pop(key, None)
			continue
	return res_warnings, res_failed, res_hashes, res_params, res_descriptions

def launch_start(data):
	from accelerator.launch import run
	from accelerator.compat import PY2
	from accelerator.dispatch import close_fds
	if PY2:
		data = {k: v.encode('utf-8') if isinstance(v, unicode) else v for k, v in data.items()}
	prof_r, prof_w = os.pipe()
	# Disable the GC here, leaving it disabled in the child (the method).
	# The idea is that most methods do not actually benefit from the GC, but
	# they may well be significantly slowed by it.
	# Additionally, as seen in https://bugs.python.org/issue31558
	# the GC sometimes causes considerable extra COW after fork.
	# (If prepare_res is something GC-tracked.)
	# On 3.7+ we also call gc.freeze before forking the analysis processes.
	try:
		gc.disable()
		child = os.fork()
		if not child: # we are the child
			try:
				os.setpgrp() # this pgrp is killed if the job fails
				os.close(prof_r)
				keep = [
					prof_w,
					int(os.getenv('BD_STATUS_FD')),
					int(os.getenv('BD_TERM_FD')),
				]
				close_fds(keep)
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
	from accelerator.compat import pickle
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
	def __init__(self, pid, sock, python):
		self.pid = pid
		self.sock = sock
		self.python = python
		self.cookie = 0
		self._waiters = {}
		self._lock = Lock()
		self._thread = Thread(
			target=self._receiver,
			name="%d receiver" % (pid,),
		)
		self._thread.daemon = True
		self._thread.start()

	# runs on it's own thread (in the server), one per Runner object
	def _receiver(self):
		from accelerator.compat import QueueFull, pickle, itervalues
		while True:
			try:
				hdr = recvall(self.sock, 5)
				if not hdr:
					break
				op, length = struct.unpack('<cI', hdr)
				data = recvall(self.sock, length)
				cookie, data = pickle.loads(data)
				q = self._waiters.pop(cookie)
				q.put(data)
			except Exception:
				print_exc()
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
		from accelerator.compat import Queue
		q = Queue(1)
		self._waiters[cookie] = q
		return q.get

	# this is called from request threads, so needs locking to avoid races
	def _do(self, op, data):
		from accelerator.compat import pickle
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

	def load_methods(self, all_packages, data):
		return self._do(b'm', (all_packages, data))

	def launch_start(self, data):
		return self._do(b's', data)

	def launch_finish(self, child, prof_r, workdir, jobid, method):
		return self._do(b'f', (child, prof_r, workdir, jobid, method))

	def launch_waitpid(self, child):
		return self._do(b'w', child)

runners = {}
def new_runners(config, used_versions):
	from accelerator.dispatch import run
	from accelerator.compat import itervalues, iteritems
	killed = set()
	for runner in itervalues(runners):
		if id(runner) not in killed:
			runner.kill()
			killed.add(id(runner))
	runners.clear()
	candidates = {'DEFAULT': sys.executable}
	for cnt in (1, 2, 3):
		candidates['.'.join(map(str, sys.version_info[:cnt]))] = sys.executable
	candidates.update(config.interpreters)
	todo = {k: v for k, v in candidates.items() if k in used_versions}
	exe2r = {}
	for k, py_exe in iteritems(todo):
		if py_exe in exe2r:
			runners[k] = exe2r[py_exe]
		else:
			sock_p, sock_c = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
			cmd = [py_exe, __file__, str(sock_c.fileno()), sys.path[0]]
			pid = run(cmd, [sock_p.fileno()], [sock_c.fileno()], False)
			sock_c.close()
			runners[k] = Runner(pid=pid, sock=sock_p, python=py_exe)
			exe2r[py_exe] = runners[k]
	return runners

if __name__ == "__main__":
	# sys.path needs to contain the project dir, but not the accelerator dir
	sys.path[0] = sys.argv[2]

	# As of python 3.8 the default start_method is 'spawn' on macOS.
	# This doesn't work for us. 'fork' is fairly unsafe on macOS,
	# but it's better than not working at all. See
	# https://bugs.python.org/issue33725
	# for more information.
	import multiprocessing
	if hasattr(multiprocessing, 'set_start_method'):
		multiprocessing.set_start_method('fork')

	from accelerator.autoflush import AutoFlush
	from accelerator.compat import pickle
	from accelerator.dispatch import update_valid_fds

	sys.stdout = AutoFlush(sys.stdout)
	sys.stderr = AutoFlush(sys.stderr)
	sock = socket.fromfd(int(sys.argv[1]), socket.AF_UNIX, socket.SOCK_STREAM)
	sock_lock = Lock()
	update_valid_fds()

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

	while True:
		op, length = struct.unpack('<cI', recvall(sock, 5, True))
		data = recvall(sock, length, True)
		cookie, data = pickle.loads(data)
		if op == b'm':
			res = load_methods(*data)
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
		elif op == b'w':
			# It would be nice to be able to just ignore children
			# (set SIGCHLD to SIG_IGN), but the server might want to
			# killpg the child, so we need it to stick around.
			os.waitpid(data, 0)
			respond(cookie, None)
