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

from compat import PY3, iteritems, itervalues, pickle

from extras import DotDict

def load_methods(data):
	res_warnings = []
	res_failed = []
	res_hashes = {}
	res_params = {}
	for package, key in data:
		filename = '%s/a_%s.py' % (package, key,)
		modname = '%s.a_%s' % (package, key)
		try:
			with open(filename, 'rb') as fh:
				src = fh.read()
			h = hashlib.sha1(src)
			hash = int(h.hexdigest(), 16)
			mod = import_module(modname)
			prefix = os.path.dirname(mod.__file__) + '/'
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
				if isinstance(dep, str):
					if not dep.startswith('/'):
						dep = prefix + dep
					with open(dep, 'rb') as fh:
						hash_extra ^= int(hashlib.sha1(fh.read()).hexdigest(), 16)
					likely_deps.discard(os.path.basename(dep))
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
				start = src.index('equivalent_hashes')
				end   = src.index('}', start)
				h = hashlib.sha1(src[:start])
				h.update(src[end:])
				verifier = "%040x" % (int(h.hexdigest(), 16) ^ hash_extra,)
				if verifier in equivalent_hashes:
					res_hashes[key] += equivalent_hashes[verifier]
				else:
					res_warnings.append('%s.a_%s has equivalent_hashes, but missing verifier %s' % (package, key, verifier,))
		except Exception:
			print_exc()
			res_failed.append(modname)
			continue
	return res_warnings, res_failed, res_hashes, res_params

def launch(data):
	pass

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

	def load_methods(self, data):
		data = pickle.dumps(data, 2)
		header = struct.pack('<cI', b'm', len(data))
		self.sock.sendall(header + data)
		op, length = struct.unpack('<cI', recvall(self.sock, 5))
		data = recvall(self.sock, length)
		return pickle.loads(data)

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
	sock = socket.fromfd(int(sys.argv[1]), socket.AF_UNIX, socket.SOCK_STREAM)

	while True:
		op, length = struct.unpack('<cI', recvall(sock, 5, True))
		data = recvall(sock, length, True)
		data = pickle.loads(data)
		if op == b'm':
			res = load_methods(data)
		elif op == b'l':
			res = launch(data)
		res = pickle.dumps(res, 2)
		header = struct.pack('<cI', op, len(res))
		sock.sendall(header + res)
