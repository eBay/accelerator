from __future__ import print_function
from __future__ import division

from importlib import import_module
from types import ModuleType
from traceback import print_exc
import hashlib
import os

from compat import iteritems

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
