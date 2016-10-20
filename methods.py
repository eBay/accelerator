from __future__ import print_function
from __future__ import division

import os
import hashlib
from importlib import import_module
from types import ModuleType
from datetime import datetime, date, time, timedelta
from linecache import clearcache
from traceback import print_exc
from imp import reload

from compat import iteritems, itervalues, first_value, NoneType, unicode, long

from extras import DotDict, OptionString, OptionEnum, OptionDefault, RequiredOption


class MethodLoadException(Exception):
	def __init__(self, lst):
		Exception.__init__(self, 'Failed to load ' + ', '.join(lst))
		self.module_list = lst

class Methods(object):
	def __init__(self, package_list, configfilename):
		self.package_list = package_list
		self.db = {}
		for package in self.package_list:
			tmp = read_method_conf(os.path.join(package, configfilename))
			for x in tmp:
				if x in self.db:
					print("METHOD:  ERROR, method \"%s\" defined both in \"%s\" and \"%s\"!" % (
						x, package, self.db[x]['package']))
					exit(1)
			for x in tmp.values():
				x['package'] = os.path.basename(package)
			self.db.update(tmp)
		# build dependency tree for all methods
		self.deptree = {}
		for method in self.db:
			self.deptree[method] = self._build_dep_tree(method, tree={})
		self.link = {k: v.get('link') for k, v in iteritems(self.db)}

	def _build_dep_tree(self, method, tree={}):
		if method not in self.db:
			print("METHOD:  Error, no such method exists: \"%s\"" % method)
			exit(1)
		dependencies = self.db[method].get('dep', [])
		tree.setdefault(method, {'dep' : dependencies, 'level' : -1, 'method' : method})
		if not dependencies:
			tree[method]['level'] = 0
		else:
			for dep in dependencies:
				self._build_dep_tree(dep, tree=tree)
				tree[method]['level'] = max(
					tree[method]['level'],
					tree[dep]['level']+1,
				)
		return tree

	def new_deptree(self, top_method):
		return self._build_dep_tree(top_method, tree={})



# Collect information on methods
class SubMethods(Methods):
	hash = {}
	params = {}
	typing = {}
	def __init__(self, package_list, configfilename):
		clearcache() # inspect module stupidly caches stuff
		super(SubMethods, self).__init__(package_list, configfilename)
		warnings = []
		failed = []
		for key, val in iteritems(self.db):
			package = val['package']
			filename = '%s/a_%s.py' % (package, key,)
			modname = '%s.a_%s' % (package, key)
			try:
				with open(filename, 'rb') as F:
					src = F.read()
				h = hashlib.sha1(src)
				hash = int(h.hexdigest(), 16)
				mod = import_module(modname)
				# Reload known dependencies first, so new symbols
				# in them don't break reloading of the method
				for dep in getattr(mod, 'depend_extra', ()):
					if isinstance(dep, ModuleType):
						reload(dep)
				mod = reload(mod)
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
			except Exception:
				print_exc()
				failed.append(modname)
				continue
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
				warnings.append('%s.a_%s should probably depend_extra on %s' % (package, key, dep[:-3],))
			self.hash[key] = ("%040x" % (hash ^ hash_extra,),)
			self.params[key] = params = DotDict()
			v1_style = v2_style = False
			for v1_name, v2_name, default in (('default_options', 'options', {},), ('input_datasets', 'datasets', (),), ('input_jobids', 'jobids', (),),):
				v1 = getattr(mod, v1_name, None)
				v2 = getattr(mod, v2_name, None)
				v1_style |= (v1 is not None)
				v2_style |= (v2 is not None)
				if callable(v1): # really, really old methods
					warnings.append("Ancient method %s.a_%s should not have callable %s" % (package, key, v1_name,))
					v1 = v1()
				params[v2_name] = v1 or v2 or default
			assert not v1_style or not v2_style, 'Specify either default_options/input_datasets/input_jobids or options/datasets/jobids in %s.a_%s' % (package, key,)
			params.old_style = v1_style
			self.typing[key] = options2typing(key, params.options)
			params.defaults = params2defaults(params)
			params.required = options2required(params.options)
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
					self.hash[key] += equivalent_hashes[verifier]
				else:
					warnings.append('%s.a_%s has equivalent_hashes, but missing verifier %s' % (package, key, verifier,))
		def prt(a, prefix):
			maxlen = (max(len(e) for e in a) + len(prefix))
			line = '=' * maxlen
			print()
			print(line)
			for e in sorted(a):
				msg = prefix + e
				print(msg + ' ' * (maxlen - len(msg)))
			print(line)
			print()
		if warnings:
			prt(warnings, 'WARNING: ')
		if failed:
			print('\033[47;31;1m')
			prt(failed, 'FAILED to import ')
			print('\033[m')
			raise MethodLoadException(failed)

	def params2optset(self, params):
		optset = set()
		for optmethod, method_params in iteritems(params):
			for group, d in iteritems(method_params):
				filled_in = dict(self.params[optmethod].defaults[group])
				filled_in.update(d)
				for optname, optval in iteritems(filled_in):
					optset.add('%s %s-%s %s' % (optmethod, group, optname, _reprify(optval),))
		return optset

def _reprify(o):
	if isinstance(o, OptionDefault):
		o = o.default
	if isinstance(o, (bytes, unicode, int, float, long, bool, NoneType)):
		return repr(o)
	if isinstance(o, set):
		return '{%s}' % (', '.join(map(_reprify, sorted(o))),)
	if isinstance(o, (list, tuple)):
		return '[%s]' % (', '.join(map(_reprify, o)),)
	if isinstance(o, dict):
		return '{%s}' % (', '.join('%s: %s' % (_reprify(k), _reprify(v),) for k, v in sorted(iteritems(o))),)
	if isinstance(o, (datetime, date, time, timedelta,)):
		return str(o)
	raise Exception('Unhandled %s in dependency resolution' % (type(o),))



def params2defaults(params):
	d = DotDict()
	for key in ('datasets', 'jobids',):
		r = {}
		for v in params[key]:
			if isinstance(v, list):
				r[v[0]] = []
			else:
				r[v] = None
		d[key] = r
	def fixup(item):
		if isinstance(item, dict):
			d = {k: fixup(v) for k, v in iteritems(item)}
			if len(d) == 1 and first_value(d) is None and first_value(item) is not None:
				return {}
			return d
		if isinstance(item, (list, tuple, set,)):
			l = [fixup(v) for v in item]
			if l == [None] and list(item) != [None]:
				l = []
			return type(item)(l)
		if isinstance(item, type):
			return None
		assert isinstance(item, (bytes, unicode, int, float, long, bool, OptionEnum, NoneType, datetime, date, time, timedelta)), type(item)
		return item
	def fixup0(item):
		if isinstance(item, RequiredOption):
			item = item.value
		if isinstance(item, OptionDefault):
			item = item.default
		return fixup(item)
	d.options = {k: fixup0(v) for k, v in iteritems(params.options)}
	return d


def options2required(options):
	res = set()
	def chk(key, value):
		if value is OptionString or isinstance(value, RequiredOption):
			res.add(key)
		elif isinstance(value, OptionEnum):
			if None not in value._valid:
				res.add(key)
		elif isinstance(value, dict):
			for v in itervalues(value):
				chk(key, v)
		elif isinstance(value, (list, tuple, set,)):
			for v in value:
				chk(key, v)
	for key, value in iteritems(options):
		chk(key, value)
	return res


def options2typing(method, options):
	from extras import JobWithFile
	res = {}
	def value2spec(value):
		if isinstance(value, list):
			if not value:
				return
			fmt = '[%s]'
			value = value[0]
		else:
			fmt = '%s'
		typ = None
		if value is JobWithFile or isinstance(value, JobWithFile):
			typ = 'JobWithFile'
		elif isinstance(value, set):
			typ = 'set'
		elif value in (datetime, date, time, timedelta,):
			return value.__name__
		elif isinstance(value, (datetime, date, time, timedelta,)):
			return type(value).__name__
		if typ:
			return fmt % (typ,)
	def collect(key, value, path=''):
		path = "%s/%s" % (path, key,)
		if isinstance(value, dict):
			for v in itervalues(value):
				collect('*', v, path)
			return
		spec = value2spec(value)
		assert res.get(path, spec) == spec, 'Method %s has incompatible types in options%s' % (method, path,)
		res[path] = spec
	for k, v in iteritems(options):
		collect(k, v)
	# reverse by key len, so something inside a dict always comes before
	# the dict itself. (We don't currently have any dict-like types, but we
	# might later.)
	return sorted(([k[1:], v] for k, v in iteritems(res) if v), key=lambda i: -len(i[0]))


def read_method_conf(filename, debug=False):
	""" read and parse the methods.conf file """
	db = {}
	with open(filename) as F:
		for line in F:
			data = line.split('#')[0]
			data = data.replace(',', ' ').split()
			if not data:
				continue
			l0 = data.pop(0)
			if l0 != "@":
				method = l0
				db.setdefault(method, DotDict(list, list))
			if data:
				cmd = data.pop(0)
				assert cmd in ('dep', 'link',), line
				db[method][cmd].extend(data)
	return db
