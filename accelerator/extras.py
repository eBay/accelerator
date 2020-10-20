############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
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

import os
import datetime
import json
from traceback import print_exc
from collections import OrderedDict
import sys

from accelerator.compat import PY2, PY3, pickle, izip, iteritems, first_value
from accelerator.compat import num_types, uni, unicode, str_types

from accelerator.job import Job, JobWithFile
from accelerator.statmsg import status

def _fn(filename, jobid, sliceno):
	if filename.startswith('/'):
		assert not jobid, "Don't specify full path (%r) and jobid (%s)." % (filename, jobid,)
		assert not sliceno, "Don't specify full path (%r) and sliceno." % (filename,)
	elif jobid:
		filename = Job(jobid).filename(filename, sliceno)
	elif sliceno is not None:
		filename = '%s.%d' % (filename, sliceno,)
	return filename

def _typelistnone(v, t):
	if isinstance(v, list):
		return [t(v) if v else None for v in v]
	elif v:
		return t(v)
	else:
		return None

def _job_params(jobid):
	from accelerator.setupfile import load_setup
	d = load_setup(jobid)
	for method, tl in iteritems(d.get('_typing', {})):
		_apply_typing(d.params[method].options, tl)
	d.update(d.params[d.method])
	return d

def job_params(jobid=None, default_empty=False):
	if default_empty and not jobid:
		return DotDict(
			options=DotDict(),
			datasets=DotDict(),
			jobs=DotDict(),
		)
	from accelerator.dataset import Dataset
	from accelerator.job import Job
	d = _job_params(jobid)
	d.datasets = DotDict({k: _typelistnone(v, Dataset) for k, v in d.datasets.items()})
	d.jobs = DotDict({k: _typelistnone(v, Job) for k, v in d.jobs.items()})
	d.jobid = Job(d.jobid)
	return d

def job_post(jobid):
	job = Job(jobid)
	d = job.json_load('post.json')
	version = d.get('version', 0)
	if version == 0:
		prefix = job.path + '/'
		d.files = sorted(fn[len(prefix):] if fn.startswith(prefix) else fn for fn in d.files)
		version = 1
	if version != 1:
		raise Exception("Don't know how to load post.json version %d (in %s)" % (d.version, jobid,))
	return d

def pickle_save(variable, filename='result.pickle', sliceno=None, temp=None, _hidden=False):
	filename = _fn(filename, None, sliceno)
	with FileWriteMove(filename, temp, _hidden=_hidden) as fh:
		# use protocol version 2 so python2 can read the pickles too.
		pickle.dump(variable, fh, 2)

# default to encoding='bytes' because datetime.* (and probably other types
# too) saved in python 2 fail to unpickle in python 3 otherwise. (Official
# default is 'ascii', which is pretty terrible too.)
def pickle_load(filename='result.pickle', jobid=None, sliceno=None, encoding='bytes'):
	filename = _fn(filename, jobid, sliceno)
	with status('Loading ' + filename):
		with open(filename, 'rb') as fh:
			if PY3:
				return pickle.load(fh, encoding=encoding)
			else:
				return pickle.load(fh)


def json_encode(variable, sort_keys=True, as_str=False):
	"""Return variable serialised as json bytes (or str with as_str=True).

	You can pass tuples and sets (saved as lists).
	On py2 you can also pass bytes that will be passed through compat.uni.

	If you set sort_keys=False you can use OrderedDict to get whatever
	order you like.
	"""
	if sort_keys:
		dict_type = dict
	else:
		dict_type = OrderedDict
	def typefix(e):
		if isinstance(e, dict):
			return dict_type((typefix(k), typefix(v)) for k, v in iteritems(e))
		elif isinstance(e, (list, tuple, set,)):
			return [typefix(v) for v in e]
		elif PY2 and isinstance(e, bytes):
			return uni(e)
		else:
			return e
	variable = typefix(variable)
	res = json.dumps(variable, indent=4, sort_keys=sort_keys)
	if PY3 and not as_str:
		res = res.encode('ascii')
	return res

def json_save(variable, filename='result.json', sliceno=None, sort_keys=True, _encoder=json_encode, temp=False):
	filename = _fn(filename, None, sliceno)
	with FileWriteMove(filename, temp) as fh:
		fh.write(_encoder(variable, sort_keys=sort_keys))
		fh.write(b'\n')

def _unicode_as_utf8bytes(obj):
	if isinstance(obj, unicode):
		return obj.encode('utf-8')
	elif isinstance(obj, dict):
		return DotDict((_unicode_as_utf8bytes(k), _unicode_as_utf8bytes(v)) for k, v in iteritems(obj))
	elif isinstance(obj, list):
		return [_unicode_as_utf8bytes(v) for v in obj]
	else:
		return obj

def json_decode(s, unicode_as_utf8bytes=PY2):
	if unicode_as_utf8bytes:
		return _unicode_as_utf8bytes(json.loads(s))
	else:
		return json.loads(s, object_pairs_hook=DotDict)

def json_load(filename='result.json', jobid=None, sliceno=None, unicode_as_utf8bytes=PY2):
	filename = _fn(filename, jobid, sliceno)
	if PY3:
		with open(filename, 'r', encoding='utf-8') as fh:
			data = fh.read()
	else:
		with open(filename, 'rb') as fh:
			data = fh.read()
	return json_decode(data, unicode_as_utf8bytes)


def debug_print_options(options, title=''):
	print('-' * 53)
	if title:
		print('-', title)
		print('-' * 53)
	max_k = max(len(str(k)) for k in options)
	for key, val in sorted(options.items()):
		print("%s = %r" % (str(key).ljust(max_k), val))
	print('-' * 53)


def stackup():
	"""Returns (filename, lineno) for the first caller not in the accelerator dir."""

	from inspect import stack
	blacklist = os.path.dirname(__file__)
	for stk in stack()[1:]:
		if os.path.dirname(stk[1]) != blacklist:
			return stk[1], stk[2]
	return '?', -1

saved_files = {}

class FileWriteMove(object):
	"""with FileWriteMove(name, temp=None) as fh: ...
	Opens file with a temp name and renames it in place on exit if no
	exception occured. Tries to remove temp file if exception occured.

	The temp-level of the file is recorded in saved_files.
	"""
	def __init__(self, filename, temp=None, _hidden=False):
		from accelerator.g import running
		self.filename = filename
		self.tmp_filename = '%s.%dtmp' % (filename, os.getpid(),)
		if temp is None: # unspecified
			if running == 'analysis':
				print('WARNING: Should specify file permanence on %s line %d' % stackup(), file=sys.stderr)
		self.temp = temp
		if temp:
			if running != 'analysis':
				print('WARNING: Only analysis should make temp files (%s line %d).' % stackup(), file=sys.stderr)
		self._hidden = _hidden

	def __enter__(self):
		self._status = status('Saving ' + self.filename)
		self._status.__enter__()
		# stupid python3 feels that w and x are exclusive, while python2 requires both.
		fh = getattr(self, '_open', open)(self.tmp_filename, 'xb' if PY3 else 'wbx')
		self.close = fh.close
		return fh
	def __exit__(self, e_type, e_value, e_tb):
		self._status.__exit__(None, None, None)
		self.close()
		if e_type is None:
			os.rename(self.tmp_filename, self.filename)
			if not self._hidden:
				saved_files[self.filename] = self.temp
		else:
			try:
				os.unlink(self.tmp_filename)
			except Exception:
				print_exc()

class ResultIter(object):
	def __init__(self, slices):
		slices = range(slices)
		self._slices = iter(slices)
		tuple_len = pickle_load("Analysis.tuple")
		if tuple_len is False:
			self._is_tupled = False
		else:
			self._is_tupled = True
			self._loaders = [self._loader(ix, iter(slices)) for ix in range(tuple_len)]
			self._tupled = izip(*self._loaders)
	def __iter__(self):
		return self
	def _loader(self, ix, slices):
		for sliceno in slices:
			yield pickle_load("Analysis.%d." % (ix,), sliceno=sliceno)
	def __next__(self):
		if self._is_tupled:
			return next(self._tupled)
		else:
			return pickle_load("Analysis.", sliceno=next(self._slices))
	next = __next__

class ResultIterMagic(object):
	"""Wrap a ResultIter to give magic merging functionality,
	and so that you get an error if you attempt to use it after it is first
	exhausted. This is to avoid bugs, for example using analysis_res as if
	it was a list.
	"""

	def __init__(self, slices, reuse_msg="Attempted to iterate past end of iterator.", exc=Exception):
		self._inner = ResultIter(slices)
		self._reuse_msg = reuse_msg
		self._exc = exc
		self._done = False
		self._started = False

	def __iter__(self):
		return self

	def __next__(self):
		try:
			self._started = True
			item = next(self._inner)
		except StopIteration:
			if self._done:
				raise self._exc(self._reuse_msg)
			else:
				self._done = True
				raise
		return item
	next = __next__

	def merge_auto(self):
		"""Merge values from iterator using magic.
		Currenly supports data that has .update, .itervalues and .iteritems
		methods.
		If value has an .itervalues method the merge continues down to that
		level, otherwise the value will be overwritten by later slices.
		Don't try to use this if all your values don't have the same depth,
		or if you have empty dicts at the last level.
		"""
		if self._started:
			raise self._exc("Will not merge after iteration started")
		if self._inner._is_tupled:
			return (self._merge_auto_single(it, ix) for ix, it in enumerate(self._inner._loaders))
		else:
			return self._merge_auto_single(self, -1)

	def _merge_auto_single(self, it, ix):
		# find a non-empty one, so we can look at the data in it
		data = next(it)
		if isinstance(data, num_types):
			# special case for when you have something like (count, dict)
			return sum(it, data)
		if isinstance(data, list):
			for part in it:
				data.extend(part)
			return data
		while not data:
			try:
				data = next(it)
			except StopIteration:
				# All were empty, return last one
				return data
		depth = 0
		to_check = data
		while hasattr(to_check, "values"):
			if not to_check:
				raise self._exc("Empty value at depth %d (index %d)" % (depth, ix,))
			to_check = first_value(to_check)
			depth += 1
		if hasattr(to_check, "update"): # like a set
			depth += 1
		if not depth:
			raise self._exc("Top level has no .values (index %d)" % (ix,))
		def upd(aggregate, part, level):
			if level == depth:
				aggregate.update(part)
			else:
				for k, v in iteritems(part):
					if k in aggregate:
						upd(aggregate[k], v, level + 1)
					else:
						aggregate[k] = v
		for part in it:
			upd(data, part, 1)
		return data


class DotDict(dict):
	"""Like a dict, but with d.foo as well as d['foo'].
	(Names beginning with _ will have to use d['_foo'] syntax.)
	The normal dict.f (get, items, ...) still return the functions.
	"""

	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		return self[name]

	def __setattr__(self, name, value):
		if name[0] == "_":
			raise AttributeError(name)
		self[name] = value

	def __delattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		del self[name]

class _ListTypePreserver(list):
	"""Base class to inherit from in list subclasses that want their custom type preserved when slicing."""

	def __getslice__(self, i, j):
		return self[slice(i, j)]

	def __getitem__(self, item):
		if isinstance(item, slice):
			return self.__class__(list.__getitem__(self, item))
		else:
			return list.__getitem__(self, item)

	def __add__(self, other):
		return self.__class__(list(self) + other)

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))

class OptionEnumValue(str):
	@staticmethod
	def _mktype(name, valid, prefixes):
		return type('OptionEnumValue' + name, (OptionEnumValue,), {'_valid': valid, '_prefixes': prefixes})

	# be picklable
	def __reduce__(self):
		return _OptionEnumValue_restore, (self.__class__.__name__[15:], str(self), self._valid, self._prefixes)

def _OptionEnumValue_restore(name, value, valid, prefixes):
	return OptionEnumValue._mktype(name, valid, prefixes)(value)

class OptionEnum(object):
	"""A little like Enum in python34, but string-like.
	(For JSONable method option enums.)

	>>> foo = OptionEnum('a b c*')
	>>> foo.a
	'a'
	>>> foo.a == 'a'
	True
	>>> foo.a == foo['a']
	True
	>>> isinstance(foo.a, OptionEnumValue)
	True
	>>> isinstance(foo['a'], OptionEnumValue)
	True
	>>> foo['cde'] == 'cde'
	True
	>>> foo['abc']
	Traceback (most recent call last):
	...
	KeyError: 'abc'

	Pass either foo (for a default of None) or one of the members
	as the value in options{}. You get a string back, which compares
	equal to the member of the same name.

	Set none_ok if you accept None as the value.

	If a value ends in * that matches all endings. You can only access
	these as foo['cde'] (for use in options{}).
	"""

	def __new__(cls, values, none_ok=False):
		if isinstance(values, str_types):
			values = values.replace(',', ' ').split()
		values = list(values)
		if PY2:
			values = [v.encode('utf-8') if isinstance(v, unicode) else v for v in values]
		valid = set(values)
		prefixes = []
		for v in values:
			if v.endswith('*'):
				prefixes.append(v[:-1])
		if none_ok:
			valid.add(None)
		name = ''.join(v.title() for v in values)
		sub = OptionEnumValue._mktype(name, valid, prefixes)
		d = {}
		for value in values:
			d[value] = sub(value)
		d['_values'] = values
		d['_valid'] = valid
		d['_prefixes'] = prefixes
		d['_sub'] = sub
		return object.__new__(type('OptionEnum' + name, (cls,), d))
	def __getitem__(self, name):
		try:
			return getattr(self, name)
		except AttributeError:
			for cand_prefix in self._prefixes:
				if name.startswith(cand_prefix):
					return self._sub(name)
			raise KeyError(name)
	# be picklable
	def __reduce__(self):
		return OptionEnum, (self._values, None in self._valid)

class _OptionString(str):
	"""Marker value to specify in options{} for requiring a non-empty string.
	You can use plain OptionString, or you can use OptionString('example'),
	without making 'example' the default.
	"""
	def __call__(self, example):
		return _OptionString(example)
	def __repr__(self):
		if self:
			return 'OptionString(%r)' % (str(self),)
		else:
			return 'OptionString'
OptionString = _OptionString('')

class RequiredOption(object):
	"""Specify that this option is mandatory (that the caller must specify a value).
	None is accepted as a specified value if you pass none_ok=True.
	"""
	def __init__(self, value, none_ok=False):
		self.value = value
		self.none_ok = none_ok

class OptionDefault(object):
	"""Default selection for complexly typed options.
	foo={'bar': OptionEnum(...)} is a mandatory option.
	foo=OptionDefault({'bar': OptionEnum(...)}) isn't.
	(Default None unless specified.)
	"""
	def __init__(self, value, default=None):
		self.value = value
		self.default = default

typing_conv = dict(
	set=set,
	JobWithFile=lambda a: JobWithFile(*a),
	datetime=lambda a: datetime.datetime(*a),
	date=lambda a: datetime.date(*a[:3]),
	time=lambda a: datetime.time(*a[3:]),
	timedelta=lambda a: datetime.timedelta(seconds=a),
)

def _mklist(t):
	def make(lst):
		return [t(e) for e in lst]
	return make

def _apply_typing(options, tl):
	for k, t in tl:
		if t.startswith('['):
			assert t.endswith(']')
			t = _mklist(typing_conv[t[1:-1]])
		else:
			t = typing_conv[t]
		d = options
		k = k.split('/')
		for kk in k[:-1]:
			d = d[kk]
		k = k[-1]
		if k == '*':
			for k, v in d.items():
				d[k] = None if v is None else t(v)
		else:
			v = d[k]
			if v is not None:
				v = t(v)
			d[k] = v
