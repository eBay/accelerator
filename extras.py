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

from __future__ import print_function
from __future__ import division

import os
import time
import datetime
import json
from traceback import print_exc
from collections import namedtuple
from sys import stderr, argv

from compat import PY3, pickle, izip, iteritems, first_value, num_types

from jobid import resolve_jobid_filename
from status import status

def full_filename(filename, ext, sliceno=None, jobid=None):
	if not filename or not filename[0]:
		# Fallback to default in calling function
		return None
	if isinstance(filename, JobWithFile):
		if jobid:
			raise Exception("Don't specify a jobid when passing a JobWithFile as filename")
		if sliceno is None:
			assert not filename.sliced, "A sliced file requires a sliceno"
		else:
			assert filename.sliced, "An unsliced file can not have a sliceno"
		jobid, filename = filename[:2]
	if not filename.endswith(ext):
		filename += ext
	if sliceno is not None:
		filename = filename.replace(ext, '%02d' % (int(sliceno),)) + ext
	if jobid is not None:
		filename = resolve_jobid_filename(jobid, filename)
	return filename

def job_params(jobid=None, default_empty=False):
	if default_empty and not jobid:
		return DotDict(
			options=DotDict(),
			datasets=DotDict(),
			jobids=DotDict(),
		)
	d = json_load('setup.json', jobid)
	for method, tl in iteritems(d.get('_typing', {})):
		_apply_typing(d.params[method].options, tl)
	d.update(d.params[d.method])
	return d

def job_post(jobid):
	return json_load('post.json', jobid)

def pickle_save(variable, filename='result', sliceno=None, temp=None):
	filename = full_filename(filename, '.pickle', sliceno)
	if temp == Temp.DEBUG and temp is not True and '--debug' not in argv:
		return
	with FileWriteMove(filename, temp) as fh:
		# use protocol version 2 so python2 can read the pickles too.
		pickle.dump(variable, fh, 2)

# default to encoding='bytes' because datetime.* (and probably other types
# too) saved in python 2 fail to unpickle in python 3 otherwise. (Official
# default is 'ascii', which is pretty terrible too.)
def pickle_load(filename='result', jobid='', sliceno=None, verbose=False, default=None, encoding='bytes'):
	filename = full_filename(filename, '.pickle', sliceno, jobid)
	if not filename and default is not None:
		return default
	if verbose:
		print('Pickle load "%s" ... ' % (filename,), end='')
		t0 = time.time()
	try:
		with status('Loading ' + filename):
			with open(filename, 'rb') as fh:
				if PY3:
					ret = pickle.load(fh, encoding=encoding)
				else:
					ret = pickle.load(fh)
	except IOError:
		if default is not None:
			return default
		raise
	if verbose:
		print('done (%f seconds).' % (time.time()-t0,))
	return ret


def json_encode(variable, sort_keys=True, as_str=False):
	if sort_keys:
		def enc_elem(e):
			if isinstance(e, dict):
				return {k: enc_elem(v) for k, v in iteritems(e)}
			elif isinstance(e, (list, tuple, set,)):
				return [enc_elem(v) for v in e]
			elif PY3:
				return e
			elif hasattr(e, 'encode'):
				return e.encode('ascii')
			else:
				return e
		variable = enc_elem(variable)
	res = json.dumps(variable, indent=4, sort_keys=sort_keys)
	if PY3 and not as_str:
		res = res.encode('ascii')
	return res

def json_save(variable, filename='result', jobid=None, sliceno=None, sort_keys=True, _encoder=json_encode, temp=False):
	filename = full_filename(filename, '.json', sliceno, jobid)
	with FileWriteMove(filename, temp) as fh:
		fh.write(_encoder(variable, sort_keys=sort_keys))
		fh.write(b'\n')

def json_decode(s):
	return json.loads(s, object_pairs_hook=_json_hook)

def json_load(filename='result', jobid='', sliceno=None, default=None):
	filename = full_filename(filename, '.json', sliceno, jobid)
	if not filename and default is not None:
		return default
	try:
		with open(filename, 'r') as fh:
			data = fh.read()
	except IOError:
		if default is not None:
			return default
		raise
	return json_decode(data)


def debug_print_options(options, title=''):
	print('-' * 53)
	if title:
		print('-', title)
		print('-' * 53)
	max_k = max(len(str(k)) for k in options)
	for key, val in sorted(options.items()):
		print("%s = %r" % (str(key).ljust(max_k), val))
	print('-' * 53)

def symlink(filename, destpath):
	dest_fn = os.path.join(destpath, filename)
	try:
		os.remove(dest_fn + '_')
	except OSError:
		pass
	os.symlink(os.path.abspath(filename), dest_fn + '_')
	os.rename(dest_fn + '_', dest_fn)


def printresult(v, path, stdout=True):
	"""
	v = [ (string, filename), ...]

	print string to stdout (if True), and to file in directory specified by path.
	typically, path is set to RESULT_DIRECTORY

	"""
	for s, fname in v:
		if stdout:
			print(s)
		with open(fname, 'wb') as F:
			F.write(s)
		symlink(fname, path)


def stackup():
	"""Returns (filename, lineno) for the first caller not
	in the same file as the caller of this function"""

	from inspect import stack
	blacklist = None
	for stk in stack()[1:]:
		if blacklist:
			if stk[1] != blacklist:
				return stk[1], stk[2]
		else:
			blacklist = stk[1]
	return '?', -1

class Temp:
	# File temporaryness constants
	# (need to have these values to work in the cleanup code)
	# You can use True and False for TEMP and PERMANENT when calling.
	PERMANENT = 0 # aka False
	DEBUG     = 1 # only saved if --debug
	DEBUGTEMP = 2 # always saved, only preserved if --debug
	TEMP      = 3 # aka True

saved_files = {}

class FileWriteMove(object):
	"""with FileWriteMove(name, temp=None) as fh: ...
	Opens file with a temp name and renames it in place on exit if no
	exception occured. Tries to remove temp file if exception occured.

	The temp-level of the file is recorded in saved_files.
	"""
	def __init__(self, filename, temp=None):
		from g import running
		self.filename = filename
		self.tmp_filename = '%s.%dtmp' % (filename, os.getpid(),)
		temp = {'False': Temp.PERMANENT, 'True': Temp.TEMP}.get(temp, temp)
		if temp is None: # unspecified
			temp = Temp.PERMANENT
			if running == 'analysis':
				print('WARNING: Should specify file permanence on %s line %d' % stackup(), file=stderr)
		assert temp in range(4), 'temp should be True, False or a value from Temp'
		self.temp = temp
		if temp in (Temp.DEBUGTEMP, Temp.TEMP,):
			if running != 'analysis':
				print('WARNING: Only analysis should make temp files (%s line %d).' % stackup(), file=stderr)

	def __enter__(self):
		self._status = status('Saving ' + self.filename)
		self._status.__enter__()
		# stupid python3 feels that w and x are exclusive, while python2 requires both.
		fh = open(self.tmp_filename, 'xb' if PY3 else 'wbx')
		self.close = fh.close
		return fh
	def __exit__(self, e_type, e_value, e_tb):
		self._status.__exit__(None, None, None)
		self.close()
		if e_type is None:
			os.rename(self.tmp_filename, self.filename)
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
	d.foo returns '' for unset values by default, but you can specify
	_attr_default and _item_default constructors (or None to get errors).
	Normally you should specify _default to set them both to the same thing.
	The normal dict.f (get, items, ...) still return the functions.
	"""

	def __init__(self, *a, **kw):
		have = set()
		if '_attr_default' in kw:
			attr_default = kw.pop('_attr_default')
			have.add('attr')
		else:
			attr_default = str
		if '_item_default' in kw:
			item_default = kw.pop('_item_default')
			have.add('item')
		else:
			item_default = None
		if '_default' in kw:
			assert not have, 'Specify either _default or _attr_default + _item_default'
			attr_default = item_default = kw.pop('_default')
			have = {'attr', 'item'}
		if len(a) == 2: # contructors, not a dict, we assume
			assert not have, 'Specify either _default or _attr_default + _item_default'
			attr_default, item_default = a
			have = {'attr', 'item'}
			a = ()
		assert attr_default is None or callable(attr_default)
		assert item_default is None or callable(item_default)
		dict.__setattr__(self, '_attr_default', attr_default)
		dict.__setattr__(self, '_item_default', item_default)
		dict.__init__(self, *a, **kw)
	__setattr__ = dict.__setitem__
	__delattr__ = dict.__delitem__
	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		if name not in self:
			default = dict.__getattribute__(self, '_attr_default')
			if not default:
				raise AttributeError(name)
			self[name] = default()
		return dict.__getitem__(self, name)
	def __getitem__(self, name):
		if name not in self:
			default = dict.__getattribute__(self, '_item_default')
			if not default:
				raise KeyError(name)
			self[name] = default()
		return dict.__getitem__(self, name)

class OptionEnumValue(str):
	# be picklable
	def __reduce__(self):
		return type, (self.__class__.__name__, (OptionEnumValue,), {'_valid': self._valid, '_prefixes': self._prefixes})
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
		if isinstance(values, str):
			values = values.replace(',', ' ').split()
		values = list(values)
		valid = set(values)
		prefixes = []
		for v in values:
			if v.endswith('*'):
				prefixes.append(v[:-1])
		if none_ok:
			valid.add(None)
		name = ''.join(v.title() for v in values)
		sub = type('OptionEnumValue' + name, (OptionEnumValue,), {'_valid': valid, '_prefixes': prefixes})
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

def _OptionStringUnpickle():
	return OptionString
_OptionStringUnpickle.__safe_for_unpickling__ = True
class OptionString(str):
	"""Marker value to specify in options{} for requiring a non-empty string.
	You can use plain OptionString, or you can use OptionString('example'),
	without making 'example' the default.
	"""
	def __call__(self, example):
		return self
	# Be the same object after unpickling
	def __reduce__(self):
		return _OptionStringUnpickle, ()
OptionString = OptionString('<OptionString>')

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

JobWithFile = namedtuple('JobWithFile', 'jobid filename sliced extra')
JobWithFile.__new__.__defaults__ = (None, None, False, None)

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
if PY3:
	_json_hook = DotDict
else:
	# I wish we were using python 3..
	def _json_hook(seq):
		def enc(v):
			if isinstance(v, unicode):
				return v.encode('utf-8')
			if isinstance(v, list):
				return [enc(e) for e in v]
			return v
		return DotDict((enc(k), enc(v)) for k, v in seq)

