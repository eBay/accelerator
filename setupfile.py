from __future__ import print_function
from __future__ import division

import os
from collections import OrderedDict
from json import dumps
from datetime import datetime, date, time, timedelta

from compat import iteritems, unicode, long, PY3

from extras import DotDict, json_load, json_save, json_encode

class SetupCompat:
	"""A regrettable (compatible) API around job options"""

	def __init__(self, dir_or_dict):
		if isinstance(dir_or_dict, dict):
			self.data = DotDict(dir_or_dict)
		else:
			fn = os.path.join(dir_or_dict, 'setup.json')
			self.data = json_load(fn)

	def method(self):
		return self.data.method

	def package(self):
		return self.data.package

	def caption(self):
		return self.data.caption

	def alloptions(self):
		res = {}
		for method, d in iteritems(self.data.params):
			part = DotDict(d.options)
			part.update(("dataset-" + k, v) for k, v in iteritems(d.datasets))
			part.update(("jobid-" + k, v) for k, v in iteritems(d.jobids))
			res[method] = part
		return res

	def options(self, default_options={}):
		res = DotDict(default_options)
		res.update(self.data.params[self.data.method].options)
		return res

	def inheritoptions(self, method):
		return self.data.params[method].options

	def alldepopts(self):
		res = DotDict()
		for method, d in iteritems(self.data.params):
			res.update(("%s:%s" % (method, k), v) for k, v in iteritems(d.options))
		return res

	def alldepjobids(self):
		return self.data.link

setup = setup_from_string = SetupCompat

def generate(caption, method, params, link=None, package=None, why_build=False):
	data = DotDict()
	data.caption = caption
	data.method  = method
	if link is not None:
		data.link = link
	if package:
		data.package = package
	if why_build:
		data.why_build = why_build
	data.params = params
	return data

def update_setup(jobid, **kw):
	data = json_load('setup.json', jobid=jobid)
	data.update(kw)
	save_setup(jobid, data)
	return data

# It's almost worth making your own json encoder. Almost.
def _sorted_set(s):
	# like sorted(s), except None is ok.
	if None in s:
		s = set(s)
		s.remove(None)
		res = sorted(s)
		res.append(None)
		return res
	else:
		return sorted(s)

def encode_setup(data, sort_keys=True, as_str=False):
	def copy(src):
		if isinstance(src, dict):
			dst = OrderedDict()
			for k in sorted(src):
				dst[k] = copy(src[k])
			return dst
		elif isinstance(src, (list, tuple,)):
			return [copy(v) for v in src]
		elif isinstance(src, set):
			return [copy(v) for v in _sorted_set(src)]
		elif isinstance(src, datetime):
		    return [src.year, src.month, src.day, src.hour, src.minute, src.second, src.microsecond]
		elif isinstance(src, date):
		    return [src.year, src.month, src.day]
		elif isinstance(src, time):
		    return [1970, 1, 1, src.hour, src.minute, src.second, src.microsecond]
		elif isinstance(src, timedelta):
		    return src.total_seconds()
		else:
			assert isinstance(src, (str, unicode, int, float, long, bool)) or src is None, type(src)
			return src
	data = copy(data)
	res = _encode_with_compact(copy(data), ('starttime', 'endtime', 'profile', '_typing',))
	if PY3 and not as_str:
		res = res.encode('ascii')
	return res

def _round_floats(d, ndigits):
	res = OrderedDict()
	for k, v in iteritems(d):
		if isinstance(v, float):
			v = round(v, ndigits)
		if isinstance(v, dict):
			v = _round_floats(v, ndigits)
		if isinstance(v, list):
			v = [round(e, ndigits) if isinstance(e, float) else e for e in v]
		res[k] = v
	return res

def _encode_with_compact(data, compact_keys, extra_indent=0, separator='\n'):
	compact = []
	for k in compact_keys:
		if k in data:
			if k == 'profile':
				d = _round_floats(data[k], 3)
				fmted = _encode_with_compact(d, ('analysis', 'per_slice',), 1, '')
			else:
				fmted = dumps(data[k])
			compact.append('    "%s": %s,' % (k, fmted,))
			del data[k]
	res = json_encode(data, as_str=True)
	if compact:
		res = '{\n%s%s%s' % ('\n'.join(compact), separator, res[1:],)
	res = res.replace('\n', ('\n' + ' ' * extra_indent * 4))
	return res

def save_setup(jobid, data):
	json_save(data, 'setup.json', jobid=jobid, _encoder=encode_setup)
