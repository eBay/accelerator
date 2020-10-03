############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
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

from __future__ import print_function
from __future__ import division

from collections import OrderedDict
from json import dumps
from datetime import datetime, date, time, timedelta

from accelerator.compat import iteritems, unicode, long, PY3

from accelerator.extras import DotDict, json_load, json_save, json_encode
from accelerator.job import Job

def generate(caption, method, params, package=None, description=None, why_build=False):
	data = DotDict()
	data.caption = caption
	data.method  = method
	if package:
		data.package = package
	data.versions = DotDict()
	if description:
		data.versions.python_path = description['interpreter']
		data.versions.python = description['interpreter_version']
		if description['accelerator_version']:
			data.versions.accelerator = description['accelerator_version']
	if why_build:
		data.why_build = why_build
	data.params = params
	return data

def load_setup(jobid):
	"""Loads but does not type setup.json from jobid.
	You probably want to use extras.job_params instead.
	"""
	d = json_load('setup.json', jobid)
	version = d.version
	if version == 1:
		d.jobs = d.pop('jobids')
		version = 2
	if version == 2:
		if 'exectime' not in d and 'profile' in d:
			d.exectime = d.pop('profile')
		d.versions = DotDict()
		python_path = d.pop('python', None)
		if python_path:
			d.versions.python_path = python_path
		version = 3
	if version == 3:
		if '_typing' in d:
			d['_typing'] = {d.method: d['_typing']}
		d.params = {d.method: DotDict({k: d[k] for k in ('options', 'datasets', 'jobs')})}
	else:
		raise Exception("Don't know how to load setup.json version %d (in %s)" % (d.version, jobid,))
	return d

def update_setup(jobid, **kw):
	data = load_setup(jobid)
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
	res = _encode_with_compact(
		copy(data),
		compact_keys=('starttime', 'endtime', 'exectime', '_typing',),
		special_keys=('options', 'datasets', 'jobs',),
	)
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

def _encode_with_compact(data, compact_keys, extra_indent=0, separator='\n', special_keys=()):
	compact = []
	special = []
	for k in compact_keys:
		if k in data:
			if k == 'exectime':
				d = _round_floats(data[k], 3)
				fmted = _encode_with_compact(d, ('analysis', 'per_slice',), 1, '')
			else:
				fmted = dumps(data[k])
			compact.append('    "%s": %s,' % (k, fmted,))
			del data[k]
	for k in special_keys:
		if k in data:
			fmted = dumps(data[k], indent=4, sort_keys=True)
			special.append('    "%s": %s' % (k, fmted.replace('\n', '\n    '),))
			del data[k]
	res = json_encode(data, as_str=True)
	if compact:
		res = '{\n%s%s%s' % ('\n'.join(compact), separator, res[1:],)
	if special:
		res = '%s,\n\n%s\n}' % (res[:-2], ',\n'.join(special),)
	res = res.replace('\n', ('\n' + ' ' * extra_indent * 4))
	return res

def save_setup(jobid, data):
	data = dict(data)
	data['version'] = 3
	data.update(data['params'][data['method']])
	del data['params']
	if '_typing' in data:
		data['_typing'] = data['_typing'][data['method']]
	filename = Job(jobid).filename('setup.json')
	json_save(data, filename, _encoder=encode_setup)
