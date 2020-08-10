############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

from traceback import print_exc
from datetime import datetime, date, time, timedelta

from accelerator.compat import iteritems, itervalues, first_value, str_types, int_types, num_types, unicode

from accelerator.extras import OptionEnum, OptionEnumValue, _OptionString, OptionDefault, RequiredOption, typing_conv
from accelerator.job import JobWithFile

class OptionException(Exception):
	pass

_date_types = (datetime, date, time, timedelta)

class DepTree:

	def __init__(self, methods, setup):
		tree = methods.new_deptree(setup.method)
		self.methods = methods
		self.top_method = setup.method
		self.tree = tree
		self.add_flags({'make' : False, 'link' : False, })
		seen = set()
		for method, data in iteritems(self.tree):
			seen.add(method)
			data['params'] = {method: setup.params[method]}
		unmatched = {method: params for method, params in iteritems(setup.params) if method not in seen}
		if unmatched:
			from accelerator.extras import json_encode
			print("DepTree Warning:  Unmatched options remain:", json_encode(unmatched, as_str=True))
		def collect(method):
			# All methods that method depend on
			for child in tree[method]['dep']:
				yield child
				for method in collect(child):
					yield method
		# This probably updates some with the same data several times,
		# but this is cheap (key: dictref updates, nothing more.)
		for method, data in iteritems(self.tree):
			for submethod in set(collect(method)):
				data['params'].update(tree[submethod]['params'])
		self._fix_options(False)
		self._fix_jobids('jobs')
		self._fix_jobids('datasets')

	def add_flags(self, flags):
		uid = 0
		for x, y in self.tree.items():
			y.update(flags)
			y.update({'uid' : uid, })
			uid += 1

	def get_reqlist(self):
		for method, data in self.tree.items():
			full_params = {}
			for submethod, given_params in iteritems(data['params']):
				params = {k: dict(v) for k, v in iteritems(self.methods.params[submethod].defaults)}
				for k, v in iteritems(given_params):
					params[k].update(v)
				full_params[submethod] = params
			yield method, data['uid'], self.methods.params2optset(full_params)

	def fill_in_default_options(self):
		self._fix_options(True)

	def _fix_jobids(self, key):
		for method, data in iteritems(self.tree):
			method_params = data['params'][method]
			data = method_params[key]
			method_wants = self.methods.params[method][key]
			res = {}
			for jobid_name in method_wants:
				if isinstance(jobid_name, str_types):
					value = data.get(jobid_name)
					assert value is None or isinstance(value, str), 'Input %s on %s not a string as required' % (jobid_name, method,)
				elif isinstance(jobid_name, list):
					if len(jobid_name) != 1 or not isinstance(jobid_name[0], str_types):
						raise OptionException('Bad %s item on %s: %s' % (key, method, repr(jobid_name),))
					jobid_name = jobid_name[0]
					value = data.get(jobid_name)
					if value:
						if isinstance(value, str_types):
							value = [e.strip() for e in value.split(',')]
					else:
						value = []
					assert isinstance(value, list), 'Input %s on %s not a list or string as required' % (jobid_name, method,)
				else:
					raise OptionException('%s item of unknown type %s on %s: %s' % (key, type(jobid_name), method, repr(jobid_name),))
				res[jobid_name] = value
			method_params[key] = res
			spill = set(data) - set(res)
			if spill:
				raise OptionException('Unknown %s on %s: %s' % (key, method, ', '.join(sorted(spill)),))

	def _fix_options(self, fill_in):
		for method, data in iteritems(self.tree):
			data = data['params'][method]
			options = self.methods.params[method].options
			res_options = {}
			def typefuzz(t):
				if issubclass(t, str_types):
					return str_types
				if issubclass(t, int_types):
					return int_types
				return t
			def convert(default_v, v):
				if isinstance(default_v, RequiredOption):
					if v is None and not default_v.none_ok:
						raise OptionException('Option %s on method %s requires a non-None value (%r)' % (k, method, default_v.value,))
					default_v = default_v.value
				if default_v is None or v is None:
					if isinstance(default_v, _OptionString):
						raise OptionException('Option %s on method %s requires a non-empty string value' % (k, method,))
					if hasattr(default_v, '_valid') and v not in default_v._valid:
						raise OptionException('Option %s on method %s requires a value in %s' % (k, method, default_v._valid,))
					if isinstance(default_v, OptionDefault):
						v = default_v.default
					return v
				if isinstance(default_v, OptionDefault):
					default_v = default_v.value
				if isinstance(default_v, dict) and isinstance(v, dict):
					if default_v:
						sample_v = first_value(default_v)
						for chk_v in itervalues(default_v):
							assert isinstance(chk_v, type(sample_v))
						return {k: convert(sample_v, v) for k, v in iteritems(v)}
					else:
						return v
				if isinstance(default_v, (list, set, tuple,)) and isinstance(v, str_types + (list, set, tuple,)):
					if isinstance(v, str_types):
						v = (e.strip() for e in v.split(','))
					if default_v:
						sample_v = first_value(default_v)
						for chk_v in default_v:
							assert isinstance(chk_v, type(sample_v))
						v = (convert(sample_v, e) for e in v)
					return type(default_v)(v)
				if isinstance(default_v, (OptionEnum, OptionEnumValue,)):
					if not (v or None) in default_v._valid:
						ok = False
						for cand_prefix in default_v._prefixes:
							if v.startswith(cand_prefix):
								ok = True
								break
						if not ok:
							raise OptionException('%r not a permitted value for option %s on method %s (%s)' % (v, k, method, default_v._valid))
					return v or None
				if isinstance(default_v, str_types + num_types) and isinstance(v, str_types + num_types):
					if isinstance(default_v, _OptionString):
						v = str(v)
						if not v:
							raise OptionException('Option %s on method %s requires a non-empty string value' % (k, method,))
						return v
					if isinstance(default_v, unicode) and isinstance(v, bytes):
						return v.decode('utf-8')
					return type(default_v)(v)
				if (isinstance(default_v, type) and isinstance(v, typefuzz(default_v))) or isinstance(v, typefuzz(type(default_v))):
					return v
				if isinstance(default_v, bool) and isinstance(v, (str, int)):
					lv = str(v).lower()
					if lv in ('true', '1', 't', 'yes', 'on',):
						return True
					if lv in ('false', '0', 'f', 'no', 'off', '',):
						return False
				if isinstance(default_v, _date_types):
					default_v = type(default_v)
				if default_v in _date_types:
					try:
						return typing_conv[default_v.__name__](v)
					except Exception:
						raise OptionException('Failed to convert option %s %r to %s on method %s' % (k, v, default_v, method,))
				if isinstance(v, str_types) and not v:
					return type(default_v)()
				if isinstance(default_v, JobWithFile) or default_v is JobWithFile:
					defaults = ('', '', False, None,)
					if default_v is JobWithFile:
						default_v = defaults
					if not isinstance(v, (list, tuple,)) or not (2 <= len(v) <= 4):
						raise OptionException('Option %s (%r) on method %s is not %s compatible' % (k, v, method, type(default_v)))
					v = tuple(v) + defaults[len(v):] # so all of default_v gets convert()ed.
					v = [convert(dv, vv) for dv, vv in zip(default_v, v)]
					return JobWithFile(*v)
				raise OptionException('Failed to convert option %s of %s to %s on method %s' % (k, type(v), type(default_v), method,))
			for k, v in iteritems(data['options']):
				if k in options:
					try:
						res_options[k] = convert(options[k], v)
					except OptionException:
						raise
					except Exception:
						print_exc()
						raise OptionException('Failed to convert option %s on method %s' % (k, method,))
				else:
					raise OptionException('Unknown option %s on method %s' % (k, method,))
			if fill_in:
				missing = set(options) - set(res_options)
				missing_required = missing & self.methods.params[method].required
				if missing_required:
					raise OptionException('Missing required options {%s} on method %s' % (', '.join(sorted(missing_required)), method,))
				defaults = self.methods.params[method].defaults
				res_options.update({k: defaults.options[k] for k in missing})
			data['options'] = res_options

	def get_item_by_uid(self, uid):
		for v in itervalues(self.tree):
			if v['uid'] == uid:
				return v

	def set_link(self, uid, job):
		item = self.get_item_by_uid(uid)
		item['link'] = job.id
		item['total_time'] = job.total

	def propagate_make(self):
		self._recursive_propagate_make(self.top_method)

	def _recursive_propagate_make(self, node):
		for child in self.tree[node]['dep']:
			self._recursive_propagate_make(child)
		self.tree[node]['make'] = (
			True in [self.tree[x]['make'] for x in self.tree[node]['dep'] if x]) or (
			not self.tree[node]['link'])

	def get_sorted_joblist(self):
		return sorted(self.tree.values(), key = lambda x : x['level'])

	def get_link(self, node):
		return self.tree[node]['link']


	def debugprint(self):
		for x, y in sorted(self.tree.items(), key = lambda x: -int(x[1]['level'])):
			print(' %15s' % x, end=' ')
			for k in y:
				print('%5s=%5s' % (k, y[k]), end=' ')
		print()
