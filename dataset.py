# -*- coding: utf-8 -*-

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
from __future__ import unicode_literals

import os
from keyword import kwlist
from collections import namedtuple
from itertools import compress
from functools import partial
from inspect import getargspec

from compat import unicode, uni, ifilter, imap, izip, iteritems, str_types, builtins, open

import blob
from extras import DotDict, job_params
from jobid import resolve_jobid_filename
from gzwrite import typed_writer

kwlist = set(kwlist)
# Add some python3 keywords
kwlist.update({'False', 'None', 'True', 'nonlocal', 'async', 'await'})
iskeyword = frozenset(kwlist).__contains__

# A dataset is defined by a pickled DotDict containing at least the following (all strings are unicode):
#     version = (2, 2,),
#     filename = "filename" or None,
#     hashlabel = "column name" or None,
#     caption = "caption",
#     columns = {"column name": DatasetColumn,},
#     previous = "previous_jid/datasetname" or None,
#     parent = "parent_jid/datasetname" or None,
#     lines = [line, count, per, slice,],
#     cache = ((id, data), ...), # key is missing if there is no cache in this dataset
#     cache_distance = datasets_since_last_cache, # key is missing if previous is None
#
# A DatasetColumn has these fields:
#     type = "type", # something that exists in type2iter
#     name = "name", # a clean version of the column name, valid in the filesystem and as a python identifier.
#     location = something, # where the data for this column lives
#         in version 2 this is "jobid/path/to/file" if .offsets else "jobid/path/with/%s/for/sliceno"
#     min = minimum value in this dataset or None
#     max = maximum value in this dataset or None
#     offsets = (offset, per, slice) or None for non-merged slices.
#
# Going from a DatasetColumn to a filename is like this for version 2 datasets:
#     jid, path = dc.location.split('/', 1)
#     if dc.offsets:
#         resolve_jobid_filename(jid, path)
#         seek to dc.offsets[sliceno], read only ds.lines[sliceno] values.
#     else:
#         resolve_jobid_filename(jid, path % sliceno)
# There is a ds.column_filename function to do this for you (not the seeking, obviously).
#
# The dataset pickle is jid/name/dataset.pickle, so jid/default/dataset.pickle for the default dataset.

def _clean_name(n, seen_n):
	n = ''.join(c if c.isalnum() else '_' for c in n)
	if n[0].isdigit():
		n = '_' + n
	while n in seen_n or iskeyword(n):
		n += '_'
	seen_n.add(n)
	return n

def _dsid(t):
	if not t:
		return None
	if isinstance(t, (tuple, list)):
		jid, name = t
		if not jid:
			return None
		t = '%s/%s' % (jid.split('/')[0], uni(name) or 'default')
	if '/' not in t:
		t += '/default'
	return uni(t)

# If we want to add fields to later versions, using a versioned name will
# allow still loading the old versions without messing with the constructor.
_DatasetColumn_2_0 = namedtuple('_DatasetColumn_2_0', 'type name location min max offsets')
DatasetColumn = _DatasetColumn_2_0

class _New_dataset_marker(unicode): pass
_new_dataset_marker = _New_dataset_marker('new')

_ds_cache = {}
def _ds_load(obj):
	n = unicode(obj)
	if n not in _ds_cache:
		_ds_cache[n] = blob.load(obj._name('pickle'), obj.jobid)
		_ds_cache.update(_ds_cache[n].get('cache', ()))
	return _ds_cache[n]

class Dataset(unicode):
	"""
	Represents a dataset. Is also a string 'jobid/name', or just 'jobid' if
	name is 'default' (for better backwards compatibility).
	
	You usually don't have to make these yourself, because datasets.foo is
	already a Dataset instance (or None).
	
	You can pass jobid="jid/name" or jobid="jid", name="name", or skip
	name completely for "default".
	
	You can also pass jobid={jid: dsname} to resolve dsname from the datasets
	passed to jid. This gives None if that option was unset.
	
	These decay to a (unicode) string when pickled.
	"""

	def __new__(cls, jobid, name=None):
		if isinstance(jobid, (tuple, list)):
			jobid = _dsid(jobid)
		elif isinstance(jobid, dict):
			assert not name, "Don't pass both a separate name and jobid as {job: dataset}"
			assert len(jobid) == 1, "Only pass a single {job: dataset}"
			jobid, dsname = next(iteritems(jobid))
			if not jobid:
				return None
			jobid = job_params(jobid, default_empty=True).datasets.get(dsname)
			if not jobid:
				return None
		if '/' in jobid:
			assert not name, "Don't pass both a separate name and jobid as jid/name"
			jobid, name = jobid.split('/', 1)
		assert jobid, "If you really meant to use yourself as a dataset, pass params.jobid explicitly."
		name = uni(name or 'default')
		assert '/' not in name
		if name == 'default':
			suffix = ''
		else:
			suffix = '/' + name
		if jobid is _new_dataset_marker:
			from g import JOBID
			fullname = JOBID + suffix
		else:
			fullname = jobid + suffix
		obj = unicode.__new__(cls, fullname)
		obj.name = uni(name or 'default')
		if jobid is _new_dataset_marker:
			obj._data = DotDict({
				'version': (2, 2,),
				'filename': None,
				'hashlabel': None,
				'caption': '',
				'columns': {},
				'parent': None,
				'previous': None,
				'lines': [],
			})
			obj.jobid = None
		else:
			obj.jobid = jobid
			obj._data = DotDict(_ds_load(obj))
			assert obj._data.version[0] == 2 and obj._data.version[1] >= 2, "%s/%s: Unsupported dataset pickle version %r" % (jobid, name, obj._data.version,)
			obj._data.columns = dict(obj._data.columns)
		return obj

	# Look like a string after pickling
	def __reduce__(self):
		return unicode, (unicode(self),)

	@property
	def columns(self):
		"""{name: DatasetColumn}"""
		return self._data.columns

	@property
	def previous(self):
		return self._data.previous

	@property
	def parent(self):
		return self._data.parent

	@property
	def filename(self):
		return self._data.filename

	@property
	def hashlabel(self):
		return self._data.hashlabel

	@property
	def caption(self):
		return self._data.caption

	@property
	def lines(self):
		return self._data.lines

	@property
	def shape(self):
		return (len(self.columns), sum(self.lines),)

	def link_to_here(self, name='default', column_filter=None):
		"""Use this to expose a subjob as a dataset in your job:
		Dataset(subjid).link_to_here()
		will allow access to the subjob dataset under your jid.
		Specify column_filter as an iterable of columns to include
		if you don't want all of them."""
		if column_filter:
			column_filter = set(column_filter)
			filtered_columns = {k: v for k, v in self._data.columns.items() if k in column_filter}
			left_over = column_filter - set(filtered_columns)
			assert not left_over, "Columns in filter not available in dataset: %r" % (left_over,)
			assert filtered_columns, "Filter produced no desired columns."
			self._data.columns = filtered_columns
		from g import JOBID
		self._data.parent = '%s/%s' % (self.jobid, self.name,)
		self.jobid = uni(JOBID)
		self.name = uni(name)
		self._save()

	def _column_iterator(self, sliceno, col, **kw):
		from sourcedata import type2iter
		dc = self.columns[col]
		mkiter = partial(type2iter[dc.type], **kw)
		def one_slice(sliceno):
			fn = self.column_filename(col, sliceno)
			if dc.offsets:
				return mkiter(fn, seek=dc.offsets[sliceno], max_count=self.lines[sliceno])
			else:
				return mkiter(fn)
		if sliceno is None:
			from g import SLICES
			from itertools import chain
			return chain(*[one_slice(s) for s in range(SLICES)])
		else:
			return one_slice(sliceno)

	def _iterator(self, sliceno, columns=None):
		res = []
		not_found = []
		for col in columns or sorted(self.columns):
			if col in self.columns:
				res.append(self._column_iterator(sliceno, col))
			else:
				not_found.append(col)
		assert not not_found, 'Columns %r not found in %s/%s' % (not_found, self.jobid, self.name)
		return res

	def _hashfilter(self, sliceno, hashlabel, it):
		from g import SLICES
		return compress(it, self._column_iterator(None, hashlabel, hashfilter=(sliceno, SLICES)))

	def column_filename(self, colname, sliceno=None):
		dc = self.columns[colname]
		jid, name = dc.location.split('/', 1)
		if dc.offsets:
			return resolve_jobid_filename(jid, name)
		else:
			if sliceno is None:
				sliceno = '%s'
			return resolve_jobid_filename(jid, name % (sliceno,))

	def chain(self, length=-1, reverse=False, stop_ds=None):
		if stop_ds:
			# resolve all formats to the same format
			stop_ds = Dataset(stop_ds)
		chain = []
		current = self
		while length != len(chain) and current != stop_ds:
			chain.append(current)
			if not current.previous:
				break
			current = Dataset(current.previous)
		if not reverse:
			chain.reverse()
		return chain

	def iterate_chain(self, sliceno, columns=None, length=-1, range=None, sloppy_range=False, reverse=False, hashlabel=None, stop_ds=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True):
		"""Iterate a list of datasets. See .chain and .iterate_list for details."""
		chain = self.chain(length, reverse, stop_ds)
		return self.iterate_list(sliceno, columns, chain, range=range, sloppy_range=sloppy_range, hashlabel=hashlabel, pre_callback=pre_callback, post_callback=post_callback, filters=filters, translators=translators, status_reporting=status_reporting)

	def iterate(self, sliceno, columns=None, hashlabel=None, filters=None, translators=None, status_reporting=True):
		"""Iterate just this dataset. See .iterate_list for details."""
		return self.iterate_list(sliceno, columns, [self], hashlabel=hashlabel, filters=filters, translators=translators, status_reporting=status_reporting)

	@staticmethod
	def iterate_list(sliceno, columns, datasets, range=None, sloppy_range=False, hashlabel=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True):
		"""Iterator over the specified columns from datasets
		(iterable of dataset-specifiers, or single dataset-specifier).
		callbacks are called before and after each dataset is iterated.

		filters decide which rows to include and can be a callable
		(called with the candidate tuple), or a dict {name: filter}.
		In the latter case each individual filter is called with the column
		value, or if it's None uses the column value directly.
		All filters must say yes to yield a row.
		examples:
		filters={'some_col': some_dict.get}
		filters={'some_col': some_set.__contains__}
		filters={'some_col': some_str.__eq__}
		filters=lambda line: line[0] == line[1]

		translators transform data values. It can be a callable (called with the
		candidate tuple and expected to return a tuple of the same length) or a
		dict {name: translation}.
		Each translation can be a function (called with the column value and
		returning the new value) or dict. Items missing in the dict yield None,
		which can be removed with filters={'col': None}.

		Translators run before filters.

		You can also pass a single name (a str) as columns, in which case you
		don't get a tuple back (just the values). Tuple-filters/translators also
		get just the value in this case (column versions are unaffected).
		
		If you pass a false value for columns you get all columns in name order.
		
		range limits which rows you see. Specify {colname: (start, stop)} and
		only rows where start <= colvalue < stop will be returned.
		If you set sloppy_range=True you may get all rows from datasets that
		contain any rows you asked for. (This can be faster.)

		status_reporting should normally be left as True, which will give you
		information about this iteration in ^T, but there is one case where you
		need to turn it off:
		If you manually zip a bunch of iterators, only one should do status
		reporting. (Otherwise it looks like you have nested iteration in ^T,
		and you will get warnings about incorrect ending order of statuses.)
		"""

		if isinstance(datasets, str_types + (Dataset, dict)):
			datasets = [datasets]
		datasets = [ds if isinstance(ds, Dataset) else Dataset(ds) for ds in datasets]
		if not columns:
			columns = datasets[0].columns
		if isinstance(columns, str_types):
			columns = [columns]
			want_tuple = False
		else:
			if isinstance(columns, dict):
				columns = sorted(columns)
			want_tuple = True
		to_iter = []
		if sliceno is None:
			from g import SLICES
		if range:
			assert len(range) == 1, "Specify exactly one range column."
			range_k, (range_bottom, range_top,) = next(iteritems(range))
			if range_bottom is None and range_top is None:
				range = None
		for d in datasets:
			if sum(d.lines) == 0:
				continue
			if range:
				c = d.columns[range_k]
				if range_top is not None and c.min >= range_top:
					continue
				if range_bottom is not None and c.max < range_bottom:
					continue
			if sliceno is None:
				for ix in builtins.range(SLICES):
					to_iter.append((d, ix, False,))
			else:
				if hashlabel and d.hashlabel != hashlabel:
					assert hashlabel in d.columns, "Can't rehash %s on non-existant column %s" % (d, hashlabel,)
					rehash = hashlabel
				else:
					rehash = False
				to_iter.append((d, sliceno, rehash,))
		filter_func = Dataset._resolve_filters(columns, filters, want_tuple)
		translation_func, translators = Dataset._resolve_translators(columns, translators)
		if sloppy_range:
			range = None
		from itertools import chain
		return chain.from_iterable(Dataset._iterate_datasets(to_iter, columns, pre_callback, post_callback, filter_func, translation_func, translators, want_tuple, range, status_reporting))

	@staticmethod
	def _resolve_filters(columns, filters, want_tuple):
		if filters and not callable(filters):
			# Sort in column order, to allow selecting an efficient order.
			filters = sorted((columns.index(name), f,) for name, f in filters.items())
			if not want_tuple:
				return filters[0][1] or bool
			# Build "lambda t: f0(t[0]) and f1(t[1]) and ..."
			fs = []
			arg_n = []
			arg_v = []
			for ix, f in filters:
				if f is None or f is bool:
					# use value directly
					fs.append('t[%d]' % (ix,))
				else:
					n = 'f%d' % (ix,)
					arg_n.append(n)
					arg_v.append(f)
					fs.append('%s(t[%d])' % (n, ix,))
			f = 'lambda t: ' + ' and '.join(fs)
			# Add another lambda to put all fN into local variables.
			# (This is faster than putting them in "locals", you get
			# LOAD_DEREF instead of LOAD_GLOBAL.)
			f = 'lambda %s: %s' % (', '.join(arg_n), f)
			return eval(f, {}, {})(*arg_v)
		else:
			return filters

	@staticmethod
	def _resolve_translators(columns, translators):
		if not translators:
			return None, {}
		if callable(translators):
			return translators, {}
		else:
			res = {}
			for name, f in translators.items():
				if not callable(f):
					f = f.get
				res[columns.index(name)] = f
			return None, res

	@staticmethod
	def _iterate_datasets(to_iter, columns, pre_callback, post_callback, filter_func, translation_func, translators, want_tuple, range, status_reporting):
		skip_ds = None
		def argfixup(func, is_post):
			if func:
				if len(getargspec(func).args) == 1:
					seen_ds = [None]
					def wrapper(d, sliceno=None):
						if d != seen_ds[0]:
							if is_post:
								if seen_ds[0] and seen_ds[0] != skip_ds:
									func(seen_ds[0])
							else:
								func(d)
							seen_ds[0] = d
					return wrapper, True
			return func, False
		pre_callback, unsliced_pre_callback = argfixup(pre_callback, False)
		post_callback, unsliced_post_callback = argfixup(post_callback, True)
		if not to_iter:
			return
		if range:
			range_k, (range_bottom, range_top,) = next(iteritems(range))
			range_check = range_check_function(range_bottom, range_top)
			if range_k in columns and range_k not in translators and not translation_func:
				has_range_column = True
				range_i = columns.index(range_k)
				if want_tuple:
					range_f = lambda t: range_check(t[range_i])
				else:
					range_f = range_check
			else:
				has_range_column = False
		if status_reporting:
			from status import status
		else:
			from status import dummy_status as status
		def fmt_dsname(d, sliceno, rehash):
			if rehash:
				return d + ':REHASH'
			else:
				return '%s:%d' % (d, sliceno)
		if len(to_iter) == 1:
			msg_head = 'Iterating ' + fmt_dsname(*to_iter[0])
			def update_status(update, ix, d, sliceno, rehash):
				pass
		else:
			msg_head = 'Iterating %s to %s' % (fmt_dsname(*to_iter[0]), fmt_dsname(*to_iter[-1]),)
			def update_status(update, ix, d, sliceno, rehash):
				update('%s, %d/%d (%s)' % (msg_head, ix, len(to_iter), fmt_dsname(d, sliceno, rehash)))
		with status(msg_head) as update:
			for ix, (d, sliceno, rehash) in enumerate(to_iter, 1):
				if unsliced_post_callback:
					post_callback(d)
				update_status(update, ix, d, sliceno, rehash)
				if pre_callback:
					if d == skip_ds:
						continue
					try:
						pre_callback(d, sliceno)
					except SkipSlice:
						if unsliced_pre_callback:
							skip_ds = d
						continue
					except SkipJob:
						skip_ds = d
						continue
				it = d._iterator(None if rehash else sliceno, columns)
				for ix, trans in translators.items():
					it[ix] = imap(trans, it[ix])
				if want_tuple:
					it = izip(*it)
				else:
					it = it[0]
				if rehash:
					it = d._hashfilter(sliceno, rehash, it)
				if translation_func:
					it = imap(translation_func, it)
				if range:
					c = d.columns[range_k]
					if c.min is not None and (not range_check(c.min) or not range_check(c.max)):
						if has_range_column:
							it = ifilter(range_f, it)
						else:
							if rehash:
								filter_it = d._hashfilter(sliceno, rehash, d._column_iterator(None, range_k))
							else:
								filter_it = d._column_iterator(sliceno, range_k)
							it = compress(it, imap(range_check, filter_it))
				if filter_func:
					it = ifilter(filter_func, it)
				yield it
				if post_callback and not unsliced_post_callback:
					post_callback(d, sliceno)
			if unsliced_post_callback:
				post_callback(None)

	@staticmethod
	def new(columns, filenames, lines, minmax={}, filename=None, hashlabel=None, caption=None, previous=None, name='default'):
		"""columns = {"colname": "type"}, lines = [n, ...] or {sliceno: n}"""
		columns = {uni(k): uni(v) for k, v in columns.items()}
		if hashlabel:
			hashlabel = uni(hashlabel)
			assert hashlabel in columns, hashlabel
		res = Dataset(_new_dataset_marker, name)
		res._data.lines = list(Dataset._linefixup(lines))
		res._data.hashlabel = hashlabel
		res._append(columns, filenames, minmax, filename, caption, previous, name)
		return res

	@staticmethod
	def _linefixup(lines):
		from g import SLICES
		if isinstance(lines, dict):
			assert set(lines) == set(range(SLICES)), "Lines must be specified for all slices"
			lines = [c for _, c in sorted(lines.items())]
		assert len(lines) == SLICES, "Lines must be specified for all slices"
		return lines

	def append(self, columns, filenames, lines, minmax={}, filename=None, hashlabel=None, hashlabel_override=False, caption=None, previous=None, name='default'):
		if hashlabel:
			hashlabel = uni(hashlabel)
			if not hashlabel_override:
				assert self.hashlabel == hashlabel, 'Hashlabel mismatch %s != %s' % (self.hashlabel, hashlabel,)
		assert self._linefixup(lines) == self.lines, "New columns don't have the same number of lines as parent columns"
		columns = {uni(k): uni(v) for k, v in columns.items()}
		self._append(columns, filenames, minmax, filename, caption, previous, name)

	def _minmax_merge(self, minmax):
		def minmax_fixup(a, b):
			res_min = a[0]
			if res_min is None: res_min = b[0]
			res_max = a[1]
			if res_max is None: res_max = b[1]
			return [res_min, res_max]
		res = {}
		for part in minmax.values():
			for name, mm in part.items():
				if mm != (None, None):
					omm = minmax_fixup(res.get(name, (None, None,)), mm)
					mm = minmax_fixup(mm, omm)
					res[name] = [min(mm[0], omm[0]), max(mm[1], omm[1])]
		return res

	def _append(self, columns, filenames, minmax, filename, caption, previous, name):
		from sourcedata import type2iter
		from g import JOBID
		jobid = uni(JOBID)
		name = uni(name)
		filenames = {uni(k): uni(v) for k, v in filenames.items()}
		assert set(columns) == set(filenames), "columns and filenames don't have the same keys"
		if self.jobid and (self.jobid != jobid or self.name != name):
			self._data.parent = '%s/%s' % (self.jobid, self.name,)
		self.jobid = jobid
		self.name = name
		self._data.filename = uni(filename) or self._data.filename or None
		self._data.caption  = uni(caption) or self._data.caption or jobid
		self._data.previous = _dsid(previous)
		for n in ('cache', 'cache_distance'):
			if n in self._data: del self._data[n]
		minmax = self._minmax_merge(minmax)
		for n, t in sorted(columns.items()):
			if t not in type2iter:
				raise Exception('Unknown type %s on column %s' % (t, n,))
			mm = minmax.get(n, (None, None,))
			self._data.columns[n] = DatasetColumn(
				type=uni(t),
				name=filenames[n],
				location='%s/%s/%%s.%s' % (jobid, self.name, filenames[n]),
				min=mm[0],
				max=mm[1],
				offsets=None,
			)
			self._maybe_merge(n)
		self._update_caches()
		self._save()

	def _update_caches(self):
		if self.previous:
			d = Dataset(self.previous)
			cache_distance = d._data.get('cache_distance', 1) + 1
			if cache_distance == 64:
				cache_distance = 0
				chain = self.chain(64)
				self._data['cache'] = tuple((unicode(d), d._data) for d in chain[1:])
			self._data['cache_distance'] = cache_distance

	def _maybe_merge(self, n):
		from g import SLICES
		if SLICES < 2:
			return
		fn = self.column_filename(n)
		sizes = [os.path.getsize(fn % (sliceno,)) for sliceno in range(SLICES)]
		if sum(sizes) / SLICES > 524288: # arbitrary guess of good size
			return
		offsets = []
		pos = 0
		with open(fn % ('m',), 'wb') as m_fh:
			for sliceno, size in enumerate(sizes):
				with open(fn % (sliceno,), 'rb') as p_fh:
					data = p_fh.read()
				assert len(data) == size, "Slice %d is %d bytes, not %d?" % (sliceno, len(data), size,)
				os.unlink(fn % (sliceno,))
				m_fh.write(data)
				offsets.append(pos)
				pos += size
		c = self._data.columns[n]
		self._data.columns[n] = c._replace(
			offsets=offsets,
			location=c.location % ('m',),
		)

	def _save(self):
		if not os.path.exists(self.name):
			os.mkdir(self.name)
		blob.save(self._data, self._name('pickle'), temp=False)
		with open(self._name('txt'), 'w', encoding='utf-8') as fh:
			nl = False
			if self.hashlabel:
				fh.write('hashlabel %s\n' % (self.hashlabel,))
				nl = True
			if self.previous:
				fh.write('previous %s\n' % (self.previous,))
				nl = True
			if nl:
				fh.write('\n')
			col_list = sorted((k, c.type, c.location,) for k, c in self.columns.items())
			lens = tuple(max(minlen, max(len(t[i]) for t in col_list)) for i, minlen in ((0, 4), (1, 4), (2, 8)))
			template = '%%%ds  %%%ds  %%-%ds\n' % lens
			fh.write(template % ('name', 'type', 'location'))
			fh.write(template % tuple('=' * l for l in lens))
			for t in col_list:
				fh.write(template % t)

	def _name(self, thing):
		return '%s/dataset.%s' % (self.name, thing,)

_datasetwriters = {}

_nodefault = object()

class DatasetWriter(object):
	"""
	Create in prepare, use in analysis. Or do the whole thing in
	synthesis.
	
	You can pass these through prepare_res, or get them by trying to
	create a new writer in analysis (don't specify any arguments except
	an optional name).
	
	There are three writing functions with different arguments:
	
	dw.write_dict({column: value})
	dw.write_list([value, value, ...])
	dw.write(value, value, ...)
	
	Values are in the same order as you add()ed the columns (which is in
	sorted order if you passed a dict). The dw.write() function names the
	arguments from the columns too.
	
	If you set hashlabel you can use dw.hashcheck(v) to check if v
	belongs in this slice. You can also just call the writer, and it will
	discard anything that does not belong in this slice.
	
	If you are not in analysis and you wish to use the functions above
	you need to call dw.set_slice(sliceno) first.
	
	If you do not, you can instead get one of the splitting writer
	functions, that select which slice to use based on hashlabel, or
	round robin if there is no hashlabel.
	
	dw.get_split_write_dict()({column: value})
	dw.get_split_write_list()([value, value, ...])
	dw.get_split_write()(value, value, ...)
	
	These should of course be assigned to a local name for performance.
	
	It is permitted (but probably useless) to mix different write or
	split functions, but you can only use either write functions or
	split functions.
	
	You can also use dw.writers[colname] to get a typed_writer and use
	it as you please. The one belonging to the hashlabel will be
	filtering, and returns True if this is the right slice.
	
	If you need to handle everything yourself, set meta_only=True and
	use dw.column_filename(colname) to find the right files to write to.
	In this case you also need to call dw.set_lines(sliceno, count)
	before finishing. You should also call
	dw.set_minmax(sliceno, {colname: (min, max)}) if you can.
	"""

	_split = _split_dict = _split_list = _allwriters_ = None

	def __new__(cls, columns={}, filename=None, hashlabel=None, hashlabel_override=False, caption=None, previous=None, name='default', parent=None, meta_only=False, for_single_slice=None):
		"""columns can be {'name': 'type'} or {'name': DatasetColumn}
		to simplify basing your dataset on another."""
		name = uni(name)
		assert '/' not in name, name
		from g import running
		if running == 'analysis':
			assert name in _datasetwriters, 'Dataset with name "%s" not created' % (name,)
			assert not columns and not filename and not hashlabel and not caption and not parent and for_single_slice is None, "Don't specify any arguments (except optionally name) in analysis"
			return _datasetwriters[name]
		else:
			assert name not in _datasetwriters, 'Duplicate dataset name "%s"' % (name,)
			os.mkdir(name)
			obj = object.__new__(cls)
			obj._running = running
			obj.filename = uni(filename)
			obj.hashlabel = uni(hashlabel)
			obj.hashlabel_override = hashlabel_override,
			obj.caption = uni(caption)
			obj.previous = _dsid(previous)
			obj.name = uni(name)
			obj.parent = _dsid(parent)
			obj.columns = {}
			obj.meta_only = meta_only
			obj._for_single_slice = for_single_slice
			obj._clean_names = {}
			if parent:
				obj._pcolumns = Dataset(parent).columns
				obj._seen_n = set(c.name for c in obj._pcolumns.values())
			else:
				obj._pcolumns = {}
				obj._seen_n = set()
			obj._started = False
			obj._lens = {}
			obj._minmax = {}
			obj._order = []
			for k, v in sorted(columns.items()):
				if isinstance(v, tuple):
					v = v.type
				obj.add(k, v)
			_datasetwriters[name] = obj
			return obj

	def add(self, colname, coltype, default=_nodefault):
		from g import running
		assert running == self._running, "Add all columns in the same step as creation"
		assert not self._started, "Add all columns before setting slice"
		colname = uni(colname)
		coltype = uni(coltype)
		assert colname not in self.columns, colname
		assert colname
		typed_writer(coltype) # gives error for unknown types
		self.columns[colname] = (coltype, default)
		self._order.append(colname)
		if colname in self._pcolumns:
			self._clean_names[colname] = self._pcolumns[colname].name
		else:
			self._clean_names[colname] = _clean_name(colname, self._seen_n)

	def set_slice(self, sliceno):
		import g
		assert g.running != 'analysis' or self._for_single_slice == g.sliceno, "Only use set_slice in analysis together with for_single_slice"
		self._set_slice(sliceno)

	def _set_slice(self, sliceno):
		assert self._started < 2, "Don't use both set_slice and a split writer"
		self.close()
		self.sliceno = sliceno
		writers = self._mkwriters(sliceno)
		if not self.meta_only:
			self.writers = writers
			self._mkwritefuncs()

	def column_filename(self, colname, sliceno=None):
		if sliceno is None:
			sliceno = self.sliceno
		return '%s/%d.%s' % (self.name, sliceno, self._clean_names[colname],)

	def _mkwriters(self, sliceno, filtered=True):
		assert self.columns, "No columns in dataset"
		if self.hashlabel:
			assert self.hashlabel in self.columns, "Hashed column (%s) missing" % (self.hashlabel,)
		self._started = 2 - filtered
		if self.meta_only:
			return
		writers = {}
		for colname, (coltype, default) in self.columns.items():
			wt = typed_writer(coltype)
			kw = {} if default is _nodefault else {'default': default}
			fn = self.column_filename(colname, sliceno)
			if filtered and colname == self.hashlabel:
				from g import SLICES
				w = wt(fn, hashfilter=(sliceno, SLICES), **kw)
				self.hashcheck = w.hashcheck
			else:
				w = wt(fn, **kw)
			writers[colname] = w
		return writers

	def _mkwritefuncs(self):
		hl = self.hashlabel
		w_l = [self.writers[c].write for c in self._order]
		w = {k: w.write for k, w in self.writers.items()}
		if hl:
			hw = w.pop(hl)
			w_i = w.items()
			def write_dict(values):
				if hw(values[hl]):
					for k, w in w_i:
						w(values[k])
			self.write_dict = write_dict
			hix = self._order.index(hl)
		else:
			w_i = w.items()
			def write_dict(values):
				for k, w in w_i:
					w(values[k])
			self.write_dict = write_dict
			hix = -1
		w_d = {'w%d' % (ix,): w for ix, w in enumerate(w_l)}
		names = [self._clean_names[n] for n in self._order]
		f = ['def write(' + ', '.join(names) + '):']
		f_list = ['def write_list(values):']
		if len(names) == 1: # only the hashlabel, no check needed
			f.append(' w0(%s)' % tuple(names))
			f_list.append(' w0(values[0])')
		else:
			if hl:
				f.append(' if w%d(%s):' % (hix, names[hix],))
				f_list.append(' if w%d(values[%d]):' % (hix, hix,))
			for ix in range(len(names)):
				if ix != hix:
					f.append('  w%d(%s)' % (ix, names[ix],))
					f_list.append('  w%d(values[%d])' % (ix, ix,))
		eval(compile('\n'.join(f), '<DatasetWriter generated write>', 'exec'), w_d)
		self.write = w_d['write']
		eval(compile('\n'.join(f_list), '<DatasetWriter generated write_list>', 'exec'), w_d)
		self.write_list = w_d['write_list']

	@property
	def _allwriters(self):
		if self._allwriters_:
			return self._allwriters_
		from g import SLICES
		self._allwriters_ = [self._mkwriters(sliceno, False) for sliceno in range(SLICES)]
		return self._allwriters_

	def get_split_write(self):
		return self._split or self._mksplit()['split']

	def get_split_write_list(self):
		return self._split_list or self._mksplit()['split_list']

	def get_split_write_dict(self):
		return self._split_dict or self._mksplit()['split_dict']

	def _mksplit(self):
		import g
		if g.running == 'analysis':
			assert self._for_single_slice == g.sliceno, "Only use dataset in designated slice"
		assert self._started != 1, "Don't use both a split writer and set_slice"
		w_d = {}
		names = [self._clean_names[n] for n in self._order]
		w_d['names'] = names
		def key(t):
			return self._order.index(t[0])
		def d2l(d):
			return [w.write for _, w in sorted(d.items(), key=key)]
		w_d['writers'] = [d2l(d) for d in self._allwriters]
		f_____ = ['def split(' + ', '.join(names) + '):']
		f_list = ['def split_list(v):']
		f_dict = ['def split_dict(d):']
		from g import SLICES
		hl = self.hashlabel
		if hl:
			w_d['h'] = self._allwriters[0][hl].hash
			f_____.append('w_l = writers[h(%s) %% %d]' % (hl, SLICES,))
			f_list.append('w_l = writers[h(v[%d]) %% %d]' % (self._order.index(hl), SLICES,))
			f_dict.append('w_l = writers[h(d[%r]) %% %d]' % (hl, SLICES,))
		else:
			from itertools import cycle
			w_d['c'] = cycle(range(SLICES))
			f_____.append('w_l = writers[next(c)]')
			f_list.append('w_l = writers[next(c)]')
			f_dict.append('w_l = writers[next(c)]')
		for ix in range(len(names)):
			f_____.append('w_l[%d](%s)' % (ix, names[ix],))
			f_list.append('w_l[%d](v[%d])' % (ix, ix,))
			f_dict.append('w_l[%d](d[%r])' % (ix, self._order[ix],))
		eval(compile('\n '.join(f_____), '<DatasetWriter generated split_write>'     , 'exec'), w_d)
		eval(compile('\n '.join(f_list), '<DatasetWriter generated split_write_list>', 'exec'), w_d)
		eval(compile('\n '.join(f_dict), '<DatasetWriter generated split_write_dict>', 'exec'), w_d)
		self._split = w_d['split']
		self._split_list = w_d['split_list']
		self._split_dict = w_d['split_dict']
		return w_d

	def _close(self, sliceno, writers):
		lens = {}
		minmax = {}
		for k, w in writers.items():
			lens[k] = w.count
			minmax[k] = (w.min, w.max,)
			w.close()
		len_set = set(lens.values())
		assert len(len_set) == 1, "Not all columns have the same linecount in slice %d: %r" % (sliceno, lens)
		self._lens[sliceno] = len_set.pop()
		self._minmax[sliceno] = minmax

	def close(self):
		if self._started == 2:
			for sliceno, writers in enumerate(self._allwriters):
				self._close(sliceno, writers)
		else:
			if hasattr(self, 'writers'):
				self._close(self.sliceno, self.writers)
				del self.writers

	def discard(self):
		del _datasetwriters[self.name]
		from shutil import rmtree
		rmtree(self.name)

	def set_lines(self, sliceno, count):
		assert self.meta_only, "Don't try to set lines for writers that actually write"
		self._lens[sliceno] = count

	def set_minmax(self, sliceno, minmax):
		assert self.meta_only, "Don't try to set minmax for writers that actually write"
		self._minmax[sliceno] = minmax

	def finish(self):
		"""Normally you don't need to call this, but if you want to
		pass yourself as a dataset to a subjob you need to call
		this first."""
		from g import running, SLICES
		assert running == self._running or running == 'synthesis', "Finish where you started or in synthesis"
		self.close()
		assert len(self._lens) == SLICES, "Not all slices written, missing %r" % (set(range(SLICES)) - set(self._lens),)
		args = dict(
			columns={k: v[0].split(':')[-1] for k, v in self.columns.items()},
			filenames=self._clean_names,
			lines=self._lens,
			minmax=self._minmax,
			filename=self.filename,
			hashlabel=self.hashlabel,
			caption=self.caption,
			previous=self.previous,
			name=self.name,
		)
		if self.parent:
			res = Dataset(self.parent)
			res.append(hashlabel_override=self.hashlabel_override, **args)
		else:
			res = Dataset.new(**args)
		del _datasetwriters[self.name]
		return res

def range_check_function(bottom, top):
	"""Returns a function that checks if bottom <= arg < top, allowing bottom and/or top to be None"""
	import operator
	if top is None:
		if bottom is None:
			# Can't currently happen (checked before calling this), but let's do something reasonable
			return lambda _: True
		else:
			return partial(operator.le, bottom)
	elif bottom is None:
		return partial(operator.gt, top)
	else:
		def range_f(v):
			return v >= bottom and v < top
		return range_f

class SkipJob(Exception):
	"""Raise this in pre_callback to skip iterating the coming job
	(or the remaining slices of it)"""

class SkipSlice(Exception):
	"""Raise this in pre_callback to skip iterating the coming slice
	(if your callback doesn't want sliceno, this is the same as SkipJob)"""
