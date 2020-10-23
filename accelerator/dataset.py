# -*- coding: utf-8 -*-

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
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
from collections import namedtuple, Counter
from itertools import compress, islice
from functools import partial
from contextlib import contextmanager
from operator import itemgetter

from accelerator.compat import unicode, uni, ifilter, imap, iteritems
from accelerator.compat import builtins, open, getarglist, izip, izip_longest
from accelerator.compat import str_types, int_types, FileNotFoundError

from accelerator import blob
from accelerator.extras import DotDict, job_params, _ListTypePreserver
from accelerator.job import Job
from accelerator.gzwrite import typed_writer
from accelerator.error import NoSuchDatasetError, DatasetUsageError, DatasetError

kwlist = set(kwlist)
# Add some keywords that are not in all versions
kwlist.update({
	'exec', 'print',                                       # only in py2
	'False', 'None', 'True', 'nonlocal', 'async', 'await', # only in py3
})

iskeyword = frozenset(kwlist).__contains__

# A dataset is defined by a pickled DotDict containing at least the following (all strings are unicode):
#     version = (3, 1,),
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
#     type = "type", # something that exists in type2iter and doesn't start with _
#     backing_type = "type", # something that exists in type2iter (v2 used type for this)
#                              (as the support for v2 has been removed, backing_type will
#                              always be the same as type. until something changes again.)
#     name = "name", # a clean version of the column name, valid in the filesystem and as a python identifier.
#     location = something, # where the data for this column lives
#         in version 2 and 3 this is "jobid/path/to/file" if .offsets else "jobid/path/with/%s/for/sliceno"
#     min = minimum value in this dataset or None
#     max = maximum value in this dataset or None
#     offsets = (offset, per, slice) or None for non-merged slices.
#     none_support = bool # not present in version 3.0, implicitly True there except for bits-types.
#
# Going from a DatasetColumn to a filename is like this for version 2 and 3 datasets:
#     jid, path = dc.location.split('/', 1)
#     jid = Job(jid)
#     if dc.offsets:
#         jid.filename(path)
#         seek to dc.offsets[sliceno], read only ds.lines[sliceno] values.
#     else:
#         jid.filename(path % sliceno)
# There is a ds.column_filename function to do this for you (not the seeking, obviously).
#
# The dataset pickle is jid/name/dataset.pickle, so jid/default/dataset.pickle for the default dataset.

def _clean_name(n, seen_n):
	n = ''.join(c if c.isalnum() else '_' for c in n)
	if n[0].isdigit():
		n = '_' + n
	# .lower because filenames are based on these, and filesystem
	# might not be case sensitive.
	while n.lower() in seen_n or iskeyword(n):
		n += '_'
	seen_n.add(n.lower())
	return n

def _dsid(t):
	if not t:
		return None
	if isinstance(t, (tuple, list)):
		jid, name = t
		if not jid:
			return None
		t = '%s/%s' % (jid.split('/')[0], uni(name) or 'default')
	elif isinstance(t, DatasetWriter):
		from accelerator.g import job
		t = '%s/%s' % (job, t.name)
	if '/' not in t:
		t += '/default'
	return uni(t)

# If we want to add fields to later versions, using a versioned name will
# allow still loading the old versions without messing with the constructor.
_DatasetColumn_3_1 = namedtuple('_DatasetColumn_3_1', 'type backing_type name location min max offsets none_support')
DatasetColumn = _DatasetColumn_3_1
# It's probably usually best to generate the new type so the rest of the code needs no special handling.
class _DatasetColumn_3_0(object):
	def __new__(cls, type, backing_type, name, location, min, max, offsets):
		none_support = not backing_type.startswith('bits')
		return _DatasetColumn_3_1(type, backing_type, name, location, min, max, offsets, none_support)

class _New_dataset_marker(unicode): pass
_new_dataset_marker = _New_dataset_marker('new')
_no_override = object()

_ds_cache = {}
def _ds_load(obj):
	n = unicode(obj)
	if n not in _ds_cache:
		fn = obj.job.filename(obj._name('pickle'))
		if not os.path.exists(fn):
			raise NoSuchDatasetError('Dataset %r does not exist' % (n,))
		_ds_cache[n] = blob.load(fn)
		_ds_cache.update(_ds_cache[n].get('cache', ()))
	return _ds_cache[n]

def _namechk(name):
	if not name:
		raise DatasetUsageError("Dataset name can't be empty")
	name = uni(name)
	for c in '/\n\0%':
		if c in name:
			raise DatasetUsageError("Dataset name can't contain " + repr(c))
	return name

_dummy_iter = iter(())

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
			if name:
				raise DatasetUsageError("Don't pass both a separate name and jobid as {job: dataset}")
			if len(jobid) != 1:
				raise DatasetUsageError("Only pass a single {job: dataset}")
			jobid, dsname = next(iteritems(jobid))
			if not jobid:
				return None
			jobid = job_params(jobid, default_empty=True).datasets.get(dsname)
			if not jobid:
				return None
		if not jobid:
			raise DatasetUsageError("If you really meant to use yourself as a dataset, pass your jobid explicitly.")
		if '/' in jobid:
			if name:
				raise DatasetUsageError("Don't pass both a separate name and jobid as jid/name")
			jobid, name = jobid.split('/', 1)
		name = _namechk(name or 'default')
		if name == 'default':
			suffix = ''
		else:
			suffix = '/' + name
		if jobid is _new_dataset_marker:
			from accelerator.g import job
			fullname = job + suffix
		else:
			fullname = jobid + suffix
		obj = unicode.__new__(cls, fullname)
		obj.name = uni(name or 'default')
		if jobid is _new_dataset_marker:
			obj._data = DotDict({
				'version': (3, 0,),
				'filename': None,
				'hashlabel': None,
				'caption': '',
				'columns': {},
				'parent': None,
				'previous': None,
				'lines': [],
			})
			obj.job = None
		else:
			obj.job = Job(jobid)
			obj._data = DotDict(_ds_load(obj))
			if obj._data.version[0] != 3:
				raise DatasetError("%s/%s: Unsupported dataset pickle version %r" % (jobid, name, obj._data.version,))
			obj._data.columns = dict(obj._data.columns)
		obj._cache = {}
		return obj

	# Look like a string after pickling
	def __reduce__(self):
		return unicode, (unicode(self),)

	@property
	def columns(self):
		"""{name: DatasetColumn}"""
		return self._data.columns

	def _cached(self, thing):
		org_value = self._data[thing]
		if not org_value:
			return
		if self._cache.get(thing, (None,))[0] != org_value:
			if isinstance(org_value, tuple):
				value = tuple(Dataset(v) for v in org_value)
			else:
				value = Dataset(org_value)
			self._cache[thing] = (org_value, value)
		return self._cache[thing][1]

	@property
	def previous(self):
		return self._cached('previous')

	@property
	def parent(self):
		return self._cached('parent')

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

	def link_to_here(self, name='default', column_filter=None, override_previous=_no_override, filename=None):
		"""Use this to expose a subjob as a dataset in your job:
		Dataset(subjid).link_to_here()
		will allow access to the subjob dataset under your jid.
		Specify column_filter as an iterable of columns to include
		if you don't want all of them.
		Use override_previous to rechain (or unchain) the dataset.
		You can change the filename too, or clear it by setting ''.
		"""
		d = Dataset(self)
		if column_filter:
			column_filter = set(column_filter)
			filtered_columns = {k: v for k, v in d._data.columns.items() if k in column_filter}
			left_over = column_filter - set(filtered_columns)
			if left_over:
				raise DatasetUsageError("Columns in filter not available in dataset: %r" % (left_over,))
			if not filtered_columns:
				raise DatasetUsageError("Filter produced no desired columns.")
			d._data.columns = filtered_columns
		from accelerator.g import job
		if override_previous is not _no_override:
			override_previous = _dsid(override_previous)
			if override_previous:
				# make sure it's valid
				Dataset(override_previous)
			d._data.previous = override_previous
			d._update_caches()
		d._data.parent = '%s/%s' % (d.job, d.name,)
		if filename is not None:
			d._data.filename = filename or None
		d.job = job
		d.name = uni(name)
		d._save()
		_datasets_written.append(d.name)
		return Dataset(d.job, d.name)

	def merge(self, other, name='default', previous=None, allow_unrelated=False):
		"""Merge this and other dataset. Columns from other take priority.
		If datasets do not have a common ancestor you get an error unless
		allow_unrelated is set. The new dataset always has the previous
		specified here (even if None).
		Returns the new dataset.
		"""
		from accelerator.g import job
		new_ds = Dataset(self)
		other = Dataset(other)
		name = _namechk(name)
		if previous:
			previous = Dataset(previous)
		else:
			previous = None
		if self == other:
			raise DatasetUsageError("Can't merge with myself (%s)" % (other,))
		if self.lines != other.lines:
			raise DatasetUsageError("%s and %s don't have the same line counts" % (self, other,))
		hashlabels = {self.hashlabel, other.hashlabel}
		hashlabels.discard(None)
		if len(hashlabels) > 1:
			raise DatasetUsageError("Hashlabel mismatch, %s has %s, %s has %s" % (self, self.hashlabel, other, other.hashlabel,))
		new_ds._data.columns.update(other._data.columns)
		if not allow_unrelated:
			def parents(ds, tips):
				if not isinstance(ds, tuple):
					ds = (ds,)
				for p in ds:
					pds = Dataset(p)
					if pds.parent:
						parents(pds.parent, tips)
					else:
						tips.add(pds)
				return tips
			related = parents(self, set()) & parents(other, set())
			if not related:
				raise DatasetUsageError("%s and %s have no common ancenstors, set allow_unrelated to allow this" % (self, other,))
		new_ds.job = job
		new_ds.name = name
		new_ds._data.previous = previous
		new_ds._data.parent = (self, other)
		new_ds._data.filename = None
		new_ds._data.caption = None
		new_ds._update_caches()
		new_ds._save()
		_datasets_written.append(name)
		return job.dataset(name) # new_ds has the wrong string value, so we must make a new instance here.

	def _column_iterator(self, sliceno, col, _type=None, **kw):
		if sliceno is not None and self.lines[sliceno] == 0:
			return _dummy_iter
		from accelerator.sourcedata import type2iter
		dc = self.columns[col]
		mkiter = partial(type2iter[_type or dc.backing_type], **kw)
		def one_slice(sliceno):
			fn = self.column_filename(col, sliceno)
			if dc.offsets:
				return mkiter(fn, seek=dc.offsets[sliceno], max_count=self.lines[sliceno])
			else:
				return mkiter(fn)
		if sliceno is None:
			from accelerator.g import slices
			from itertools import chain
			return chain(*[one_slice(s) for s in range(slices)])
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
		if not_found:
			raise DatasetError("Columns %r not found in %s/%s" % (not_found, self.job, self.name,))
		return res

	def _hashfilter(self, sliceno, hashlabel, it):
		from accelerator.g import slices
		return compress(it, self._column_iterator(None, hashlabel, hashfilter=(sliceno, slices)))

	def column_filename(self, colname, sliceno=None):
		dc = self.columns[colname]
		jid, name = dc.location.split('/', 1)
		jid = Job(jid)
		if dc.offsets:
			return jid.filename(name)
		else:
			if sliceno is None:
				sliceno = '%s'
			return jid.filename(name % (sliceno,))

	def chain(self, length=-1, reverse=False, stop_ds=None):
		if stop_ds:
			# resolve all formats to the same format
			stop_ds = Dataset(stop_ds)
		chain = DatasetChain()
		current = self
		while length != len(chain) and current != stop_ds:
			chain.append(current)
			if not current.previous:
				break
			current = Dataset(current.previous)
		if not reverse:
			chain.reverse()
		return chain

	def iterate_chain(self, sliceno, columns=None, length=-1, range=None, sloppy_range=False, reverse=False, hashlabel=None, stop_ds=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True, rehash=False, slice=None):
		"""Iterate a list of datasets. See .chain and .iterate_list for details."""
		chain = self.chain(length, reverse, stop_ds)
		return self.iterate_list(sliceno, columns, chain, range=range, sloppy_range=sloppy_range, hashlabel=hashlabel, pre_callback=pre_callback, post_callback=post_callback, filters=filters, translators=translators, status_reporting=status_reporting, rehash=rehash, slice=slice)

	def iterate(self, sliceno, columns=None, hashlabel=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True, rehash=False, slice=None):
		"""Iterate just this dataset. See .iterate_list for details."""
		return self.iterate_list(sliceno, columns, [self], hashlabel=hashlabel, pre_callback=pre_callback, post_callback=post_callback, filters=filters, translators=translators, status_reporting=status_reporting, rehash=rehash, slice=slice)

	@staticmethod
	def iterate_list(sliceno, columns, datasets, range=None, sloppy_range=False, hashlabel=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True, rehash=False, slice=None):
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

		If you pass sliceno=None you get all slices.
		If you pass sliceno="roundrobin" you also get all slices, but one value
		at a time across slices. (This can be used to iterate a csvimport in the
		order of the original file.)

		If you specify a hashlabel and rehash=False (the default) you will
		get an error if the a dataset does not use the specified hashlabel.
		If you specify rehash=True such datasets will be rehashed during
		iteration. You should usually build a new rehashed dataset (using
		the dataset_hashpart method), but this is available for when it makes
		sense.

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

		slice takes a slice object defining which lines you want returned.
		Negative offsets are allowed. Both negative and positive offsets
		outside the range of the specified datasets give an error.
		This is equivalent to islice(iterate_list(...), start, stop, step)
		except it's faster, allows negative offsets and errors on too big
		offsets.
		For convenience you can also pass slice=<some int> which is equivalent
		to slice(<some int>, None). Note that this is not the same as
		slice(<some int>), but more useful here.
		"""

		if isinstance(datasets, str_types + (Dataset, dict)):
			datasets = [datasets]
		datasets = [ds if isinstance(ds, Dataset) else Dataset(ds) for ds in datasets]
		slices = len(datasets[0].lines)
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
		if range:
			if len(range) != 1:
				raise DatasetUsageError("Specify exactly one range column.")
			range_k, (range_bottom, range_top,) = next(iteritems(range))
			if range_bottom is None and range_top is None:
				range = None
		if slice:
			if isinstance(slice, int_types):
				slice = builtins.slice(slice, None, 1)
			slice = builtins.slice(slice.start or 0, slice.stop, slice.step or 1)
			if slice.step <= 0:
				raise DatasetUsageError("only positive slice steps are supported")
			if slice.start < 0 or (slice.stop or 0) < 0:
				msg = "slice with negative index is not supported with "
				if range: raise DatasetUsageError(msg + "range")
				if rehash: raise DatasetUsageError(msg + "rehash")
				if filters: raise DatasetUsageError(msg + "filters")
			if isinstance(sliceno, int_types):
				total_lines = sum(d.lines[sliceno] for d in datasets)
			else:
				total_lines = sum(sum(d.lines) for d in datasets)
			if slice.start < 0:
				if -slice.start > total_lines:
					raise DatasetUsageError("Wanted last %d lines, but only %d lines available" % (-slice.start, total_lines,))
				slice = builtins.slice(total_lines + slice.start, slice.stop, slice.step)
			if (slice.stop or 0) < 0:
				if -slice.stop > total_lines:
					raise DatasetUsageError("Wanted to stop %d lines before end, but only %d lines available" % (-slice.stop, total_lines,))
				slice = builtins.slice(slice.start, total_lines + slice.stop, slice.step)
			if slice.start > total_lines:
				raise DatasetUsageError("Wanted to skip %d lines, but only %d lines available" % (slice.start, total_lines,))
			if (slice.stop or 0) > total_lines:
				raise DatasetUsageError("Wanted to stop after %d lines, but only %d lines available" % (slice.stop, total_lines,))
			if slice.start == total_lines:
				return iter(())
			if slice.stop is not None:
				if slice.stop < slice.start:
					raise DatasetUsageError("Wanted %r, but start is bigger than stop" % (slice,))
				if slice.start == slice.stop:
					return iter(())
			def adj_slice(lines):
				stop = slice.stop
				if stop is not None:
					stop -= lines
				return builtins.slice(slice.start - lines, stop, slice.step)
			slice_skipping = True
			slice_first = True
			if slice.start == 0 and slice.stop is None and slice.step == 1:
				slice = None
		if not slice:
			slice_skipping = False
			slice_first = False
		for d in datasets:
			if isinstance(sliceno, int_types):
				lines_here = d.lines[sliceno]
			else:
				lines_here = sum(d.lines)
			if lines_here == 0:
				continue
			if slice_skipping and slice.start >= lines_here:
				slice = adj_slice(lines_here)
				continue
			else:
				slice_skipping = False
			if range:
				c = d.columns[range_k]
				if range_top is not None and c.min >= range_top:
					continue
				if range_bottom is not None and c.max < range_bottom:
					continue
			if hashlabel and d.hashlabel != hashlabel:
				if not rehash:
					raise DatasetUsageError("%s has hashlabel %s, not %s" % (d, d.hashlabel, hashlabel,))
				if hashlabel not in d.columns:
					raise DatasetUsageError("Can't rehash %s on non-existant column %s" % (d, hashlabel,))
				rehash_on = hashlabel
			else:
				rehash_on = False
			if sliceno is None:
				if slice_first:
					for first_slice in builtins.range(slices):
						if slice.start < d.lines[first_slice]:
							break
						slice = adj_slice(d.lines[first_slice])
				else:
					first_slice = 0
				for ix in builtins.range(first_slice, slices):
					# Ignore rehashing - order is generally not guaranteed with sliceno=None
					to_iter.append((d, ix, False,))
			else:
				to_iter.append((d, sliceno, rehash_on,))
			slice_first = False
		if not rehash and not pre_callback and not post_callback and not isinstance(sliceno, str_types):
			# No reason to include empty slices then
			to_iter = [t for t in to_iter if t[0].lines[t[1]]]
		filter_func = Dataset._resolve_filters(columns, filters, want_tuple)
		translation_func, translators = Dataset._resolve_translators(columns, translators)
		if sloppy_range:
			range = None
		from itertools import chain
		kw = dict(
			columns=columns,
			pre_callback=pre_callback,
			post_callback=post_callback,
			filter_func=filter_func,
			translation_func=translation_func,
			translators=translators,
			want_tuple=want_tuple,
			range=range,
			status_reporting=status_reporting,
		)
		if sliceno == "roundrobin":
			# We do our own status reporting
			kw["status_reporting"] = False
			def rr_inner(d, rehash):
				todo = []
				for ix in builtins.range(slices):
					part = (d, ix, rehash)
					todo.append(chain.from_iterable(Dataset._iterate_datasets([part], **kw)))
				fv = object() # unique marker
				return (
					v for v in
					chain.from_iterable(izip_longest(*todo, fillvalue=fv))
					if v is not fv
				)
			def rr_outer():
				with Dataset._iterstatus(status_reporting, to_iter) as update:
					for ix, (d, sliceno, rehash) in enumerate(to_iter, 1):
						update(ix, d, sliceno, rehash)
						yield rr_inner(d, rehash)
			res = chain.from_iterable(rr_outer())
		else:
			res = chain.from_iterable(Dataset._iterate_datasets(to_iter, **kw))
		if slice and (slice.start or slice.stop or slice.step > 1):
			res = islice(res, slice.start, slice.stop, slice.step)
		return res

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
	@contextmanager
	def _iterstatus(status_reporting, to_iter):
		if not status_reporting:
			yield lambda *_: None
			return
		from accelerator.statmsg import status
		def fmt_dsname(d, sliceno, rehash):
			if rehash:
				return d + ':REHASH'
			else:
				return '%s:%s' % (d, sliceno)
		if len(to_iter) == 1:
			msg_head = 'Iterating ' + fmt_dsname(*to_iter[0])
			def update_status(ix, d, sliceno, rehash):
				pass
		else:
			msg_head = 'Iterating %s to %s' % (fmt_dsname(*to_iter[0]), fmt_dsname(*to_iter[-1]),)
			def update_status(ix, d, sliceno, rehash):
				update('%s, %d/%d (%s)' % (msg_head, ix, len(to_iter), fmt_dsname(d, sliceno, rehash)))
		with status(msg_head) as update:
			yield update_status

	@staticmethod
	def _iterate_datasets(to_iter, columns, pre_callback, post_callback, filter_func, translation_func, translators, want_tuple, range, status_reporting):
		skip_ds = None
		def argfixup(func, is_post):
			if func:
				if len(getarglist(func)) == 1:
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
		with Dataset._iterstatus(status_reporting, to_iter) as update:
			for ix, (d, sliceno, rehash) in enumerate(to_iter, 1):
				if unsliced_post_callback:
					try:
						post_callback(d)
					except StopIteration:
						return
				update(ix, d, sliceno, rehash)
				if pre_callback:
					if d == skip_ds:
						continue
					try:
						pre_callback(d, sliceno)
					except SkipSlice:
						if unsliced_pre_callback:
							skip_ds = d
						continue
					except SkipDataset:
						skip_ds = d
						continue
					except StopIteration:
						return
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
					try:
						post_callback(d, sliceno)
					except StopIteration:
						return
			if unsliced_post_callback:
				try:
					post_callback(None)
				except StopIteration:
					return

	@staticmethod
	def new(columns, filenames, lines, minmax={}, filename=None, hashlabel=None, caption=None, previous=None, name='default'):
		"""columns = {"colname": "type"}, lines = [n, ...] or {sliceno: n}"""
		columns = {uni(k): (uni(v[0]), bool(v[1])) if isinstance(v, tuple) else (uni(v), False) for k, v in columns.items()}
		if hashlabel:
			hashlabel = uni(hashlabel)
			if hashlabel not in columns:
				raise DatasetUsageError("Hashlabel (%s) does not exist" % (hashlabel,))
		res = Dataset(_new_dataset_marker, name)
		res._data.lines = list(Dataset._linefixup(lines))
		res._data.hashlabel = hashlabel
		res._append(columns, filenames, minmax, filename, caption, previous, None, name)
		return res

	@staticmethod
	def _linefixup(lines):
		from accelerator.g import slices
		if isinstance(lines, dict):
			if set(lines) != set(range(slices)):
				raise DatasetUsageError("Lines must be specified for all slices")
			lines = [c for _, c in sorted(lines.items())]
		if len(lines) != slices:
			raise DatasetUsageError("Lines must be specified for all slices")
		return lines

	def append(self, columns, filenames, lines, minmax={}, filename=None, hashlabel=None, hashlabel_override=False, caption=None, previous=None, column_filter=None, name='default'):
		hashlabel = uni(hashlabel)
		if hashlabel_override:
			self._data.hashlabel = hashlabel
		elif hashlabel and self.hashlabel != hashlabel:
			raise DatasetUsageError("Hashlabel mismatch %s != %s" % (self.hashlabel, hashlabel,))
		if self._linefixup(lines) != self.lines:
			raise DatasetUsageError("New columns don't have the same number of lines as parent columns")
		columns = {uni(k): (uni(v[0]), bool(v[1])) if isinstance(v, tuple) else (uni(v), False) for k, v in columns.items()}
		self._append(columns, filenames, minmax, filename, caption, previous, column_filter, name)

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
				mm = tuple(mm)
				if mm != (None, None):
					omm = minmax_fixup(res.get(name, (None, None,)), mm)
					mm = minmax_fixup(mm, omm)
					res[name] = [min(mm[0], omm[0]), max(mm[1], omm[1])]
		return res

	def _append(self, columns, filenames, minmax, filename, caption, previous, column_filter, name):
		from accelerator.sourcedata import type2iter
		from accelerator.g import job
		name = uni(name)
		filenames = {uni(k): uni(v) for k, v in filenames.items()}
		if set(columns) != set(filenames):
			raise DatasetUsageError("columns and filenames don't have the same keys")
		if self.job and (self.job != job or self.name != name):
			self._data.parent = '%s/%s' % (self.job, self.name,)
		self.job = job
		self.name = name
		self._data.filename = uni(filename) or self._data.filename or None
		self._data.caption  = uni(caption) or self._data.caption or uni(job)
		self._data.previous = _dsid(previous)
		for n in ('cache', 'cache_distance'):
			if n in self._data: del self._data[n]
		minmax = self._minmax_merge(minmax)
		if column_filter:
			column_filter = set(column_filter)
			filtered_columns = {k: v for k, v in self._data.columns.items() if k in column_filter}
			left_over = column_filter - set(filtered_columns)
			if left_over:
				raise DatasetUsageError("Columns in filter not available in dataset: %r" % (left_over,))
			self._data.columns = filtered_columns
		for n, (t, none_support) in sorted(columns.items()):
			if t not in type2iter:
				raise DatasetUsageError('Unknown type %s on column %s' % (t, n,))
			mm = minmax.get(n, (None, None,))
			t = uni(t)
			self._data.columns[n] = DatasetColumn(
				type=t,
				backing_type=t,
				name=filenames[n],
				location='%s/%s/%%s.%s' % (job, self.name, filenames[n]),
				min=mm[0],
				max=mm[1],
				offsets=None,
				none_support=none_support,
			)
			self._maybe_merge(n)
		self._update_caches()
		self._save()

	def _update_caches(self):
		for k in ('cache', 'cache_distance',):
			if k in self._data:
				del self._data[k]
		try:
			d = self.previous
		except NoSuchDatasetError:
			j, n = self._data['previous'].split('/', 1)
			if self.job == j and n in _datasetwriters:
				raise DatasetUsageError('previous dataset %r must be .finish()ed first.' % (n,))
			else:
				raise
		if d:
			cache_distance = d._data.get('cache_distance', 1) + 1
			if cache_distance == 64:
				cache_distance = 0
				chain = self.chain(64)
				self._data['cache'] = tuple((unicode(d), d._data) for d in chain[:-1])
			self._data['cache_distance'] = cache_distance

	def _maybe_merge(self, n):
		from accelerator.g import slices
		if slices < 2:
			return
		fn = self.column_filename(n)
		def getsize(sliceno):
			try:
				return os.path.getsize(fn % (sliceno,))
			except FileNotFoundError:
				return None
		sizes = [(sliceno, getsize(sliceno)) for sliceno, cnt in enumerate(self.lines)]
		bare_sizes = [z for _, z in sizes if z]
		if sum(bare_sizes) / (len(bare_sizes) or 1) > 524288: # arbitrary guess of good size
			return
		offsets = []
		pos = 0
		with open(fn % ('m',), 'wb') as m_fh:
			for sliceno, size in sizes:
				if size:
					with open(fn % (sliceno,), 'rb') as p_fh:
						data = p_fh.read()
					assert len(data) == size, "Slice %d is %d bytes, not %d?" % (sliceno, len(data), size,)
					m_fh.write(data)
				offsets.append(pos)
				if size is not None:
					os.unlink(fn % (sliceno,))
					pos += size
		c = self._data.columns[n]
		self._data.columns[n] = c._replace(
			offsets=offsets,
			location=c.location % ('m',),
		)

	def _save(self):
		if not os.path.exists(self.name):
			os.mkdir(self.name)
		blob.save(self._data, self._name('pickle'), temp=False, _hidden=True)
		with self.job.open(self._name('txt'), 'w', encoding='utf-8') as fh:
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
_datasets_written = []

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

	If you set a column type to None that column is not inherited from the
	parent dataset. (Only works as an init argument, not with dw.add.)

	If you want support for None values in a column you can pass
	none_support=True to dw.add, or {colname: (coltype, True)} to the
	constructor. If you pass a DatasetColumn (from ds.columns[name]) you
	will inherit both type and None-support of that column.

	If you set hashlabel you can use dw.hashcheck(v) to check if v
	belongs in this slice. You can also call enable_hash_discard
	(in each slice, or after each set_slice), then the writer will
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
		"""columns can be {'name': 'type'} or {'name': ('type', none_support)}.
		It can also be {'name': DatasetColumn} to simplify basing your dataset on another."""
		name = _namechk(name)
		from accelerator.g import running
		if running == 'analysis':
			if name not in _datasetwriters:
				raise DatasetUsageError('Dataset with name "%s" not created' % (name,))
			if columns or filename or hashlabel or hashlabel_override or caption or previous or parent or meta_only or for_single_slice is not None:
				raise DatasetUsageError("Don't specify any arguments (except optionally name) in analysis")
			return _datasetwriters[name]
		else:
			if name in _datasetwriters:
				raise DatasetUsageError('Duplicate dataset name "%s"' % (name,))
			os.mkdir(name)
			obj = object.__new__(cls)
			obj._running = running
			obj.filename = uni(filename)
			obj.hashlabel = uni(hashlabel)
			obj.hashlabel_override = hashlabel_override
			obj.caption = uni(caption)
			obj.previous = _dsid(previous)
			obj.name = name
			obj.columns = {}
			obj.meta_only = meta_only
			obj._for_single_slice = for_single_slice
			obj._clean_names = {}
			discard_columns = {k for k, v in columns.items() if v is None}
			columns = {k: v for k, v in columns.items() if v is not None}
			if parent:
				obj.parent = Dataset(parent)
				if not hashlabel_override:
					if obj.hashlabel:
						if obj.hashlabel != obj.parent.hashlabel:
							raise DatasetUsageError("Hashlabel mismatch %s != %s" % (obj.hashlabel, obj.parent.hashlabel,))
					elif obj.parent.hashlabel in columns:
						obj.hashlabel = obj.parent.hashlabel
				parent_cols = obj.parent.columns
				unknown_discards = discard_columns - set(parent_cols)
				if unknown_discards:
					raise DatasetUsageError("Can't discard non-existant columns %r" % (unknown_discards,))
				obj._pcolumns = {k: v for k, v in parent_cols.items() if k not in discard_columns}
				obj._seen_n = set(c.name.lower() for c in obj._pcolumns.values())
				obj._column_filter = set(obj._pcolumns)
			else:
				if discard_columns:
					raise DatasetUsageError("Can't discard columns without a parent")
				obj.parent = None
				obj._pcolumns = {}
				obj._seen_n = set()
			obj._started = False
			obj._lens = {}
			obj._minmax = {}
			obj._order = []
			for k, v in sorted(columns.items()):
				if v is None:
					continue
				if isinstance(v, tuple):
					if hasattr(v, 'type'):
						obj.add(k, v.type, none_support=v.none_support)
					else:
						obj.add(k, v[0], none_support=v[1])
				else:
					obj.add(k, v)
			_datasetwriters[name] = obj
			return obj

	def add(self, colname, coltype, default=_nodefault, none_support=False):
		from accelerator.g import running
		if running != self._running:
			raise DatasetUsageError("Add all columns in the same step as creation")
		if self._started:
			raise DatasetUsageError("Add all columns before setting slice")
		colname = uni(colname)
		coltype = uni(coltype)
		assert colname
		if colname in self.columns:
			raise DatasetUsageError("Column %s already exists" % (colname,))
		try:
			typed_writer(coltype) # gives error for unknown types
		except ValueError as e:
			raise DatasetUsageError(str(e))
		if none_support and coltype.startswith('bits'):
			raise DatasetUsageError("%s columns can't have None support" % (coltype,))
		self.columns[colname] = (coltype, default, none_support)
		self._order.append(colname)
		if colname in self._pcolumns:
			self._clean_names[colname] = self._pcolumns[colname].name
		else:
			self._clean_names[colname] = _clean_name(colname, self._seen_n)

	# These will be overwritten by set_slice
	def write(self, *a, **kw):
		raise DatasetUsageError("Call .set_slice(sliceno) before writing.")
	write_list = write
	write_dict = write

	def set_slice(self, sliceno):
		from accelerator import g
		if g.running == 'analysis' and self._for_single_slice != g.sliceno:
			if self._for_single_slice is not None:
				raise DatasetUsageError("This writer is for slice %d" % (self._for_single_slice,))
			else:
				raise DatasetUsageError("Only use set_slice in analysis together with for_single_slice")
		self._set_slice(sliceno)

	def _set_slice(self, sliceno):
		from accelerator.g import slices
		if self._started == 2:
			raise DatasetUsageError("Don't use both set_slice and a split writer")
		if not isinstance(sliceno, int_types) or sliceno < 0 or sliceno >= slices:
			raise DatasetUsageError("sliceno must be int in range(%d)" % (slices,))
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

	def enable_hash_discard(self):
		"""Make the write functions silently discard data that does not
		hash to the current slice."""
		if not self.hashlabel:
			raise DatasetUsageError("Can't enable hash discard without hashlabel")
		if self._started != 1:
			raise DatasetUsageError("Call enable_hash_discard after set_slice")
		self._mkwritefuncs(discard=True)

	def _mkwriters(self, sliceno, filtered=True):
		if not self.columns:
			raise DatasetUsageError("No columns in dataset")
		if self.hashlabel and self.hashlabel not in self.columns:
			raise DatasetUsageError("Hashed column (%s) missing" % (self.hashlabel,))
		self._started = 2 - filtered
		if self.meta_only:
			return
		writers = {}
		for colname, (coltype, default, none_support) in self.columns.items():
			wt = typed_writer(coltype)
			kw = {'none_support': none_support} if default is _nodefault else {'default': default, 'none_support': none_support}
			fn = self.column_filename(colname, sliceno)
			if filtered and colname == self.hashlabel:
				from accelerator.g import slices
				w = wt(fn, hashfilter=(sliceno, slices), **kw)
				self.hashcheck = w.hashcheck
			else:
				w = wt(fn, **kw)
			writers[colname] = w
		return writers

	def _mkwritefuncs(self, discard=False):
		hl = self.hashlabel
		w_l = [self.writers[c].write for c in self._order]
		w = {k: w.write for k, w in self.writers.items()}
		wrong_slice_msg = "Attempted to write data for wrong slice"
		if hl:
			hw = w.pop(hl)
			w_i = w.items()
			def write_dict(values):
				if hw(values[hl]):
					for k, w in w_i:
						w(values[k])
				elif not discard:
					raise DatasetUsageError(wrong_slice_msg)
			self.write_dict = write_dict
			hix = self._order.index(hl)
		else:
			w_i = w.items()
			def write_dict(values):
				for k, w in w_i:
					w(values[k])
			self.write_dict = write_dict
			hix = -1
		names = [self._clean_names[n] for n in self._order]
		used_names = set(names)
		w_names = [_clean_name('w%d' % (ix,), used_names) for ix in range(len(w_l))]
		w_d = dict(zip(w_names, w_l))
		errcls = _clean_name('DatasetUsageError', used_names)
		w_d[errcls] = DatasetUsageError
		f = ['def write(' + ', '.join(names) + '):']
		f_list = ['def write_list(values):']
		if len(names) == 1: # only the hashlabel, no check needed
			f.append(' %s(%s)' % (w_names[0], names[0],))
			f_list.append(' %s(values[0])' % (w_names[0],))
		else:
			if hl:
				f.append(' if %s(%s):' % (w_names[hix], names[hix],))
				f_list.append(' if %s(values[%d]):' % (w_names[hix], hix,))
			for ix in range(len(names)):
				if ix != hix:
					f.append('  %s(%s)' % (w_names[ix], names[ix],))
					f_list.append('  %s(values[%d])' % (w_names[ix], ix,))
			if hl and not discard:
				f.append(' else: raise %s(%r)' % (errcls, wrong_slice_msg,))
				f_list.append(' else: raise %s(%r)' % (errcls, wrong_slice_msg,))
		eval(compile('\n'.join(f), '<DatasetWriter generated write>', 'exec'), w_d)
		self.write = w_d['write']
		eval(compile('\n'.join(f_list), '<DatasetWriter generated write_list>', 'exec'), w_d)
		self.write_list = w_d['write_list']

	@property
	def _allwriters(self):
		if self._allwriters_:
			return self._allwriters_
		from accelerator.g import slices
		self._allwriters_ = [self._mkwriters(sliceno, False) for sliceno in range(slices)]
		return self._allwriters_

	def get_split_write(self):
		return self._split or self._mksplit()['split']

	def get_split_write_list(self):
		return self._split_list or self._mksplit()['split_list']

	def get_split_write_dict(self):
		return self._split_dict or self._mksplit()['split_dict']

	def _mksplit(self):
		from accelerator import g
		if g.running == 'analysis' and self._for_single_slice != g.sliceno:
			if self._for_single_slice is not None:
				raise DatasetUsageError("This writer is for slice %d" % (self._for_single_slice,))
			else:
				raise DatasetUsageError("Only use a split writer in analysis together with for_single_slice")
		if self._started == 1:
			raise DatasetUsageError("Don't use both a split writer and set_slice")
		if self.parent and self.parent.hashlabel and not self.hashlabel:
			raise DatasetUsageError("Can't use a split writer on hashed dataset when not writing the hash column.")
		names = [self._clean_names[n] for n in self._order]
		def key(t):
			return self._order.index(t[0])
		def d2l(d):
			return [w.write for _, w in sorted(d.items(), key=key)]
		f_____ = ['def split(' + ', '.join(names) + '):']
		f_list = ['def split_list(v):']
		f_dict = ['def split_dict(d):']
		from accelerator.g import slices
		hl = self.hashlabel
		used_names = set(names)
		name_w_l = _clean_name('w_l', used_names)
		name_hsh = _clean_name('hsh', used_names)
		name_cyc = _clean_name('cyc', used_names)
		name_next = _clean_name('next', used_names)
		name_writers = _clean_name('writers', used_names)
		w_d = {}
		w_d[name_writers] = [d2l(d) for d in self._allwriters]
		w_d[name_next] = next
		if hl:
			w_d[name_hsh] = self._allwriters[0][hl].hash
			prefix = '%s = %s[%s(' % (name_w_l, name_writers, name_hsh,)
			f_____.append('%s%s) %% %d]' % (prefix, self._clean_names[hl], slices,))
			f_list.append('%sv[%d]) %% %d]' % (prefix, self._order.index(hl), slices,))
			f_dict.append('%sd[%r]) %% %d]' % (prefix, hl, slices,))
		else:
			from itertools import cycle
			w_d[name_cyc] = cycle(range(slices))
			code = '%s = %s[%s(%s)]' % (name_w_l, name_writers, name_next, name_cyc,)
			f_____.append(code)
			f_list.append(code)
			f_dict.append(code)
		for ix in range(len(names)):
			f_____.append('%s[%d](%s)' % (name_w_l, ix, names[ix],))
			f_list.append('%s[%d](v[%d])' % (name_w_l, ix, ix,))
			f_dict.append('%s[%d](d[%r])' % (name_w_l, ix, self._order[ix],))
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
		if len(len_set) != 1:
			raise DatasetUsageError("Not all columns have the same linecount in slice %d: %r" % (sliceno, lens))
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
		if not self.meta_only:
			raise DatasetUsageError("Don't try to set lines for writers that actually write")
		self._lens[sliceno] = count

	def set_minmax(self, sliceno, minmax):
		if not self.meta_only:
			raise DatasetUsageError("Don't try to set minmax for writers that actually write")
		self._minmax[sliceno] = minmax

	def finish(self):
		"""Normally you don't need to call this, but if you want to
		pass yourself as a dataset to a subjob you need to call
		this first."""
		from accelerator.g import running, slices, job
		if running != self._running and running != 'synthesis':
			raise DatasetUsageError("Finish where you started or in synthesis")
		self.close()
		if len(self._lens) != slices:
			raise DatasetUsageError("Not all slices written, missing %r" % (set(range(slices)) - set(self._lens),))
		args = dict(
			columns={k: (v[0].split(':')[-1], v[2]) for k, v in self.columns.items()},
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
			self.parent.append(hashlabel_override=self.hashlabel_override, column_filter=self._column_filter, **args)
			res = Dataset((job, self.name))
		else:
			res = Dataset.new(**args)
		del _datasetwriters[self.name]
		_datasets_written.append(self.name)
		return res

class DatasetChain(_ListTypePreserver):
	"""
	These are lists of datasets returned from Dataset.chain.
	They exist to provide some convenience methods on chains.
	"""

	def _minmax(self, column, minmax):
		vl = []
		for ds in self:
			c = ds.columns.get(column)
			if c:
				v = getattr(c, minmax)
				if v is not None:
					vl.append(v)
		if vl:
			return getattr(builtins, minmax)(vl)

	def min(self, column):
		"""Min value for column over the whole chain.
		Will be None if no dataset in the chain contains column,
		if all datasets are empty or if column has a type without
		min/max tracking"""
		return self._minmax(column, 'min')

	def max(self, column):
		"""Max value for column over the whole chain.
		Will be None if no dataset in the chain contains column,
		if all datasets are empty or if column has a type without
		min/max tracking"""
		return self._minmax(column, 'max')

	def lines(self, sliceno=None):
		"""Number of rows in this chain, optionally for a specific slice."""
		if sliceno is None:
			sel = sum
		else:
			sel = itemgetter(sliceno)
		return sum(sel(ds.lines) for ds in self)

	def column_counts(self):
		"""Counter {colname: occurances}"""
		from itertools import chain
		return Counter(chain.from_iterable(ds.columns.keys() for ds in self))

	def column_count(self, column):
		"""How many datasets in this chain contain column"""
		return sum(column in ds.columns for ds in self)

	def with_column(self, column):
		"""Chain without any datasets that don't contain column"""
		return self.__class__(ds for ds in self if column in ds.columns)

	def none_support(self, column):
		"""If any dataset in the chain has None support for this column"""
		return True in (ds.columns[column].none_support for ds in self if column in ds.columns)

	def iterate(self, sliceno, columns=None, range=None, sloppy_range=False, hashlabel=None, pre_callback=None, post_callback=None, filters=None, translators=None, status_reporting=True, rehash=False, slice=None):
		"""Iterate the datasets in this chain. See Dataset.iterate_list for usage"""
		return Dataset.iterate_list(sliceno, columns, self, range=range, sloppy_range=sloppy_range, hashlabel=hashlabel, pre_callback=pre_callback, post_callback=post_callback, filters=filters, translators=translators, status_reporting=status_reporting, rehash=rehash, slice=slice)


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

class SkipDataset(Exception):
	"""Raise this in pre_callback to skip iterating the coming dataset
	(or the remaining slices of it)"""

class SkipSlice(Exception):
	"""Raise this in pre_callback to skip iterating the coming slice
	(if your callback doesn't want sliceno, this is the same as SkipDataset)"""

def job_datasets(job):
	"""All datasets in a job"""
	if isinstance(job, Dataset):
		job = job.job
	else:
		job = Job(job)
	fn = job.filename('datasets.txt')
	if not os.path.exists(fn):
		# It's not an error to list datasets in a job without them.
		return []
	with open(fn, 'r', encoding='utf-8') as fh:
		names = [line[:-1] for line in fh]
	res = []
	# Do this backwards to improve chances that we take advantage of cache.
	# (Names are written to datasets.txt in finish() order.)
	for name in reversed(names):
		res.append(Dataset(job, name))
	return list(reversed(res))
