from __future__ import print_function
from __future__ import division

from itertools import chain
from inspect import getargspec

from compat import ifilter, imap, izip, str_types

from extras import job_params
from dataset import Dataset
from status import status


class SkipJob(Exception):
	"""Raise this in pre_callback to skip iterating the coming job
	(or the remaining slices of it)"""

class SkipSlice(Exception):
	"""Raise this in pre_callback to skip iterating the coming slice
	(if your callback doesn't want sliceno, this is the same as SkipJob)"""


def jobchain_prev(tip_jobid=None):
	"""Wrap jobchain for a common use of jobchain: find previous jobid
	to this one."""

	# +bool because tip is included if set
	jobid = jobchain(length=1 + bool(tip_jobid), tip_jobid=tip_jobid)
	if jobid:
		return jobid[0]
	else:
		return ''


def jobchain(length=-1, reverse=False, tip_jobid=None, stop_jobid=None):
	"""Look backwards over "previous" (jobid or dataset) from tip_jobid
	(default current job) and return length (or all) latest jobids
	(includes tip only if explicitly specified)
	Return up to but not including stop_jobid.
	stop_jobid can be a {job: optname} dict, resolving dataset/jobid "optname" from job"""

	def x2opt(jobid, optname="previous"):
		params = job_params(jobid)
		return params.jobids.get(optname) or params.datasets.get(optname)

	if not stop_jobid:
		stop_jobid = ()
	elif isinstance(stop_jobid, str_types):
		stop_jobid = (stop_jobid,)
	elif isinstance(stop_jobid, dict):
		stuff = stop_jobid.items()
		stop_jobid = set()
		for parent, var in stuff:
			stop_jobid.add(x2opt(parent, var))
	assert isinstance(stop_jobid, (list, tuple, set,)), "stop_jobid must be str, dict or set-ish"

	jobid = tip_jobid
	if tip_jobid:
		l_jobid = [tip_jobid]
		length -= 1
	else:
		l_jobid = []
	while length:
		jobid = x2opt(jobid)
		if not jobid:
			break
		if jobid in stop_jobid:
			break
		l_jobid.append(jobid)
		length -= 1
	if not reverse:
		l_jobid.reverse()
	return l_jobid

def iterate_datasets(sliceno, names_list, jobids, hashlabel=None, pre_callback=None, post_callback=None, filters=None, translators=None):
	"""Iterator over the variables names_list from jobids (str or list)
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

	You can also pass a single name (a str) as names_list, in which case you
	don't get a tuple back (just the values). Tuple-filters/translators also
	get just the value in this case (column versions are unaffected).
	"""

	if isinstance(jobids, Dataset):
		jobids = [jobids]
	if isinstance(jobids, str_types):
		jobids = [jid.strip() for jid in jobids.split(',')]
	if not names_list:
		names_list = Dataset(jobids[0]).columns
	if isinstance(names_list, str_types):
		names_list = [names_list]
		want_tuple = False
	else:
		if isinstance(names_list, dict):
			names_list = sorted(names_list)
		want_tuple = True
	to_iter = []
	if sliceno is None:
		from g import SLICES
	for jobid in jobids:
		d = jobid if isinstance(jobid, Dataset) else Dataset(jobid)
		jobid = jobid.split('/')[0]
		if sliceno is None:
			for ix in range(SLICES):
				to_iter.append((jobid, d, ix, False,))
		else:
			if hashlabel and d.hashlabel != hashlabel:
				assert hashlabel in d.columns, "Can't rehash %s on non-existant column %s" % (d, hashlabel,)
				rehash = hashlabel
			else:
				rehash = False
			to_iter.append((jobid, d, sliceno, rehash,))
	filter_func = _resolve_filters(names_list, filters)
	translation_func, translators = _resolve_translators(names_list, translators)
	return chain.from_iterable(_iterate_datasets(to_iter, names_list, pre_callback, post_callback, filter_func, translation_func, translators, want_tuple))

def _resolve_filters(names_list, filters):
	if filters and not callable(filters):
		# Sort in column order, to allow selecting an efficient order.
		filters = sorted((names_list.index(name), f,) for name, f in filters.items())
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

def _resolve_translators(names_list, translators):
	if not translators:
		return None, None
	if callable(translators):
		return translators, None
	else:
		res = {}
		for name, f in translators.items():
			if not callable(f):
				f = f.get
			res[names_list.index(name)] = f
		return None, res

def _iterate_datasets(to_iter, names_list, pre_callback, post_callback, filter_func, translation_func, translators, want_tuple):
	skip_jobid = None
	def argfixup(func, is_post):
		if func:
			if len(getargspec(func).args) == 1:
				seen_jobid = [None]
				def wrapper(jobid, sliceno=None):
					if jobid != seen_jobid[0]:
						if is_post:
							if seen_jobid[0] and seen_jobid[0] != skip_jobid:
								func(seen_jobid[0])
						else:
							func(jobid)
						seen_jobid[0] = jobid
				return wrapper, True
		return func, False
	pre_callback, unsliced_pre_callback = argfixup(pre_callback, False)
	post_callback, unsliced_post_callback = argfixup(post_callback, True)
	if not to_iter:
		return
	starting_at = '%s:%d' % (to_iter[0][0], to_iter[0][2],)
	if len(to_iter) == 1:
		msg = 'Iterating ' + starting_at
	else:
		msg = 'Iterating %d dataset slices starting at %s' % (len(to_iter), starting_at,)
	with status(msg):
		for ix, (jobid, d, sliceno, rehash) in enumerate(to_iter):
			if unsliced_post_callback:
				post_callback(jobid)
			if pre_callback:
				if jobid == skip_jobid:
					continue
				try:
					pre_callback(jobid, sliceno)
				except SkipSlice:
					if unsliced_pre_callback:
						skip_jobid = jobid
					continue
				except SkipJob:
					skip_jobid = jobid
					continue
			it = d._iterator(None if rehash else sliceno, names_list)
			for ix, trans in (translators or {}).items():
				it[ix] = imap(trans, it[ix])
			if want_tuple:
				it = izip(*it)
			else:
				it = it[0]
			if rehash:
				it = d._hashfilter(sliceno, rehash, it)
			if translation_func:
				it = imap(translation_func, it)
			if filter_func:
				it = ifilter(filter_func, it)
			with status('(%d/%d) %s:%s' % (ix, len(to_iter), jobid, 'REHASH' if rehash else sliceno,)):
				yield it
			if post_callback and not unsliced_post_callback:
				post_callback(jobid, sliceno)
		if unsliced_post_callback:
			post_callback(None)

def iterate_datasetchain(sliceno, names_list, length=-1, reverse=False, tip_jobid=None, hashlabel=None, stop_jobid=None, pre_callback=None, post_callback=None, filters=None, translators=None):
	"""Combines jobchain and iterate_dataset in the obvious way"""

	return iterate_datasets(sliceno, names_list, jobchain(length, reverse, tip_jobid, True, stop_jobid), hashlabel, pre_callback, post_callback, filters, translators)
