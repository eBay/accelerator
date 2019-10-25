############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2019 Carl Drougge                       #
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

from __future__ import division
from __future__ import absolute_import

import cffi
from resource import getpagesize
from os import unlink, symlink
from mmap import mmap, PROT_READ

from accelerator.compat import NoneType, unicode, imap, iteritems, itervalues, PY2

from accelerator.extras import OptionEnum, json_save, DotDict
from accelerator.gzwrite import typed_writer
from accelerator.dataset import DatasetWriter
from accelerator.report import report
from accelerator.sourcedata import type2iter
from . import dataset_typing

depend_extra = (dataset_typing,)

description = r'''
Convert one or more columns in a dataset from bytes/ascii/unicode to any type.
'''

# Without filter_bad the method fails when a value fails to convert and
# doesn't have a default. With filter_bad the value is filtered out
# together with all other values on the same line.
#
# With filter_bad a new dataset is produced - columns not specified in
# column2type become inaccessible.
#
# If you need to preserve unconverted columns with filter_bad, specify them
# as converted to bytes.

TYPENAME = OptionEnum(dataset_typing.convfuncs.keys())

options = {
	'column2type'               : {'COLNAME': TYPENAME},
	'defaults'                  : {}, # {'COLNAME': value}, unspecified -> method fails on unconvertible unless filter_bad
	'rename'                    : {}, # {'OLDNAME': 'NEWNAME'} doesn't shadow OLDNAME.
	'caption'                   : 'typed dataset',
	'discard_untyped'           : bool, # Make unconverted columns inaccessible ("new" dataset)
	'filter_bad'                : False, # Implies discard_untyped
	'numeric_comma'             : False, # floats as "3,14"
}

datasets = ('source', 'previous',)

byteslike_types = ('bytes', 'ascii', 'unicode',)

ffi = cffi.FFI()
backend = dataset_typing.backend

def prepare():
	backend.init()
	d = datasets.source
	columns = {}
	for colname, coltype in iteritems(options.column2type):
		assert d.columns[colname].type in byteslike_types, colname
		coltype = coltype.split(':', 1)[0]
		columns[options.rename.get(colname, colname)] = dataset_typing.typerename.get(coltype, coltype)
	if options.filter_bad or options.discard_untyped:
		assert options.discard_untyped is not False, "Can't keep untyped when filtering bad"
		parent = None
	else:
		parent = datasets.source
	return DatasetWriter(
		columns=columns,
		caption=options.caption,
		hashlabel=options.rename.get(d.hashlabel, d.hashlabel),
		hashlabel_override=True,
		parent=parent,
		previous=datasets.previous,
		meta_only=True,
	)

def analysis(sliceno):
	if options.numeric_comma:
		try_locales = [
			'da_DK', 'nb_NO', 'nn_NO', 'sv_SE', 'fi_FI',
			'en_ZA', 'es_ES', 'es_MX', 'fr_FR', 'ru_RU',
			'de_DE', 'nl_NL', 'it_IT',
		]
		for localename in try_locales:
			localename = localename.encode('ascii')
			if not backend.numeric_comma(localename):
				break
			if not backend.numeric_comma(localename + b'.UTF-8'):
				break
		else:
			raise Exception("Failed to enable numeric_comma, please install at least one of the following locales: " + " ".join(try_locales))
	if options.filter_bad:
		badmap_fh = open('badmap%d' % (sliceno,), 'w+b')
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, True)
		if sum(itervalues(bad_count)):
			final_bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, badmap_fh, False)
			final_bad_count = max(itervalues(final_bad_count))
		else:
			final_bad_count = 0
		badmap_fh.close()
	else:
		bad_count, default_count, minmax, link_candidates = analysis_lap(sliceno, None, False)
		final_bad_count = 0
	for src, dst in link_candidates:
		symlink(src, dst)
	return bad_count, final_bad_count, default_count, minmax

# make any unicode args bytes, for cffi calls.
def bytesargs(*a):
	return [v.encode('utf-8') if isinstance(v, unicode) else ffi.NULL if v is None else v for v in a]

# In python3 indexing into bytes gives integers (b'a'[0] == 97),
# this gives the same behaviour on python2. (For use with mmap.)
class IntegerBytesWrapper(object):
	def __init__(self, inner):
		self.inner = inner
	def close(self):
		self.inner.close()
	def __getitem__(self, key):
		return ord(self.inner[key])
	def __setitem__(self, key, value):
		self.inner[key] = chr(value)

def analysis_lap(sliceno, badmap_fh, first_lap):
	known_line_count = 0
	badmap_size = 0
	badmap_fd = -1
	res_bad_count = {}
	res_default_count = {}
	res_minmax = {}
	link_candidates = []
	if first_lap:
		record_bad = options.filter_bad
		skip_bad = 0
	else:
		record_bad = 0
		skip_bad = options.filter_bad
	minmax_fn = 'minmax%d' % (sliceno,)
	dw = DatasetWriter()
	for colname, coltype in iteritems(options.column2type):
		out_fn = dw.column_filename(options.rename.get(colname, colname))
		fmt = fmt_b = None
		if coltype in dataset_typing.convfuncs:
			shorttype = coltype
			_, cfunc, pyfunc = dataset_typing.convfuncs[coltype]
		else:
			shorttype, fmt = coltype.split(':', 1)
			_, cfunc, pyfunc = dataset_typing.convfuncs[shorttype + ':*']
		if cfunc:
			cfunc = shorttype.replace(':', '_')
		if pyfunc:
			tmp = pyfunc(coltype)
			if callable(tmp):
				pyfunc = tmp
				cfunc = None
			else:
				pyfunc = None
				cfunc, fmt, fmt_b = tmp
		if coltype == 'number':
			cfunc = 'number'
		elif coltype == 'number:int':
			coltype = 'number'
			cfunc = 'number'
			fmt = "int"
		assert cfunc or pyfunc, coltype + " didn't have cfunc or pyfunc"
		coltype = shorttype
		d = datasets.source
		assert d.columns[colname].type in byteslike_types, colname
		if options.filter_bad:
			line_count = d.lines[sliceno]
			if known_line_count:
				assert line_count == known_line_count, (colname, line_count, known_line_count)
			else:
				known_line_count = line_count
				pagesize = getpagesize()
				badmap_size = (line_count // 8 // pagesize + 1) * pagesize
				badmap_fh.truncate(badmap_size)
				badmap_fd = badmap_fh.fileno()
		in_fn = d.column_filename(colname, sliceno)
		if d.columns[colname].offsets:
			offset = d.columns[colname].offsets[sliceno]
			max_count = d.lines[sliceno]
		else:
			offset = 0
			max_count = -1
		if cfunc:
			default_value = options.defaults.get(colname, ffi.NULL)
			default_len = 0
			if default_value is None:
				default_value = ffi.NULL
				default_value_is_None = True
			else:
				default_value_is_None = False
				if default_value != ffi.NULL:
					if isinstance(default_value, unicode):
						default_value = default_value.encode("utf-8")
					default_len = len(default_value)
			bad_count = ffi.new('uint64_t [1]', [0])
			default_count = ffi.new('uint64_t [1]', [0])
			c = getattr(backend, 'convert_column_' + cfunc)
			res = c(*bytesargs(in_fn, out_fn, minmax_fn, default_value, default_len, default_value_is_None, fmt, fmt_b, record_bad, skip_bad, badmap_fd, badmap_size, bad_count, default_count, offset, max_count))
			assert not res, 'Failed to convert ' + colname
			res_bad_count[colname] = bad_count[0]
			res_default_count[colname] = default_count[0]
			coltype = coltype.split(':', 1)[0]
			with type2iter[dataset_typing.typerename.get(coltype, coltype)](minmax_fn) as it:
				res_minmax[colname] = list(it)
			unlink(minmax_fn)
		else:
			# python func
			nodefault = object()
			if colname in options.defaults:
				default_value = options.defaults[colname]
				if default_value is not None:
					if isinstance(default_value, unicode):
						default_value = default_value.encode('utf-8')
					default_value = pyfunc(default_value)
			else:
				default_value = nodefault
			if options.filter_bad:
				badmap = mmap(badmap_fd, badmap_size)
				if PY2:
					badmap = IntegerBytesWrapper(badmap)
			bad_count = 0
			default_count = 0
			dont_minmax_types = {'bytes', 'ascii', 'unicode', 'json'}
			real_coltype = dataset_typing.typerename.get(coltype, coltype)
			do_minmax = real_coltype not in dont_minmax_types
			with typed_writer(real_coltype)(out_fn) as fh:
				col_min = col_max = None
				for ix, v in enumerate(d._column_iterator(sliceno, colname, _type='bytes')):
					if skip_bad:
						if badmap[ix // 8] & (1 << (ix % 8)):
							bad_count += 1
							continue
					try:
						v = pyfunc(v)
					except ValueError:
						if default_value is not nodefault:
							v = default_value
							default_count += 1
						elif record_bad:
							bad_count += 1
							bv = badmap[ix // 8]
							badmap[ix // 8] = bv | (1 << (ix % 8))
							continue
						else:
							raise Exception("Invalid value %r with no default in %s" % (v, colname,))
					if do_minmax and not isinstance(v, NoneType):
						if col_min is None:
							col_min = col_max = v
						if v < col_min: col_min = v
						if v > col_max: col_max = v
					fh.write(v)
			if options.filter_bad:
				badmap.close()
			res_bad_count[colname] = bad_count
			res_default_count[colname] = default_count
			res_minmax[colname] = [col_min, col_max]
	return res_bad_count, res_default_count, res_minmax, link_candidates

def synthesis(params, analysis_res, prepare_res):
	r = report()
	res = DotDict()
	d = datasets.source
	analysis_res = list(analysis_res)
	if options.filter_bad:
		num_lines_per_split = [num - data[1] for num, data in zip(d.lines, analysis_res)]
		res.bad_line_count_per_slice = [data[1] for data in analysis_res]
		res.bad_line_count_total = sum(res.bad_line_count_per_slice)
		r.println('Slice   Bad line count')
		for sliceno, cnt in enumerate(res.bad_line_count_per_slice):
			r.println('%5d   %d' % (sliceno, cnt,))
		r.println('total   %d' % (res.bad_line_count_total,))
		r.line()
		r.println('Slice   Bad line number')
		reported_count = 0
		for sliceno, data in enumerate(analysis_res):
			fn = 'badmap%d' % (sliceno,)
			if data[1] and reported_count < 32:
				with open(fn, 'rb') as fh:
					badmap = mmap(fh.fileno(), 0, prot=PROT_READ)
					for ix, v in enumerate(imap(ord, badmap)):
						if v:
							for jx in range(8):
								if v & (1 << jx):
									r.println('%5d   %d' % (sliceno, ix * 8 + jx,))
									reported_count += 1
									if reported_count >= 32: break
							if reported_count >= 32: break
					badmap.close()
			unlink(fn)
		if reported_count >= 32:
			r.println('...')
		r.line()
		res.bad_line_count_per_column = {}
		r.println('Bad line count   Column')
		for colname in sorted(analysis_res[0][0]):
			cnt = sum(data[0][colname] for data in analysis_res)
			r.println('%14d   %s' % (cnt, colname,))
			res.bad_line_count_per_column[colname] = cnt
		r.line()
	else:
		num_lines_per_split = d.lines
	dw = prepare_res
	for sliceno, count in enumerate(num_lines_per_split):
		dw.set_lines(sliceno, count)
	if options.defaults:
		r.println('Defaulted values')
		res.defaulted_per_slice = {}
		res.defaulted_total = {}
		for colname in sorted(options.defaults):
			r.println('    %s:' % (colname,))
			r.println('        Slice   Defaulted line count')
			res.defaulted_per_slice[colname] = [data[2][colname] for data in analysis_res]
			res.defaulted_total[colname] = sum(res.defaulted_per_slice[colname])
			for sliceno, cnt in enumerate(res.defaulted_per_slice[colname]):
				r.println('        %5d   %d' % (sliceno, cnt,))
			r.println('        total   %d' % (res.defaulted_total[colname],))
		r.line()
	for sliceno, data in enumerate(analysis_res):
		dw.set_minmax(sliceno, data[3])
	d = dw.finish()
	res.good_line_count_per_slice = num_lines_per_split
	res.good_line_count_total = sum(num_lines_per_split)
	r.line()
	r.println('Total of %d lines converted' % (res.good_line_count_total,))
	r.close()
	json_save(res)
