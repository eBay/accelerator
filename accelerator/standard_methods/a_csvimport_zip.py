############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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
from __future__ import unicode_literals
from __future__ import absolute_import

description = r'''
Call csvimport on one or more files in a zip file.

Takes all the options csvimport takes. (filename is the name of the zip)

Also takes "inside_filenames" which is a dict
{"filename in zip": "dataset name"}
or empty to import all files with a cleaned up filename as dataset name.
If the zip contains several files with the same name you can only get
all of them by not specifying inside_filenames. Which one you get if
you do specify a name that occurs multiple times is unspecified, but
currently it's the first one.

You can use include_re and exclude_re (higher priority) to select what
to include.

If there is only one file imported from the zip (whether specified
explicitly or because the zip only contains one file) you also get that
as the default dataset.

You can also get the files in the zip chained, controlled by the "chaining"
option. There are four possibilites:

off:         Don't chain the imports.
on:          Chain the imports in the order the files are in the zip file
             This is the default.
by_filename: Chain in filename order.
by_dsname:   Chain in dataset name order. Since inside_filenames
             is a dict this is your only way of controlling its order.

If you chain you will also get the last dataset as the default dataset,
to make it easy to find. Naming a non-last dataset "default" is an error.

If you set strip_dirs the filename (as used for both sorting and naming
datasets, but not when matching regexes) will not include directories. The
default is to include directories.
'''

from zipfile import ZipFile
from shutil import copyfileobj
from os.path import join
import re

from accelerator.compat import uni

from . import a_csvimport
from accelerator import DotDict, OptionEnum, build

depend_extra = (a_csvimport,)

options = DotDict(a_csvimport.options)
options.inside_filenames = {} # {"filename in zip": "dataset name"} or empty to import all files
options.chaining = OptionEnum('off on by_filename by_dsname').on
options.include_re = "" # Regex of files to include. (Matches anywhere, use ^$ as needed.)
options.exclude_re = "" # Regex of files to exclude, takes priority over include.
options.strip_dirs = False # Strip directories from filename (a/b/c -> c)

datasets = ('previous', )

def namefix(d, name):
	ok = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz._-'
	name = ''.join(c if c in ok else '_' for c in uni(name))
	if name == 'default' and options.chaining != 'off':
		name = 'default_'
	while name in d:
		name += '_'
	return name

def prepare(job):
	def tmpfn():
		cnt = 0
		while True:
			cnt += 1
			yield job.filename(str(cnt))
	tmpfn = tmpfn()
	namemap = dict(options.inside_filenames)
	if namemap and (options.include_re or options.exclude_re):
		raise Exception("Don't specify both inside_filenames and regexes.")
	used_names = set()
	res = []
	include_re = re.compile(options.include_re or r'.')
	exclude_re = re.compile(options.exclude_re or r'^$')
	with ZipFile(join(job.input_directory, options.filename), 'r') as z:
		for info in z.infolist():
			fn = ffn = info.filename
			if fn.endswith('/') or info.external_attr & 0x40000000:
				# skip directories
				continue
			if options.strip_dirs:
				fn = fn.rsplit('/', 1)[-1]
			if options.inside_filenames:
				if fn in namemap:
					res.append((next(tmpfn), info, namemap.pop(fn), fn,))
					if not namemap:
						break
			elif include_re.search(ffn) and not exclude_re.search(ffn):
				name = namefix(used_names, fn)
				used_names.add(name)
				res.append((next(tmpfn), info, name, fn,))
	if namemap:
		raise Exception("The following files were not found in %s: %r" % (options.filename, set(namemap),))
	if options.chaining == 'by_filename':
		res.sort(key=lambda x: x[3])
	if options.chaining == 'by_dsname':
		res.sort(key=lambda x: x[2])
	if options.chaining != 'off':
		assert 'default' not in (x[2] for x in res[:-1]), 'When chaining the dataset named "default" must be last (or non-existant)'
	return [x[:3] for x in res]

def analysis(sliceno, slices, prepare_res, job):
	with ZipFile(join(job.input_directory, options.filename), 'r') as z:
		for tmpfn, zfn, dsn in prepare_res[sliceno::slices]:
			with z.open(zfn) as rfh:
				with job.open(tmpfn, 'wb', temp=True) as wfh:
					copyfileobj(rfh, wfh)

def synthesis(prepare_res):
	opts = DotDict((k, v) for k, v in options.items() if k in a_csvimport.options)
	lst = prepare_res
	previous = datasets.previous
	for fn, info, dsn in lst:
		opts.filename = fn
		show_fn = '%s:%s' % (options.filename, info.filename,)
		ds = build('csvimport', options=opts, previous=previous, caption='Import of ' + show_fn).dataset()
		previous = ds.link_to_here(dsn, filename=show_fn)
		if options.chaining == 'off':
			previous = None
	if (len(lst) == 1 or options.chaining != 'off') and dsn != 'default':
		ds.link_to_here('default', filename=show_fn)
