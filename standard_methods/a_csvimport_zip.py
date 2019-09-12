############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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
'''

from zipfile import ZipFile
from shutil import copyfileobj
from os import unlink

from compat import uni

from . import a_csvimport
from extras import DotDict, resolve_jobid_filename, OptionEnum
import subjobs
from dataset import Dataset

depend_extra = (a_csvimport,)

options = DotDict(a_csvimport.options)
options.inside_filenames = {} # {"filename in zip": "dataset name"} or empty to import all files
options.chaining = OptionEnum('off on by_filename by_dsname').on

datasets = ('previous', )

def namefix(d, name):
	ok = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz._-'
	name = ''.join(c if c in ok else '_' for c in uni(name))
	if name == 'default' and options.chaining != 'off':
		name = 'default_'
	while name in d:
		name += '_'
	return name

def prepare(params):
	def tmpfn():
		cnt = 0
		while True:
			cnt += 1
			yield resolve_jobid_filename(params.jobid, str(cnt))
	tmpfn = tmpfn()
	namemap = dict(options.inside_filenames)
	used_names = set()
	res = []
	with ZipFile(options.filename, 'r') as z:
		for info in z.infolist():
			if options.inside_filenames:
				if info.filename in namemap:
					res.append((next(tmpfn), info, namemap.pop(info.filename),))
			else:
				name = namefix(used_names, info.filename)
				used_names.add(name)
				res.append((next(tmpfn), info, name,))
	if namemap:
		raise Exception("The following files were not found in %s: %r" % (options.filename, set(namemap),))
	if options.chaining == 'by_filename':
		res.sort(key=lambda x: x[1].filename)
	if options.chaining == 'by_dsname':
		res.sort(key=lambda x: x[2])
	if options.chaining != 'off':
		assert 'default' not in (x[2] for x in res[:-1]), 'When chaining the dataset named "default" must be last (or non-existant)'
	return res

def analysis(sliceno, prepare_res, params):
	with ZipFile(options.filename, 'r') as z:
		for tmpfn, zfn, dsn in prepare_res[sliceno::params.slices]:
			with z.open(zfn) as rfh:
				with open(tmpfn, 'wb') as wfh:
					copyfileobj(rfh, wfh)

def synthesis(prepare_res, params):
	opts = DotDict(options)
	del opts.inside_filenames
	del opts.chaining
	lst = prepare_res
	previous = datasets.previous
	for fn, info, dsn in lst:
		opts.filename = fn
		jid = subjobs.build('csvimport', options=opts, datasets=dict(previous=previous), caption="Import of %s from %s" % (info.filename, options.filename,))
		unlink(fn)
		previous = Dataset(jid).link_to_here(dsn)
		if options.chaining == 'off':
			previous = None
	if (len(lst) == 1 or options.chaining != 'off') and dsn != 'default':
		Dataset(jid).link_to_here('default')
