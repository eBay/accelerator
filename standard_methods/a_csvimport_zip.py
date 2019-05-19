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
you do specify a name that occurs multiple times is undefined, but I would
bet on the last one.

If there is only one file imported from the zip (whether specified
explicitly or because the zip only contains one file) you also get that
as the default dataset.
'''

from zipfile import ZipFile
from shutil import copyfileobj
from os import unlink

from compat import uni

from . import a_csvimport
from extras import DotDict, resolve_jobid_filename
import subjobs
from dataset import Dataset

depend_extra = (a_csvimport,)

options = DotDict(a_csvimport.options)
options.inside_filenames = {} # {"filename in zip": "dataset name"} or empty to import all files

def namefix(d, name):
	ok = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.'
	name = ''.join(c if c in ok else '_' for c in uni(name))
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
	if options.inside_filenames:
		return [(next(tmpfn), zfn, dsn,) for zfn, dsn in sorted(options.inside_filenames.items())]
	used_names = set()
	res = []
	with ZipFile(options.filename, 'r') as z:
		for info in z.infolist():
			name = namefix(used_names, info.filename)
			used_names.add(name)
			res.append((next(tmpfn), info, name,))
	return res

def analysis(sliceno, prepare_res, params):
	res = []
	with ZipFile(options.filename, 'r') as z:
		for tmpfn, zfn, dsn in prepare_res[sliceno::params.slices]:
			with z.open(zfn) as rfh:
				with open(tmpfn, 'wb') as wfh:
					copyfileobj(rfh, wfh)
			res.append((tmpfn, dsn,))
	return res

def synthesis(analysis_res):
	opts = DotDict(options)
	del opts.inside_filenames
	lst = analysis_res.merge_auto()
	for fn, dsn in lst:
		opts.filename = fn
		jid = subjobs.build('csvimport', options=opts)
		unlink(fn)
		Dataset(jid).link_to_here(dsn)
	if len(lst) == 1 and dsn != 'default':
		Dataset(jid).link_to_here('default')
