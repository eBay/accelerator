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

description = r'''
Verify the zip wrapper for csvimport.
'''

from zipfile import ZipFile
from io import BytesIO
from gzip import GzipFile

from accelerator import subjobs
from accelerator.dataset import Dataset
from accelerator import g

file_a = b'0\nfoo\n'
list_a = [b'foo']
file_b = b'0\nbar\n'
list_b = [b'bar']

def compress(data):
	# In python3 I could just use gzip.compress..
	with BytesIO() as bio:
		with GzipFile(fileobj=bio, mode='wb') as fh:
			fh.write(data)
		return bio.getvalue()

def verify(zipname, inside_filenames, want_ds, **kw):
	opts=dict(
		filename=g.job.filename(zipname),
		inside_filenames=inside_filenames,
	)
	opts.update(kw)
	jid = subjobs.build('csvimport_zip', options=opts)
	for dsn, want_data in want_ds.items():
		got_data = list(Dataset(jid, dsn).iterate(None, '0'))
		assert got_data == want_data, "%s/%s from %s didn't contain %r, instead contained %r" % (jid, dsn, zipname, want_data, got_data)

def synthesis():
	# Simple case, a single file in the zip.
	with ZipFile('a.zip', 'w') as z:
		z.writestr('a', file_a)
	with ZipFile('b.zip', 'w') as z:
		z.writestr('b', file_b)
	# Several files.
	with ZipFile('both.zip', 'w') as z:
		z.writestr('a', file_a)
		z.writestr('b', file_b)
	# With one of the files gziped.
	with ZipFile('both, b compressed.zip', 'w') as z:
		z.writestr('a', file_a)
		z.writestr('b', compress(file_b))
	# Repeated name inside.
	with ZipFile('both called a.zip', 'w') as z:
		z.writestr('a', file_a)
		z.writestr('a', file_b)
	# Same, with one compressed.
	with ZipFile('both called a, first compressed.zip', 'w') as z:
		z.writestr('a', compress(file_a))
		z.writestr('a', file_b)
	# A whole bunch of files.
	with ZipFile('many files.zip', 'w') as z:
		manyfiles = {}
		for i in range(65, 91):
			a = chr(i)
			b = b'contents of ' + a.encode('ascii')
			z.writestr(a, b)
			manyfiles[a] = [b]
	# Make sure having a file named "default" doesn't cause issues.
	with ZipFile('named default.zip', 'w') as z:
		z.writestr('default', file_b)
	verify('a.zip', {'a': 'foo'}, {'foo': list_a, 'default': list_a})
	verify('a.zip', {}, {'a': list_a, 'default': list_a})
	verify('b.zip', {'b': 'bar'}, {'bar': list_b, 'default': list_b})
	verify('b.zip', {}, {'b': list_b, 'default': list_b})
	verify('both.zip', {'a': 'foo', 'b': 'bar'}, {'foo': list_a, 'bar': list_b})
	verify('both.zip', {}, {'a': list_a, 'b': list_b})
	verify('both.zip', {'a': 'foo'}, {'foo': list_a, 'default': list_a})
	verify('both, b compressed.zip', {'a': 'foo', 'b': 'bar'}, {'foo': list_a, 'bar': list_b})
	verify('both, b compressed.zip', {}, {'a': list_a, 'b': list_b})
	verify('both called a.zip', {}, {'a': list_a, 'a_': list_b})
	verify('both called a, first compressed.zip', {}, {'a': list_a, 'a_': list_b})
	verify('many files.zip', {}, manyfiles, labelsonfirstline=False, labels=['0'])
	verify('named default.zip', {}, {'default': list_b})
	# Use inside_filenames to test this again in a different way.
	verify('a.zip', {'a': 'default'}, {'default': list_a})
