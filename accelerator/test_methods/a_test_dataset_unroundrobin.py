############################################################################
#                                                                          #
# Copyright (c) 2020 Carl Drougge                                          #
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
Test that dataset_unroundrobin produces the correct order.
'''

from accelerator import subjobs

def synthesis(job, slices):
	# Test with just two lines, to check that empty slices are not a problem.
	dw = job.datasetwriter(name='two lines', columns=dict(a='number'))
	for sliceno in range(slices):
		dw.set_slice(sliceno)
		if sliceno < 2:
			dw.write(sliceno)
	ds_2l = dw.finish()
	ds_unrr = subjobs.build('dataset_unroundrobin', source=ds_2l).dataset()
	assert list(ds_2l.iterate(None)) == list(ds_unrr.iterate(None))

	# Test with several lines, to check that order is correct.
	dw = job.datasetwriter(name='rr', columns=dict(a='int32', b='unicode'))
	for sliceno in range(slices):
		dw.set_slice(sliceno)
		dw.write(sliceno, 'u %d' % (sliceno,))
		dw.write(sliceno, 'line 2')
		dw.write(sliceno, 'line 3')
		if sliceno == 0:
			dw.write(-1, 'line 4 just in slice 0')
	ds_rr = dw.finish()
	ds_unrr = subjobs.build('dataset_unroundrobin', source=ds_rr).dataset()
	it = ds_unrr.iterate(None)
	def want(a, b):
		try:
			got = next(it)
		except StopIteration:
			raise Exception('missing lines in %s' % (ds_unrr,))
		assert got == (a, b), "Wanted %r, got %r from %s" % ((a, b,), got, ds_unrr,)
	for sliceno in range(slices):
		want(sliceno, 'u %d' % (sliceno,))
	for sliceno in range(slices):
		want(sliceno, 'line 2')
	for sliceno in range(slices):
		want(sliceno, 'line 3')
	want(-1, 'line 4 just in slice 0')
	try:
		next(it)
		raise Exception("Extra lines in %s" % (ds_unrr,))
	except StopIteration:
		pass

	# Check that exporting and then importing gives the expected order
	exported = subjobs.build('csvexport', source=ds_unrr, filename='unrr.csv')
	imported = subjobs.build('csvimport', filename=exported.filename('unrr.csv'))
	imported = subjobs.build('dataset_type', source=imported, column2type=dict(a='int32_10', b='ascii')).dataset()
	for sliceno in range(slices):
		assert list(imported.iterate(sliceno)) == list(ds_rr.iterate(sliceno)), "%s did not match %s in slice %d, export or import does not match roundrobin expectations" % (imported, ds_rr, sliceno)

	# Check that empty slices in the middle are not a problem.
	dw = job.datasetwriter(name='empty slices', columns=dict(a='number'))
	for sliceno in range(slices):
		dw.set_slice(sliceno)
		if sliceno == 1:
			dw.write(1)
			dw.write(3)
			dw.write(4)
		elif sliceno == slices - 1:
			dw.write(2)
	ds_empty = dw.finish()
	ds_unrr = subjobs.build('dataset_unroundrobin', source=ds_empty).dataset()
	assert list(ds_unrr.iterate(None, 'a')) == [1, 2, 3, 4]
	# Verify that slice distribution was not changed.
	assert list(ds_unrr.iterate(1, 'a')) == [1, 2, 3]
	assert list(ds_unrr.iterate(slices - 1, 'a')) == [4]
