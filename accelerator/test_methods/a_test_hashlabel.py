############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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
Test that hashlabel does what it says in both split_write and hashcheck.
Then test that rehashing gives the expected result, and that using the
wrong hashlabel without rehashing is not allowed.
'''

from accelerator.dataset import DatasetWriter, Dataset
from accelerator.extras import DotDict
from accelerator.gzwrite import typed_writer
from accelerator.error import DatasetUsageError

all_data = list(zip(range(10000), reversed(range(10000))))

def prepare(params):
	assert params.slices >= 2, "Hashing won't do anything with just one slice"
	dws = DotDict()
	for name, hashlabel in (
		("unhashed_manual", None), # manually interlaved
		("unhashed_split", None), # split_write interlaved
		("up_checked", "up"), # hashed on up using dw.hashcheck
		("up_split", "up"), # hashed on up using split_write
		("down_checked", "down"), # hashed on down using dw.hashcheck
		("down_discarded", "down"), # hashed on down using discarding writes
		("down_discarded_list", "down"), # hashed on down using discarding list writes
		("down_discarded_dict", "down"), # hashed on down using discarding dict writes
	):
		dw = DatasetWriter(name=name, hashlabel=hashlabel)
		dw.add("up", "int32")
		dw.add("down", "int32")
		dws[name] = dw
	return dws

def analysis(sliceno, prepare_res, params):
	dws = prepare_res
	dws.down_discarded.enable_hash_discard()
	dws.down_discarded_list.enable_hash_discard()
	dws.down_discarded_dict.enable_hash_discard()
	for ix, (up, down) in enumerate(all_data):
		if dws.up_checked.hashcheck(up):
			dws.up_checked.write(up, down)
		if dws.down_checked.hashcheck(down):
			dws.down_checked.write(up, down)
		if ix % params.slices == sliceno:
			dws.unhashed_manual.write(up, down)
		dws.down_discarded.write(up, down)
		dws.down_discarded_list.write_list([up, down])
		dws.down_discarded_dict.write_dict(dict(up=up, down=down))
	# verify that we are not allowed to write in the wrong slice without enable_hash_discard
	if not dws.up_checked.hashcheck(0):
		good = True
		for fn, a in (
			("write", (0, 0,)),
			("write_list", ([0, 0],)),
			("write_dict", (dict(up=0, down=0),)),
		):
			try:
				getattr(dws.up_checked, fn)(*a)
				good = False
			except Exception:
				pass
			assert good, "%s allowed writing in wrong slice" % (fn,)

def synthesis(prepare_res, params, job, slices):
	dws = prepare_res
	for dw in (dws.unhashed_split, dws.up_split,):
		w = dw.get_split_write_list()
		for row in all_data:
			w(row)
	for dw in dws.values():
		dw.finish()

	# Verify that the different ways of writing gave the same result
	for names in (
		("unhashed_split", "unhashed_manual"),
		("up_checked", "up_split"),
		("down_checked", "down_discarded", "down_discarded_list", "down_discarded_dict"),
	):
		dws = {name: job.dataset(name) for name in names}
		assert dws == {name: Dataset((params.jobid, name)) for name in names}, "Old style Dataset((params.jobid, name)) broken"
		for sliceno in range(slices):
			data = {name: list(dws[name].iterate(sliceno)) for name in names}
			good = data[names[0]]
			for name in names[1:]:
				assert data[name] == good, "%s doesn't match %s in slice %d" % (names[0], name, sliceno,)

	# Verify that both up and down hashed on the expected column
	hash = typed_writer("int32").hash
	for colname in ("up", "down"):
		ds = job.dataset(colname + "_checked")
		for sliceno in range(slices):
			for value in ds.iterate(sliceno, colname):
				assert hash(value) % slices == sliceno, "Bad hashing on %s in slice %d" % (colname, sliceno,)

	# Verify that up and down are not the same, to catch hashing
	# not actually hashing.
	up = list(job.dataset("up_checked").iterate(None))
	down = list(job.dataset("down_checked").iterate(None))
	assert up != down, "Hashlabel did not change slice distribution"
	# And check that the data is still the same.
	assert sorted(up) == sorted(down) == all_data, "Hashed datasets have wrong data"

	# Verify that rehashing works.
	# (Can't use sliceno None, because that won't rehash, and even if it did
	# the order wouldn't match. Order doesn't even match in the rehashed
	# individual slices.)
	up = job.dataset("up_checked")
	down = job.dataset("down_checked")
	unhashed = job.dataset("unhashed_manual")
	for sliceno in range(slices):
		a = list(up.iterate(sliceno))
		b = list(down.iterate(sliceno, hashlabel="up", rehash=True))
		c = list(unhashed.iterate(sliceno, hashlabel="up", rehash=True))
		assert sorted(a) == sorted(b) == sorted(c), "Rehashing is broken (slice %d)" % (sliceno,)

	# And finally verify that we are not allowed to specify the wrong hashlabel
	good = True
	try:
		up.iterate(None, hashlabel="down")
		good = False
	except DatasetUsageError:
		pass
	try:
		unhashed.iterate(None, hashlabel="down")
		good = False
	except DatasetUsageError:
		pass
	assert good, "Iteration allowed on the wrong hashlabel"
