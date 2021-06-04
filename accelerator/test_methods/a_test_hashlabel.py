############################################################################
#                                                                          #
# Copyright (c) 2019-2021 Carl Drougge                                     #
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
	# all the numeric types should hash the same (for values they have in common)
	for name, hashlabel, typ in (
		("unhashed_manual"    , None  , "int32"),     # manually interlaved
		("unhashed_split"     , None  , "int64"),     # split_write interlaved
		("up_checked"         , "up"  , "float32"),   # hashed on up using dw.hashcheck
		("up_split"           , "up"  , "float64"),   # hashed on up using split_write
		("down_checked"       , "down", "bits32"),    # hashed on down using dw.hashcheck
		("down_discarded"     , "down", "bits64"),    # hashed on down using discarding writes
		("down_discarded_list", "down", "number"),    # hashed on down using discarding list writes
		("down_discarded_dict", "down", "complex32"), # hashed on down using discarding dict writes
		# we have too many types, so we need more datasets
		("unhashed_complex64" , None  , "complex64"),
	):
		dw = DatasetWriter(name=name, hashlabel=hashlabel)
		dw.add("up", typ)
		dw.add("down", typ)
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
			dws.unhashed_complex64.write(up, down)
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

# complex isn't sortable
def uncomplex(t):
	if isinstance(t[0], complex):
		return tuple(v.real for v in t)
	else:
		return t

def synthesis(prepare_res, params, job, slices):
	dws = prepare_res
	for dw in (dws.unhashed_split, dws.up_split,):
		w = dw.get_split_write_list()
		for row in all_data:
			w(row)
	for dw in dws.values():
		dw.finish()

	# Verify that the different ways of writing gave the same result
	all_ds = []
	for names in (
		("unhashed_split", "unhashed_manual", "unhashed_complex64"),
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
		all_ds.append(list(dws.values()))

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
	for ds, chk_ix in ((up, 2), (down, 1)):
		for sliceno in range(slices):
			want = sorted(ds.iterate(sliceno))
			for chk_ds in all_ds[0] + all_ds[chk_ix]:
				assert chk_ds.hashlabel != ds.hashlabel
				got = chk_ds.iterate(sliceno, hashlabel=ds.hashlabel, rehash=True)
				got = sorted(map(uncomplex, got))
				assert want == got, "Rehashing is broken for %s (slice %d of %s)" % (chk_ds.columns[ds.hashlabel].type, sliceno, chk_ds,)

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
