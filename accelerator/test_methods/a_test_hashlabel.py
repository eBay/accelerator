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

from accelerator.dataset import DatasetWriter
from accelerator.extras import DotDict
from accelerator.dsutil import typed_writer
from accelerator.error import DatasetUsageError
from accelerator.compat import unicode

from datetime import datetime, date

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
		("unhashed_bytes"     , None  , "bytes"),
		("up_ascii"           , "up"  , "ascii"),
		("down_unicode"       , "down", "unicode"),
		# datetime on 1970-01-01 hashes like time
		("up_datetime"        , "up"  , "datetime"),
		("down_time"          , "down", "time"),
		# date doesn't hash the same as anything else, so compare it to itself
		("up_date"            , "up"  , "date"),
		("down_date"          , "down", "date"),
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
	dws.up_datetime.enable_hash_discard()
	dws.down_time.enable_hash_discard()
	dws.up_ascii.enable_hash_discard()
	dws.down_unicode.enable_hash_discard()
	dws.up_date.enable_hash_discard()
	dws.down_date.enable_hash_discard()
	for ix, (up, down) in enumerate(all_data):
		if dws.up_checked.hashcheck(up):
			dws.up_checked.write(up, down)
		if dws.down_checked.hashcheck(down):
			dws.down_checked.write(up, down)
		if ix % params.slices == sliceno:
			dws.unhashed_manual.write(up, down)
			dws.unhashed_complex64.write(up, down)
			dws.unhashed_bytes.write(str(up).encode("ascii"), str(down).encode("ascii"))
		dws.down_discarded.write(up, down)
		dws.down_discarded_list.write_list([up, down])
		dws.down_discarded_dict.write_dict(dict(up=up, down=down))
		dt_up = datetime(1970, 1, 1, 0, 0, 0, up)
		dt_down = datetime(1970, 1, 1, 0, 0, 0, down)
		dws.up_datetime.write(dt_up, dt_down)
		dws.down_time.write(dt_up.time(), dt_down.time())
		dws.up_date.write(date.fromordinal(up + 1), date.fromordinal(down + 1))
		dws.down_date.write(date.fromordinal(up + 1), date.fromordinal(down + 1))
		dws.up_ascii.write(str(up), str(down))
		dws.down_unicode.write(unicode(up), unicode(down))
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

# datetime doesn't compare to time (or numbers, for the all_data check)
def undatetime(t):
	if hasattr(t[0], "microsecond"):
		return tuple(v.microsecond for v in t)
	elif hasattr(t[0], "toordinal"):
		return tuple(v.toordinal() - 1 for v in t)
	else:
		return t

def unstr(t):
	if isinstance(t[0], (bytes, unicode)):
		return tuple(int(v) for v in t)
	else:
		return t

def cleanup(lst):
	return [unstr(undatetime(uncomplex(t))) for t in lst]

def synthesis(prepare_res, params, job, slices):
	dws = prepare_res
	for dw in (dws.unhashed_split, dws.up_split,):
		w = dw.get_split_write_list()
		for row in all_data:
			w(row)
	hl2ds = {None: [], "up": [], "down": []}
	all_ds = {}
	special_cases = {
		"up_datetime", "down_time", "up_date", "down_date",
		"unhashed_bytes", "up_ascii", "down_unicode",
	}
	for name, dw in dws.items():
		ds = dw.finish()
		all_ds[ds.name] = ds
		if ds.name not in special_cases:
			hl2ds[ds.hashlabel].append(ds)

	# Verify that the different ways of writing gave the same result
	for hashlabel in (None, "up", "down"):
		for sliceno in range(slices):
			data = [(ds, list(ds.iterate(sliceno))) for ds in hl2ds[hashlabel]]
			good = data[0][1]
			for name, got in data:
				assert got == good, "%s doesn't match %s in slice %d" % (data[0][0], name, sliceno,)

	# Verify that both up and down hashed on the expected column
	hash = typed_writer("int32").hash
	for colname in ("up", "down"):
		ds = all_ds[colname + "_checked"]
		for sliceno in range(slices):
			for value in ds.iterate(sliceno, colname):
				assert hash(int(value)) % slices == sliceno, "Bad hashing on %s in slice %d" % (colname, sliceno,)

	# Verify that up and down are not the same, to catch hashing
	# not actually hashing.
	for up_name, down_name in (
		("up_checked", "down_checked"),
		("up_datetime", "down_time"),
		("up_date", "down_date"),
		("up_ascii", "down_unicode"),
	):
		up = cleanup(all_ds[up_name].iterate(None))
		down = cleanup(all_ds[down_name].iterate(None))
		assert up != down, "Hashlabel did not change slice distribution (%s vs %s)" % (up_name, down_name)
		# And check that the data is still the same.
		assert sorted(up) == sorted(down) == all_data, "Hashed datasets have wrong data (%s vs %s)" % (up_name, down_name)

	# Verify that rehashing works.
	# (Can't use sliceno None, because that won't rehash, and even if it did
	# the order wouldn't match. Order doesn't even match in the rehashed
	# individual slices.)
	def test_rehash(want_ds, chk_ds_lst):
		want_ds = all_ds[want_ds]
		for sliceno in range(slices):
			want = sorted(cleanup(want_ds.iterate(sliceno)))
			for chk_ds in chk_ds_lst:
				assert chk_ds.hashlabel != want_ds.hashlabel
				got = chk_ds.iterate(sliceno, hashlabel=want_ds.hashlabel, rehash=True)
				got = sorted(cleanup(got))
				assert want == got, "Rehashing is broken for %s (slice %d of %s)" % (chk_ds.columns[want_ds.hashlabel].type, sliceno, chk_ds,)
	test_rehash("up_checked", hl2ds[None] + hl2ds["down"])
	test_rehash("down_checked", hl2ds[None] + hl2ds["up"])
	test_rehash("up_datetime", [all_ds["down_time"]])
	test_rehash("down_time", [all_ds["up_datetime"]])
	test_rehash("down_date", [all_ds["up_date"]])
	test_rehash("up_ascii", [all_ds["unhashed_bytes"], all_ds["down_unicode"]])
	test_rehash("down_unicode", [all_ds["unhashed_bytes"], all_ds["up_ascii"]])

	# And finally verify that we are not allowed to specify the wrong hashlabel
	good = True
	try:
		all_ds["up_checked"].iterate(None, hashlabel="down")
		good = False
	except DatasetUsageError:
		pass
	try:
		all_ds["unhashed_manual"].iterate(None, hashlabel="down")
		good = False
	except DatasetUsageError:
		pass
	assert good, "Iteration allowed on the wrong hashlabel"

	# verify that non-integral floats hash the same in the five types that can have them
	# using + 0.5 is safe for the values we use, it can be exactly represented in 32 bit floats.
	float_data = [v + 0.5 for v, _ in all_data]
	float_ds_lst = []
	for typ in ("float32", "float64", "complex32", "complex64", "number"):
		dw = job.datasetwriter(name="floattest_" + typ, columns={"value": typ}, hashlabel="value")
		write = dw.get_split_write()
		for v in float_data:
			write(v)
		float_ds_lst.append(dw.finish())
	for sliceno in range(slices):
		values = [(ds, list(ds.iterate(sliceno, "value"))) for ds in float_ds_lst]
		want_ds, want = values.pop()
		for ds, got in values:
			assert got == want, "%s did not match %s in slice %d" % (ds, want_ds, sliceno,)
