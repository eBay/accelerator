#!/usr/bin/env python2.7
#
# Verify general operation and a few corner cases.

from __future__ import division, print_function

import gzlines

TMP_FN = "_tmp_test.gz"

inf, ninf = float("inf"), float("-inf")

# The UInt types don't accept floats, the others Int types do.
# This is not really intentional, but it's easier and not obviously wrong,
# so it stays.

for name, data, bad_cnt, res_data in (
	("Float64", ["0", float, 0 , 4.2, -0.01, 1e42, inf, ninf], 2, [0.0, 4.2, -0.01, 1e42, inf, ninf]),
	("Float32", ["0", float, 0L, 4.2, -0.01, 1e42, inf, ninf], 2, [0.0, 4.199999809265137, -0.009999999776482582, inf , inf, ninf]),
	("Int64"  , ["0", int, 0x8000000000000000, 0.1, 0x7fffffffffffffff, -5L], 3, [0, 0x7fffffffffffffff, -5]),
	("UInt64" , ["0", int, -5L, -5, 0.1, 0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000L], 5, [0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000]),
	("Int32"  , ["0", int, 0x80000000, 0.1, 0x7fffffff, -5L], 3, [0, 0x7fffffff, -5]),
	("UInt32" , ["0", int, -5L, -5, 0.1, 0x80000000, 0x7fffffff, 0x80000000L], 5, [0x80000000, 0x7fffffff, 0x80000000]),
	("Bool"   , ["0", bool, 0.0, True, False, 0, 1L], 2, [False, True, False, False, True]),
):
	print(name)
	r_typ = getattr(gzlines, "Gz" + name)
	w_typ = getattr(gzlines, "GzWrite" + name)
	with w_typ(TMP_FN) as fh:
		for ix, value in enumerate(data):
			try:
				fh.write(value)
				assert ix >= bad_cnt, repr(value)
			except (ValueError, TypeError, OverflowError):
				assert ix < bad_cnt, repr(value)
	# Okay, errors look good
	with r_typ(TMP_FN) as fh:
		res = list(fh)
		assert res == res_data, res
	# Data comes back as expected.
	for ix, default in enumerate(data):
		# Verify that defaults are accepted where expected
		try:
			with w_typ(TMP_FN, default=default) as fh:
				pass
			assert ix >= bad_cnt, repr(default)
		except Exception:
			assert ix < bad_cnt, repr(default)
		if ix >= bad_cnt:
			with w_typ(TMP_FN, default=default) as fh:
				for value in data:
					try:
						fh.write(value)
					except (ValueError, TypeError, OverflowError):
						assert 0, "No default: %r" % (value,)
			# No errors when there is a default
			with r_typ(TMP_FN) as fh:
				res = list(fh)
				assert res == [res_data[ix - bad_cnt]] * bad_cnt + res_data, res
			# Great, all default values came out right in the file!

print("Append test")
# And finally verify appending works as expected.
with gzlines.GzWriteInt64(TMP_FN) as fh:
	fh.write(42)
with gzlines.GzWriteInt64(TMP_FN, mode="a") as fh:
	fh.write(18)
with gzlines.GzInt64(TMP_FN) as fh:
	assert list(fh) == [42, 18]
