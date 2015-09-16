#!/usr/bin/env python2.7
#
# Verify general operation and a few corner cases.

from __future__ import division, print_function

from datetime import datetime, date, time
import gzlines

TMP_FN = "_tmp_test.gz"

inf, ninf = float("inf"), float("-inf")

# The UInt types don't accept floats, the others Int types do.
# This is not really intentional, but it's easier and not obviously wrong,
# so it stays.

dttm0 = datetime(1789, 7, 14, 12, 42, 1, 82933)
dttm1 = datetime(2500, 12, 31, 23, 59, 59, 999999)
dttm2 = datetime(2015, 1, 1, 0, 0, 0, 0)
dt0 = date(1985, 7, 10)
tm0 = time(0, 0, 0, 0)
tm1 = time(2, 42, 0, 3)
tm2 = time(23, 59, 59, 999999)

for name, data, bad_cnt, res_data in (
	("Float64" , ["0", float, 0 , 4.2, -0.01, 1e42, inf, ninf], 2, [0.0, 4.2, -0.01, 1e42, inf, ninf]),
	("Float32" , ["0", float, 0L, 4.2, -0.01, 1e42, inf, ninf], 2, [0.0, 4.199999809265137, -0.009999999776482582, inf , inf, ninf]),
	("Int64"   , ["0", int, 0x8000000000000000, 0.1, 0x7fffffffffffffff, -5L], 3, [0, 0x7fffffffffffffff, -5]),
	("UInt64"  , ["0", int, -5L, -5, 0.1, 0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000L], 5, [0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000]),
	("Int32"   , ["0", int, 0x80000000, 0.1, 0x7fffffff, -5L], 3, [0, 0x7fffffff, -5]),
	("UInt32"  , ["0", int, -5L, -5, 0.1, 0x80000000, 0x7fffffff, 0x80000000L], 5, [0x80000000, 0x7fffffff, 0x80000000]),
	("Bool"    , ["0", bool, 0.0, True, False, 0, 1L], 2, [False, True, False, False, True]),
	("Lines"   , [42, str, "a", "foo bar baz"], 2, ["a", "foo bar baz"]),
	("DateTime", [42, "now", tm0, dttm0, dttm1, dttm2], 3, [dttm0, dttm1, dttm2]),
	("Date"    , [42, "now", tm0, dttm0, dttm1, dttm2, dt0], 3, [dttm0.date(), dttm1.date(), dttm2.date(), dt0]),
	("Time"    , [42, "now", dttm0, tm0, tm1, tm2], 3, [tm0, tm1, tm2]),
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
	if name == "Lines":
		continue # no default support
	for ix, default in enumerate(data):
		# Verify that defaults are accepted where expected
		try:
			with w_typ(TMP_FN, default=default) as fh:
				pass
			assert ix >= bad_cnt, repr(default)
		except AssertionError:
			raise
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
