#!/usr/bin/env python2.7
#
# Verify general operation and a few corner cases.

from __future__ import division, print_function

import gzlines

TMP_FN = "_tmp_test.gz"

inf, ninf = float("inf"), float("-inf")

for name, data, bad_cnt, default, res_data in (
	("Float64", ["0", float, 0 , 1e42, inf, ninf], 2,  0.5, [0.0, 1e42, inf, ninf]),
	("Float32", ["0", float, 0L, 1e42, inf, ninf], 2, -0.5, [0.0, inf , inf, ninf]),
	("Int64"  , ["0", 0.0, int, 0x8000000000000000, 0x7fffffffffffffff, -5L], 4, 18, [0x7fffffffffffffff, -5]),
	("UInt64" , ["0", 0.0, int, -5L, -5, 0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000L], 5, 18, [0x8000000000000000, 0x7fffffffffffffff, 0x8000000000000000]),
	("Int32"  , ["0", 0.0, int, 0x80000000, 0x7fffffff, -5L], 4, 18, [0x7fffffff, -5]),
	("UInt32" , ["0", 0.0, int, -5L, -5, 0x80000000, 0x7fffffff, 0x80000000L], 5, 18, [0x80000000, 0x7fffffff, 0x80000000]),
	("Bool"   , ["0", bool, 0.0, True, False, 0, 1L], 3, False, [True, False, False, True]),
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
	with r_typ(TMP_FN) as fh:
		res = list(fh)
		assert res == res_data, res
