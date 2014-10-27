#!/usr/bin/env python

from sys import argv
if len(argv) > 1:
	from gzlines_cffi import gzlines
else:
	from gzlines import gzlines
from time import time

t = time()
count = 0
with gzlines("/tmp/foo.gz") as fh:
	for line in fh:
		count += 1
print count, time() - t
