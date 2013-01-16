#!/usr/bin/env python

from gzlines import gzlines

with gzlines("/tmp/foo.gz") as fh:
	for line in fh:
		print line
