#!/usr/bin/env python
#
# Print information about a dataset(chain)

from __future__ import division, print_function

import sys
from glob import glob
from os.path import join, exists, realpath
from functools import partial

from configfile import get_config
from jobid import WORKSPACES
from dataset import Dataset

# find config files near script location, build WORKSPACES from them
rel = partial(join, sys.path[0])
for fn in glob(rel("*.conf")) + glob(rel("../*.conf")) + glob(rel("conf/*")) + glob(rel("../conf/*")):
	if not fn.lower().endswith(".template"):
		try:
			cfg = get_config(fn, False)
		except Exception:
			continue
		WORKSPACES.update({k: v[0] for k, v in cfg['workspace'].items()})

# for every name argument
for n in sys.argv[1:]:
	if exists(n):
		# it's a path - dig out parts, maybe update WORKSPACES
		n = realpath(n)
		if n.endswith("/dataset.pickle"):
			n = n.rsplit("/", 1)[0]
		if exists(join(n, "dataset.pickle")):
			# includes ds name
			base, jid, name = n.rsplit("/", 2)
			n = (jid, name)
		else:
			# bare jid (no ds name)
			base, jid = n.rsplit("/", 1)
			n = jid
		k = jid.split("-", 1)[0]
		if WORKSPACES.get(k, base) != base:
			print("### Overriding workspace %s to %s" % (k, base,))
		WORKSPACES[k] = base
	ds = Dataset(n)
	print("Parent:", ds.parent)
	print("Columns:")
	# there's a better way to do this, right?
	len_n = len_t = 0
	for n, c in ds.columns.items():
		len_n = max(len_n, len(n))
		len_t = max(len_t, len(c.type))
	template = "    %%%ds  %%%ds" % (len_n, len_t,)
	for n, c in ds.columns.items():
		print(template % (n, c.type,))
	print("%d lines" % (sum(ds.lines),))
	if ds.previous:
		chain = ds.chain()
		print("Chain length %d, from %s to %s" % (len(chain), chain[0], chain[-1],))
		print("%d total lines" % (sum(sum(ds.lines) for ds in chain)),)
