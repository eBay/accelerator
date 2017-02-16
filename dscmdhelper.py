# common functionality for ds* commands

from __future__ import division, print_function

import sys
from glob import glob
from os.path import join, exists, realpath
from functools import partial
from locale import resetlocale

from configfile import get_config
from jobid import WORKSPACES
from dataset import Dataset

def init():
	# initialize locale - for number formatting
	resetlocale()
	# find config files near script location, build WORKSPACES from them
	rel = partial(join, sys.path[0])
	for fn in glob(rel("*.conf")) + glob(rel("../*.conf")) + glob(rel("conf/*")) + glob(rel("../conf/*")):
		if not fn.lower().endswith(".template"):
			try:
				cfg = get_config(fn, False)
			except Exception:
				continue
			WORKSPACES.update({k: v[0] for k, v in cfg['workspace'].items()})

def name2ds(n):
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
	return Dataset(n)
