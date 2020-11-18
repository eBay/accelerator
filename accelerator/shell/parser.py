############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Carl Drougge                            #
# Modifications copyright (c) 2019 Anders Berkeman                         #
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

# common functionality for ds* commands

from __future__ import division, print_function

from os.path import join, exists, realpath

from accelerator.job import WORKDIRS
from accelerator.dataset import Dataset

def name2ds(n):
	if exists(n):
		# it's a path - dig out parts, maybe update WORKDIRS
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
		k = jid.rsplit("-", 1)[0]
		if WORKDIRS.get(k, base) != base:
			print("### Overriding workdir %s to %s" % (k, base,))
		WORKDIRS[k] = base
	elif n.startswith('/'):
		# meant to be a path, but it does not exist
		return None
	ds = Dataset(n)
	slices = ds.job.params.slices
	from accelerator import g
	if hasattr(g, 'slices'):
		assert g.slices == slices, "Dataset %s needs %d slices, by we are already using %d slices" % (ds, slices, g.slices)
	else:
		g.slices = slices
	return ds
