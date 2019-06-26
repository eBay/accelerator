############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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

import sys
from os.path import dirname, realpath, join, exists
from locale import resetlocale

cfg = None

class UserError(Exception):
	pass

def load_cfg(basedir='.'):
	global cfg

	from configfile import get_config
	from jobid import WORKSPACES

	basedir = realpath(basedir)
	dn = basedir
	while dn != '/':
		fn = join(dn, 'accelerator.conf')
		if exists(fn):
			cfg = get_config(fn, False)
			break
		dn = dirname(dn)
	if not cfg:
		raise UserError("Could not find 'accelerator.conf' in %r or any of its parents." % (basedir,))
	WORKSPACES.update((k, v[0]) for k, v in cfg['workdir'].items())
	return cfg

def setup():
	resetlocale()
	accdir = dirname(__file__)
	while accdir in sys.path:
		sys.path.pop(sys.path.index(accdir))
	sys.path.insert(0, accdir)
	load_cfg('.')

def cmd(argv):
	try:
		setup()
	except UserError as e:
		print(e, file=sys.stderr)
		return 1
	return 0
