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
from os import getcwd, chdir
from os.path import dirname, realpath, join
from locale import resetlocale
from glob import glob
from argparse import ArgumentParser

cfg = None

class UserError(Exception):
	pass

def find_cfgs(basedir='.', wildcard=False):
	"""Find all accelerator.conf (or accelerator*.conf if wildcard=True)
	starting at basedir and continuing all the way to /, yielding them
	from the deepest directory first, starting with accelerator.conf (if
	present) and then the rest in sorted order."""

	cfgname = 'accelerator.conf'
	if wildcard:
		pattern = 'accelerator*.conf'
	else:
		pattern = cfgname
	orgdir = getcwd()
	basedir = realpath(basedir)
	while basedir != '/':
		try:
			chdir(basedir)
			fns = sorted(glob(pattern))
		finally:
			chdir(orgdir)
		if cfgname in fns:
			fns.remove(cfgname)
			fns.insert(0, cfgname)
		for fn in fns:
			yield join(basedir, fn)
		basedir = dirname(basedir)

def load_some_cfg(basedir='.', all=False):
	global cfg

	basedir = realpath(basedir)
	cfgs = find_cfgs(basedir, wildcard=all)
	if all:
		found_any = False
		# Start at the root, so closer cfgs override those further away.
		for fn in reversed(list(cfgs)):
			found_any = True
			load_cfg(fn)
		if not found_any:
			raise UserError("Could not find 'accelerator*.conf' in %r or any of its parents." % (basedir,))
	else:
		try:
			fn = next(cfgs)
		except StopIteration:
			raise UserError("Could not find 'accelerator.conf' in %r or any of its parents." % (basedir,))
		load_cfg(fn)

def load_cfg(fn):
	global cfg

	from configfile import get_config
	from jobid import WORKSPACES

	cfg = get_config(fn, False)
	WORKSPACES.update((k, v[0]) for k, v in cfg['workdir'].items())
	return cfg

def setup(config_fn=None, all_cfgs=False):
	resetlocale()
	accdir = dirname(__file__)
	while accdir in sys.path:
		sys.path.pop(sys.path.index(accdir))
	sys.path.insert(0, accdir)
	if config_fn:
		assert not all_cfgs, "Don't specify both a config_fn and all_cfgs."
		load_cfg(config_fn)
	else:
		load_some_cfg(all=all_cfgs)

def cmd_dsgrep(args, argv):
	from accelerator.dsgrep import main
	return main(argv, ' dsgrep')

ALL_CFGS_COMMANDS = {'dsgrep'}

COMMANDS = dict(
	dsgrep=cmd_dsgrep,
)

def cmd(argv):
	ap = ArgumentParser(add_help=False)
	ap.add_argument('--config', metavar='CONFIG_FILE', help='Configuration file')
	ap.add_argument('command')
	args, argv = ap.parse_known_args(argv)
	if args.command not in COMMANDS:
		print('Unknown command "%s"' % (args.command,), file=sys.stderr)
	try:
		setup(args.config, all_cfgs=args.command in ALL_CFGS_COMMANDS)
	except UserError as e:
		print(e, file=sys.stderr)
		return 1
	return COMMANDS[args.command](args, argv)
