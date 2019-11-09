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
from argparse import ArgumentParser, RawDescriptionHelpFormatter

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
			try:
				load_cfg(fn)
				found_any = True
			except Exception:
				# As long as we find at least one we're happy.
				pass
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

	from accelerator.configfile import load_config
	from accelerator.job import WORKDIRS

	cfg = load_config(fn)
	for k, v in cfg.workdirs.items():
		if WORKDIRS.get(k, v) != v:
			print("WARNING: %s overrides workdir %s" % (fn, k,), file=sys.stderr)
		WORKDIRS[k] = v
	return cfg

def unpath(path):
	while path in sys.path:
		sys.path.pop(sys.path.index(path))

def setup(config_fn=None, debug_cmd=False):
	resetlocale()
	# Make sure the accelerator dir in not in sys.path
	# (as it might be if running without installing.)
	unpath(dirname(__file__))
	if config_fn is False:
		return
	if config_fn:
		load_cfg(config_fn)
	else:
		load_some_cfg(all=debug_cmd)
	if not debug_cmd:
		# We want the project directory to be first in sys.path.
		unpath(cfg['project_directory'])
		sys.path.insert(0, cfg['project_directory'])
		# For consistency we also always want the project dir
		# as working directory.
		chdir(cfg['project_directory'])

def cmd_dsgrep(argv):
	from accelerator.dsgrep import main
	return main(argv)
cmd_dsgrep.help = '''Search for a pattern in one or more datasets'''

def cmd_dsinfo(argv):
	from accelerator.dsinfo import main
	return main(argv)
cmd_dsinfo.help = '''Display information about datasets'''

def cmd_run(argv):
	from accelerator.build import main
	return main(argv)
cmd_run.help = '''Run a build script'''

def cmd_daemon(argv):
	from accelerator.daemon import main
	main(argv, cfg)
cmd_daemon.help = '''Run the main daemon'''

def cmd_init(argv):
	from accelerator.init import main
	main(argv)
cmd_init.help = '''Create a project directory'''

def cmd_urd(argv):
	from accelerator.urd import main
	main(argv)
cmd_urd.help = '''Run the urd daemon'''

DEBUG_COMMANDS = {'dsgrep', 'dsinfo',}

COMMANDS = dict(
	dsgrep=cmd_dsgrep,
	dsinfo=cmd_dsinfo,
	run=cmd_run,
	daemon=cmd_daemon,
	init=cmd_init,
	urd=cmd_urd,
)

class HelpFixArgumentParser(ArgumentParser):
	'''We don't want this argument parser to eat --help for our
	sub commands, but we do want it to take help when no command
	is specified'''

	def __init__(self, argv, **kw):
		self.__argv = argv
		ArgumentParser.__init__(self, **kw)

	def error(self, message):
		if '--help' in self.__argv or '-h' in self.__argv:
			self.print_help()
			self.exit(0)
		ArgumentParser.error(self, message)

def main():
	from accelerator.autoflush import AutoFlush
	argv = sys.argv[1:]
	sys.stdout = AutoFlush(sys.stdout)
	sys.stderr = AutoFlush(sys.stderr)
	epilog = ['commands:', '']
	cmdlen = max(len(cmd) for cmd in COMMANDS)
	template = '  %%%ds  %%s' % (cmdlen,)
	for cmd, func in sorted(COMMANDS.items()):
		epilog.append(template % (cmd, func.help,))
	epilog.append('')
	epilog.append('Use %(prog)s <command> --help for <command> usage.')
	parser = HelpFixArgumentParser(
		argv,
		add_help=False,
		epilog='\n'.join(epilog),
		formatter_class=RawDescriptionHelpFormatter,
	)
	parser.add_argument('--config', metavar='CONFIG_FILE', help='Configuration file')
	parser.add_argument('command')
	args, argv = parser.parse_known_args(argv)
	if args.command not in COMMANDS:
		parser.print_help(file=sys.stderr)
		print(file=sys.stderr)
		print('Unknown command "%s"' % (args.command,), file=sys.stderr)
		sys.exit(2)
	try:
		config_fn = args.config
		if args.command == 'init':
			config_fn = False
		setup(config_fn, debug_cmd=args.command in DEBUG_COMMANDS)
		return COMMANDS[args.command](argv)
	except UserError as e:
		print(e, file=sys.stderr)
		return 1
