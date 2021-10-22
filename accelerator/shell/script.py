############################################################################
#                                                                          #
# Copyright (c) 2021 Anders Berkeman                                       #
# Modifications copyright (c) 2021 Carl Drougge                            #
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
from glob import glob
from os.path import dirname, basename
from importlib import import_module
from accelerator.compat import terminal_size, ArgumentParser
from accelerator.colourwrapper import colour
from accelerator.shell import printdesc


def main(argv, cfg):
	descr = "lists and describes build scripts"
	parser = ArgumentParser(
		prog=argv.pop(0),
		description=descr,
	)
	parser.add_argument('-s', '--short', action='store_true', help='short listing')
	parser.add_argument('-p', '--path', action='store_true', help='show package paths')
	parser.add_argument('match', nargs='*', default=[], help='substring used for matching')
	args = parser.parse_intermixed_args(argv)
	columns = terminal_size().columns

	if not args.match:
		# no args => list everything in short format
		args.match = ['']
		args.short = True

	packages = []
	for package in cfg.method_directories:
		path = dirname(import_module(package).__file__)
		scripts = []
		packages.append((package, path, scripts))
		for item in sorted(glob(path + '/build.py') + glob(path + '/build_*.py')):
			name = basename(item[:-3])
			modname = '.'.join((package, name))
			if any(m in modname for m in args.match):
				try:
					module = import_module(modname)
				except Exception as e:
					print('%s%s: %s%s' % (colour.RED, item, e, colour.RESET), file=sys.stderr)
					continue
				scripts.append((name, getattr(module, 'description', '')))

	for package, path, scripts in sorted(packages):
		if scripts:
			if args.path:
				print(path + '/')
			else:
				print(package)
			printdesc(sorted(scripts), columns, full=not args.short)
