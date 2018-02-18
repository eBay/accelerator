#!/usr/bin/env python3
# -*- coding: iso-8859-1 -*-

############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

from optparse import OptionParser
import sys
from importlib import import_module
from os.path import realpath, dirname
from inspect import getargspec
from os import environ

from compat import quote_plus, PY3

import automata_common
from dispatch import JobError
from autoflush import AutoFlush


def find_automata(a, package, script):
	if package:
		package = [package]
	else:
		package = sorted(a.config()['method_directories'])
	if not script:
		script = 'automata'
	if not script.startswith('automata'):
		script = 'automata_' + script
	for p in package:
		module_name = p + '.' + script
		try:
			module_ref = import_module(module_name)
			print(module_name)
			return module_ref
		except ImportError as e:
			if PY3:
				if not e.msg[:-1].endswith(script):
					raise
			else:
				if not e.message.endswith(script):
					raise
	raise Exception('No automata "%s" found in {%s}' % (script, ', '.join(package)))

def run_automata(options):
	if options.port:
		assert not options.socket, "Specify either socket or port (with optional hostname)"
		url = 'http://' + (options.hostname or 'localhost') + ':' + str(options.port)
	else:
		assert not options.hostname, "Specify either socket or port (with optional hostname)"
		url = 'unixhttp://' + quote_plus(realpath(options.socket or './socket.dir/default'))

	a = automata_common.Automata(url, verbose=options.verbose, flags=options.flags.split(','), infoprints=True)

	if options.abort:
		a.abort()
		return

	try:
		a.wait(ignore_old_errors=not options.just_wait)
	except JobError:
		# An error occured in a job we didn't start, which is not our problem.
		pass

	if options.just_wait:
		return

	module_ref = find_automata(a, options.package, options.script)

	if getargspec(module_ref.main).args == ['urd']: # the future!
		if 'URD_AUTH' in environ:
			user, password = environ['URD_AUTH'].split(':', 1)
		else:
			user, password = None, None
		info = a.info()
		urd = automata_common.Urd(a, info, user, password, options.horizon)
		if options.quick:
			a.update_method_deps()
		else:
			a.update_methods()
		module_ref.main(urd)
		return

	assert not options.horizon, '--horizon is only compatible with urd-enabled automatas'

	module_ref.auto = automata_common
	module_ref.a    = a
	module_ref.PATH = a.info()['path']
	module_ref.Seq  = a.seq

	# Args you can get to autamata_foo.main
	# I would say automata, seq and path are the only reasonable names here.
	path = module_ref.PATH
	argd = dict(a=a, automata=a, PATH=path, path=path)

	# run automata script
	kw = {}
	for arg in getargspec(module_ref.main).args:
		kw[arg] = argd[arg]
	module_ref.main(**kw)
	return


def main(argv):
	parser = OptionParser(usage="Usage: %prog [options] [script]")
	parser.add_option('-p', '--port',     dest="port",     default=None,        help="framework listening port", )
	parser.add_option('-H', '--hostname', dest="hostname", default=None,        help="framework hostname", )
	parser.add_option('-S', '--socket',   dest="socket",   default=None,        help="framework unix socket (default ./socket.dir/default)", )
	parser.add_option('-s', '--script',   dest="script",   default=None      ,  help="automata script to run. package/[automata_]script.py. default \"automata\". Can be bare arg too.",)
	parser.add_option('-P', '--package',  dest="package",  default=None      ,  help="package where to look for script, default all method directories in alphabetical order", )
	parser.add_option('-f', '--flags',    dest="flags",    default='',          help="comma separated list of flags", )
	parser.add_option('-A', '--abort',    dest="abort",    action='store_true', help="abort (fail) currently running job(s)", )
	parser.add_option('-q', '--quick',    dest="quick",    action='store_true', help="skip method updates and checking workdirs for new jobs", )
	parser.add_option('-w', '--just_wait',dest="just_wait",action='store_true', help="just wait for running job, don't run any automata", )
	parser.add_option('--verbose',        dest="verbose",  default='status',    help="verbosity style {no, status, dots, log}")
	parser.add_option('--quiet',          dest="quiet",    action='store_true', help="same as --verbose=no")
	parser.add_option('--horizon',        dest="horizon",  default=None,        help="Time horizon - dates after this are not visible in urd.latest")

	options, args = parser.parse_args(argv)
	if len(args) == 1:
		assert options.script is None, "Don't specify both --script and a bare script name."
		options.script = args[0]
	else:
		assert not args, "Don't know what to do with args %r" % (args,)

	options.verbose = {'no': False, 'status': True, 'dots': 'dots', 'log': 'log'}[options.verbose]
	if options.quiet: options.verbose = False

	run_automata(options)


if __name__ == "__main__":
	# sys.path needs to contain .. (the project dir), put it after accelerator
	sys.path.insert(1, dirname(sys.path[0]))
	sys.stdout = AutoFlush(sys.stdout)
	sys.stderr = AutoFlush(sys.stderr)
	main(sys.argv[1:])
