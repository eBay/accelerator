############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Anders Berkeman                         #
# Modifications copyright (c) 2018-2019 Carl Drougge                       #
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

from argparse import ArgumentParser, RawTextHelpFormatter
import sys
from importlib import import_module
from os.path import realpath
from os import environ

from accelerator.compat import quote_plus, PY3, getarglist

from accelerator import automata_common
from accelerator.dispatch import JobError


def find_automata(a, package, script):
	all_packages = sorted(a.config()['method_directories'])
	if package:
		if package in all_packages:
			package = [package]
		else:
			for cand in all_packages:
				if cand.endswith('.' + package):
					package = [cand]
					break
			else:
				raise Exception('No method directory found for %r in %r' % (package, all_packages))
	else:
		package = all_packages
	if not script.startswith('build'):
		script = 'build_' + script
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
	raise Exception('No build script "%s" found in {%s}' % (script, ', '.join(package)))

def run_automata(options):
	if options.port:
		assert not options.socket, "Specify either socket or port (with optional hostname)"
		url = 'http://' + (options.hostname or 'localhost') + ':' + str(options.port)
	else:
		assert not options.hostname, "Specify either socket or port (with optional hostname)"
		url = 'unixhttp://' + quote_plus(realpath(options.socket or './socket.dir/default'))

	a = automata_common.Automata(url, verbose=options.verbose, flags=options.flags.split(','), infoprints=True, print_full_jobpath=options.fullpath)

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

	assert getarglist(module_ref.main) == ['urd'], "Only urd-enabled automatas are supported"
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


def main(argv):
	parser = ArgumentParser(
		usage="run [options] [script]",
		formatter_class=RawTextHelpFormatter,
	)
	parser.add_argument('-p', '--port',     default=None,        help="framework listening port", )
	parser.add_argument('-H', '--hostname', default=None,        help="framework hostname", )
	parser.add_argument('-S', '--socket',   default=None,        help="framework unix socket (default ./socket.dir/default)", )
	parser.add_argument('-f', '--flags',    default='',          help="comma separated list of flags", )
	parser.add_argument('-A', '--abort',    action='store_true', help="abort (fail) currently running job(s)", )
	parser.add_argument('-q', '--quick',    action='store_true', help="skip method updates and checking workdirs for new jobs", )
	parser.add_argument('-w', '--just_wait',action='store_true', help="just wait for running job, don't run any build script", )
	parser.add_argument('-F', '--fullpath', action='store_true', help="print full path to jobdirs")
	parser.add_argument('--verbose',        default='status',    help="verbosity style {no, status, dots, log}")
	parser.add_argument('--quiet',          action='store_true', help="same as --verbose=no")
	parser.add_argument('--horizon',        default=None,        help="time horizon - dates after this are not visible in\nurd.latest")
	parser.add_argument('script',           default='build'   ,  help="build script to run. default \"build\".\nsearches under all method directories in alphabetical\norder if it does not contain a dot.\nprefixes build_ to last element unless specified.\npackage name suffixes are ok.\nso for example \"test_methods.tests\" expands to\n\"accelerator.test_methods.build_tests\".", nargs='?')

	options = parser.parse_args(argv)

	if '.' in options.script:
		options.package, options.script = options.script.rsplit('.', 1)
	else:
		options.package = None

	options.verbose = {'no': False, 'status': True, 'dots': 'dots', 'log': 'log'}[options.verbose]
	if options.quiet: options.verbose = False

	try:
		run_automata(options)
		return 0
	except JobError:
		# If it's a JobError we don't care about the local traceback,
		# we want to see the job traceback, and maybe know what line
		# we built the job on.
		print_minimal_traceback()
	return 1


def print_minimal_traceback():
	ac_fn = automata_common.__file__
	if ac_fn[-4:] in ('.pyc', '.pyo',):
		# stupid python2
		ac_fn = ac_fn[:-1]
	blacklist_fns = {ac_fn}
	last_interesting = None
	_, e, tb = sys.exc_info()
	while tb is not None:
		code = tb.tb_frame.f_code
		if code.co_filename not in blacklist_fns:
			last_interesting = tb
		tb = tb.tb_next
	lineno = last_interesting.tb_lineno
	filename = last_interesting.tb_frame.f_code.co_filename
	print("Failed to build job %s on %s line %d" % (e.jobid, filename, lineno,))
