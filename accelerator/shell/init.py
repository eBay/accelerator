############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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


a_example = r"""description = r'''
This is just an example. It doesn't even try to do anything useful.

You can run it to see that your installation works.
'''

options = dict(
	message=str,
)

def analysis(sliceno):
	return sliceno

def synthesis(analysis_res):
	print("Sum of all sliceno:", sum(analysis_res))
	print("Message:", options.message)
"""


build_script = r"""def main(urd):
	urd.build('example', message='Hello world!')
"""


config_template = r"""# The configuration is a collection of key value pairs.
#
# Values are specified as
# key: value
# or for several values
# key:
# 	value 1
# 	value 2
# 	...
# (any leading whitespace is ok)
#
# Use ${{VAR}} or ${{VAR=DEFAULT}} to use environment variables.

slices: {slices}
workdirs:
	{name} ./workdirs/{name}

# Target workdir defaults to the first workdir, but you can override it.
# (this is where jobs without a workdir override are built)
target workdir: {name}

method packages:
	{name}
	accelerator.standard_methods
	accelerator.test_methods

urd: local # can also be URL/socket to your urd

# [host]:port or path where board will listen.
# You can also start board separately with "ax board".
board listen: .socket.dir/board

result directory: ./results
input directory: {input}

# If you want to run methods on different python interpreters you can
# specify names for other interpreters here, and put that name after
# the method in methods.conf.
# You automatically get four names for the interpreter that started
# the server: DEFAULT, {major}, {major}.{minor} and {major}.{minor}.{micro} (adjusted to the actual
# version used). You can override these here, except DEFAULT.
# interpreters:
# 	2.7 /path/to/python2.7
# 	test /path/to/beta/python
"""


def main(argv):
	from os import makedirs, listdir, chdir
	from os.path import exists, join, realpath
	from sys import version_info
	from argparse import ArgumentParser
	from accelerator.error import UserError

	parser = ArgumentParser(
		prog=argv.pop(0),
		description=r'''
			Creates an accelerator project directory.
			Defaults to the current directory.
			Creates accelerator.conf, a method dir, a workdir and result dir.
			Both the method directory and workdir will be named <NAME>,
			"dev" by default.
		''',
	)
	parser.add_argument('--slices', default=None, type=int, help='override slice count detection')
	parser.add_argument('--name', default='dev', help='name of method dir and workdir, default "dev"')
	parser.add_argument('--input', default='# /some/path where you want import methods to look.', help='input directory')
	parser.add_argument('--force', action='store_true', help='go ahead even though directory is not empty, or workdir exists with incompatible slice count')
	parser.add_argument('directory', default='.', help='project directory to create. default "."', metavar='DIR', nargs='?')
	options = parser.parse_args(argv)

	assert options.name
	assert '/' not in options.name
	assert ' ' not in options.name
	if not options.input.startswith('#'):
		options.input = realpath(options.input)
	prefix = realpath(options.directory)
	workdir = join(prefix, 'workdirs', options.name)
	slices_conf = join(workdir, '.slices')
	try:
		with open(slices_conf, 'r') as fh:
			workdir_slices = int(fh.read())
	except IOError:
		workdir_slices = None
	if workdir_slices and options.slices is None:
		options.slices = workdir_slices
	if options.slices is None:
		from multiprocessing import cpu_count
		options.slices = cpu_count()
	if workdir_slices and workdir_slices != options.slices and not options.force:
		raise UserError('Workdir %r has %d slices, refusing to continue with %d slices' % (workdir, workdir_slices, options.slices,))

	if not options.force and exists(options.directory) and listdir(options.directory):
		raise UserError('Directory %r is not empty.' % (options.directory,))
	if not exists(options.directory):
		makedirs(options.directory)
	chdir(options.directory)
	for dir_to_make in ('.socket.dir', 'urd.db',):
		if not exists(dir_to_make):
			makedirs(dir_to_make, 0o750)
	for dir_to_make in (workdir, 'results',):
		if not exists(dir_to_make):
			makedirs(dir_to_make)
	with open(slices_conf, 'w') as fh:
		fh.write('%d\n' % (options.slices,))
	method_dir = options.name
	if not exists(method_dir):
		makedirs(method_dir)
	with open(join(method_dir, '__init__.py'), 'w') as fh:
		pass
	with open(join(method_dir, 'methods.conf'), 'w') as fh:
		fh.write('example\n')
	with open(join(method_dir, 'a_example.py'), 'w') as fh:
		fh.write(a_example)
	with open(join(method_dir, 'build.py'), 'w') as fh:
		fh.write(build_script)
	with open('accelerator.conf', 'w') as fh:
		fh.write(config_template.format(
			name=options.name,
			slices=options.slices,
			input=options.input,
			major=version_info.major,
			minor=version_info.minor,
			micro=version_info.micro,
		))
