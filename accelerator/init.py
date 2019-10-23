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


a_example = r"""description = r'''
This is just an example. It doesn't even try to do anything useful.

You can run it to see that your installation works.
'''

def analysis(sliceno):
	return sliceno

def synthesis(analysis_res):
	print("Sum of all sliceno:", sum(analysis_res))
"""


config_template = r"""# var=value, value can have ${{VAR=DEFAULT}} to import env vars.

# workdir=NAME:PATH:SLICES
# You can have as many workdir lines as you want
workdir={name}:{workdir}:{slices}

# You can only have one target workdir.
# All built jobs end up there.
target_workdir={name}

# List all other workdirs you want to import here (comma separated)
source_workdirs={name}

# Methods are imported from these directories (comma separated)
method_directories={name},accelerator.standard_methods

# automata scripts save things here
result_directory={prefix}/results

# import methods look under here
source_directory={source}

logfilename={prefix}/daemon.log
"""


def main(argv):
	from os import makedirs, listdir, environ
	from os.path import exists, join, realpath
	from accelerator.shell import UserError
	from argparse import ArgumentParser

	parser = ArgumentParser(
		prog='init',
		description=r'''
			Creates an accelerator project directory.
			Defaults to the current directory.
			Creates accelerator.conf and a method directory.
			Also creates workdirs and result dir (in ~/accelerator by default).
			Both the method directory and workdir will be named <NAME>,
			"dev" by default.
		''',
	)
	parser.add_argument('--slices', default=None, type=int, help='Override slice count detection')
	parser.add_argument('--name', default='dev', help='Name of method dir and workdir, default "dev"')
	parser.add_argument('--directory', default='.', help='project directory to create. default "."', metavar='DIR')
	parser.add_argument('--prefix', default=join(environ['HOME'], 'accelerator'), help='Put workdirs and daemon.log here')
	parser.add_argument('--source', default='/some/other/path', help='source directory')
	parser.add_argument('--force', action='store_true', help='Go ahead even though directory is not empty, or workdir exists with incompatible slice count')
	options = parser.parse_args(argv)

	assert options.name
	options.prefix = realpath(options.prefix)
	options.source = realpath(options.source)
	workdir = join(options.prefix, 'workdirs', options.name)
	if not exists(workdir):
		makedirs(workdir)
	slices_conf = join(workdir, options.name + '-slices.conf')
	try:
		with open(slices_conf, 'r') as fh:
			workdir_slices = int(fh.read())
	except OSError:
		workdir_slices = None
	if workdir_slices and options.slices is None:
		options.slices = workdir_slices
	if options.slices is None:
		from multiprocessing import cpu_count
		options.slices = cpu_count()
	if workdir_slices and workdir_slices != options.slices and not options.force:
		raise UserError('Workdir %r has %d slices, refusing to continue with %d slices' % (workdir, workdir_slices, options.slices,))

	if not exists(options.directory):
		makedirs(options.directory)
	if not options.force and listdir(options.directory):
		raise UserError('Directory %r is not empty.' % (options.directory,))
	with open(slices_conf, 'w') as fh:
		fh.write('%d\n' % (options.slices,))
	method_dir = join(options.directory, options.name)
	if not exists(method_dir):
		makedirs(method_dir)
	with open(join(method_dir, 'methods.conf'), 'w') as fh:
		fh.write('example\n')
	with open(join(method_dir, 'a_example.py'), 'w') as fh:
		fh.write(a_example)
	with open(join(options.directory, 'accelerator.conf'), 'w') as fh:
		fh.write(config_template.format(name=options.name, prefix=options.prefix, workdir=workdir, slices=options.slices, source=options.source))
