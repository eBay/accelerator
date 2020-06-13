#!/usr/bin/env python

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

from setuptools import setup, find_packages, Extension
from importlib import import_module
from os.path import exists
import os
from datetime import datetime
from subprocess import check_output, check_call, CalledProcessError
from io import open
import re

def dirty():
	for extra in ([], ['--cached'],):
		cmd = ['git', 'diff-index', '--quiet', 'HEAD'] + extra
		try:
			check_call(cmd)
		except CalledProcessError as e:
			if e.returncode == 1:
				return '.dirty'
			else:
				raise
	return ''

with open('README.md', 'r', encoding='utf-8') as fh:
	long_description = []
	for line in fh:
		if not line.startswith('[PyPI]') and not line.startswith('`pip'):
			long_description.append(line)
	long_description = ''.join(long_description)

if exists('PKG-INFO'):
	with open('PKG-INFO', 'r', encoding='utf-8') as fh:
		for line in fh:
			if line.startswith('Version: '):
				version = line.strip().split()[1]
				break
else:
	version = datetime.utcnow().strftime('%Y.%m.%d')
	env_version = os.environ.get('ACCELERATOR_BUILD_VERSION')
	if os.environ.get('ACCELERATOR_BUILD') == 'IS_RELEASE':
		if dirty():
			raise Exception("Refusing to build release from dirty repo")
		if env_version:
			assert re.match(r'20\d\d\.\d\d\.\d\d$', env_version)
			version = env_version
	else:
		if env_version:
			assert re.match(r'20\d\d\.\d\d\.\d\d\.dev\d+$', env_version)
			version = env_version
		else:
			commit = check_output(['git', 'rev-parse', 'HEAD']).strip()[:10].decode('ascii')
			version = "%s.dev1+%s%s" % (version, commit, dirty(),)

gzutilmodule = Extension(
	"accelerator.gzutil",
	sources=["gzutil/siphash24.c", "gzutil/gzutilmodule.c"],
	libraries=["z"],
	extra_compile_args=['-std=c99', '-O3'],
)

def method_mod(name):
	code = import_module('accelerator.standard_methods.' + name).c_module_code
	fn = 'accelerator/standard_methods/_generated_' + name + '.c'
	if exists(fn):
		with open(fn, 'r') as fh:
			old_code = fh.read()
	else:
		old_code = None
	if code != old_code:
		with open(fn, 'w') as fh:
			fh.write(code)
	return Extension(
		'accelerator.standard_methods._' + name,
		sources=[fn],
		libraries=['z'],
		extra_compile_args=['-std=c99', '-O3'],
	)

dataset_typemodule = method_mod('dataset_type')
csvimportmodule = method_mod('csvimport')

setup(
	name="accelerator",
	version=version,
	packages=find_packages(),

	entry_points={
		'console_scripts': [
			'ax = accelerator.shell:main',
		],
	},

	install_requires=[
		'setproctitle>=1.1.8', # not actually required
		'bottle>=0.12.7, <0.13',
	],
	python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4",

	ext_modules=[gzutilmodule, dataset_typemodule, csvimportmodule],

	package_data={
		'': ['*.txt', 'methods.conf', 'board/*.tpl'],
	},

	author="Carl Drougge",
	author_email="bearded@longhaired.org",
	url="https://exax.org/",
	description="A tool for fast and reproducible processing of large amounts of data.",
	long_description=long_description,
	long_description_content_type="text/markdown",
	project_urls={
		"Source": "https://github.com/eBay/accelerator",
		"Reference manual": "https://berkeman.github.io/pdf/acc_manual.pdf",
	},

	classifiers=[
		"Development Status :: 4 - Beta",
		"Environment :: Console",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"License :: OSI Approved :: Apache Software License",
		"Operating System :: POSIX",
		"Operating System :: POSIX :: BSD :: FreeBSD",
		"Operating System :: POSIX :: Linux",
		"Programming Language :: Python :: 2",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: Implementation :: CPython",
		"Programming Language :: C",
	],
)
