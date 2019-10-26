#!/usr/bin/env python

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

from setuptools import setup, find_packages, Extension
from importlib import import_module
from os.path import exists

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
	version="0.99",
	packages=find_packages(),

	entry_points={
		'console_scripts': [
			'bd = accelerator.shell:main',
		],
	},

	install_requires=[
		'ujson>=1.35',
		'setproctitle>=1.1.8', # not actually required
		'bottle>=0.12.7',
	],

	ext_modules=[gzutilmodule, dataset_typemodule, csvimportmodule],

	package_data={
		'': ['*.txt', 'methods.conf'],
	},
)
