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

gzutilmodule = Extension(
	"accelerator.gzutil",
	sources=["gzutil/siphash24.c", "gzutil/gzutilmodule.c"],
	libraries=["z"],
	extra_compile_args=['-std=c99', '-O3'],
)

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
		'cffi>=1.7.0',
	],

	ext_modules=[gzutilmodule],

	package_data={
		'': ['*.txt', 'methods.conf'],
	},
)
