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

from distutils.core import setup, Extension

gzutilmodule = Extension("gzutil", sources = ["siphash24.c", "gzutilmodule.c"], libraries=["z"], extra_compile_args=['-std=c99', '-O3'])

setup(name="gzutil", version="2.9.3", description="Read/write values from/to gz files", ext_modules=[gzutilmodule])
