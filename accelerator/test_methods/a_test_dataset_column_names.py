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

description = r'''
Test writing datasets with strange column names, and column names whose cleaned names collide.
'''

from accelerator.dataset import DatasetWriter

def synthesis(params):
	dw = DatasetWriter(name="parent")
	in_parent = [ # list because order matters
		"-",      # becomes _ because everything must be a valid python identifier.
		"a b",    # becomes a_b because everything must be a valid python identifier.
		"42",     # becomes _42 because everything must be a valid python identifier.
		"print",  # becomes print_ because print is a keyword (in py2).
		"print@", # becomes print__ because print_ is taken.
		"None",   # becomes None_ because None is a keyword (in py3).
	]
	for colname in in_parent:
		dw.add(colname, "unicode")
	w = dw.get_split_write()
	w(_="- 1", a_b="a b 1", _42="42 1", print_="print 1", None_="None 1", print__="Will be overwritten 1")
	w(_="- 2", a_b="a b 2", _42="42 2", print_="print 2", None_="None 2", print__="Will be overwritten 2")
	parent = dw.finish()
	dw = DatasetWriter(name="child", parent=parent)
	in_child = [ # order still matters
		"print_*", # becomes print___ because print__ is taken.
		"print_",  # becomes print____ because all shorter are taken.
		"normal",  # no collision.
		"Normal",  # becomes Normal_ because these comparisons are case insensitive.
		"print@",  # re-uses print__ from the parent dataset.
	]
	for colname in in_child:
		dw.add(colname, "unicode")
	w = dw.get_split_write()
	w(print__="print@ 1", print___="print_* 1", print____="print_ 1", normal="normal 1", Normal_="Normal 1")
	w(print__="print@ 2", print___="print_* 2", print____="print_ 2", normal="normal 2", Normal_="Normal 2")
	child = dw.finish()
	for colname in in_parent + in_child:
		data = set(child.iterate(None, colname))
		assert data == {colname + " 1", colname + " 2"}, "Bad data for %s: %r" % (colname, data)
