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
Test writing datasets with strange column names, column names whose cleaned
names collide and column names used in the generated split_write function.
'''

from accelerator.dataset import DatasetWriter

internal_names_analysis = ['a', 'b', 'c', 'w1', 'write']

def mk_dw(name, cols, **kw):
	dw = DatasetWriter(name=name, **kw)
	for colname in cols:
		dw.add(colname, "unicode")
	return dw

def prepare():
	return mk_dw("internal_names_analysis", internal_names_analysis)

def analysis(sliceno, prepare_res):
	prepare_res.write(*['a %d' % (sliceno,)] * 5)
	prepare_res.write_list(['b %d' % (sliceno,)] * 5)
	prepare_res.write_dict(dict(zip(internal_names_analysis, ['c %d' % (sliceno,)] * 5)))

def synthesis(prepare_res, slices):
	ds = prepare_res.finish()
	for sliceno in range(slices):
		assert list(ds.iterate(sliceno, internal_names_analysis)) == [('a %d' % (sliceno,),) * 5, ('b %d' % (sliceno,),) *5, ('c %d' % (sliceno,),) *5]

	in_parent = [ # list because order matters
		"-",      # becomes _ because everything must be a valid python identifier.
		"a b",    # becomes a_b because everything must be a valid python identifier.
		"42",     # becomes _42 because everything must be a valid python identifier.
		"print",  # becomes print_ because print is a keyword (in py2).
		"print@", # becomes print__ because print_ is taken.
		"None",   # becomes None_ because None is a keyword (in py3).
	]
	dw = mk_dw("parent", in_parent)
	w = dw.get_split_write()
	w(_="- 1", a_b="a b 1", _42="42 1", print_="print 1", None_="None 1", print__="Will be overwritten 1")
	w(_="- 2", a_b="a b 2", _42="42 2", print_="print 2", None_="None 2", print__="Will be overwritten 2")
	parent = dw.finish()
	in_child = [ # order still matters
		"print_*", # becomes print___ because print__ is taken.
		"print_",  # becomes print____ because all shorter are taken.
		"normal",  # no collision.
		"Normal",  # becomes Normal_ because these comparisons are case insensitive.
		"print@",  # re-uses print__ from the parent dataset.
	]
	dw = mk_dw("child", in_child, parent=parent)
	w = dw.get_split_write()
	w(print__="print@ 1", print___="print_* 1", print____="print_ 1", normal="normal 1", Normal_="Normal 1")
	w(print__="print@ 2", print___="print_* 2", print____="print_ 2", normal="normal 2", Normal_="Normal 2")
	child = dw.finish()
	for colname in in_parent + in_child:
		data = set(child.iterate(None, colname))
		assert data == {colname + " 1", colname + " 2"}, "Bad data for %s: %r" % (colname, data)

	def chk_internal(name, **kw):
		internal = ("writers", "w_l", "cyc", "hsh", "next",)
		dw = mk_dw(name, internal, **kw)
		dw.get_split_write()(*internal)
		dw.get_split_write_list()(internal)
		dw.get_split_write_dict()(dict(zip(internal, internal)))
		got = list(dw.finish().iterate(None, internal))
		assert got == [internal] * 3
	chk_internal(name="internal_names")
	chk_internal(name="internal_names_hashed", hashlabel="hsh")
