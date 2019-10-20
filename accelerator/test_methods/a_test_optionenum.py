# -*- coding: utf-8 -*-

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
Test OptionEnum construction and enforcement.
'''

from accelerator.extras import OptionEnum
from accelerator.compat import PY2
from accelerator import subjobs
from accelerator import blob

abcd = OptionEnum("a b c d", none_ok=True)
efgh = OptionEnum(chr(n) for n in range(101, 105))
ijkl = OptionEnum(list("ijkl"), none_ok=True)
mnSTARop = OptionEnum("m n* o p")
qSTARrSTARst = OptionEnum("q* r* s t")
uni = OptionEnum("b l ä")

options = dict(
	abcd=abcd,
	efgh=efgh.g,
	ijkl=ijkl["k"],
	mnSTARop=mnSTARop.m,
	qSTARrSTARst=qSTARrSTARst["quu"],
	uni=uni.b,
	dict=dict(ijkl=ijkl),
	list=[mnSTARop.o],
	inner=False,
)

defaults = dict(options)
defaults["abcd"] = None
defaults["dict"] = {}

def check(**options):
	pass_options = dict(options)
	pass_options["inner"] = True
	want_res = dict(defaults)
	want_res.update(pass_options)
	jid = subjobs.build("test_optionenum", options=pass_options)
	res = blob.load(jobid=jid)
	assert res == want_res, "%r != %r from %r" % (res, want_res, options,)

def check_unbuildable(**options):
	try:
		check(**options)
	except Exception as e:
		if e.args[0].startswith("Submit failed"):
			return
		raise
	raise Exception("Building with options = %r should have failed but didn't" % (options,))

def synthesis():
	if options.inner:
		return dict(options)
	check(abcd="a", efgh="h")
	check(ijkl="j", efgh="e")
	if PY2:
		# Passing "ä" is fine on PY2 too, but we get UTF-8 byte strings back.
		check(uni=b"\xc3\xa4")
	else:
		check(uni="ä")
	check(ijkl=None, dict=dict(foo="j"))
	check(mnSTARop="p", qSTARrSTARst="qwe", ijkl="k")
	check(mnSTARop="nah", qSTARrSTARst="really good value\n")
	check(list=["noo"])
	check_unbuildable(abcd="A")
	check_unbuildable(efgh="a")
	check_unbuildable(qSTARrSTARst="sdf")
	check_unbuildable(mnSTARop=None)
	check_unbuildable(ijkl=None, dict=dict(foo="b"))
	check_unbuildable(list=["moo"])
