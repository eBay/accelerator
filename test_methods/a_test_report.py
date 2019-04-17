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

from report import Report

description = r'''
Verify the Report class.
'''

options = dict(
	s="foo",
	i=42,
	l=[1, 2, 3],
)

def verify(jobid, wanted):
	with open("report.txt") as fh:
		def ln(want="-" * 80):
			got = fh.readline()
			assert got == want + "\n", "want %r got %r" % (want, got,)
		ln()
		line = fh.readline()
		assert line.endswith("[" + jobid + "]\n"), line
		ln()
		ln('Method "test_report" report.')
		ln('Caption "fsm_test_report"')
		ln()
		ln("Options")
		ln("  i : 42 ")
		ln("  l :")
		ln("      1")
		ln("      2")
		ln("      3")
		ln("  s : foo ")
		ln()
		data = fh.read()
		assert data.endswith("\n" + ("-" * 80) + "\n")
		data = data[:-82]
		assert data == wanted, "wanted %r got %r" % (wanted, data,)

def synthesis(params):
	r = Report()
	r.println("old interface with .close")
	r.close()
	verify(params.jobid, "old interface with .close")
	with Report() as r:
		r.write("new interface")
		r.println(" with with")
	verify(params.jobid, "new interface with with")
	r = Report()
	with r:
		r.println("Text printvec")
		r.line()
		r.printvec("abcdefghij", 5)
		r.line()
		r.printvec(["foo", "bar", "baz"], 6)
	verify(params.jobid, """Text printvec
--------------------------------------------------------------------------------
    0 a             1 b             2 c             3 d             4 e         
    5 f             6 g             7 h             8 i             9 j         
--------------------------------------------------------------------------------
    0 foo        1 bar        2 baz    """)
