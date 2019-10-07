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
Verify that the output from methods is captured correctly for all valid
combinations of prepare/analysis/synthesis, both in OUTPUT dir and in
status stacks.
'''

from random import randint
import sys
import os

import subjobs
from extras import resolve_jobid_filename
from automata_common import Automata
import g

def test(params, p=False, a=False, s=False):
	prefix = "A bit of text."
	opts = {'prefix': prefix}
	name = 'test_output_'
	cookie = randint(10000, 99999)
	if p:
		name += 'p'
		opts['p'] = "Some words\nfrom prepare\nwith %d in them." % (cookie,)
	if a:
		name += 'a'
		opts['a'] = "A few words\nfrom analysis(%%d)\nwith the cookie %d in them." % (cookie,)
	if s:
		name += 's'
		opts['s'] = "Words\nfrom synthesis\ncookie is %d." % (cookie,)
	jid = subjobs.build(name, options=opts)
	d = resolve_jobid_filename(jid, 'OUTPUT/')
	chked = set()
	def chk(part):
		if isinstance(part, int):
			data = opts['a'] % (part,)
			part = str(part)
		else:
			data = opts[part[0]]
		chked.add(part)
		with open(d +  part, 'r') as fh:
			got = fh.read()
		want = prefix + '\n' + data + '\n'
		assert got == prefix + '\n' + data + '\n', "%s produced %r in %s, expected %r" % (jid, got, part, want,)
	if p:
		chk('prepare')
	if s:
		chk('synthesis')
	if a:
		for sliceno in range(params.slices):
			chk(sliceno)
	unchked = set(os.listdir(d)) - chked
	assert not unchked, "Unexpected OUTPUT files from %s: %r" % (jid, unchked,)

def synthesis(params):
	test(params, s=True)
	test(params, p=True, s=True)
	test(params, p=True, a=True, s=True)
	test(params, p=True, a=True)
	test(params, a=True, s=True)
	test(params, a=True)


# This is run in all parts in the subjobs.
# The code is here so it's not repeated.

def sub_part(sliceno, opts):
	a = Automata(g.daemon_url, verbose=True)
	pid = os.getpid()
	def verify(want):
		timeout = 0
		got = None
		for _ in range(25):
			status_stacks = a._server_idle(timeout)[1]
			for line in status_stacks:
				if line[0] == pid and line[1] < 0:
					# this is our tail
					got = line[2]
					if got == want:
						return
			# it might not have reached the daemon yet
			timeout += 0.01
		# we've given it 3 seconds, it's not going to happen.
		raise Exception("Wanted to see tail output of %r, but saw %r" % (want, got,))
	print(opts.prefix, file=sys.stderr)
	verify(opts.prefix + '\n')
	if isinstance(sliceno, int):
		msg = opts.a % (sliceno,)
	else:
		msg = opts[sliceno]
	print(msg)
	verify(opts.prefix + '\n' + msg + '\n')
