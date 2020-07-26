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

from accelerator import subjobs
from accelerator.build import Automata
from accelerator import g

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
	d = jid.filename('OUTPUT/')
	chked = set()
	all = []
	def chk(part):
		output = jid.output(part)
		if isinstance(part, int):
			data = opts['a'] % (part,)
			part = str(part)
		else:
			data = opts[part[0]]
		chked.add(part)
		with open(d +  part, 'r') as fh:
			got = fh.read().replace('\r\n', '\n')
		want = prefix + '\n' + data + '\n'
		assert got == prefix + '\n' + data + '\n', "%s produced %r in %s, expected %r" % (jid, got, part, want,)
		assert output == got, 'job.output disagrees with manual file reading for %s in %s. %r != %r' % (part, jid, output, got,)
		all.append(got)
	if p:
		chk('prepare')
	if a:
		for sliceno in range(params.slices):
			chk(sliceno)
	if s:
		chk('synthesis')
	unchked = set(os.listdir(d)) - chked
	assert not unchked, "Unexpected OUTPUT files from %s: %r" % (jid, unchked,)
	output = jid.output()
	got = ''.join(all)
	assert output == got, 'job.output disagrees with manual file reading for <all> in %s. %r != %r' % (jid, output, got,)

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
	a = Automata(g.server_url, verbose=True)
	pid = os.getpid()
	def verify(want):
		timeout = 0
		got = None
		for _ in range(25):
			status_stacks = a._server_idle(timeout)[1]
			for line in status_stacks:
				if line[0] == pid and line[1] < 0:
					# this is our tail
					got = line[2].replace('\r\n', '\n')
					if got == want:
						return
			# it might not have reached the server yet
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
