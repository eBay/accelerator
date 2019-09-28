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

import os
from select import select
from multiprocessing import Process
from time import sleep

from workarounds import nonblocking
from compat import setproctitle

def setup(slices, include_prepare, include_analysis):
	os.mkdir('OUTPUT')
	names = []
	masters = []
	slaves = []
	def mk(name):
		a, b = os.pipe()
		masters.append(a)
		slaves.append(b)
		names.append(name)
	# order matters here, non-analysis outputs are pop()ed from the end.
	if include_analysis:
		for sliceno in range(slices):
			mk(str(sliceno))
	mk('synthesis')
	if include_prepare:
		mk('prepare')
	return names, masters, slaves

def run_reader(names, masters, slaves):
	p = Process(target=reader, args=(names, masters, slaves,), name='iowrapper_reader')
	p.start()

def reader(names, masters, slaves):
	setproctitle('iowrapper_reader')
	os.chdir('OUTPUT')
	for fd in slaves:
		os.close(fd)
	fd2name = dict(zip(masters, names))
	fd2fd = {}
	missed = [False]
	def try_print(data=b'\n*** Some output not printed ***\n'):
		try:
			os.write(1, data)
		except OSError:
			missed[0] = True
	# set output nonblocking, so we can't be blocked by terminal io
	# (errors generated here can of course still block us, but since
	# they shouldn't happen we do want to see them.)
	with nonblocking(1):
		while masters:
			if missed[0]:
				# Some output failed to print last time around.
				# Wait up to one second for new data and then try
				# to write a message about that (before the new data).
				ready, _, _ = select(masters, [], [], 1.0)
				missed[0] = False
				try_print()
			else:
				ready, _, _ = select(masters, [], [])
			for fd in ready:
				data = os.read(fd, 65536)
				if data:
					if fd not in fd2fd:
						fd2fd[fd] = os.open(fd2name[fd], os.O_CREAT | os.O_WRONLY, 0o666)
					os.write(fd2fd[fd], data)
					try_print(data)
				else:
					if fd in fd2fd:
						os.close(fd2fd[fd])
						del fd2fd[fd]
					masters.remove(fd)
					os.close(fd)
		if missed[0]:
			missed[0] = False
			try_print()
			if missed[0]:
				# Give it a little time, then give up.
				sleep(0.03)
				try_print()
