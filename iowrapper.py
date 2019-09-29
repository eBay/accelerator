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
import signal

from workarounds import nonblocking
from compat import setproctitle


def main(logfile):
	os.environ['BD_TERM_FD'] = str(os.dup(1))
	a, b = os.pipe()
	logdir, logfile = os.path.split(logfile)
	run_reader(logfile, [a], [b], 'main iowrapper.reader', logdir, True)
	os.dup2(b, 1)
	os.dup2(b, 2)
	os.close(a)
	os.close(b)


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


def run_reader(names, masters, slaves, process_name='iowrapper.reader', basedir='OUTPUT', is_main=False):
	args = (names, masters, slaves, process_name, basedir, is_main,)
	p = Process(target=reader, args=args, name=process_name)
	p.start()
	if not is_main:
		os.close(int(os.environ['BD_TERM_FD']))
		del os.environ['BD_TERM_FD']


def reader(names, masters, slaves, process_name, basedir, is_main):
	signal.signal(signal.SIGTERM, signal.SIG_IGN)
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	setproctitle(process_name)
	out_fd = int(os.environ['BD_TERM_FD'])
	os.chdir(basedir)
	for fd in slaves:
		os.close(fd)
	if is_main:
		fd = os.open(names, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o666)
		fd2fd = dict.fromkeys(masters, fd)
	else:
		fd2name = dict(zip(masters, names))
		fd2fd = {}
	missed = [False]
	def try_print(data=b'\n\x1b[31m*** Some output not printed ***\x1b[m\n'):
		try:
			os.write(out_fd, data)
		except OSError:
			missed[0] = True
	# set output nonblocking, so we can't be blocked by terminal io.
	# errors generated here go to stderr, which is the real stderr
	# in the main iowrapper (so it can block) and goes to the main
	# iowrapper in the method iowrappers (so it can still block, but
	# is unlikely to do so for long, and will end up in the log).
	with nonblocking(out_fd):
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
