############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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
from subprocess import Popen
from time import sleep
import signal
from pty import openpty
import errno

from accelerator.workarounds import nonblocking
from accelerator.compat import setproctitle
from accelerator import statmsg


def main():
	# Run cat between ourselves and the real terminal, so we can set our
	# output (this pipe) non-blocking without messing up the real stdout.
	a, b = os.pipe()
	os.environ['BD_TERM_FD'] = str(b)
	Popen(['cat'], close_fds=True, stdin=a)
	# Use a pty for programs under us, so they stay line buffered.
	a, b = openpty()
	run_reader({}, None, [a], [b], 'main iowrapper.reader', None, True)
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
		a, b = openpty()
		masters.append(a)
		slaves.append(b)
		names.append(name)
	# order matters here, non-analysis outputs are pop()ed from the end.
	if include_analysis:
		for sliceno in range(slices):
			mk(str(sliceno))
	else:
		slices = 0 # for fd2pid later
	mk('synthesis')
	if include_prepare:
		mk('prepare')
	fd2pid = dict.fromkeys(masters[slices:], os.getpid())
	return fd2pid, names, masters, slaves


def run_reader(fd2pid, names, masters, slaves, process_name='iowrapper.reader', basedir='OUTPUT', is_main=False):
	args = (fd2pid, names, masters, slaves, process_name, basedir, is_main,)
	p = Process(target=reader, args=args, name=process_name)
	p.start()
	if not is_main:
		os.close(int(os.environ['BD_TERM_FD']))
		del os.environ['BD_TERM_FD']


MAX_OUTPUT = 640

def reader(fd2pid, names, masters, slaves, process_name, basedir, is_main):
	signal.signal(signal.SIGTERM, signal.SIG_IGN)
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	setproctitle(process_name)
	out_fd = int(os.environ['BD_TERM_FD'])
	for fd in slaves:
		os.close(fd)
	fd2fd = {}
	if not is_main:
		os.chdir(basedir)
		fd2name = dict(zip(masters, names))
		outputs = dict.fromkeys(masters, b'')
		if len(fd2pid) == 2:
			status_blacklist = set(fd2pid.values())
			assert len(status_blacklist) == 1, "fd2pid should only map to 1 value initially: %r" % (fd2pid,)
		else:
			status_blacklist = ()
			assert len(fd2pid) == 1, "fd2pid should have 1 or 2 elements initially"
	missed = [False]
	output_happened = False
	def try_print(data=b'\n\x1b[31m*** Some output not printed ***\x1b[m\n'):
		try:
			os.write(out_fd, data)
		except OSError:
			missed[0] = True
	# set output nonblocking, so we can't be blocked by terminal io.
	# errors generated here go to stderr, which is the real stderr
	# in the main iowrapper (so it can block) and goes to the main
	# iowrapper in the method iowrappers (so it can still block, but
	# is unlikely to do so for long).
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
				try:
					data = os.read(fd, 65536)
				except OSError as e:
					# On Linux a pty will return
					# OSError: [Errno 5] Input/output error
					# instead of b'' for EOF. Don't know why.
					# Let's try to be a little restrictive in what we catch.
					if e.errno != errno.EIO:
						raise
					data = b''
				if data:
					if not is_main:
						if fd not in fd2pid:
							fd2pid[fd] = int(data[:16], 16)
							data = data[16:]
							if not data:
								continue
						if fd not in fd2fd:
							fd2fd[fd] = os.open(fd2name[fd], os.O_CREAT | os.O_WRONLY, 0o666)
						os.write(fd2fd[fd], data)
					try_print(data)
					output_happened = True
					if not is_main:
						outputs[fd] = (outputs[fd] + data[-MAX_OUTPUT:])[-MAX_OUTPUT:]
						statmsg._output(fd2pid[fd], outputs[fd].decode('utf-8', 'replace'))
				else:
					if fd in fd2fd:
						os.close(fd2fd[fd])
						del fd2fd[fd]
					masters.remove(fd)
					os.close(fd)
					if not is_main:
						try:
							pid = fd2pid.pop(fd)
							if pid in status_blacklist:
								# don't do it for prepare as synthesis has the same PID.
								status_blacklist.remove(pid)
								# but clear the output if needed.
								if outputs[fd]:
									statmsg._clear_output(pid)
							else:
								statmsg._end(pid=pid)
						except Exception:
							# Failure can happen here if the method exits
							# before analysis (fd2pid not fully populated).
							pass
		if missed[0]:
			missed[0] = False
			try_print()
			if missed[0]:
				# Give it a little time, then give up.
				sleep(0.03)
				try_print()
	if not output_happened and not is_main:
		os.chdir('..')
		os.rmdir(basedir)
