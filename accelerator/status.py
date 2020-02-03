############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2019 Carl Drougge                       #
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

# In methods you only use the status function from this module.
# It works like this:
#
# with status("some text") as update:
#   long running code here
#
# then you can see how long your code has been doing that part in ^T.
# If you want to change the status text (without resetting the time,
# and possibly with other statuses nested below it) you can call the
# update function with the new text. If you don't need to update the
# text you can of course use just "with status(...):".
#
# Several built in functions will call this for you, notably dataset
# iterators and pickle load/save functions.

from __future__ import print_function
from __future__ import division

from contextlib import contextmanager
from time import time, sleep
from traceback import print_exc
from threading import Lock
from weakref import WeakValueDictionary
import os

from accelerator.compat import str_types, iteritems

from accelerator import g
from accelerator.status_messaging import _send


status_tree = {}
status_all = WeakValueDictionary()
status_stacks_lock = Lock()


# all currently (or recently) running launch.py PIDs
class Children(set):
	def add(self, pid):
		with status_stacks_lock:
			set.add(self, pid)
	def remove(self, pid):
		with status_stacks_lock:
			d = status_all.get(pid)
			if d and d.parent_pid in status_all:
				p = status_all[d.parent_pid]
				if pid in p.children:
					del p.children[pid]
			if pid in status_tree:
				del status_tree[pid]
			set.remove(self, pid)
children = Children()


_cookie = 0

@contextmanager
def status(msg):
	if g.running == 'server':
		yield lambda _: None
		return
	global _cookie
	_cookie += 1
	cookie = str(_cookie)
	t = str(time())
	typ = 'push'
	# capture the PID here, because update might be called in a different process
	pid = os.getpid()
	def update(msg):
		assert msg and isinstance(msg, str_types) and '\0' not in msg
		_send(typ, '\0'.join((msg, t, cookie)), pid=pid)
	update(msg)
	typ = 'update'
	try:
		yield update
	finally:
		_send('pop', cookie)

# Same interface as status, but without the actual reporting
@contextmanager
def dummy_status(msg):
	assert msg and isinstance(msg, str_types) and '\0' not in msg
	yield lambda _: None

def _start(msg, parent_pid, is_analysis=''):
	_send('start', '%d\0%s\0%s\0%f' % (parent_pid, is_analysis, msg, time(),))

def _end(pid=None):
	_send('end', '', pid=pid)

def _output(pid, msg):
	_send('output', '%f\0%s' % (time(), msg,), pid=pid)


def status_stacks_export():
	res = []
	last = [None]
	current = None
	def fmt(tree, start_indent=0):
		for pid, d in sorted(iteritems(tree), key=lambda i: (i[1].stack or ((0,),))[0][0]):
			last[0] = d
			indent = start_indent
			for msg, t, _ in d.stack:
				res.append((pid, indent, msg, t))
				indent += 1
			fmt(d.children, indent)
			if d.output:
				res.append((pid, -1,) + d.output)
	try:
		with status_stacks_lock:
			fmt(status_tree)
		if last[0]:
			current = last[0].summary
			if len(last[0].stack) > 1 and not current[1].endswith('analysis'):
				msg, t, _ = last[0].stack[1]
				current = (current[0], '%s %s' % (current[1], msg,), t,)
	except Exception:
		print_exc()
		res.append((0, 0, 'ERROR', time()))
	return res, current

def print_status_stacks(stacks=None):
	if stacks == None:
		stacks, _ = status_stacks_export()
	report_t = time()
	for pid, indent, msg, t in stacks:
		if indent < 0:
			print("%6d TAIL OF OUTPUT: (%.1f seconds ago)" % (pid, report_t - t,))
			msgs = list(filter(None, msg.split('\n')))[-3:]
			for msg in msgs:
				print("       \x1b[32m" + msg + "\x1b[m")
		else:
			print("%6d STATUS: %s%s (%.1f seconds)" % (pid, "    " * indent, msg, report_t - t))


def _find(pid, cookie):
	stack = status_all[pid].stack
	# should normally be the last one, or at least close to it.
	for ix in range(len(stack) -1, -1, -1):
		if stack[ix][2] == cookie:
			return stack, ix
	return stack, None

def statmsg_sink(sock):
	from accelerator.extras import DotDict
	while True:
		data = None
		try:
			data = sock.recv(1500)
			typ, pid, msg = data.decode('utf-8').split('\0', 2)
			pid = int(pid)
			with status_stacks_lock:
				if typ == 'push':
					msg, t, cookie = msg.split('\0', 3)
					t = float(t)
					status_all[pid].stack.append((msg, t, cookie))
				elif typ == 'pop':
					stack, ix = _find(pid, msg)
					if ix == len(stack) - 1:
						stack.pop()
					else:
						print('POP OF WRONG STATUS: %d:%s (index %s of %d)' % (pid, msg, ix, len(stack)))
				elif typ == 'update':
					msg, _, cookie = msg.split('\0', 3)
					stack, ix = _find(pid, cookie)
					if ix is None:
						print('UPDATE TO UNKNOWN STATUS %d:%s: %s' % (pid, cookie, msg))
					else:
						stack[ix] = (msg, stack[ix][1], cookie)
				elif typ == 'output':
					t, msg = msg.split('\0', 1)
					t = float(t)
					status_all[pid].output = (msg, t,)
				elif typ == 'start':
					parent_pid, is_analysis, msg, t = msg.split('\0', 3)
					parent_pid = int(parent_pid)
					t = float(t)
					d = DotDict()
					d.parent_pid = parent_pid
					d.children   = {}
					d.stack      = [(msg, t, None)]
					d.summary    = (t, msg, t,)
					d.output     = None
					if parent_pid in status_all:
						if is_analysis:
							msg, parent_t, _ = status_all[parent_pid].stack[0]
							d.summary = (parent_t, msg + ' analysis', t,)
						status_all[parent_pid].children[pid] = d
					else:
						status_tree[pid] = d
					status_all[pid] = d
					del d
				elif typ == 'end':
					d = status_all.get(pid)
					if d:
						if d.parent_pid in status_all:
							p = status_all[d.parent_pid]
							if pid in p.children:
								del p.children[pid]
							del p
						del d
					if pid in  status_tree:
						del status_tree[pid]
				else:
					print('UNKNOWN MESSAGE: %r' % (data,))
		except Exception:
			print('Failed to process %r:' % (data,))
			print_exc()


def statmsg_endwait(pid, timeout):
	"""Wait for pid to be removed from status_stacks (to send 'end')"""
	for _ in range(10):
		with status_stacks_lock:
			d = status_all.get(pid)
			if not d:
				return
		sleep(timeout / 10)
