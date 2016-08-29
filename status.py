from contextlib import contextmanager
from time import time, strftime
from traceback import print_exc
from threading import Lock
from weakref import WeakValueDictionary

import g
from status_messaging import _send


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


@contextmanager
def status(msg):
	if g.running == 'daemon':
		yield
		return
	if isinstance(msg, unicode):
		msg = msg.encode('utf-8')
	assert msg and isinstance(msg, str) and '\0' not in msg
	_send('push', '%s\0%f' % (msg, time(),))
	try:
		yield
	finally:
		_send('pop', '')

def _start(msg, parent_pid, is_analysis=''):
	_send('start', '%d\0%s\0%s\0%f' % (parent_pid, is_analysis, msg, time(),))

def _end():
	_send('end', '')


def status_stacks_export():
	res = []
	last = [None]
	current = None
	def fmt(tree, start_indent=0):
		for pid, d in sorted(tree.iteritems(), key=lambda i: (i[1].stack or ((0,),))[0][0]):
			last[0] = d
			indent = start_indent
			for msg, t in d.stack:
				res.append((pid, indent, msg, t))
				indent += 1
			fmt(d.children, indent)
	try:
		with status_stacks_lock:
			fmt(status_tree)
		if last[0]:
			current = last[0].summary
			if len(last[0].stack) > 1 and not current[1].endswith('analysis'):
				msg, t = last[0].stack[1]
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
		print "%6d STATUS: %s%s (%.1f seconds)" % (pid, "    " * indent, msg, report_t - t)


def statmsg_sink(logfilename, sock):
	from extras import DotDict
	print 'write log to \"%s\".' % logfilename
	with open(logfilename, 'wb') as fh:
		ix = 0
		while True:
			data = None
			try:
				data = sock.recv(1500)
				typ, pid, msg = data.split('\0', 2)
				pid = int(pid)
				with status_stacks_lock:
					if typ == 'push':
						msg, t = msg.split('\0', 2)
						t = float(t)
						status_all[pid].stack.append((msg, t,))
					elif typ == 'pop':
						status_all[pid].stack.pop()
					elif typ == 'start':
						parent_pid, is_analysis, msg, t = msg.split('\0', 3)
						parent_pid = int(parent_pid)
						t = float(t)
						d = DotDict(_default=None)
						d.parent_pid = parent_pid
						d.children   = {}
						d.stack      = [(msg, t,)]
						d.summary    = (t, msg, t,)
						if parent_pid in status_all:
							if is_analysis:
								msg, parent_t = status_all[parent_pid].stack[0]
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
					elif typ == 'statmsg':
						fh.write('%s %5d: %s\n' % (strftime("%Y-%m-%d %H:%M:%S"), ix, data,))
						fh.flush()
						ix += 1
					else:
						print 'UNKNOWN MESSAGE: %r' % (data,)
			except Exception:
				print 'Failed to process %r:' % (data,)
				print_exc()
