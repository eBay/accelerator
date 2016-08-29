import socket
import os
import time
import string

sock = None

def _send(typ, message):
	global sock
	if not sock:
		fd = int(os.getenv('BD_STATUS_FD'))
		sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_DGRAM)
	if len(message) > 1400:
		message = message[:300] + '\n....\n' + message[-1100:]
	msg = '%s\0%d\0%s' % (typ, os.getpid(), message,)
	for ix in range(5):
		try:
			sock.send(msg)
			return
		except socket.error as e:
			print 'Failed to send statmsg (type %s, try %d): %s' % (typ, ix, e)
			time.sleep(0.1 + ix)

def statmsg(message, plain=False, verbose=False):
	# ignore verbose
	if not plain:
		message = '  %s' % message
	_send('statmsg', message)


def staterr(message):
	statmsg('E ' + message)

def statlaunch(jobid, method, msg):
	s = '| ' + string.ljust('[%s] %s' % (jobid, method), 44) + string.rjust('%s' % msg, 11) + ' |'
	statmsg(s, plain=True)

class TimedStatus:
	def __init__(self, delta):
		self.t = time.time()
		self.delta = delta

	def status(self, message):
		t = time.time()
		if t - self.t > self.delta:
			statmsg(message)
			self.t = t
