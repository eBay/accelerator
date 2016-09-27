from __future__ import print_function
from __future__ import division

import socket
import os
import time

sock = None

def _send(typ, message):
	global sock
	if not sock:
		fd = int(os.getenv('BD_STATUS_FD'))
		sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_DGRAM)
	if len(message) > 1400:
		message = message[:300] + '\n....\n' + message[-1100:]
	msg = ('%s\0%d\0%s' % (typ, os.getpid(), message,)).encode('utf-8')
	for ix in range(5):
		try:
			sock.send(msg)
			return
		except socket.error as e:
			print('Failed to send statmsg (type %s, try %d): %s' % (typ, ix, e))
			time.sleep(0.1 + ix)

def statmsg(message, plain=False, verbose=False):
	# ignore verbose
	if not plain:
		message = '  %s' % message
	_send('statmsg', message)
