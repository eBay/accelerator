from __future__ import print_function
from __future__ import division

from compat import PY3, unquote_plus

if PY3:
	from urllib.request import install_opener, build_opener, AbstractHTTPHandler
	from http.client import HTTPConnection
else:
	from urllib2 import install_opener, build_opener, AbstractHTTPHandler
	from httplib import HTTPConnection

import socket

# Why is this crap not included by default?
class UnixHTTPConnection(HTTPConnection):
	def __init__(self, host, *a, **kw):
		HTTPConnection.__init__(self, 'localhost', *a, **kw)
		self.unix_path = unquote_plus(host.split(':', 1)[0])

	def connect(self):
		s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		s.connect(self.unix_path)
		self.sock = s

class UnixHTTPHandler(AbstractHTTPHandler):
	def unixhttp_open(self, req):
		return self.do_open(UnixHTTPConnection, req)

	unixhttp_request = AbstractHTTPHandler.do_request_

install_opener(build_opener(UnixHTTPHandler))
