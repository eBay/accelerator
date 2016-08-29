import urllib
import urllib2
import httplib
import socket

# Why is this crap not included by default?
class UnixHTTPConnection(httplib.HTTPConnection):
	def __init__(self, host, *a, **kw):
		httplib.HTTPConnection.__init__(self, 'localhost', *a, **kw)
		self.unix_path = urllib.unquote_plus(host.split(':', 1)[0])

	def connect(self):
		s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		s.connect(self.unix_path)
		self.sock = s

class UnixHTTPHandler(urllib2.AbstractHTTPHandler):
	def unixhttp_open(self, req):
		return self.do_open(UnixHTTPConnection, req)

	unixhttp_request = urllib2.AbstractHTTPHandler.do_request_

urllib2.install_opener(urllib2.build_opener(UnixHTTPHandler))
