############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Carl Drougge                            #
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

from accelerator.compat import PY3, unquote_plus

if PY3:
	from urllib.request import install_opener, build_opener, AbstractHTTPHandler
	from http.client import HTTPConnection
else:
	from urllib2 import install_opener, build_opener, AbstractHTTPHandler
	from httplib import HTTPConnection

import socket

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


from wsgiref.simple_server import WSGIServer, WSGIRequestHandler
if PY3:
	from socketserver import UnixStreamServer
else:
	from SocketServer import UnixStreamServer

class WSGIUnixServer(UnixStreamServer, WSGIServer):
	def server_bind(self):
		# Everything expects a (host, port) pair, except the unix bind function.
		save = self.server_address
		self.server_address = save[0]
		UnixStreamServer.server_bind(self)
		self.server_address = save
		self.server_name, self.server_port = self.server_address
		self.setup_environ()

class WSGIUnixRequestHandler(WSGIRequestHandler):
	def __init__(self, request, client_address, server):
		# Everything expects a (host, port) pair, so let's provide something like that.
		client_address = (client_address, 0)
		WSGIRequestHandler.__init__(self, request, client_address, server)
