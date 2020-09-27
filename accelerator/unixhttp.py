############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
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
from accelerator.compat import urlopen, Request, URLError, HTTPError
from accelerator.extras import json_encode, json_decode
from accelerator.error import ServerError, UrdError, UrdPermissionError, UrdConflictError

if PY3:
	from urllib.request import install_opener, build_opener, AbstractHTTPHandler
	from http.client import HTTPConnection
else:
	from urllib2 import install_opener, build_opener, AbstractHTTPHandler
	from httplib import HTTPConnection

import sys
import time
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


import bottle

# The standard bottle WaitressServer can't handle unix sockets and doesn't set threads.
class WaitressServer(bottle.ServerAdapter):
	def run(self, handler):
		from waitress import create_server
		if self.port:
			kw = dict(host=self.host, port=self.port)
		else:
			kw = dict(unix_socket=self.host)
		server = create_server(handler, threads=12, **kw)
		server.run()


def call(url, data=None, fmt=json_decode, headers={}, server_name='server'):
	if data is not None and not isinstance(data, bytes):
		data = json_encode(data)
	req = Request(url, data=data, headers=headers)
	for attempt in (1, 2, 3, 4, 5):
		resp = None
		try:
			r = urlopen(req)
			try:
				resp = r.read()
				if PY3:
					resp = resp.decode('utf-8')
				# It seems inconsistent if we get HTTPError or not.
				if r.getcode() >= 400:
					raise HTTPError(url, r.getcode(), resp, {}, None)
				return fmt(resp)
			finally:
				try:
					r.close()
				except Exception:
					pass
		except HTTPError as e:
			if server_name == 'urd':
				if e.code == 401:
					raise UrdPermissionError()
				if e.code == 409:
					raise UrdConflictError()
			elif e.code != 503 and resp:
				return fmt(resp)
			msg = '%s says %d: %s' % (server_name, e.code, resp,)
		except URLError:
			# Don't say anything the first times, because the output
			# tests get messed up if this happens during them.
			if attempt < 3:
				msg = None
			else:
				msg = 'error contacting ' + server_name
		except ValueError as e:
			msg = 'Bad data from %s, %s: %s' % (server_name, type(e).__name__, e,)
		if msg:
			print(msg, file=sys.stderr)
		if attempt < 5:
			time.sleep(attempt / 15)
			if msg:
				print('Retrying (%d/4).' % (attempt,), file=sys.stderr)
	print('Giving up.', file=sys.stderr)
	if server_name == 'urd':
		raise UrdError(msg)
	else:
		raise ServerError(msg)
