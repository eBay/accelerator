############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2021 Carl Drougge                       #
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
from accelerator import g, __version__ as ax_version

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
			kw = dict(unix_socket=self.host, unix_socket_perms='777')
		server = create_server(handler, threads=12, **kw)
		server.run()


def call(url, data=None, fmt=json_decode, headers={}, server_name='server', retries=4, quiet=False):
	if data is not None and not isinstance(data, bytes):
		data = json_encode(data)
	err = None
	req = Request(url, data=data, headers=headers)
	for attempt in range(1, retries + 2):
		resp = None
		try:
			r = urlopen(req)
			try:
				resp = r.read()
				if server_name == 'server' and g.running in ('build', 'shell',):
					s_version = r.headers['Accelerator-Version'] or '<unknown (old)>'
					if s_version != ax_version:
						# Nothing is supposed to catch this, so just print and die.
						print('Server is running version %s but we are running version %s' % (s_version, ax_version,), file=sys.stderr)
						exit(1)
				if PY3:
					resp = resp.decode('utf-8')
				# It is inconsistent if we get HTTPError or not.
				# It seems we do when using TCP sockets, but not when using unix sockets.
				if r.getcode() >= 400:
					raise HTTPError(url, r.getcode(), resp, {}, None)
				return fmt(resp)
			finally:
				try:
					r.close()
				except Exception:
					pass
		except HTTPError as e:
			if resp is None and e.fp:
				resp = e.fp.read()
				if PY3:
					resp = resp.decode('utf-8')
			msg = '%s says %d: %s' % (server_name, e.code, resp,)
			if server_name == 'urd' and 400 <= e.code < 500:
				if e.code == 401:
					err = UrdPermissionError()
				if e.code == 409:
					err = UrdConflictError()
				break
			if server_name == 'server' and e.code != 503 and resp:
				return fmt(resp)
		except URLError:
			# Don't say anything the first times, because the output
			# tests get messed up if this happens during them.
			if attempt < retries - 1:
				msg = None
			else:
				msg = 'error contacting ' + server_name
		except ValueError as e:
			msg = 'Bad data from %s, %s: %s' % (server_name, type(e).__name__, e,)
		if msg and not quiet:
			print(msg, file=sys.stderr)
		if attempt < retries + 1:
			time.sleep(attempt / 15)
			if msg and not quiet:
				print('Retrying (%d/%d).' % (attempt, retries,), file=sys.stderr)
	else:
		if not quiet:
			print('Giving up.', file=sys.stderr)
	if err:
		raise err
	if server_name == 'urd':
		raise UrdError(msg)
	else:
		raise ServerError(msg)
