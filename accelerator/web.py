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

from accelerator.compat import PY3, unicode

if PY3:
	from socketserver import ThreadingMixIn
	from http.server import HTTPServer, BaseHTTPRequestHandler
	from socketserver import UnixStreamServer
	from urllib.parse import parse_qs
else:
	from SocketServer import ThreadingMixIn
	from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
	from SocketServer import UnixStreamServer
	from urlparse import parse_qs

import cgi
from traceback import print_exc

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
	request_queue_size = 512

class ThreadedUnixHTTPServer(ThreadingMixIn, UnixStreamServer):
	request_queue_size = 512

class BaseWebHandler(BaseHTTPRequestHandler):
	"""What I usually do in my web servers.
	   Implement do_request(path, args)
	   and optionally override encode_body(obj)
	   set unicode_args if you want args decoded to unicode

	   POSTs that do not contain a known form encoding will be put in
	   args with None as the key.
	   """

	unicode_args = False

	# Stop it from doing name lookups for logging
	def address_string(self):
		return self.client_address[0]

	def do_GET(self):
		self.is_head = False
		self._do_req()

	def do_HEAD(self):
		self.is_head = True
		self._do_req()

	def do_POST(self):
		length = self.headers.get('content-length')
		if not length:
			return self._bad_request()
		if hasattr(self.headers, 'get_content_type'):
			ctype = self.headers.get_content_type()
		elif self.headers.typeheader is None:
			ctype = self.headers.type
		else:
			ctype = self.headers.typeheader
		bare_ctype = ctype.split(";", 1)[0].strip()
		if bare_ctype in ("application/x-www-form-urlencoded", "multipart/form-data"):
			env = {"CONTENT_LENGTH": length,
			       "CONTENT_TYPE"  : ctype,
			       "REQUEST_METHOD": "POST"
			      }
			cgi_args = cgi.parse(self.rfile, environ=env, keep_blank_values=True)
		else:
			cgi_args = {None: [self.rfile.read(int(length))]}
		self.is_head = False
		self._do_req2(self.path, cgi_args)

	def _do_req(self):
		path = self.path.split("?")
		cgi_args = {}
		if len(path) == 2:
			cgi_args = parse_qs(path[1], keep_blank_values=True)
		elif len(path) != 1:
			return self._bad_request()
		self._do_req2(path[0], cgi_args)

	def _bad_request(self):
		self.do_response(400, "text/plain", "Bad request\n")

	def argdec(self, v):
		if self.unicode_args:
			if type(v) is unicode: return v
			try:
				return v.decode("utf-8")
			except Exception:
				try:
					return v.decode("iso-8859-1")
				except Exception:
					return u""
		return v

	def _do_req2(self, path, cgi_args):
		p_a = []
		for e in path.split("/"):
			if e == "..":
				p_a = p_a[:-1]
			elif e and e != ".":
				p_a.append(e)
		args = dict((a, self.argdec(cgi_args[a][-1])) for a in cgi_args)
		self.handle_req(p_a, args)

	def encode_body(self, body):
		"""Encode whatever you passed as body to do_response as byte stream.
		   Should be overridden if you pass anything but str or an object
		   with a unicode-compatible encode method."""
		if isinstance(body, bytes): return body
		return body.encode("utf-8")

	def do_response(self, code, content_type, body, extra_headers = []):
		try:
			body = self.encode_body(body)
			self.send_response(code)
			self.send_header("Content-Type", content_type)
			self.send_header("Content-Length", str(len(body)))
			for hdr, val in extra_headers:
				self.send_header(hdr, val)
			self.end_headers()
			if self.is_head: return
			self.wfile.write(body)
		except Exception:
			print_exc()
