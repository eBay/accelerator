############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

# a few things that differ between python2 and python3

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import sys
try:
	from setproctitle import setproctitle as _setproctitle, getproctitle
	# setproctitle init may be delayed until called (v1.2+) and should happen early
	getproctitle()
	del getproctitle
except ImportError:
	def _setproctitle(title): pass

if sys.version_info[0] == 2:
	PY2 = True
	PY3 = False
	import __builtin__ as builtins
	import cPickle as pickle
	FileNotFoundError = builtins.OSError
	from urllib import quote_plus, unquote_plus, urlencode
	from urllib2 import urlopen, Request, URLError, HTTPError
	from itertools import izip, izip_longest, imap, ifilter
	from Queue import Queue, Full as QueueFull, Empty as QueueEmpty
	from types import NoneType
	str_types = (str, unicode,)
	int_types = (int, long,)
	num_types = (int, float, long,)
	unicode = builtins.unicode
	long = builtins.long
	def iterkeys(d):
		return d.iterkeys()
	def itervalues(d):
		return d.itervalues()
	def iteritems(d):
		return d.iteritems()
	from io import open
	def getarglist(func):
		from inspect import getargspec
		return getargspec(func).args
	def terminal_size():
		from termios import TIOCGWINSZ
		import struct
		from fcntl import ioctl
		from collections import namedtuple
		from os import environ
		def ifgood(name):
			try:
				v = int(environ[name])
				if v > 0:
					return v
			except (KeyError, ValueError):
				pass
		lines, columns = ifgood('LINES'), ifgood('COLUMNS')
		if not lines or not columns:
			try:
				fb_lines, fb_columns, _, _ = struct.unpack('HHHH', ioctl(0, TIOCGWINSZ, struct.pack('HHHH', 0, 0, 0, 0)))
			except Exception:
				fb_lines, fb_columns = 24, 80
		return namedtuple('terminal_size', 'columns lines')(columns or fb_columns, lines or fb_lines)
else:
	PY2 = False
	PY3 = True
	import builtins
	FileNotFoundError = builtins.FileNotFoundError
	import pickle
	from urllib.parse import quote_plus, unquote_plus
	from urllib.request import urlopen, Request
	from urllib.error import URLError, HTTPError
	izip = zip
	from itertools import zip_longest as izip_longest
	imap = map
	ifilter = filter
	from queue import Queue, Full as QueueFull, Empty as QueueEmpty
	NoneType = type(None)
	str_types = (str,)
	int_types = (int,)
	num_types = (int, float,)
	unicode = str
	open = builtins.open
	long = int
	def iterkeys(d):
		return iter(d.keys())
	def itervalues(d):
		return iter(d.values())
	def iteritems(d):
		return iter(d.items())
	def urlencode(query):
		from urllib.parse import urlencode
		return urlencode(query).encode('utf-8')
	def getarglist(func):
		from inspect import getfullargspec
		return getfullargspec(func).args
	from shutil import get_terminal_size as terminal_size

def first_value(d):
	return next(itervalues(d) if isinstance(d, dict) else iter(d))

def uni(s):
	if s is None:
		return None
	if isinstance(s, bytes):
		try:
			return s.decode('utf-8')
		except UnicodeDecodeError:
			return s.decode('iso-8859-1')
	return unicode(s)

# This is used in the method launcher to set different titles for each
# phase/slice. You can use it in the method to override that if you want.
def setproctitle(title):
	from accelerator import g
	if hasattr(g, 'params'):
		title = '%s %s (%s)' % (g.job, uni(title), g.params.method,)
	elif hasattr(g, 'job'):
		title = '%s %s' % (g.job, uni(title),)
	else:
		title = uni(title)
	if PY2:
		title = title.encode('utf-8')
	_setproctitle(title)

# parse_intermixed_args is new in 3.7
def parse_intermixed_args(parser, argv):
	f = getattr(parser, 'parse_intermixed_args', parser.parse_args)
	return f(argv)
