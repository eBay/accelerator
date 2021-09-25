############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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
from __future__ import unicode_literals

import sys, os
from functools import partial

from accelerator.compat import PY2

class Colour:
	"""Constants and functions for colouring output.

	Available as constants named .COLOUR, functions named .colour and
	as direct calls on the object taking (value, *attrs).
	Colours are BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE,
	DEFAULT and BLACKBG, REDBG etc.
	BOLD, ITALIC, UNDERLINE, BLINK, INVERT, and STRIKE are also available.

	When using the constants, end with .RESET.

	>>> colour.RED + 'foo' + colour.RESET == colour.red('foo') == colour('foo', 'red')

	colour(v, 'red', 'bold') and similar produce shorter sequences than other
	ways of combining several attributes.
	"""

	def __init__(self):
		self._all = dict(
			BOLD='1',
			ITALIC='3',
			UNDERLINE='4',
			BLINK='5',
			INVERT='7',
			STRIKE='9',
		)
		for num, name in enumerate([
			'BLACK', 'RED', 'GREEN', 'YELLOW',
			'BLUE', 'MAGENTA', 'CYAN', 'WHITE',
			None, 'DEFAULT',
		]):
			if name:
				self._all[name] = '3%d' % (num,)
				self._all[name + 'BG'] = '4%d' % (num,)
		for k in self._all:
			setattr(self, k.lower(), partial(self._single, k))
		self._on = {k: '\x1b[%sm' % (v,) for k, v in self._all.items()}
		self._on['RESET'] = '\x1b[m'
		if PY2:
			self._on = {k.encode('ascii'): v.encode('ascii') for k, v in self._on.items()}
			self._off = dict.fromkeys(self._on, b'')
		else:
			self._off = dict.fromkeys(self._on, '')

	def enable(self):
		"Turn colours on"
		self.__dict__.update(self._on)
		self.enabled = True

	def disable(self):
		"Turn colours off (make all constants empty)"
		self.__dict__.update(self._off)
		self.enabled = False

	def _single(self, attr, value):
		return self(value, attr)

	def __call__(self, value, *attrs):
		pre = post = ''
		if self.enabled and attrs:
			stuff = []
			for a in attrs:
				want = a.upper()
				if want not in self._all:
					raise Exception('Unknown colour/attr %r' % (a,))
				stuff.append(self._all[want])
			pre = '\x1b[' + ';'.join(stuff) + 'm'
			post = self.RESET
		if isinstance(value, bytes):
			return b'%s%s%s' % (pre.encode('utf-8'), value, post.encode('utf-8'),)
		return '%s%s%s' % (pre, value, post,)

colour = Colour()

# trying to support both https://no-color.org/ and https://bixense.com/clicolors/
if os.getenv('CLICOLOR_FORCE', '0') != '0':
	colour.enable()
elif os.getenv('NO_COLOR') is not None:
	colour.disable()
elif os.getenv('CLICOLOR', '1') != '0' and sys.stdout.isatty():
	colour.enable()
else:
	colour.disable()
