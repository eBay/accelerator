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
	These can be prefixed with BRIGHT and/or suffixed with BG.
	DEFAULT[BG] restores the default colour.

	BOLD, FAINT, ITALIC, UNDERLINE, BLINK, INVERT, and STRIKE are also
	available. These can be prefixed with NOT to turn them off.

	When using the constants, you should usually end with .RESET.

	>>> colour.RED + 'foo' + colour.DEFAULT == colour.red('foo') == colour('foo', 'red')

	colour(v, 'red', 'bold') and similar produce shorter sequences than other
	ways of combining several attributes.

	You can also use colour(v, '#RRGGBB[bg]'), but terminal support is not
	great.

	The functions take force=True to return escape sequences even if colour
	is disabled and reset=True to reset all attributes before (and after)
	this sequence. (By default only the changed attributes are reset.)
	"""

	def __init__(self):
		self._all = dict(
			BOLD='1',
			FAINT='2',
			ITALIC='3',
			UNDERLINE='4',
			BLINK='5',
			INVERT='7',
			STRIKE='9',
		)
		self._all.update([('NOT' + k, '2' + v) for k, v in self._all.items()])
		self._all['NOTBOLD'] = '22'
		for num, name in enumerate([
			'BLACK', 'RED', 'GREEN', 'YELLOW',
			'BLUE', 'MAGENTA', 'CYAN', 'WHITE',
		]):
			for prefix, base in (('', 30), ('BRIGHT', 90)):
				self._all[prefix + name] = str(base + num)
				self._all[prefix + name + 'BG'] = str(base + 10 + num)
		self._all['DEFAULT'] = '39'
		self._all['DEFAULTBG'] = '49'
		for k in self._all:
			setattr(self, k.lower(), partial(self._single, k))
		self._on = {k: '\x1b[%sm' % (v,) for k, v in self._all.items()}
		self._on['RESET'] = '\x1b[m'
		self._all['RESET'] = ''
		if PY2:
			self._on = {k.encode('ascii'): v.encode('ascii') for k, v in self._on.items()}
			self._off = dict.fromkeys(self._on, b'')
		else:
			self._off = dict.fromkeys(self._on, '')
		self.__all__ = [k for k in dir(self) if not k.startswith('_')]
		self.enable()

	def configure_from_environ(self, environ=None, stdout=None):
		# trying to support both https://no-color.org/ and https://bixense.com/clicolors/
		if environ is None:
			environ = os.environ
		if environ.get('CLICOLOR_FORCE', '0') != '0':
			self.enable()
		elif environ.get('NO_COLOR') is not None:
			self.disable()
		elif environ.get('CLICOLOR', '1') != '0' and (stdout or sys.stdout).isatty():
			self.enable()
		else:
			self.disable()

	def enable(self):
		"Turn colours on"
		self.__dict__.update(self._on)
		self.enabled = True

	def disable(self):
		"Turn colours off (make all constants empty)"
		self.__dict__.update(self._off)
		self.enabled = False

	def _single(self, attr, value, reset=False, force=False):
		return self(value, attr, reset=reset, force=force)

	# When we drop python 2 we can change this to use normal keywords
	def __call__(self, value, *attrs, **kw):
		bad_kw = set(kw) - {'force', 'reset'}
		if bad_kw:
			raise TypeError('Unknown keywords %r' % (bad_kw,))
		if not attrs:
			raise TypeError('specify at least one attr')
		if (self.enabled or kw.get('force')):
			if kw.get('reset'):
				pre = ['0']
			else:
				pre = []
			post = set()
			for a in attrs:
				want = a.upper()
				default = self._all['DEFAULTBG' if want.endswith('BG') else 'DEFAULT']
				if want.startswith('#'):
					if want.endswith('BG'):
						prefix = '48'
						want = want[:-2]
					else:
						prefix = '38'
					if len(want) != 7:
						raise Exception('Bad colour spec %r' % (a,))
					try:
						r, g, b = (str(int(w, 16)) for w in (want[1:3], want[3:5], want[5:7]))
					except ValueError:
						raise Exception('Bad colour spec %r' % (a,))
					pre.extend((prefix, '2', r, g, b))
					post.add(default)
				else:
					if want not in self._all:
						raise Exception('Unknown colour/attr %r' % (a,))
					pre.append(self._all[want])
					post.add(self._all.get('NOT' + want, default))
			pre = '\x1b[' + ';'.join(pre) + 'm'
			if kw.get('reset'):
				post = ()
			post = '\x1b[' + ';'.join(sorted(post)) + 'm'
		else:
			pre = post = ''
		if isinstance(value, bytes):
			return b'%s%s%s' % (pre.encode('utf-8'), value, post.encode('utf-8'),)
		return '%s%s%s' % (pre, value, post,)

colour = Colour()
colour.configure_from_environ()
