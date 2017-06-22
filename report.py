############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

import time

from compat import open, uni

import g

class report():
	def __init__(self, stdout=False):
		self.stdout = stdout
		self.s = ''
		self.line()
		t = time.strftime('%a, %d %b %Y %H:%M:%S +0000', time.gmtime())
		self.println( '[%s]%s[%s]' % (t, ' ' * (76 - len(t) - len(g.JOBID)), g.JOBID,))
		self.line()
		self.println('Method \"%s\" report.' % g.METHOD)
		caption = g.CAPTION
		if caption:
			self.println('Caption \"%s\"' % caption)
		self.line()
		self._options(g.options)
		self.line()

	def line(self):
		self.println('-' * 80)

	def println(self, s):
		self.write(s + '\n')

	def write(self, s):
		self.s += s

	def printvec(self, vec, columns):
		spacing = 80 // columns - 6
		for ix, x in enumerate(vec):
			self.write( '  %3d %s'  % (ix, str(x).ljust(spacing),))
			if ix % columns == columns - 1:
				self.write('\n')
		if ix % columns != columns - 1:
			self.write('\n')

	def _options(self, optionsdict, title='Options'):
		if not optionsdict:
			return
		self.println(title)
		maxlen = max(len(k) for k in optionsdict)
		for k, v in optionsdict.items():
			k = str(k).ljust(maxlen)
			if isinstance(v, (list, tuple)):
				self.println('  %s :' % (k,))
				for t in v:
					self.println('  %s   %s' % (' ' * maxlen, t,))
			else:
				self.println("  %s : %s " % (k, v,))

	def close(self):
		self.line()
		with open('report.txt', 'w', encoding='utf-8') as F:
			F.write(uni(self.s))
		if self.stdout:
			print(self.s)
