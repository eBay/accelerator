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

# workarounds for sadnesses

import os
import signal
import termios
import fcntl
import atexit
from contextlib import contextmanager

class SignalWrapper(object):
	"""Some misguided kernels (like Linux) feel that SIGINFO is not needed.
	Perhapt some other kernels feel similarly about other signals?"""

	def __init__(self, signal_names=['SIGINFO'], key_values=[20], skip_input_if_possible=True):
		"""signal_names is a list of signal names, which will be listened for if they exist.
		key_values is a list of (terminal input) key values, each will
		be considered equivalent of a signal if pressed.
		If skip_input_if_possible is True no input will be read if all signals exist.
		No differentiation is made between the signals."""

		self.key_values = set(key_values)
		self.restore = {}
		self.signal_set = False
		self.clean = False
		self.use_input = False
		all_sigs = True
		atexit.register(self.cleanup)
		for name in signal_names:
			sig = getattr(signal, name, None)
			if sig is not None:
				old = signal.signal(sig, self.signal_arrived)
				self.restore[sig] = old
				signal.siginterrupt(sig, False)
			else:
				all_sigs = False
		if all_sigs and skip_input_if_possible:
			return
		try:
			self.tc_original = termios.tcgetattr(0)
		except Exception:
			# If this fails we can't use terminal input.
			# Probably because we don't have a terminal, but the
			# reason doesn't really matter, ignore it regardless.
			return
		tc_changed = list(self.tc_original)
		tc_changed[3] &= ~(termios.ICANON | termios.IEXTEN)
		self.use_input = True
		termios.tcsetattr(0, termios.TCSADRAIN, tc_changed)

	def cleanup(self):
		if not self.clean:
			for sig, old in self.restore.items():
				if old is None:
					old = signal.SIG_DFL
				signal.signal(sig, old)
			if self.use_input:
				termios.tcsetattr(0, termios.TCSADRAIN, self.tc_original)
			self.clean = True

	def check(self):
		if self.use_input:
			with nonblocking(0):
				while True:
					try:
						if ord(os.read(0, 1)) in self.key_values:
							self.signal_set = True
					except OSError:
						break
		if self.signal_set:
			self.signal_set = False
			return True
		else:
			return False

	def signal_arrived(self, sig, frame):
		self.signal_set = True

@contextmanager
def nonblocking(fd):
	fl_original = fcntl.fcntl(fd, fcntl.F_GETFL)
	fl_changed = fl_original | os.O_NONBLOCK
	fcntl.fcntl(fd, fcntl.F_SETFL, fl_changed)
	try:
		yield
	finally:
		fcntl.fcntl(fd, fcntl.F_SETFL, fl_original)
