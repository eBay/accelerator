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

import sys
from os import environ
from os.path import join
from subprocess import Popen, PIPE
import json
from accelerator.compat import unquote_plus


def main(argv, cfg):
	prog = argv.pop(0)
	if argv and argv[0] in ('server', 'urd',):
		which = argv.pop(0)
	else:
		which = 'urd'
	if '--help' in argv or '-h' in argv or not argv:
		fh = sys.stdout if argv else sys.stderr
		print('usage: %s [server|urd] [curl options] path' % (prog,), file=fh)
		print('%s server talks to the server, %s urd talks to urd (default)' % (prog, prog,), file=fh)
		print(file=fh)
		print('examples:', file=fh)
		print('  %s %s/example/latest' % (prog, environ['USER'],), file=fh)
		print('  %s server status' % (prog,), file=fh)
		return
	url_end = argv.pop()
	socket_opts = []
	if which == 'urd':
		url_start = cfg.urd
	else: # server
		url_start = cfg.url
	if url_start.startswith('unixhttp://'):
		url_start = url_start.split('://', 1)[1]
		if '/' in url_start:
			socket, url_start = url_start.split('/', 1)
		else:
			socket, url_start = url_start, ''
		socket_opts = ['--unix-socket', unquote_plus(socket)]
		url_start = join('http://.', url_start)
	argv = ['curl', '-sS'] + socket_opts + argv + [join(url_start, url_end)]
	curl = Popen(argv, stdout=PIPE)
	output, _ = curl.communicate()
	if output:
		try:
			output = output.decode('utf-8')
			output = json.dumps(json.loads(output), indent=4)
		except Exception:
			pass
		print(output)
	return curl.wait()
