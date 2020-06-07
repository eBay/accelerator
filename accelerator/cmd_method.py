############################################################################
#                                                                          #
# Copyright (c) 2020 Carl Drougge                                          #
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

from accelerator.compat import urlopen, PY3, terminal_size
from accelerator import unixhttp; unixhttp
from accelerator.extras import json_decode
from collections import defaultdict

def main(argv, cfg):
	prog = argv.pop(0)
	if '--help' in argv or '-h' in argv:
		print('usage: %s [method]' % (prog,))
		print('gives description and options for method,')
		print('or lists methods with no method specified.')
		return
	req = urlopen(cfg.url + '/methods')
	resp = req.read()
	if PY3:
		resp = resp.decode('utf-8')
	methods = json_decode(resp)
	if argv:
		for name in argv:
			if name in methods:
				data = methods[name]
				print('%s.%s:' % (data.package, name,))
				if data.description:
					for line in data.description.split('\n'):
						if line:
							print(' ', line)
						else:
							print()
					print()
				if data.version != 'DEFAULT':
					print('Runs on', data.version)
					print()
				for k in ('datasets', 'jobs', 'options'):
					if data[k]:
						print('%s:' % (k,))
						for v in data[k]:
							print(' ', v)
			else:
				print('Method %r not found' % (name,))
	else:
		by_package = defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		columns = terminal_size().columns
		for package, names in sorted(by_package.items()):
			print('%s:' % (package,))
			for name in names:
				max_len = columns - 4 - len(name)
				description = methods[name].description.split('\n')[0]
				if description and max_len > 10:
					if len(description) > max_len:
						max_len -= 4
						parts = description.split()
						description = ''
						for part in parts:
							if len(description) + len(part) + 1 > max_len:
								break
							description = '%s %s' % (description, part,)
						description += ' ...'
					print('  %s: %s' % (name, description,))
				else:
					print('  %s' % (name,))
