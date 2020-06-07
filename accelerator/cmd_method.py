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
	columns = terminal_size().columns
	if argv:
		for name in argv:
			if name in methods:
				data = methods[name]
				print('%s.%s:' % (data.package, name,))
				if data.description.text:
					for line in data.description.text.split('\n'):
						if line:
							print(' ', line)
						else:
							print()
					print()
				if data.version != 'DEFAULT':
					print('Runs on', data.version)
					print()
				for k in ('datasets', 'jobs',):
					if data.description.get(k):
						print('%s:' % (k,))
						for k, v in data.description[k].items():
							if v:
								print('  %s # %s' % (k, v[0],))
								prefix = ' ' * (len(k) + 3) + '#'
								for cmt in v[1:]:
									print(prefix, cmt)
							else:
								print(' ', k)
				if data.description.get('options'):
					print('options:')
					for k, v in data.description.options.items():
						if len(v) > 1:
							single_line = '  %s = %s # %s' % (k, v[0], v[1],)
							if len(single_line) > columns or len(v) > 2:
								for cmt in v[1:]:
									print('  #', cmt)
								print('  %s = %s' % (k, v[0],))
							else:
								print(single_line)
						else:
							print('  %s = %s' % (k, v[0],))
			else:
				print('Method %r not found' % (name,))
	else:
		by_package = defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		for package, names in sorted(by_package.items()):
			print('%s:' % (package,))
			for name in names:
				max_len = columns - 4 - len(name)
				description = methods[name].description.text.split('\n')[0]
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
