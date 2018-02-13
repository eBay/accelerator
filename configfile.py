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

import re
import os

from compat import quote_plus


def get_config( filename, verbose=True ):
	with open(filename) as F:
		c = parse_config(F.read(), filename)
	if verbose:
		print_config(c)
	return c


def print_config(config):
	print('-'*79)
	X = {
		'method_directories' : lambda x : ', '.join('\"%s\"' % t for t in x),
		'workdir'            : lambda x : ''.join('\n  -    %-12s %-40s %2d' % (s, t[0], t[1]) for s, t in x.items()),
	}
	for x, y in config.items():
		print("  %-30s : " % (x,), end="")
		if x in X:
			print(X[x](y))
		else:
			print(y)
	print('-'*79)


_re_var = re.compile(r'\$\{([^\}=]*)(?:=([^\}]*))?\}')
def _interpolate(s):
	"""Replace ${FOO=BAR} with os.environ.get('FOO', 'BAR')
	(just ${FOO} is of course also supported, but not $FOO)"""
	return _re_var.subn(lambda m: os.environ.get(m.group(1), m.group(2)), s)[0]


def resolve_socket_url(path):
	if '://' in path:
		return path
	else:
		return 'unixhttp://' + quote_plus(os.path.realpath(path))

def parse_config(string, filename=None):
	ret = {}
	for line in string.split('\n'):
		line = line.split('#')[0].strip()
		if len(line)==0:
			continue
		try:
			key, val = line.split('=', 1)
			val = _interpolate(val)
			if key =='workdir':
				# create a dict {name : (path, slices), ...}
				ret.setdefault(key, {})
				val = val.split(':')
				name = val[0]
				path = val[1]
				if len(val)==2:
					# there is no slice information
					slices = -1
				else:
					slices = val[2]
				ret[key][name] = (path, int(slices))

			elif key in ('source_workdirs', 'method_directories',):
				# create a set of (name, ...)
				ret.setdefault(key, set())
				ret[key].update(val.split(','))
			elif key == 'urd':
				ret[key] = resolve_socket_url(val)
			else:
				ret[key] = val
		except:
			print("Error parsing config %s: \"%s\"" % (filename, line,))
	if 'workdir' not in ret:
		raise Exception("Error, missing workdir in config " + filename)
	return ret


def sanity_check(config_dict):
	ok = True
	if 'target_workdir' not in config_dict:
		print("# Error in configfile, must specify target_workdir.")
		ok = False
	if 'workdir' not in config_dict:
		print("# Error in configfile, must specify at least one workdir.")
		ok = False
	if not ok:
		exit(1)
