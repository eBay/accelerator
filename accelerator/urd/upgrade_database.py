#!/usr/bin/env python2.7
# -*- coding: iso-8859-1 -*-

############################################################################
#                                                                          #
# Copyright (c) 2018 eBay Inc.                                             #
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
import json
from datetime import datetime
import os
import shutil


def upgrade_ts(ts):
	if len(ts) == 8:
		return datetime.strptime(ts, "%Y%m%d").strftime("%Y-%m-%d")
	elif len(ts) == 11:
		return datetime.strptime(ts, "%Y%m%d %H").strftime("%Y-%m-%dT%H")
	elif len(ts) == 13:
		return datetime.strptime(ts, "%Y%m%d %H%M").strftime("%Y-%m-%dT%H:%M")
	elif len(ts) == 15:
		return datetime.strptime(ts, "%Y%m%d %H%M%S").strftime("%Y-%m-%dT%H:%M:%S")
	elif len(ts) > 15:
		return datetime.strptime(ts, "%Y%m%d %H%M%S.%f").strftime("%Y-%m-%dT%H:%M:%S.%f")
	else:
		print("Illegal date format \"%s\"" % (ts,))
		exit(1)


def recursive_replace(obj, key, fun):
	for k, v in obj.items():
		if isinstance(v, dict):
			obj[k] = recursive_replace(v, key, fun)
	if key in obj:
		obj[key] = fun(obj[key])
	return obj


def upgrade_line(line):
	x = line.rstrip('\n').split('|')
	logfileversion = x[0]
	assert logfileversion == '2'
	x[0] = '3'
	x[1] = upgrade_ts(x[1])
	if x[2] == "add":
		x[3] = upgrade_ts(x[3])
		x[5] = json.dumps(recursive_replace(json.loads(x[5]), 'timestamp', upgrade_ts))
	elif x[2] == "truncate":
		if x[3] != '0':
			x[3] = upgrade_ts(x[3])
	else:
		print("Wrong command \"%s\"" % (x[2],))
		exit(1)
	return '|'.join(x)


help = """Upgrade an Urd database from logversion 2 to logversion 3.
(Logversion is the first number of each line in the urd database.)

Use like this:  \"upgrade_database <source> <dest>\"

Where <source> is a path to the root of an Urd database
(same path as used when starting urd with \"--path <path>\").

An upgraded database copy is created at dest>.
"""

if __name__ == "__main__":
	from sys import argv
	if len(argv) != 3:
		print(help)
		exit()
	source = argv[1]
	dest = argv[2]

	if not os.path.exists(source):
		print('Source "%s" does not exist!' % (source,))
		exit(1)

	if os.path.exists(dest):
		print('Destination "%s" already exists!' % (dest,))
		exit(1)

	for src_dir, dirs, files in os.walk(source):
		dst_dir = src_dir.replace(source, dest, 1)
		if not os.path.exists(dst_dir):
			os.mkdir(dst_dir)
			print('mkdir',dst_dir)
		for file_ in files:
			src_file = os.path.join(src_dir, file_)
			dst_file = os.path.join(dst_dir, file_)
			if src_file.endswith(".urd"):
				print('  convert', src_file, dst_file)
				with open(src_file, 'rt') as fhr:
					with open(dst_file, 'wt') as fhw:
						for line in fhr:
							line = upgrade_line(line)
							fhw.write(line + '\n')
			else:
				shutil.copy(src_file, dst_dir)
				print('copy', src_file, dst_dir + '/')
