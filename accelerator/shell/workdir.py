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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import sys
import os
from argparse import ArgumentParser

from accelerator.unixhttp import call
from accelerator.setupfile import load_setup
from accelerator.build import fmttime
from accelerator.extras import DotDict

def job_data(known, jid):
	if jid in known:
		data = known[jid]
	else:
		data = DotDict(method='???', totaltime=None, current=None)
		try:
			setup = load_setup(jid)
			data.method = setup.method
			if 'exectime' in setup:
				data.totaltime = setup.exectime.total
		except Exception:
			pass
	if isinstance(data.totaltime, (float, int)):
		data.totaltime = fmttime(data.totaltime)
	if data.totaltime is None:
		data.klass = 'unfinished'
	elif data.current:
		data.klass = 'current'
	else:
		data.klass = 'old'
	return data

def show_job(known, jid, show_jid=None):
	data = job_data(known, jid)
	print('\t'.join((show_jid or jid, data.klass, data.method, data.totaltime or '')))

def workdir_jids(cfg, name):
	jidlist = []
	for jid in os.listdir(cfg.workdirs[name]):
		if '-' in jid:
			wd, num = jid.rsplit('-', 1)
			if wd == name and num.isdigit():
				jidlist.append(int(num))
	jidlist.sort()
	return ['%s-%s' % (name, jid,) for jid in jidlist]

def main(argv, cfg):
	usage = "%(prog)s [-a | [workdir [workdir [...]]]"
	parser = ArgumentParser(usage=usage, prog=argv.pop(0))
	parser.add_argument('-a', '--all', action='store_true', help="list all workdirs")
	parser.add_argument('workdirs', nargs='*', default=[])
	args = parser.parse_args(argv)

	if args.all:
		args.workdirs.extend(sorted(cfg.workdirs))

	if not args.workdirs:
		for wd in sorted(cfg.workdirs):
			print(wd)
		return

	for name in args.workdirs:
		if name not in cfg.workdirs:
			print("No such workdir:", name, file=sys.stderr)
			continue
		known = call(cfg.url + '/workdir/' + name)
		for jid in workdir_jids(cfg, name):
			show_job(known, jid)

		try:
			latest = os.readlink(os.path.join(cfg.workdirs[name], name + '-LATEST'))
		except OSError:
			latest = None
		if latest:
			show_job(known, jid, name + '-LATEST')
