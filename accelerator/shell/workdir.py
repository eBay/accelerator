############################################################################
#                                                                          #
# Copyright (c) 2020-2021 Carl Drougge                                     #
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

from accelerator.compat import ArgumentParser, url_quote
from accelerator.unixhttp import call
from accelerator.setupfile import load_setup
from accelerator.build import fmttime
from accelerator.extras import DotDict
from accelerator.job import Job

def job_data(known, jid, full_path=False):
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
	if not data.get('path'):
		if full_path:
			data.path = Job(jid).path
		else:
			data.path = jid
	return data

def show_job(args, known, jid, as_latest=False):
	data = job_data(known, jid, args.full_path)
	path = data.path
	if as_latest:
		path = path.rsplit('-', 1)[0] + '-LATEST'
	print('\t'.join((path, data.klass, data.method, data.totaltime or '')))

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
	usage = "%(prog)s [-p] [-a | [workdir [workdir [...]]]"
	parser = ArgumentParser(usage=usage, prog=argv.pop(0))
	parser.add_argument('-a', '--all', action='store_true', help="list all workdirs")
	parser.add_argument('-p', '--full-path', action='store_true', help="show full path")
	parser.add_argument('workdirs', nargs='*', default=[])
	args = parser.parse_args(argv)

	if args.all:
		args.workdirs.extend(sorted(set(cfg.workdirs) - set(args.workdirs)))

	if not args.workdirs:
		template = '%%-%ds  %%s' % (max(len(wd) for wd in cfg.workdirs))
		for wd, path in sorted(cfg.workdirs.items()):
			print(template % (wd, path,))
		return

	for name in args.workdirs:
		if name not in cfg.workdirs:
			print("No such workdir:", name, file=sys.stderr)
			continue
		known = call(cfg.url + '/workdir/' + url_quote(name))
		for jid in workdir_jids(cfg, name):
			show_job(args, known, jid)

		try:
			latest = os.readlink(os.path.join(cfg.workdirs[name], name + '-LATEST'))
		except OSError:
			latest = None
		if latest:
			show_job(args, known, jid, True)
