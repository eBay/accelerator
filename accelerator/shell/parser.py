############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2021 Carl Drougge                       #
# Modifications copyright (c) 2019 Anders Berkeman                         #
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

# parsing of "job specs", including as part of a dataset name.
# handles jobids, paths and method names.

from __future__ import division, print_function

import sys
from os.path import join, exists, realpath, split
from os import readlink, environ
import re

from accelerator.job import WORKDIRS
from accelerator.job import Job
from accelerator.error import NoSuchJobError, NoSuchDatasetError, NoSuchWorkdirError, UrdError
from accelerator.unixhttp import call
from accelerator.compat import url_quote

class JobNotFound(NoSuchJobError):
	pass

class DatasetNotFound(NoSuchDatasetError):
	pass

def _groups(tildes):
	def char_and_count(buf):
		char, count = re.match(r'([~^]+)(\d*)$', ''.join(buf)).groups()
		count = int(count or 1) - 1
		return char[0], len(char) + count
	i = iter(tildes)
	buf = [next(i)]
	for c in i:
		if c in '~^' and buf[-1] != c:
			yield char_and_count(buf)
			buf = [c]
		else:
			buf.append(c)
	yield char_and_count(buf)

# "foo~~^3" -> "foo", [("~", 2), ("^", 3)]
def split_tildes(n, allow_empty=False):
	m = re.match(r'(.*?)([~^][~^\d]*)$', n)
	if m:
		n, tildes = m.groups()
		lst = list(_groups(tildes))
	else:
		lst = []
	assert n or allow_empty, "empty job id"
	return n, lst

def method2job(cfg, method, count=0, start_from=None):
	def get(count):
		url ='%s/method2job/%s/%s' % (cfg.url, method, count)
		if start_from:
			url += '?start_from=' + url_quote(start_from)
		return call(url)
	found = get(count)
	if 'error' in found:
		raise JobNotFound(found.error)
	return Job(found.id)

# follow jobs.previous (or datasets.previous.job if that is unavailable) count times.
def job_up(job, count):
	err_job = job
	for ix in range(count):
		prev = job.params.jobs.get('previous')
		if not prev:
			prev = job.params.datasets.get('previous')
			if prev:
				prev = prev.job
		if not prev:
			raise JobNotFound('Tried to go %d up from %s, but only %d previous jobs available' % (count, err_job, ix,))
		job = prev
	return job

def urd_call_w_tildes(cfg, path, tildes):
	res = call(cfg.urd + '/' + path, server_name='urd', retries=0, quiet=True)
	if tildes:
		up = sum(count for char, count in tildes if char == '^')
		down = sum(count for char, count in tildes if char == '~')
		tildes = down - up
		if tildes:
			key = res.user + '/' + res.build
			timestamps = call(cfg.urd + '/' + key + '/since/0', server_name='urd', retries=0, quiet=True)
			pos = timestamps.index(res.timestamp) + tildes
			if pos < 0 or pos >= len(timestamps):
				return None
			res = call(cfg.urd + '/' + key + '/' + timestamps[pos], server_name='urd', retries=0, quiet=True)
	return res

def name2job(cfg, n):
	n, tildes = split_tildes(n)
	job = _name2job(cfg, n)
	for char, count in tildes:
		if char == '~':
			job = method2job(cfg, job.method, count, start_from=job)
		else:
			job = job_up(job, count)
	return job

def _name2job(cfg, n):
	if n.startswith(':'):
		# resolve through urd
		assert cfg.urd, 'No urd configured'
		a = n[1:].rsplit(':', 1)
		if len(a) == 1:
			raise JobNotFound('looks like a partial :urdlist:[entry] spec')
		entry = a[1] or '-1'
		try:
			entry = int(entry, 10)
		except ValueError:
			pass
		path, tildes = split_tildes(a[0])
		path = path.split('/')
		if len(path) < 3:
			path.insert(0, environ.get('USER', 'NO-USER'))
		if len(path) < 3:
			path.append('latest')
		path = '/'.join(map(url_quote, path))
		try:
			urdres = urd_call_w_tildes(cfg, path, tildes)
		except UrdError as e:
			print(e, file=sys.stderr)
			urdres = None
		if not urdres:
			raise JobNotFound('urd list %r not found' % (a[0],))
		from accelerator.build import JobList
		joblist = JobList(Job(e[1], e[0]) for e in urdres.joblist)
		res = joblist.get(entry)
		if not res:
			raise JobNotFound('%r not found in %s' % (entry, path,))
		return res
	if re.match(r'[^/]+-\d+$', n):
		# Looks like a jobid
		return Job(n)
	m = re.match(r'([^/]+)-LATEST$', n)
	if m:
		# Looks like workdir-LATEST
		wd = m.group(1)
		if wd not in WORKDIRS:
			raise NoSuchWorkdirError('Not a valid workdir: "%s"' % (wd,))
		path = join(WORKDIRS[wd], n)
		try:
			n = readlink(path)
		except OSError as e:
			raise JobNotFound('Failed to read %s: %s' % (path, e,))
		return Job(n)
	if '/' not in n:
		# Must be a method then
		return method2job(cfg, n)
	if exists(join(n, 'setup.json')):
		# Looks like the path to a jobdir
		path, jid = split(realpath(n))
		job = Job(jid)
		if WORKDIRS.get(job.workdir, path) != path:
			print("### Overriding workdir %s to %s" % (job.workdir, path,))
		WORKDIRS[job.workdir] = path
		return job
	raise JobNotFound("Don't know what to do with %r." % (n,))

def split_ds_dir(n):
	"""try to split a path at the jid/ds boundary"""
	orig_n = n
	jid_cand, name = n.split('/', 1)
	if re.match(r'.+-\d+(?:[~^][~^\d]*)?$', jid_cand):
		# looks like a JID, so assume it is. start with ./ to avoid this.
		return jid_cand, name
	name_bits = []
	while '/' in n and not exists(join(n, 'setup.json')):
		n, bit = n.rsplit('/', 1)
		name_bits.append(bit)
	while n.endswith('/') or n.endswith('/.'):
		n, bit = n.rsplit('/', 1)
		name_bits.append(bit)
	if not n:
		raise JobNotFound('No setup.json found in %r' % (orig_n,))
	if not name_bits:
		name_bits = ['default']
	return n, '/'.join(reversed(name_bits))

def name2ds(cfg, n):
	job = name = tildes = None
	if n.startswith(':'):
		colon2 = n.rfind(':', 1)
		if colon2 > 0:
			tailslash = n.find('/', colon2)
			if tailslash > 0:
				name = n[tailslash + 1:]
				n = n[:tailslash]
		job = name2job(cfg, n)
	elif '/' not in n:
		job = name2job(cfg, n)
	else:
		n, name = split_ds_dir(n)
		job = name2job(cfg, n)
		name, tildes = split_tildes(name, allow_empty=True)
	ds = job.dataset(name)
	if tildes:
		def follow(key, motion):
			# follow ds.key count times
			res = ds
			for done in range(count):
				if not getattr(res, key):
					raise DatasetNotFound('Tried to go %d %s from %s, but only %d available (stopped on %s)' % (count, motion, ds, done, res,))
				res = getattr(res, key)
			return res
		for char, count in tildes:
			if char == '~':
				ds = follow('previous', 'back')
			else:
				ds = follow('parent', 'up')
	slices = ds.job.params.slices
	from accelerator import g
	if hasattr(g, 'slices'):
		assert g.slices == slices, "Dataset %s needs %d slices, by we are already using %d slices" % (ds, slices, g.slices)
	else:
		g.slices = slices
	return ds
