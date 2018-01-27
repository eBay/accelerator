#!/usr/bin/env python2.7

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

from __future__ import unicode_literals
from glob import glob
from collections import defaultdict
from bottle import route, request, auth_basic, abort
import bottle
from threading import Lock
import json
import re
from datetime import datetime
import operator

LOGFILEVERSION = '2'

lock = Lock()

def locked(func):
	def inner(*a, **kw):
		with lock:
			return func(*a, **kw)
	return inner

class DotDict(dict):
	"""Like a dict, but with d.foo as well as d['foo'].
	d.foo returns '' for unset values.
	The normal dict.f (get, items, ...) still return the functions.
	"""
	__setattr__ = dict.__setitem__
	__delattr__ = dict.__delitem__
	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		return self[name]


def joblistlike(jl):
	assert isinstance(jl, list)
	for v in jl:
		assert isinstance(v, (list, tuple)), v
		assert len(v) == 2, v
		for s in v:
			assert isinstance(s, unicode), s
	return True


class DB:
	def __init__(self, path):
		self._initialised = False
		self.path = path
		self.db = defaultdict(dict)
		self.ghost_db = defaultdict(lambda: defaultdict(list))
		if os.path.isdir(path):
			files = glob(os.path.join(path, '*/*.urd'))
			print 'init: ', files
			self._parsed = {}
			for fn in files:
				with open(fn) as fh:
					for line in fh:
						self._parse(line)
			self._playback_parsed()
		else:
			print "Creating directory \"%s\"." % (path,)
			os.makedirs(path)
		self._lasttime = None
		self._initialised = True

	def _parse(self, line):
		line = line.rstrip('\n').split('|')
		logfileversion, writets = line[:2]
		assert logfileversion == '2'
		assert writets not in self._parsed
		self._parsed[writets] = line[2:]

	def _playback_parsed(self):
		for _writets, line in sorted(self._parsed.iteritems()):
			action = line.pop(0)
			assert action in ('add', 'truncate',)
			if action == 'add':
				self._parse_add(line)
			elif action == 'truncate':
				self._parse_truncate(line)
			else:
				assert "can't happen"

	def _parse_add(self, line):
		key = line[1]
		user, automata = key.split('/')
		flags = line[4].split(',') if line[4] else []
		data = DotDict(timestamp=line[0],
			user=user,
			automata=automata,
			deps=json.loads(line[2]),
			joblist=json.loads(line[3]),
			flags=flags,
			caption=line[5],
		)
		self.add(data)

	def _parse_truncate(self, line):
		timestamp, key = line
		self.truncate(key, timestamp)

	def _validate_timestamp(self, timestamp):
		assert re.match(r"\d{8}( \d\d(\d\d(\d\d)?)?)?", timestamp), timestamp

	def _validate_data(self, data, with_deps=True):
		if with_deps:
			assert set(data) == {'timestamp', 'joblist', 'caption', 'user', 'automata', 'deps', 'flags',}
			assert isinstance(data.user, unicode)
			assert isinstance(data.automata, unicode)
			assert isinstance(data.deps, dict)
			for v in data.deps.itervalues():
				assert isinstance(v, dict)
				self._validate_data(DotDict(v), False)
		else:
			assert set(data) == {'timestamp', 'joblist', 'caption',}
		assert joblistlike(data.joblist), data.joblist
		assert data.joblist
		assert isinstance(data.caption, unicode)
		self._validate_timestamp(data.timestamp)

	def _serialise(self, action, data):
		if action == 'add':
			self._validate_data(data)
			json_deps = json.dumps(data.deps)
			json_joblist = json.dumps(data.joblist)
			key = '%s/%s' % (data.user, data.automata,)
			flags = ','.join(data.flags)
			for s in json_deps, json_joblist, data.caption, data.user, data.automata, data.timestamp, flags:
				assert '|' not in s, s
			logdata = [json_deps, json_joblist, flags, data.caption,]
		elif action == 'truncate':
			key = data.key
			logdata = []
		else:
			assert "can't happen"
		while True: # paranoia
			now = datetime.utcnow().strftime("%Y%m%d %H%M%S.%f")
			if now != self._lasttime: break
		self._lasttime = now
		s = '|'.join([LOGFILEVERSION, now, action, data.timestamp, key,] + logdata)
		print 'serialise', s
		return s

	def _is_ghost(self, data):
		for key, data in data.deps.iteritems():
			db = self.db[key]
			ts = data['timestamp']
			if ts not in db:
				return True
			for k, v in data.iteritems():
				if db[ts].get(k) != v:
					return True
		return False

	@locked
	def add(self, data):
		key = '%s/%s' % (data.user, data.automata)
		new = False
		changed = False
		ghosted = 0
		is_ghost = self._is_ghost(data)
		if is_ghost:
			db = self.ghost_db[key]
			if data.timestamp in db:
				new = False
				changed = (data not in db[data.timestamp])
			else:
				new = True
		else:
			db = self.db[key]
			if data.timestamp in db:
				new = False
				changed = (db[data.timestamp] != data)
			else:
				new = True
		flags = data.get('flags', [])
		assert flags in ([], ['update']), 'Unknown flags: %r' % (flags,)
		if changed and 'update' not in flags:
			assert self._initialised, 'Log updates without update flag: %r' % (data,)
			bottle.response.status = 409
			return {'error': 'would update'}
		if new or changed:
			data.flags = flags
			self.log('add', data) # validates, too
			del data.flags
			if is_ghost:
				db[data.timestamp].append(data)
			else:
				if changed:
					ghost_data = db[data.timestamp]
					self.ghost_db[key][data.timestamp].append(ghost_data)
				db[data.timestamp] = data
				if changed:
					ghosted = self._update_ghosts()
		res = dict(new=new, changed=changed, is_ghost=is_ghost)
		if changed:
			res['deps'] = ghosted
		return res

	def _update_ghosts(self):
		def inner():
			count = 0
			for key, db in self.db.iteritems():
				for ts, data in sorted(db.items()):
					if self._is_ghost(data):
						count += 1
						del db[ts]
						self.ghost_db[key][ts].append(data)
			return count
		res = 0
		while True:
			count = inner()
			if not count:
				break
			res += count
		return res

	@locked
	def truncate(self, key, timestamp):
		old = self.db[key]
		new = {}
		ghost = {}
		for ts, data in old.iteritems():
			if ts < timestamp:
				new[ts] = data
			else:
				ghost[ts] = data
		self.log('truncate', DotDict(key=key, timestamp=timestamp))
		self.db[key] = new
		ghost_db = self.ghost_db[key]
		for ts, data in ghost.iteritems():
			ghost_db[ts].append(data)
		if ghost:
			deps = self._update_ghosts()
		else:
			deps = 0
		return {'count': len(ghost), 'deps': deps}

	def log(self, action, data):
		if self._initialised:
			if action == 'truncate':
				user, automata = data.key.split('/')
			else:
				user, automata = data.user, data.automata
			assert '/' not in user
			assert '/' not in automata
			path = os.path.join(self.path, user)
			if not os.path.isdir(path):
				os.makedirs(path)
			with open(os.path.join(path, automata + '.urd'), 'a') as fh:
				fh.write(self._serialise(action, data) + '\n')

	@locked
	def get(self, key, timestamp):
		db = self.db[key]
		return db.get(timestamp)

	@locked
	def since(self, key, timestamp):
		return sorted(k for k in self.db[key] if k > timestamp)

	@locked
	def limited_endpoint(self, key, timestamp, cmpfunc, minmaxfunc):
		db = self.db[key]
		try:
			k = minmaxfunc(k for k in db if cmpfunc(k, timestamp))
		except ValueError: # empty sequence
			return None
		return db[k]

	@locked
	def latest(self, key):
		db = self.db[key]
		if db:
			return db[max(db)]

	@locked
	def first(self, key):
		db = self.db[key]
		if db:
			return db[min(db)]

	def keys(self):
		return filter(self.db.get, self.db)


def auth(user, passphrase):
	return authdict.get(user) == passphrase

@route('/<user>/<automata>/since/<timestamp>')
def since(user, automata, timestamp):
	return db.since(user + '/' + automata, timestamp)

@route('/<user>/<automata>/latest')
def latest(user, automata):
	return db.latest(user + '/' + automata)

@route('/<user>/<automata>/first')
def first(user, automata):
	return db.first(user + '/' + automata)

@route('/<user>/<automata>/<timestamp>')
def single(user, automata, timestamp):
	key = user + '/' + automata
	if len(timestamp) > 1 and timestamp[0] in '<>':
		if timestamp[0] == '<':
			minmaxfunc = max
			if timestamp[1] == '=':
				# we want 20140410 <= 201404 to be True
				def cmpfunc(k, ts):
					return k <= ts or k.startswith(ts)
				timestamp = timestamp[2:]
			else:
				cmpfunc = operator.lt
				timestamp = timestamp[1:]
		else:
			minmaxfunc = min
			if timestamp[1] == '=':
				cmpfunc = operator.ge
				timestamp = timestamp[2:]
			else:
				cmpfunc = operator.gt
				timestamp = timestamp[1:]
		return db.limited_endpoint(key, timestamp, cmpfunc, minmaxfunc)
	else:
		return db.get(key, timestamp)


@route('/add', method='POST')
@auth_basic(auth)
def add():
	data = DotDict(request.json or {})
	if data.user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")
	result = db.add(data)
	return result


@route('/truncate/<user>/<automata>/<timestamp>', method='POST')
@auth_basic(auth)
def truncate(user, automata, timestamp):
	if user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")
	return db.truncate(user + '/' + automata, timestamp)


#(setq indent-tabs-mode t)
@route('/list')
def slash_list():
	return sorted(db.keys())


def readauth(filename):
	d = {}
	with open(filename) as fh:
		for line in fh:
			line = line.strip()
			if not line or line.startswith('#'):  continue
			line = line.split(':')
			assert len(line) == 2, "Parse error in \"" + filename + "\" " +  ':'.join(line)
			d[line[0]] = line[1]
	return d


def jsonify(callback):
	def func(*a, **kw):
		res = callback(*a, **kw)
		if isinstance(res, (bottle.BaseResponse, bottle.BottleException)):
			return res
		return json.dumps(res)
	return func


if __name__ == "__main__":
	from argparse import ArgumentParser
	import os.path
	parser = ArgumentParser(description='pelle')
	parser.add_argument('--port', type=int, default=8080, help='server port')
	parser.add_argument('--path', type=str, default='./', help='database directory')
	args = parser.parse_args()
	print '-'*79
	print args
	print
	authdict = readauth(os.path.join(args.path, 'passwd'))
	db = DB(os.path.join(args.path, 'database'))

	bottle.install(jsonify)
	bottle.run(host='localhost', port=args.port, debug=False, reloader=False, quiet=False)
