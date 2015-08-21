#!/usr/bin/env python2.7

from __future__ import unicode_literals
from glob import glob
from collections import defaultdict
from bottle import route, request, auth_basic, abort
import bottle
from threading import Lock
import json
import re
import time

LOGFILEVERSION = '1'

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
		if os.path.isdir(path):
			files = glob(os.path.join(path, '*/*.urd'))
			print 'init: ', files
			for fn in files:
				with open(fn) as fh:
					for line in fh:
						self._parse(line)
		else:
			print "Creating directory \"%s\"." % (path,)
			os.makedirs(path)
		self._initialised = True

	def _parse(self, line):
		line = line.rstrip('\n').split('|')
		logfileversion, _writets = line[:2]
		assert logfileversion in '01'
		if logfileversion == '0':
			action = 'add'
			line = line[2:]
		elif logfileversion == '1':
			action = line[2]
			line = line[3:]
		else:
			assert "can't happen"
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
		data = DotDict(timestamp=line[0],
			user=user,
			automata=automata,
			deps=json.loads(line[2]),
			joblist=json.loads(line[3]),
			caption=line[4],
		)
		self.add(data)

	def _parse_truncate(self, line):
		timestamp, key = line
		self.truncate(key, timestamp)

	def _validate_timestamp(self, timestamp):
		assert re.match(r"\d{8}( \d\d(\d\d(\d\d)?)?)?", timestamp), timestamp

	def _validate_data(self, data, with_deps=True):
		if with_deps:
			assert set(data) == {'timestamp', 'joblist', 'caption', 'user', 'automata', 'deps',}
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
			for s in json_deps, json_joblist, data.caption, data.user, data.automata, data.timestamp:
				assert '|' not in s, s
			logdata = [json_deps, json_joblist, data.caption,]
		elif action == 'truncate':
			key = data.key
			logdata = []
		else:
			assert "can't happen"
		now = time.strftime("%Y%m%d %H%M%S")
		s = '|'.join([LOGFILEVERSION, now, action, data.timestamp, key,] + logdata)
		print 'serialise', s
		return s

	@locked
	def add(self, data):
		db = self.db['%s/%s' % (data.user, data.automata)]
		if data.timestamp in db:
			new = False
			changed = (db[data.timestamp] != data)
		else:
			new = True
		if new or changed:
			self.log('add', data) # validates, too
			db[data.timestamp] = data
		return 'new' if new else 'updated' if changed else 'unchanged'

	@locked
	def truncate(self, key, timestamp):
		old = self.db[key]
		new = {ts: data for ts, data in old.iteritems() if ts < timestamp}
		self.log('truncate', DotDict(key=key, timestamp=timestamp))
		self.db[key] = new
		return {'count': len(old) - len(new)}

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
		db = self.db[key]
		return {k: v for k, v in db.iteritems() if k > timestamp}

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
		return self.db.keys()


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
	return db.get(user + '/' + automata, timestamp)


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
	return db.keys()


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
