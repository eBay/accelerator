#!/usr/bin/env python2.7

from __future__ import unicode_literals
from collections import defaultdict
from bottle import route, request, abort, auth_basic
import bottle
from threading import Lock
import json
import re

TIMEFMT = '%Y%m%d_%H%M%S'

lock = Lock()

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
		self._fh = None
		self.db = defaultdict(dict)
		if os.path.isfile(path):
			for line in open(path):
				key, ts, data = self._parse(line)
				self.db[key][ts] = data
		else:
			print "Creating \"%s\." % (path,)
		self._fh = open(path, 'a')

	def _parse(self, line):
		line = line.rstrip('\n').split('|')
		print line
		key = line[1]
		user, automata = key.split('/')
		data = DotDict(timestamp=line[0],
			       user=user,
			       automata=automata,
			       deps=json.loads(line[2]),
			       joblist=json.loads(line[3]),
			       caption=line[4],
		       )
		self._validate_timestamp(data.timestamp)
		return key, data.timestamp, data

	def _validate_timestamp(self, timestamp):
		assert re.match(r"\d{8}( \d\d(\d\d(\d\d)?)?)?", timestamp), timestamp

	def _validate_data(self, data, with_deps=True):
		if with_deps:
			assert isinstance(data.deps, dict)
			for v in data.deps.itervalues():
				assert isinstance(v, dict)
				self._validate_data(DotDict(v), False)
		else:
			assert 'deps' not in data
		assert joblistlike(data.joblist), data.joblist
		assert data.joblist
		assert isinstance(data.user, unicode)
		assert isinstance(data.automata, unicode)
		assert isinstance(data.caption, unicode)
		self._validate_timestamp(data.timestamp)

	def _serialise(self, data):
		self._validate_data(data)
		json_deps = json.dumps(data.deps)
		json_joblist = json.dumps(data.joblist)
		for s in json_deps, json_joblist, data.caption, data.user, data.automata, data.timestamp:
			assert '|' not in s, s
		s = '|'.join([data.timestamp, "%s/%s" % (data.user, data.automata), json_deps, json_joblist, data.caption,])
		print 'serialise', s
		return s

	def add(self, data):
		with lock:
			db = self.db['%s/%s' % (data.user, data.automata)]
			print db
			if data.timestamp in db:
				new = False
				changed = (db[data.timestamp] != data)
			else:
				new = True
			if new or changed:
				self.log(data) # validates, too
				db[data.timestamp] = data
			return 'new' if new else 'updated' if changed else 'unchanged'

	def log(self, data):
		if self._fh:
			self._fh.write(self._serialise(data) + '\n')
			self._fh.flush()

	def latest(self, key):
		if key in self.db:
			db = self.db[key]
			return db[max(db)]


def auth(user, passphrase):
	return authdict.get(user) == passphrase


@route('/latest/<user>/<automata>')
def latest(user, automata):
	return db.latest(user + '/' + automata)



@route('/add', method='POST')
@auth_basic(auth)
def add():
	data = DotDict(request.json or {})
	result = db.add(data)
	return result


#(setq indent-tabs-mode t)

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
	bottle.run(host='localhost', port=args.port, debug=False, reloader=True, quiet=False)
