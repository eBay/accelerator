from __future__ import unicode_literals
from collections import defaultdict
from bottle import route, request, abort, auth_basic
import bottle
from threading import Lock
import json

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
        return self.get(name, '')





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
		data = DotDict(dict(timestamp=line[0],
				    user=user,
				    automata=automata,
				    deplist=json.loads(line[2]),
				    joblist=json.loads(line[3]),
			    ))
		return key, data.timestamp, data


	def _serialise(self, data):
		s = '|'.join([data.timestamp, "%s/%s" % (data.user, data.automata), json.dumps(data.deplist), json.dumps(data.joblist)])
		print 'serialise', s
		return s

	def add(self, data):
		with lock:
			db = self.db['%s/%s' % (data.user, data.automata)]
			if data.timestamp in db:
				new = False
				changed = db[data.timestamp] != data
			else:
				new = True
			db[data.timestamp] = data
			if new or changed:
				self.log(data)
			return 'new' if new else 'updated' if changed else 'unchanged'

	def log(self, data):
		if self._fh:
			self._fh.write(self._serialise(data) + '\n')
			self._fh.flush()

	def latest(self, key):
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
	# data = {user:string, automata:string, timestamp:string, deplist:list, joblist:JobList,}
	data = DotDict(request.json or {})
	result = db.add(data)
	return result


#(setq indent-tabs-mode t)

def readauth(filename):
	d = {}
	with open(filename) as fh:
		for line in fh:
			if line.startswith('#'):  continue
			line = line.rstrip('\n').split(':')
			if line and len(line) == 2:
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
