from collections import defaultdict
from bottle import route, run, request, abort, auth_basic
from threading import Lock


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

# user/automata



class DB:
	def __init__(self, path):
		self._fh = None
		self.db = defaultdict(dict)
		for line in open(path):
			key, ts, data = self._parse(line)
			self.db[key][ts] = data
		self._fh = open(path, 'a')

	def _parse(self, line):
		return 'a', 'b', 'c'


	def _serialise(self, data):
		return 'rs232'

	def add(self, data):
		with lock:
			db = self.db['%s/%s' % (data.user, data.automata)]
			if data.timestamp in db:
				new = False
				changed = db[data.timestamp] == data
			else:
				new = True
			db[data.timestamp] = data
			if new or changed:
				self.log(data)
			return 'new' if new else 'updated' if changed else 'unchanged'

	def log(self, data):
		if self._fh:
			self._fh.write(self._serialise(data))

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

	run(host='localhost', port=args.port, debug=False, quiet=False)
