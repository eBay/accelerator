from collections import defaultdict
from threading import Lock


TIMEFMT = '%Y%m%d_%H%M%S'

lock = Lock()


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


def latest(key):
	# key = user/automata
	data = db.latest(key)
	return data



def add(data):
	# data = {user:string, automata:string, timestamp:string, deplist:list, joblist:JobList,}
	data = request.json or {}
	if auth(data.get('user'), request.query.get('passphrase')):
		result = db.add(data)
		return result
	else:
		return HTTPError(403)



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
