from collections import namedtuple


TIMEFMT = '%Y%m%d_%H%M%S'



# user/automata



class DB:
	def __init__(self, path):
		self._fh = None
		self.db = defaultdict(dict)
		for line in open(path):
			...
		self._fh = open(path, 'a')

	@Lock(...)
	def add(self, data):
		#store to file
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
			self._fh.write(...serialise(data)...)

	def latest(self, key):
		db = self.db[key]
		return db[max(db)]




def latest(key):
	# key = user/automata
	data = db.latest(key)
	return data



def add(data):
	# data = {user:string, automata:string, timestamp:string, deplist:list, joblist:JobList,}
	if auth(data['user']):
		result = db.add(data)  # {exists, exists_and_updated, new}
		return result
	else:
		return HTTPError(403)



#(setq indent-tabs-mode t)


if __name__ == "__main__":
	path = argv..
	authfile = argv..
	db = DB(path)
	pass
