from __future__ import division

from extras import OptionEnum, JobWithFile
from blob import load, save

FlavourEnum = OptionEnum('dict dictofset')

options = {
	'pickles'   : [JobWithFile],
	'flavour'   : FlavourEnum.dict,
	'resultname': 'result',
}

def upd_dict(res, tmp):
	res.update(tmp)

def upd_dictofset(res, tmp):
	for k, v in tmp.iteritems():
		res.setdefault(k, set()).update(v)

def one_slice(sliceno):
	first = True
	updater = globals()['upd_' + options.flavour]
	for pickle in options.pickles:
		tmp = load(pickle, sliceno=sliceno)
		if first:
			res = tmp
			first = False
		else:
			updater(res, tmp)
	save(res, options.resultname, sliceno=sliceno)

def analysis(sliceno):
	if options.pickles[0].sliced:
		one_slice(sliceno)

def synthesis():
	if not options.pickles[0].sliced:
		one_slice(None)
