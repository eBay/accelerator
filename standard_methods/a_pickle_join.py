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

from __future__ import division
from __future__ import absolute_import

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
