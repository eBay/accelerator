############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
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

from __future__ import print_function
from __future__ import division

from random import randint
from collections import OrderedDict, defaultdict
from itertools import combinations
from copy import deepcopy

from accelerator.compat import iteritems

from accelerator import setupfile
from accelerator import deptree
from accelerator.extras import job_params
from accelerator.runner import runners

def find_possible_jobs(db, methods, job):
	method = job['method']
	params = {method: job['params'][method]}
	optset = methods.params2optset(params)
	if not optset:
		return {}
	def inner():
		for uid, job in db.match_exact([(method, 0, optset,)]):
			yield job.id, ()
			return # no depjobs is enough - stop
		for remset in combinations(optset, remcount):
			for uid, job in db.match_complex([(method, 0, optset - set(remset),)]):
				yield job.id, remset
	res = {}
	remcount = 0
	while not res:
		remcount += 1
		if remcount == len(optset):
			break
		for jobid, remset in inner():
			remset = tuple(s.split()[1] for s in remset)
			res[jobid] = remset
	return dict(_job_candidates_options(res))

def _job_candidates_options(candidates):
	for jobid, remset in iteritems(candidates):
		setup = job_params(jobid)
		optdiff = defaultdict(dict)
		for thing in remset:
			section, name = thing.split('-', 1)
			optdiff[section][name] = setup[section][name]
		yield jobid, optdiff

def initialise_jobs(setup, target_WorkSpace, DataBase, Methods, verbose=False):

	# create a DepTree object used to track options and make status
	DepTree = deptree.DepTree(Methods, setup)

	# compare database to deptree
	reqlist = DepTree.get_reqlist()
	for uid, job in DataBase.match_exact(reqlist):
		DepTree.set_link(uid, job)
	DepTree.propagate_make()
	why_build = setup.get('why_build')
	if why_build:
		orig_tree = deepcopy(DepTree.tree)
	DepTree.fill_in_default_options()

	# get list of jobs in execution order
	joblist = DepTree.get_sorted_joblist()
	newjoblist = [x for x in joblist if x['make']]
	num_new_jobs = len(newjoblist)

	if why_build == True or (why_build and num_new_jobs):
		res = OrderedDict()
		DepTree.tree = orig_tree
		joblist = DepTree.get_sorted_joblist()
		for job in joblist:
			if job['make']:
				res[job['method']] = find_possible_jobs(DataBase, Methods, job)
			else:
				res[job['method']] = {job['link']: {}}
		return [], {'why_build': res}

	if num_new_jobs:
		new_jobid_list = target_WorkSpace.allocate_jobs(num_new_jobs)
		# insert new jobids
		for (x,jid) in zip(newjoblist, new_jobid_list):
			x['link'] = jid
		for data in newjoblist:
			method = Methods.db[data['method']]
			new_setup = setupfile.generate(
				caption=setup.caption,
				method=data['method'],
				params=data['params'],
				package=method['package'],
				description=Methods.descriptions[data['method']],
			)
			new_setup.hash = Methods.hash[data['method']][0]
			new_setup.seed = randint(0, 2**63 - 1)
			new_setup.jobid = data['link']
			new_setup.slices = target_WorkSpace.slices
			typing = {}
			for method in data['params']:
				m_typing = Methods.typing[method]
				if m_typing:
					typing[method] = m_typing
			if typing:
				new_setup['_typing'] = typing
			setupfile.save_setup(data['link'], new_setup)
	else:
		new_jobid_list = []

	res = {j['method']: {k: v for k, v in j.items() if k in ('link', 'make', 'total_time')} for j in joblist}
	return new_jobid_list, {'jobs': res}
