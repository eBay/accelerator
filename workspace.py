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

from __future__ import print_function
from __future__ import division

import os

class WorkSpace:
	""" Handle all access to a single "physical" workdir. """

	def __init__(self, name, path, slices, writeable=False):
		""" name is workspace name, e.g. "churn" or "redlaser".  all jobids are prefixed by name.
		    path is simply where all jobids are put.
		    slices is number of slices for workdir, for example 12. """
		self.name   = name
		self.path   = path
		self.slices = int(slices)
		self.valid_jobids = set()
		self.known_jobids = set()
		self.recent_bad_jobids = set()
		self.ok = self._check_slices(writeable)


	def make_writeable(self):
		self._check_slices(True)


	def _check_slices(self, writeable):
		""" verify or write workdir specific slices file """
		filename = os.path.join(self.path, "%s-slices.conf" % (self.name,))
		ok = True
		try:
			with open(filename) as F:
				file_slices = int(F.read())
			if self.slices != file_slices:
				print("WORKSPACE:  ERROR, workdir has %d slices, but config file stipulates %d!" % (file_slices, self.slices))
				print("WORKSPACE:  Consequence:  ignore config file, use SLICES=%d." % (file_slices))
				self.slices = file_slices
		except Exception:
			if writeable:
				print("WORKSPACE:  create %s-slices.conf in %s." % (self.name, self.path))
				with open(filename, 'w') as F:
					F.write(str(self.slices)+'\n')
			else:
				print("WORKSPACE:  not a workdir \"%s\" at \"%s\"" % (self.name, self.path,))
				ok = False
		return ok


	def get_slices(self):
		""" return number of slices in workdir """
		return self.slices


	def get_path(self):
		return self.path


	def add_single_jobid(self, jobid):
		self.valid_jobids.add(jobid)


	def update(self, parallelism=4):
		"""find all new jobids on disk"""
		from os.path import exists, join
		from jobid import dirnamematcher
		from safe_pool import Pool
		from itertools import compress
		cand = set(filter(dirnamematcher(self.name), os.listdir(self.path)))
		bad = self.known_jobids - cand
		for jid in bad:
			self.known_jobids.discard(jid)
			self.valid_jobids.discard(jid)
		# @@TODO: Fix races for remote daemons:
		# Anything which was bad last time but had recently been touched needs to be rechecked:
		#     new = list(cand - (self.known_jobids - self.recent_bad_jobids))
		#     cutoff = time() - 64 # hopefully avoid races if we're on a network filsystem
		#     recent = set(j for j in new if mtime(j( > cutoff)
		#     self.recent_bad_jobids = recent - good
		# Also anything where the mtime has changed needs to be rechecked, at least
		# in DataBase.
		# So we need to keep mtimes and look at them, plus the above recent_bad_jobids
		new = list(cand - self.known_jobids)
		if new:
			pool = Pool(processes=parallelism)
			pathv = [join(self.path, j, 'post.json') for j in new]
			good = set(compress(new, pool.map(exists, pathv, chunksize=64)))
			self.valid_jobids.update(good)
			self.known_jobids.update(new)
			pool.close()


	def allocate_jobs(self, num_jobs):
		""" create num_jobs directories in self.path with jobid-compliant naming """
		from jobid import create
		highest = self._get_highest_jobnumber()
#		print('WORKSPACE:  Highest jobid is', highest)
		jobidv = [create(self.name, highest + 1 + x) for x in range(num_jobs)]
		for jobid in jobidv:
			fullpath = os.path.join(self.path, jobid)
			print("WORKSPACE:  Allocate_job \"%s\"" % fullpath)
			self.known_jobids.add(jobid)
			os.mkdir(fullpath)
		return jobidv


	def _get_highest_jobnumber(self):
		""" get highest current jobid number """
		if self.known_jobids:
			from jobid import Jobid
			return max(Jobid(jid).number for jid in self.known_jobids)
		else:
			return -1
