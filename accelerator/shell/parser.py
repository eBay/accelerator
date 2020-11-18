############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
# Modifications copyright (c) 2019 Anders Berkeman                         #
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

# parsing of "job specs", including as part of a dataset name.
# handles jobids, paths and method names.

from __future__ import division, print_function

from os.path import join, exists, realpath, split
import re

from accelerator.job import WORKDIRS
from accelerator.job import Job
from accelerator.error import NoSuchJobError
from accelerator.unixhttp import call

class JobNotFound(NoSuchJobError):
	pass

def name2job(cfg, n):
	if re.match(r'[^/]+-\d+$', n):
		# Looks like a jobid
		return Job(n)
	if '/' not in n:
		# Must be a method then
		found = call(cfg.url + '/find_latest/' + n)
		if not found:
			raise JobNotFound('No (current) job with method %s available.' % (n,))
		return Job(found.id)
	if exists(join(n, 'setup.json')):
		# Looks like the path to a jobdir
		path, jid = split(realpath(n))
		job = Job(jid)
		if WORKDIRS.get(job.workdir, path) != path:
			print("### Overriding workdir %s to %s" % (job.workdir, path,))
		WORKDIRS[job.workdir] = path
		return job
	raise JobNotFound("Don't know what to do with %r." % (n,))

def name2ds(cfg, n):
	try:
		job = name2job(cfg, n)
		name = None
	except JobNotFound:
		if '/' not in n:
			raise
		job = None
	if not job:
		n, name = n.rsplit('/', 1)
		job = name2job(cfg, n)
	ds = job.dataset(name)
	slices = ds.job.params.slices
	from accelerator import g
	if hasattr(g, 'slices'):
		assert g.slices == slices, "Dataset %s needs %d slices, by we are already using %d slices" % (ds, slices, g.slices)
	else:
		g.slices = slices
	return ds
