############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Carl Drougge                            #
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

# This used to contain all the dataset chaining stuff, but that now lives in
# the dataset module. Usually that's what you want, but sometimes you want
# to chain by job arguments, and that's still here.

from accelerator.compat import str_types

from accelerator.extras import job_params


def jobchain(length=-1, reverse=False, tip_jobid=None, stop_jobid=None):
	"""Look backwards over "previous" jobid from tip_jobid
	(default current job) and return length (or all) latest
	jobids (includes tip only if explicitly specified).
	Return up to but not including stop_jobid.
	stop_jobid can be a {job: optname} dict, resolving jobid "optname" from job"""

	if not stop_jobid:
		stop_jobid = ()
	elif isinstance(stop_jobid, str_types):
		stop_jobid = (stop_jobid,)
	elif isinstance(stop_jobid, dict):
		stuff = stop_jobid.items()
		stop_jobid = set()
		for parent, var in stuff:
			jobid = job_params(parent).jobids.get(var)
			assert jobid, "%s not set in %s" % (var, parent)
			stop_jobid.add(jobid)
	assert isinstance(stop_jobid, (list, tuple, set,)), "stop_jobid must be str, dict or set-ish"

	if length == 0:
		return []

	jobid = tip_jobid
	if tip_jobid:
		l_jobid = [tip_jobid]
		length -= 1
	else:
		l_jobid = []
	while length:
		jobid = job_params(jobid).jobids.get('previous')
		if not jobid:
			break
		if jobid in stop_jobid:
			break
		l_jobid.append(jobid)
		length -= 1
	if not reverse:
		l_jobid.reverse()
	return l_jobid
