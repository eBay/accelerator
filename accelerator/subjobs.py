############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

from accelerator import g
from accelerator.build import Automata, JobList
from accelerator.error import ServerError, JobError
from accelerator.statmsg import status
from accelerator.compat import getarglist

_a = None
_record = {}
_bad_kws = set()

jobs = JobList()

def build(method, options={}, datasets={}, jobs={}, name=None, caption=None, **kw):
	"""Just like urd.build, but for making subjobs"""

	global _a, _bad_kws
	assert g.running != 'analysis', "Analysis is not allowed to make subjobs"
	assert g.subjob_cookie, "Can't build subjobs: out of cookies"
	if not _a:
		_a = Automata(g.server_url, subjob_cookie=g.subjob_cookie)
		_a.update_method_info()
		_a.record[None] = _a.jobs = globals()['jobs']
		_bad_kws = set(getarglist(_a.call_method))
	bad_kws = _bad_kws & set(kw)
	if bad_kws:
		raise Exception('subjobs.build does not accept these keywords: %r' % (bad_kws,))
	def run():
		return _a.call_method(method, options=options, datasets=datasets, jobs=jobs, record_as=name, caption=caption, **kw)
	try:
		if name or caption:
			msg = 'Building subjob %s' % (name or method,)
			if caption:
				msg += ' "%s"' % (caption,)
			with status(msg):
				jid = run()
		else:
			jid = run()
	except ServerError as e:
		raise ServerError(e.args[0])
	except JobError as e:
		raise JobError(e.jobid, e.method, e.status)
	for d in _a.job_retur.jobs.values():
		if d.link not in _record:
			_record[d.link] = bool(d.make)
	return jid
