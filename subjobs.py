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

import g
from automata_common import JobList
from status import status

_a = None
_record = {}

jobs = JobList()

def build(method, options={}, datasets={}, jobids={}, name=None, caption=None):
	"""Just like urd.build, but for making subjobs"""
	
	global _a
	assert g.running != 'analysis', "Analysis is not allowed to make subjobs"
	assert g.subjob_cookie, "Can't build subjobs: out of cookies"
	if not _a:
		from automata_common import Automata
		_a = Automata(g.daemon_url, subjob_cookie=g.subjob_cookie)
		_a.update_method_deps()
		_a.record[None] = _a.jobs = jobs
	def run():
		return _a.call_method(method, options={method: options}, datasets={method: datasets}, jobids={method: jobids}, record_as=name, caption=caption)
	if name or caption:
		msg = 'Building subjob %s' % (name or method,)
		if caption:
			msg += ' "%s"' % (caption,)
		with status(msg):
			jid = run()
	else:
		jid = run()
	for d in _a.job_retur.jobs.values():
		if d.link not in _record:
			_record[d.link] = bool(d.make)
	return jid
