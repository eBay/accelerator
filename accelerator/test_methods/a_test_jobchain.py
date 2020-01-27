############################################################################
#                                                                          #
# Copyright (c) 2020 Carl Drougge                                          #
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
from __future__ import unicode_literals

description = r'''
Test Job.chain
'''

from accelerator import subjobs

options = dict(
	pos=-1,
)

jobs = ('previous', 'other',)

def synthesis(job):
	if options.pos == -1:
		previous = job
		other = None
		j2p = {job: -1}
		alles = [job]
		for pos in range(5):
			previous = subjobs.build('test_jobchain', pos=pos, previous=previous, other=other)
			alles.append(previous)
			j2p[previous] = pos
			if pos == 2:
				other = alles[1]
			else:
				other = None
		assert alles == previous.chain()
		assert list(reversed(alles)) == previous.chain(reverse=True)
		def chk(tip, first, **kw):
			c = alles[tip].chain(**kw)
			c = [j2p[j] for j in c]
			assert c == list(range(first, tip)), (tip, first, kw)
		chk(5, -1)
		chk(5, 3, stop_job=alles[3])
		chk(5, 2, length=3)
		chk(4, 1, length=3)
		chk(5, 3, stop_job={alles[4]: 'previous'})
		chk(5, 1, stop_job={alles[4]: 'other'})
		chk(4, 1, stop_job={alles[4]: 'other'})
		chk(5, 2, stop_job={alles[4]: 'other'}, length=3)
		chk(5, 1, stop_job={alles[4]: 'other'}, length=5)
		assert alles[2].chain(length=0) == []
		assert job.chain() == [job]
		assert job.chain(stop_job=job) == []
