############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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

description = r'''
Verify that all output, including a final exception, is saved
'''

from accelerator import subjobs, JobError
from time import sleep

options = dict(
	inner=False,
)


def synthesis():
	lines = ['printing a bunch of lines, this is line %d.' % (n,) for n in range(150)]
	if options.inner:
		for s in lines:
			print(s)
		raise Exception('this is an exception, but nothing went wrong')
	else:
		try:
			subjobs.build('test_output_on_error', inner=True)
		except JobError as e:
			job = e.job
		else:
			raise Exception("test_output_on_error with inner=True didn't fail")
		# give the iowrapper some time to finish
		for attempt in range(25):
			got_lines = job.output().split('\n')
			if got_lines[:len(lines)] == lines:
				for line in got_lines:
					if line == 'Exception: this is an exception, but nothing went wrong':
						return
			# not yet, wait a little (total of 30s)
			if attempt > 1:
				print('Output from %s has not appeared yet, waiting more (%d).' % (job, attempt,))
			sleep(attempt / 10.0)
		raise Exception('Not all output from %s was saved in OUTPUT' % (job,))
