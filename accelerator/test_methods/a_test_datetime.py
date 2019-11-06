############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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
Test the datetime types in options.
'''

from datetime import datetime, date, time, timedelta

from accelerator import subjobs
from accelerator.extras import DotDict

options = dict(
	datetime=datetime,
	time=time,
	date=date,
	timedelta=timedelta,
	inner=False,
)

def synthesis():
	if options.inner:
		res = DotDict()
		res.datetime = options.datetime + options.timedelta
		res.time = options.time.replace(minute=0)
		res.date = options.date.replace(month=1)
		return res
	else:
		opts = dict(
			datetime=datetime(2019, 11, 6, 17, 37, 2, 987654),
			time=time(17, 37, 2, 987654),
			date=date(2019, 11, 6),
			timedelta=timedelta(microseconds=987654),
			inner=True,
		)
		jid = subjobs.build('test_datetime', options=opts)
		res = jid.load()
		assert res.datetime == datetime(2019, 11, 6, 17, 37, 3, 975308)
		assert res.time == time(17, 0, 2, 987654)
		assert res.date == date(2019, 1, 6)
