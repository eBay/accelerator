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
Verify that all column types come out correctly in csvexport.
'''

from datetime import date, time, datetime

from accelerator import subjobs
from accelerator.gzwrite import _convfuncs
from accelerator.compat import PY2

def synthesis(job):
	dw = job.datasetwriter()
	todo = {
		'ascii',
		'bits32',
		'bits64',
		'bool',
		'bytes',
		'complex32',
		'complex64',
		'date',
		'datetime',
		'float32',
		'float64',
		'int32',
		'int64',
		'json',
		'number',
		'pickle',
		'time',
		'unicode',
	}
	check = {n for n in _convfuncs if not n.startswith('parsed:')}
	assert todo == check, 'Missing/extra column types: %r %r' % (check - todo, todo - check,)
	no_none = {'bits32', 'bits64', 'json'}
	for name in sorted(todo):
		if PY2 and name == 'pickle':
			# pickle columns are not supported on python 2.
			t = 'ascii'
		else:
			t = name
		dw.add(name, t, none_support=name not in no_none)
	write = dw.get_split_write()
	write(
		'a', 0xffffffff, 0xfedcba9876543210, True, b'hello',
		42, 1e100+0.00000000000000001j,
		date(2020, 6, 23), datetime(2020, 6, 23, 12, 13, 14),
		1.0, float('-inf'), -10, -20,
		{'json': True}, 0xfedcba9876543210beef,
		'...' if PY2 else 1+2j, time(12, 13, 14), 'bl\xe5',
	)
	d = {}
	d['recursion'] = d
	write(
		'b', 0, 0, False, b'bye',
		2-3j, -7,
		date(1868,  1,  3), datetime(1868,  1,  3, 13, 14, 5),
		float('inf'), float('nan'), 0, 0,
		[False, None], 42.18,
		'...' if PY2 else d, time(13, 14, 5), 'bl\xe4',
	)
	write(
		None, 72, 64, None, None,
		None, None,
		None, None,
		None, None, None, None,
		None, None,
		None, None, None,
	)
	ds = dw.finish()
	sep = '\x1e'
	exp = subjobs.build('csvexport', filename='test.csv', separator=sep, source=ds)
	with exp.open('test.csv', 'r', encoding='utf-8') as fh:
		def expect(*a):
			want = sep.join(a) + '\n'
			got = next(fh)
			assert want == got, 'wanted %r, got %r' % (want, got,)
		expect(*sorted(todo))
		expect(
			'a', '4294967295', '18364758544493064720', 'True', 'hello',
			'(42+0j)', '(1e+100+1e-17j)',
			'2020-06-23', '2020-06-23 12:13:14',
			'1.0', '-inf', '-10', '-20',
			'{"json": true}', '1203552815971897489538799',
			'...' if PY2 else '(1+2j)', '12:13:14', 'bl\xe5',
		)
		expect(
			'b', '0', '0', 'False', 'bye',
			'(2-3j)', '(-7+0j)',
			'1868-01-03', '1868-01-03 13:14:05',
			'inf', 'nan', '0', '0',
			'[false, null]', '42.18',
			'...' if PY2 else "{'recursion': {...}}", '13:14:05', 'bl\xe4',
		)
		expect(
			'None', '72', '64', 'None', 'None',
			'None', 'None',
			'None', 'None',
			'None', 'None', 'None', 'None',
			'null', 'None',
			'None', 'None', 'None',
		)
