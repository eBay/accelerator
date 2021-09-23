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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
Verify that dataset_type gets .min and .max right.
'''

from accelerator import subjobs
from accelerator.dataset import DatasetWriter

from itertools import combinations, chain
from datetime import datetime
from math import isnan

inf = float('inf')
nan = float('nan')

groupdata = {
	'small int': {
		'zero': (['0', '0', '0', '0'], 0, 0),
		'small positive': (['5', '1', '5', '3'], 1, 5),
		'small negative': (['-5', '-2', '-3'], -5, -2),
	},
	'i32': {
		'small signed': (['5', '-1', '-2', '3'], -2, 5),
		'medium signed': (['729', '9428', '-532', '47', '-334'], -532, 9428),
		'i32': (['-2147483646', '2147483647', '-2147483647'], -2147483647, 2147483647),
	},
	'u32': {
		'u32': (['2147483647', '2147483648', '4294967200'], 2147483647, 4294967200),
	},
	'i64': {
		'fitsinfloat': (['400', '3002399751580330', '-8589934595'], -8589934595, 3002399751580330),
		'i64': (['0', '999999999999', '-9223372036854775807'], -9223372036854775807, 999999999999),
	},
	'u64': {
		'u64': (['18446744073709551615', '5555555', '9393939393'], 5555555, 18446744073709551615),
	},
	'inty floats': {
		'inty floats': (['18.0', '-3.000', '5.000000000000000000000000000', '.0', '99.'], -3, 99),
	},
	'floatint': { # throwing away the decimals
		'floatint': (['-3.999', '-4', '-18.9', '.9'], -18, 0),
		'big floatint': (['-555', '-9999999999', '1.5e6'], -2147483647, 1500000),
	},
	'float32': {
		'float32': (['1.5', '2.5', '0.5', '1.0'], 0.5, 2.5),
		'float32 neg': (['-1.5', '-2.5', '-0.5', '-1'], -2.5, -0.5),
	},
	'float32 only': { # minmax shouldn't use more precision than the stored type
		'float32 rounding': (['1.5', '-2.50000004', '0.5'], -2.5, 1.5),
	},
	'float64': {
		'small float': (['0.000004', '1e-303', '5e-55'], 1e-303, 0.000004),
		'big float': (['99999999999', '1e303', '5e55'], 99999999999, 1e303),
	},
	'infinity': {
		'infinity': (['0.7', 'inf', '-0.5', '5'], -0.5, inf),
		'neg inifinity': (['0.01', '-inf', '0.5'], -inf, 0.5),
		'only inifinity': (['inf', 'inf'], inf, inf),
	},
	'nan': {
		'one nan': (['1', 'nan', '-2', '2'], -2, 2),
		'just nan': (['nan', 'nan'], nan, nan),
		'some nan': (['0', 'nan', '2', '3', '4', 'nan', '-1', '-2', 'nan'], -2, 4),
	},
	'huge': {
		'huge': (['10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069373', '10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069376', '10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069370', '10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069378'], 10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069370, 10715086071862673209484250490600018105614048117055336074437503883703510511249361224931983788156958581275946729175531468251871452856923140435984577574698574803934567774824230985421074605062371141877954182153046474983581941267398767559165543946077062914571196477686542167660429831652624386837205668069378),
		'huge with neg': (['33', '-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998', '-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999989'], -9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998, 33),
	},
}

datetime_values = (
	datetime(2021, 6, 2, 10, 46, 29, 123),
	datetime(2021, 6, 2, 10, 46, 29, 123456),
	datetime(2021, 1, 1, 3, 0, 0, 1),
	datetime(1900, 10, 20, 1, 1, 1, 1),
)

def mk_datetime(name, mk):
	lst = [mk(v) for v in datetime_values]
	if name != 'date':
		groupdata[name + ' micro'] = {
			name + ' micro': ([str(v) for v in lst], min(lst), max(lst)),
		}
		lst = [v.replace(microsecond=0) for v in lst]
	groupdata[name] = {
		name: ([str(v) for v in lst], min(lst), max(lst)),
	}

mk_datetime('datetime', lambda v: v)
mk_datetime('date', lambda v: v.date())
mk_datetime('time', lambda v: v.time())

data = {}
for d in groupdata.values():
	for name, d in d.items():
		assert name not in data
		data[name] = d

tests = {
	'bits32_10'  : ['u32'],
	'bits64_10'  : ['u32', 'u64'],
	'int32_10'   : ['small int', 'i32'],
	'int64_10'   : ['small int', 'i32', 'u32', 'i64'],
	'number'     : ['small int', 'i32', 'u32', 'i64', 'float32', 'float64', 'huge', 'infinity', 'nan'],
	'number:int' : ['small int', 'i32', 'u32', 'i64', 'huge', 'inty floats'],
	'float32'    : ['small int', 'float32', 'float32 only', 'inty floats', 'infinity', 'nan'],
	'float64'    : ['small int', 'float32', 'float64', 'inty floats', 'infinity', 'nan'],
	'floatint32s': ['floatint'],
	'datetime:%Y-%m-%d %H:%M:%S'   : ['datetime'],
	'datetime:%Y-%m-%d %H:%M:%S.%f': ['datetime micro'],
	'time:%H:%M:%S'                : ['time'],
	'time:%H:%M:%S.%f'             : ['time micro'],
	'date:%Y-%m-%d'                : ['date'],
}

sources = {}
def make_source(names):
	names = sorted(names)
	dsname = '+'.join(names)
	if dsname not in sources:
		dw = DatasetWriter(name=dsname, columns={'v': 'ascii'})
		write = dw.get_split_write()
		for name in names:
			for value in data[name][0]:
				write(value)
		sources[dsname] = (
			dw.finish(),
			min(unnan(data[name][1] for name in names)),
			max(unnan(data[name][2] for name in names)),
		)
	return sources[dsname]

def isnan_safe(v):
	return isinstance(v, float) and isnan(v)

def unnan(it):
	got_any = False
	for v in it:
		if not isnan_safe(v):
			yield v
			got_any = True
	if not got_any:
		yield nan

def chk_minmax(got, want, msg):
	if got == want:
		return
	if all(isnan_safe(v) for v in got + want):
		return
	raise Exception(msg)

def synthesis(job):
	dw = DatasetWriter(name='empty', columns={'v': 'ascii'})
	dw.get_split_write()
	empty_ds = dw.finish()
	assert empty_ds.min('non-existant column') is empty_ds.max('non-existant column') is None, 'Dataset.min/max() broken for non-existant columns'
	for typ, groups in tests.items():
		t_ds = subjobs.build('dataset_type', column2type={'v': typ}, source=empty_ds).dataset()
		minmax = (t_ds.columns['v'].min, t_ds.columns['v'].max)

		if minmax != (None, None):
			raise Exception('Typing empty dataset as %s did not give minmax == None, gave %r' % (typ, minmax,))
		all_names = list(chain.from_iterable(groupdata[group].keys() for group in groups))
		# just 1 and 2, so we don't make way too many
		for num_groups in (1, 2,):
			for names in combinations(all_names, num_groups):
				ds, mn, mx = make_source(names)
				t_ds = subjobs.build('dataset_type', column2type={'v': typ}, source=ds).dataset()
				got_minmax = (t_ds.columns['v'].min, t_ds.columns['v'].max)
				want_minmax = (mn, mx)
				chk_minmax(got_minmax, want_minmax, 'Typing %s as %s gave wrong minmax: expected %r, got %r (in %s)' % (ds, typ, want_minmax, got_minmax, t_ds,))
				chk_minmax(got_minmax, (t_ds.min('v'), t_ds.max('v')), 'Dataset.min/max() broken on ' + t_ds)
				# verify writing the same data normally also gives the correct result
				dw = DatasetWriter(name='rewrite ' + t_ds, columns=t_ds.columns)
				write = dw.get_split_write()
				for v in t_ds.iterate(None, 'v'):
					write(v)
				re_ds = dw.finish()
				got_minmax = (re_ds.columns['v'].min, re_ds.columns['v'].max)
				want_minmax = (mn, mx)
				chk_minmax(got_minmax, want_minmax, 'Rewriting %s gave the wrong minmax: expected %r, got %r (in %s)' % (t_ds, want_minmax, got_minmax, re_ds,))
