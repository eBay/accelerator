############################################################################
#                                                                          #
# Copyright (c) 2019-2020 Carl Drougge                                     #
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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from accelerator.dataset import Dataset
from accelerator.build import JobError

from datetime import date, datetime, timedelta

def main(urd):
	assert urd.info.slices >= 3, "The tests don't work with less than 3 slices (you have %d)." % (urd.info.slices,)

	print()
	print("Testing urd.build and job.load")
	want = ({'foo': 'foo', 'a': 'a'}, {'foo': None, 'b': None}, {'foo': None, 'c': None})
	job = urd.build("test_build_kws")
	assert job.load() == want
	bad = None
	try:
		urd.build("test_build_kws", options=dict(foo='bar'), foo='baz')
		bad = 'Allowed ambiguous keyword "foo"'
	except Exception:
		pass
	assert not bad, bad
	want[0]['foo'] = 'bar'
	want[0]['a'] = 'A'
	job = urd.build("test_build_kws", options=dict(foo='bar'), a='A')
	assert job.load() == want
	assert urd.build("test_build_kws", options=dict(foo='bar'), a='A', b=None, c=None) == job
	want[2]['c'] = job
	job = urd.build("test_build_kws", options=dict(foo='bar', a='override this from kw'), a='A', c=job)
	assert job.load() == want
	want[0]['foo'] = 'foo'
	want[2]['c'] = job
	job = urd.build("test_build_kws", a='A', b=None, c=job, datasets=dict(b='overridden'))
	assert job.load() == want

	print()
	print("Testing urd.begin/end/truncate/get/peek/latest/first/since")
	urd.truncate("tests_urd", 0)
	assert not urd.peek_latest("tests_urd").joblist
	urd.begin("tests_urd", 1)
	urd.build("test_build_kws")
	fin = urd.finish("tests_urd")
	assert fin == {'new': True, 'changed': False, 'is_ghost': False}, fin
	urd.begin("tests_urd", 1)
	job = urd.build("test_build_kws")
	fin = urd.finish("tests_urd")
	assert fin == {'new': False, 'changed': False, 'is_ghost': False}, fin
	urd.begin("tests_urd", 1) # will be overridden to 2 in finish
	jl = urd.latest("tests_urd").joblist
	assert jl == [job], '%r != [%r]' % (jl, job,)
	urd.build("test_build_kws", options=dict(foo='bar', a='A'))
	urd.finish("tests_urd", 2)
	dep_jl = list(urd.peek_latest("tests_urd").deps.values())[0].joblist
	assert dep_jl == jl, '%r != %r' % (dep_jl, jl,)
	assert urd.since("tests_urd", 0) == ['1', '2']
	urd.truncate("tests_urd", 2)
	assert urd.since("tests_urd", 0) == ['1']
	urd.truncate("tests_urd", 0)
	assert urd.since("tests_urd", 0) == []
	ordered_ts = [1, 2, 1000000000, '1978-01-01', '1978-01-01+0', '1978-01-01+2', '1978-01-01 00:00', '1978-01-01T00:00+42', '2017-06-27', '2017-06-27T17:00:00', '2017-06-27 17:00:00+42']
	for ts in ordered_ts:
		urd.begin("tests_urd")
		if ts == 1000000000:
			urd.get("tests_urd", '1')
		urd.build("test_build_kws")
		urd.finish("tests_urd", ts)
	urd.begin("tests_urd")
	urd.build("test_build_kws")
	urd.finish("tests_urd", ('2019-12', 3))
	ordered_ts.append('2019-12+3')
	ordered_ts = [str(v).replace(' ', 'T') for v in ordered_ts]
	assert urd.since("tests_urd", 0) == ordered_ts
	assert urd.since("tests_urd", '1978-01-01') == ordered_ts[4:]
	assert urd.peek_first("tests_urd").timestamp == '1'
	assert not urd.peek("tests_urd", 2).deps
	dep_jl = list(urd.peek("tests_urd", 1000000000).deps.values())[0].joblist
	assert dep_jl == [job]
	assert urd.peek("tests_urd", ('2017-06-27 17:00:00', 42)).timestamp == '2017-06-27T17:00:00+42'
	while ordered_ts:
		urd.truncate("tests_urd", ordered_ts.pop())
		assert urd.since("tests_urd", 0) == ordered_ts, ordered_ts
	want = [date.today() - timedelta(10), datetime.utcnow()]
	for ts in want:
		urd.begin("tests_urd", ts)
		urd.build("test_build_kws")
		urd.finish("tests_urd")
	assert urd.since("tests_urd", 0) == [str(ts).replace(' ', 'T') for ts in want]
	urd.truncate("tests_urd", 0)

	print()
	print("Testing dataset creation, export, import")
	source = urd.build("test_datasetwriter")
	urd.build("test_datasetwriter_verify", source=source)
	urd.build("test_datasetwriter_parent")
	urd.build("test_dataset_in_prepare")
	ds = Dataset(source, "passed")
	csvname = "out.csv.gz"
	csvname_uncompressed = "out.csv"
	csv = urd.build("csvexport", filename=csvname, separator="\t", source=ds)
	csv_uncompressed = urd.build("csvexport", filename=csvname_uncompressed, separator="\t", source=ds)
	csv_quoted = urd.build("csvexport", filename=csvname, quote_fields='"', source=ds)
	urd.build("csvexport", filename='slice%d.csv', sliced=True, source=ds) # unused
	reimp_csv = urd.build("csvimport", filename=csv.filename(csvname), separator="\t")
	reimp_csv_uncompressed = urd.build("csvimport", filename=csv_uncompressed.filename(csvname_uncompressed), separator="\t")
	reimp_csv_quoted = urd.build("csvimport", filename=csv_quoted.filename(csvname), quotes=True)
	urd.build("test_compare_datasets", a=reimp_csv, b=reimp_csv_uncompressed)
	urd.build("test_compare_datasets", a=reimp_csv, b=reimp_csv_quoted)
	urd.build("test_dataset_column_names")
	urd.build("test_dataset_merge")
	urd.build("test_dataset_filter_columns")

	print()
	print("Testing csvimport with more difficult files")
	urd.build("test_csvimport_corner_cases")
	urd.build("test_csvimport_separators")

	print()
	print("Testing csvexport with all column types, strange separators")
	urd.build("test_csvexport_all_coltypes")
	urd.build("test_csvexport_separators")

	print()
	print("Testing subjobs and dataset typing")
	urd.build("test_subjobs_type", typed=ds, untyped=reimp_csv)
	urd.build("test_subjobs_nesting")
	try:
		# Test if numeric_comma is broken (presumably because no suitable locale
		# was found, since there are not actually any commas in the source dataset.)
		urd.build("dataset_type", source=source, numeric_comma=True, column2type=dict(b="float64"), defaults=dict(b="0"))
		comma_broken = False
	except JobError as e:
		comma_broken = True
		urd.warn()
		urd.warn('SKIPPED NUMERIC COMMA TESTS')
		urd.warn('Follow the instructions in this error to enable numeric comma:')
		urd.warn()
		urd.warn(e.format_msg())
	urd.build("test_dataset_type_corner_cases", numeric_comma=not comma_broken)

	print()
	print("Testing dataset chaining, filtering, callbacks and rechaining")
	selfchain = urd.build("test_selfchain")
	urd.build("test_rechain", jobs=dict(selfchain=selfchain))
	urd.build("test_dataset_callbacks")

	print()
	print("Testing dataset sorting and rehashing (with subjobs again)")
	urd.build("test_sorting")
	urd.build("test_sort_stability")
	urd.build("test_sort_chaining")
	urd.build("test_sort_trigger")
	urd.build("test_hashpart")
	urd.build("test_dataset_type_hashing")
	urd.build("test_dataset_type_chaining")

	print()
	print("Test hashlabels")
	urd.build("test_hashlabel")

	print()
	print("Test dataset roundrobin iteration and slicing")
	urd.build("test_dataset_roundrobin")
	urd.build("test_dataset_slice")
	urd.build("test_dataset_unroundrobin")
	urd.build("test_dataset_unroundrobin_trigger")

	print()
	print("Test dataset_checksum")
	urd.build("test_dataset_checksum")

	print()
	print("Test csvimport_zip")
	urd.build("test_csvimport_zip")

	print()
	print("Test output handling")
	urd.build("test_output")

	print()
	print("Test datetime types in options")
	urd.build("test_datetime")

	print()
	print("Test various utility functions")
	urd.build("test_optionenum")
	urd.build("test_json")
	urd.build("test_jobwithfile")
	urd.build("test_jobchain")
	summary = urd.build("test_summary", joblist=urd.joblist)
	summary.link_result('summary.html')
