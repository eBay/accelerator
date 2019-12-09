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

from accelerator.dataset import Dataset
from accelerator.build import JobError

def main(urd):
	assert urd.info.slices >= 3, "The tests don't work with less than 3 slices (you have %d)." % (urd.info.slices,)

	print()
	print("Testing dataset creation, export, import")
	source = urd.build("test_datasetwriter")
	urd.build("test_datasetwriter_verify", datasets=dict(source=source))
	urd.build("test_dataset_in_prepare")
	ds = Dataset(source, "passed")
	csvname = "out.csv.gz"
	csvname_uncompressed = "out.csv"
	csv = urd.build("csvexport", options=dict(filename=csvname, separator="\t"), datasets=dict(source=ds))
	csv_uncompressed = urd.build("csvexport", options=dict(filename=csvname_uncompressed, separator="\t"), datasets=dict(source=ds))
	csv_quoted = urd.build("csvexport", options=dict(filename=csvname, quote_fields='"'), datasets=dict(source=ds))
	reimp_csv = urd.build("csvimport", options=dict(filename=csv.filename(csvname), separator="\t"))
	reimp_csv_uncompressed = urd.build("csvimport", options=dict(filename=csv_uncompressed.filename(csvname_uncompressed), separator="\t"))
	reimp_csv_quoted = urd.build("csvimport", options=dict(filename=csv_quoted.filename(csvname), quotes=True))
	urd.build("test_compare_datasets", datasets=dict(a=reimp_csv, b=reimp_csv_uncompressed))
	urd.build("test_compare_datasets", datasets=dict(a=reimp_csv, b=reimp_csv_quoted))
	urd.build("test_dataset_column_names")
	urd.build("test_dataset_merge")

	print()
	print("Testing csvimport with more difficult files")
	urd.build("test_csvimport_corner_cases")
	urd.build("test_csvimport_separators")

	print()
	print("Testing subjobs and dataset typing")
	urd.build("test_subjobs_type", datasets=dict(typed=ds, untyped=reimp_csv))
	urd.build("test_subjobs_nesting")
	try:
		# Test if numeric_comma is broken (presumably because no suitable locale
		# was found, since there are not actually any commas in the source dataset.)
		urd.build("dataset_type", datasets=dict(source=source), options=dict(numeric_comma=True, column2type=dict(b="float64"), defaults=dict(b="0")))
		comma_broken = False
	except JobError as e:
		comma_broken = True
		urd.warn()
		urd.warn('SKIPPED NUMERIC COMMA TESTS')
		urd.warn('Follow the instructions in this error to enable numeric comma:')
		urd.warn()
		urd.warn(e.format_msg())
	urd.build("test_dataset_type_corner_cases", options=dict(numeric_comma=not comma_broken))

	print()
	print("Testing dataset chaining, filtering, callbacks and rechaining")
	selfchain = urd.build("test_selfchain")
	urd.build("test_rechain", jobs=dict(selfchain=selfchain))

	print()
	print("Testing dataset sorting and rehashing (with subjobs again)")
	urd.build("test_sorting")
	urd.build("test_sort_stability")
	urd.build("test_sort_chaining")
	urd.build("test_rehash")
	urd.build("test_dataset_type_hashing")
	urd.build("test_dataset_type_chaining")

	print()
	print("Test hashlabels")
	urd.build("test_hashlabel")

	print()
	print("Test dataset roundrobin iteration")
	urd.build("test_dataset_roundrobin")

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
	urd.build("test_report")
