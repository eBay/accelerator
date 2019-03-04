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

from jobid import resolve_jobid_filename
from dataset import Dataset

def main(urd):
	urd.build("test_json")

	print("Testing dataset creation, export, import")
	source = urd.build("test_datasetwriter")
	urd.build("test_datasetwriter_verify", datasets=dict(source=source))
	ds = Dataset(source, "passed")
	csvname = "out.csv.gz"
	csv = urd.build("csvexport", options=dict(filename=csvname, separator="\t"), datasets=dict(source=ds))
	csv_quoted = urd.build("csvexport", options=dict(filename=csvname, quote_fields='"'), datasets=dict(source=ds))
	reimp_csv = urd.build("csvimport", options=dict(filename=resolve_jobid_filename(csv, csvname), separator="\t"))
	reimp_csv_quoted = urd.build("csvimport", options=dict(filename=resolve_jobid_filename(csv_quoted, csvname), quote_support=True))
	urd.build("test_compare_datasets", datasets=dict(a=reimp_csv, b=reimp_csv_quoted))

	print()
	print("Testing csvimport with more difficult files")
	urd.build("test_csvimport_corner_cases")
	urd.build("test_csvimport_separators")

	print()
	print("Testing subjobs and dataset typing")
	urd.build("test_subjobs_type", datasets=dict(typed=ds, untyped=reimp_csv))
	urd.build("test_dataset_old_columns")

	print()
	print("Testing dataset chaining, filtering, callbacks and rechaining")
	selfchain = urd.build("test_selfchain")
	urd.build("test_rechain", jobids=dict(selfchain=selfchain))

	print()
	print("Testing dataset sorting (with subjobs again)")
	urd.build("test_sorting")
	urd.build("test_sort_stability")

	print()
	print("Test hashlabels")
	urd.build("test_hashlabel")
