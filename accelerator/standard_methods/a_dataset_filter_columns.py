############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

from __future__ import absolute_import

description = r"""Make only some columns from a dataset visible."""

from accelerator.extras import OptionDefault

options = dict(
	# Specify only one of these options
	discard_columns = OptionDefault(["colname1", "colname2", "..."]),
	keep_columns = OptionDefault(["colname1", "colname2", "..."]),
)

datasets = ("source",)

def synthesis():
	if options.keep_columns:
		assert not options.discard_columns, "Only specify one of keep_columns and discard_columns"
		keep = set(options.keep_columns)
	else:
		assert options.discard_columns, "Specify either keep_columns or discard_columns"
		discard = set(options.discard_columns)
		keep = set(datasets.source.columns) - discard
		assert keep, "All columns discarded"
	datasets.source.link_to_here(column_filter=keep)
