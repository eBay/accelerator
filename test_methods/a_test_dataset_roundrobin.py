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
Test sliceno="roundrobin" in dataset iteration.
'''

from accelerator.dataset import DatasetWriter

def prepare(params):
	assert params.slices >= 3
	dw_3 = DatasetWriter(name="three", columns={"num": "int32"})
	dw_long = DatasetWriter(name="long", columns={"num": "int32"})
	dw_uneven = DatasetWriter(name="uneven", columns={"num": "int32"})
	return dw_3, dw_long, dw_uneven

def analysis(sliceno, prepare_res, params):
	dw_3, dw_long, dw_uneven = prepare_res
	if sliceno < 3:
		dw_3.write(sliceno)
		dw_3.write(sliceno + 3)
	for ix in range(sliceno, 100000, params.slices):
		dw_long.write(ix)
	dw_uneven.write(sliceno)
	if sliceno > 1:
		dw_uneven.write(100000 + sliceno)
	if sliceno == 2:
		dw_uneven.write(-1)
		dw_uneven.write(-2)

def synthesis(prepare_res, params):
	dw_3, dw_long, dw_uneven = prepare_res
	ds_3 = dw_3.finish()
	ds_long = dw_long.finish()
	ds_uneven = dw_uneven.finish()
	inorder = list(ds_3.iterate(None, "num"))
	assert inorder == [0, 3, 1, 4, 2, 5]
	rr = list(ds_3.iterate("roundrobin", "num"))
	assert rr == [0, 1, 2, 3, 4, 5]
	rr = list(ds_long.iterate("roundrobin", "num"))
	assert rr == list(range(100000))
	rr = list(ds_uneven.iterate("roundrobin", "num"))
	assert rr == list(range(params.slices)) + list(range(100002, 100000 + params.slices)) + [-1, -2]
