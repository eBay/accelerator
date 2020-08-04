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

try:
	# setup.py needs to import some things, let's not break that.
	from .gzutil import GzUnicode
	del GzUnicode
	before_install = False
except ImportError:
	before_install = True

if not before_install:
	from .dataset import SkipSlice, SkipDataset
	from .error import AcceleratorError, UserError, ServerError
	from .error import UrdError, UrdPermissionError, UrdConflictError
	from .error import NoSuchWhateverError, NoSuchJobError, NoSuchWorkdirError
	from .error import DatasetError, NoSuchDatasetError, DatasetUsageError
	from .error import JobError
	from .extras import DotDict, Temp
	from .extras import OptionEnum, OptionString, RequiredOption, OptionDefault
	from .job import Job, JobWithFile
	from .statmsg import status, dummy_status
	from .subjobs import build

del before_install
