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
	# When adding types to gzutil switch this to one of those
	# (to make building with a previous version installed work).
	from .gzutil import GzComplex64
	del GzComplex64
	before_install = False
except ImportError:
	before_install = True

__all__ = []

if not before_install:
	def get_version():
		import os.path
		try:
			dn = os.path.dirname(__file__)
			fn = os.path.join(dn, 'version.txt')
			with open(fn, 'r') as fh:
				return next(fh).strip()
		except Exception:
			return None
	__version__ = get_version()
	del get_version
	from .dataset import SkipSlice, SkipDataset
	__all__.extend((SkipSlice, SkipDataset,))
	from .error import AcceleratorError, UserError, ServerError
	from .error import UrdError, UrdPermissionError, UrdConflictError
	from .error import NoSuchWhateverError, NoSuchJobError, NoSuchWorkdirError
	from .error import DatasetError, NoSuchDatasetError, DatasetUsageError
	from .error import JobError
	__all__.extend((AcceleratorError, UserError, ServerError,))
	__all__.extend((UrdError, UrdPermissionError, UrdConflictError,))
	__all__.extend((NoSuchWhateverError, NoSuchJobError, NoSuchWorkdirError,))
	__all__.extend((DatasetError, NoSuchDatasetError, DatasetUsageError,))
	__all__.extend((JobError,))
	from .extras import DotDict
	from .extras import OptionEnum, OptionString, RequiredOption, OptionDefault
	__all__.extend((DotDict,))
	__all__.extend((OptionEnum, OptionString, RequiredOption, OptionDefault,))
	from .job import Job, JobWithFile
	__all__.extend((Job, JobWithFile,))
	from .statmsg import status, dummy_status
	__all__.extend((status, dummy_status,))
	from .subjobs import build
	__all__.extend((build,))

del before_install
