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

class AcceleratorError(Exception):
	"""Base class for all accelerator exception types"""
	pass

class UserError(AcceleratorError):
	"""Raised when the user (of a shell command) did something wrong"""
	pass

class ServerError(AcceleratorError):
	"""Error return from a server call (not a method failing)"""
	pass

class UrdError(AcceleratorError):
	"""Errors from urd"""
	pass

class UrdPermissionError(UrdError):
	"""Attempting to write something you are not allowed to write"""
	pass

class UrdConflictError(UrdError):
	"""Attempting to write something that already has a different value"""
	pass

class NoSuchWhateverError(AcceleratorError):
	"""Base class for errors about things not being found (job, dataset, ...)"""
	pass

class NoSuchJobError(NoSuchWhateverError):
	pass

class NoSuchWorkdirError(NoSuchWhateverError):
	pass

class DatasetError(AcceleratorError):
	"""Any error about datasets"""
	pass

class NoSuchDatasetError(DatasetError, NoSuchWhateverError):
	pass

class DatasetUsageError(DatasetError):
	"""Probably a mistake on the users part"""
	pass

class JobError(AcceleratorError):
	def __init__(self, jobid, method, status):
		AcceleratorError.__init__(self, "Failed to build %s (%s)" % (jobid, method,))
		self.jobid = jobid
		self.method = method
		self.status = status

	def format_msg(self):
		res = ["%s (%s):" % (self.jobid, self.method,)]
		for component, msg in self.status.items():
			res.append("  %s:" % (component,))
			res.append("    %s" % (msg.replace("\n", "\n    "),))
		return "\n".join(res)
