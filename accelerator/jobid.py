############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
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

import os
import re

from accelerator.compat import unicode, PY2


# WORKSPACES should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKSPACES = {}


def put_workspaces(workspaces_dict):
	global WORKSPACES
	WORKSPACES = workspaces_dict


def dirnamematcher(name):
	return re.compile(re.escape(name) + r'-[0-9]+$').match


class JobID(unicode):
	"""
	A string that is a jobid, but also has some extra properties:
	.method The job method, but probably None except in JobLists.
	.number The job number as an int.
	.workspace The workspace name (the part before -number in the jobid)
	.path The filesystem directory where the job is stored.
	And some functions:
	.filename to join .path with a filename
	.params to load setup.json from this job
	.post to load post.json from this job

	Decay's to a (unicode) string when pickled.
	"""
	def __new__(cls, jobid, method=None):
		obj = unicode.__new__(cls, jobid)
		obj.workspace, tmp = jobid.rsplit('-', 1)
		obj.number = int(tmp)
		obj.method = method
		return obj

	@classmethod
	def create(cls, name, number):
		return JobID('%s-%d' % (name, number,))

	@property
	def path(self):
		return os.path.join(WORKSPACES[self.workspace], self)

	def filename(self, filename, sliceno=None):
		if sliceno is not None:
			filename = '%s.%d' % (filename, sliceno,)
		return os.path.join(self.path, filename)

	def withfile(self, filename, sliced=False, extra=None):
		from accelerator.extras import JobWithFile
		return JobWithFile(self, filename, sliced, extra)

	def params(self):
		from accelerator.extras import job_params
		return job_params(self)

	def post(self):
		from accelerator.extras import job_post
		return job_post(self)

	def load(self, filename='result.pickle', sliceno=None, encoding='bytes'):
		"""blob.load from this job"""
		from accelerator.extras import pickle_load
		return pickle_load(self.filename(filename, sliceno), encoding=encoding)

	def json_load(self, filename='result.json', sliceno=None, unicode_as_utf8bytes=PY2):
		from accelerator.extras import json_load
		return json_load(self.filename(filename, sliceno), unicode_as_utf8bytes=unicode_as_utf8bytes)

	# Look like a string after pickling
	def __reduce__(self):
		return unicode, (unicode(self),)
