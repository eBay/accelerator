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

from accelerator.compat import unicode


# WORKSPACES should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKSPACES = {}


def put_workspaces(workspaces_dict):
	global WORKSPACES
	WORKSPACES = workspaces_dict


class JobID(unicode):
	"""
	A string that is a jobid, but also has a .method property.
	"""
	def __new__(cls, jobid, method=None):
		obj = unicode.__new__(cls, jobid)
		obj.workspace, tmp = jobid.rsplit('-', 1)
		obj.number = int(tmp)
		obj.method = method
		return obj


def dirnamematcher(name):
	return re.compile(re.escape(name) + r'-[0-9]+$').match


def create(name, number):
	return '%s-%d' % (name, number,)


def get_workspace_name(jobid):
	return jobid.rsplit('-', 1)[0]


def get_path(jobid):
	return WORKSPACES[get_workspace_name(jobid)]


def resolve_jobid_filename(jobid, filename):
	"""
	Used by extras, dataset, and sourcedata to find
	full path of filename based on lookup from
	jobid -> workspace
	"""
	if jobid:
		jobid = str(jobid)
		path = get_path(jobid)
		return os.path.join(path, jobid, filename)
	else:
		return filename
