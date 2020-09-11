############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Carl Drougge                       #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
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

from collections import namedtuple
from functools import wraps

from accelerator.compat import unicode, PY2, PY3, open, iteritems
from accelerator.error import NoSuchJobError, NoSuchWorkdirError


# WORKDIRS should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKDIRS = {}


def dirnamematcher(name):
	return re.compile(re.escape(name) + r'-[0-9]+$').match


def _cachedprop(meth):
	@property
	@wraps(meth)
	def wrapper(self):
		if meth.__name__ not in self._cache:
			self._cache[meth.__name__] = meth(self)
		return self._cache[meth.__name__]
	return wrapper

class Job(unicode):
	"""
	A string that is a jobid, but also has some extra properties:
	.method The job method (can be the "name" when from build or urd).
	.number The job number as an int.
	.workdir The workdir name (the part before -number in the jobid)
	.path The filesystem directory where the job is stored.
	.params setup.json from this job.
	.post post.json from this job.
	.datasets list of Datasets in this job.
	And some functions:
	.withfile a JobWithFile with this job.
	.filename to join .path with a filename.
	.load to load a pickle.
	.json_load to load a json file.
	.open to open a file (like standard open)
	.dataset to get a named Dataset.
	.output to get what the job printed.
	.link_result to put a link in result_directory that points to a file in this job.

	Decays to a (unicode) string when pickled.
	"""
	def __new__(cls, jobid, method=None):
		obj = unicode.__new__(cls, jobid)
		try:
			obj.workdir, tmp = jobid.rsplit('-', 1)
			obj.number = int(tmp)
		except ValueError:
			raise NoSuchJobError('Not a valid jobid: "%s".' % (jobid,))
		obj._cache = {}
		if method:
			obj._cache['method'] = method
		return obj

	@classmethod
	def _create(cls, name, number):
		return Job('%s-%d' % (name, number,))

	@_cachedprop
	def method(self):
		return self.params.method

	@property
	def path(self):
		if self.workdir not in WORKDIRS:
			raise NoSuchWorkdirError('Not a valid workdir: "%s"' % (self.workdir,))
		return os.path.join(WORKDIRS[self.workdir], self)

	def filename(self, filename, sliceno=None):
		if sliceno is not None:
			filename = '%s.%d' % (filename, sliceno,)
		return os.path.join(self.path, filename)

	def open(self, filename, mode='r', sliceno=None, encoding=None, errors=None):
		assert 'r' in mode, "Don't write to other jobs"
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		return open(self.filename(filename, sliceno), mode, encoding=encoding, errors=errors)

	def files(self, pattern='*'):
		from fnmatch import filter
		return set(filter(self.post.files, pattern))

	def withfile(self, filename, sliced=False, extra=None):
		return JobWithFile(self, filename, sliced, extra)

	@_cachedprop
	def params(self):
		from accelerator.extras import job_params
		return job_params(self)

	@_cachedprop
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

	def dataset(self, name='default'):
		from accelerator.dataset import Dataset
		return Dataset(self, name)

	@_cachedprop
	def datasets(self):
		from accelerator.dataset import job_datasets
		return job_datasets(self)

	def output(self, what=None):
		if isinstance(what, int):
			fns = [str(what)]
		else:
			assert what in (None, 'prepare', 'analysis', 'synthesis'), 'Unknown output %r' % (what,)
			if what in (None, 'analysis'):
				fns = [str(sliceno) for sliceno in range(self.params.slices)]
				if what is None:
					fns = ['prepare'] + fns + ['synthesis']
			else:
				fns = [what]
		res = []
		for fn in fns:
			fn = self.filename('OUTPUT/' + fn)
			if os.path.exists(fn):
				with open(fn, 'rt', encoding='utf-8', errors='backslashreplace') as fh:
					res.append(fh.read())
		return ''.join(res)

	def link_result(self, filename='result.pickle', linkname=None):
		"""Put a symlink to filename in result_directory
		Only use this in a build script."""
		from accelerator.g import running
		assert running == 'build', "Only link_result from a build script"
		from accelerator.shell import cfg
		if linkname is None:
			linkname = filename
		result_directory = cfg['result_directory']
		dest_fn = os.path.join(result_directory, linkname)
		try:
			os.remove(dest_fn + '_')
		except OSError:
			pass
		source_fn = os.path.join(self.path, filename)
		assert os.path.exists(source_fn), "Filename \"%s\" does not exist in jobdir \"%s\"!" % (filename, self.path)
		os.symlink(source_fn, dest_fn + '_')
		os.rename(dest_fn + '_', dest_fn)

	def chain(self, length=-1, reverse=False, stop_job=None):
		"""Like Dataset.chain but for jobs."""
		if isinstance(stop_job, dict):
			assert len(stop_job) == 1, "Only pass a single stop_job={job: name}"
			stop_job, stop_name = next(iteritems(stop_job))
			if stop_job:
				stop_job = Job(stop_job).params.jobs.get(stop_name)
		chain = []
		current = self
		while length != len(chain) and current and current != stop_job:
			chain.append(current)
			current = current.params.jobs.get('previous')
		if not reverse:
			chain.reverse()
		return chain

	# Look like a string after pickling
	def __reduce__(self):
		return unicode, (unicode(self),)


class CurrentJob(Job):
	"""The currently running job (as passed to the method),
	with extra functions for writing data."""

	def __new__(cls, jobid, params, result_directory, input_directory):
		obj = Job.__new__(cls, jobid, params.method)
		obj._cache['params'] = params
		obj.result_directory = result_directory
		obj.input_directory = input_directory
		return obj

	def save(self, obj, filename='result.pickle', sliceno=None, temp=None):
		from accelerator.extras import pickle_save
		pickle_save(obj, filename, sliceno, temp=temp)

	def json_save(self, obj, filename='result.json', sliceno=None, sort_keys=True, temp=None):
		from accelerator.extras import json_save
		json_save(obj, filename, sliceno, sort_keys=sort_keys, temp=temp)

	def datasetwriter(self, columns={}, filename=None, hashlabel=None, hashlabel_override=False, caption=None, previous=None, name='default', parent=None, meta_only=False, for_single_slice=None):
		from accelerator.dataset import DatasetWriter
		return DatasetWriter(columns=columns, filename=filename, hashlabel=hashlabel, hashlabel_override=hashlabel_override, caption=caption, previous=previous, name=name, parent=parent, meta_only=meta_only, for_single_slice=for_single_slice)

	def open(self, filename, mode='r', sliceno=None, encoding=None, errors=None, temp=None):
		"""Mostly like standard open with sliceno and temp,
		but you must use it as context manager
		with job.open(...) as fh:
		and the file will have a temp name until the with block ends.
		"""
		if 'r' in mode:
			return Job.open(self, filename, mode, sliceno, encoding, errors)
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		if PY3 and 'x' not in mode:
			mode = mode.replace('w', 'x')
		def _open(fn, _mode):
			# ignore the passed mode, use the one we have
			return open(fn, mode, encoding=encoding, errors=errors)
		from accelerator.extras import FileWriteMove
		fwm = FileWriteMove(self.filename(filename, sliceno), temp=temp)
		fwm._open = _open
		return fwm

	def register_file(self, filename):
		"""Record a file produced by this job. Normally you would use
		job.open to have this happen automatically, but if the file was
		produced in a way where that is not practical you can use this
		to register it."""
		filename = self.filename(filename)
		assert os.path.exists(filename)
		from accelerator.extras import saved_files
		saved_files[filename] = 0

	def input_filename(self, filename):
		return os.path.join(self.input_directory, filename)

	def open_input(self, filename, mode='r', encoding=None, errors=None):
		assert 'r' in mode, "Don't write to input files"
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		return open(self.input_filename(filename), mode, encoding=encoding, errors=errors)

class JobWithFile(namedtuple('JobWithFile', 'job name sliced extra')):
	def __new__(cls, job, name, sliced=False, extra=None):
		assert not name.startswith('/'), "Specify relative filenames to JobWithFile"
		return tuple.__new__(cls, (Job(job), name, bool(sliced), extra,))

	def filename(self, sliceno=None):
		if sliceno is None:
			assert not self.sliced, "A sliced file requires a sliceno"
		else:
			assert self.sliced, "An unsliced file can not have a sliceno"
		return self.job.filename(self.name, sliceno)

	def load(self, sliceno=None, encoding='bytes'):
		"""blob.load this file"""
		from accelerator.extras import pickle_load
		return pickle_load(self.filename(sliceno), encoding=encoding)

	def json_load(self, sliceno=None, unicode_as_utf8bytes=PY2):
		from accelerator.extras import json_load
		return json_load(self.filename(sliceno), unicode_as_utf8bytes=unicode_as_utf8bytes)

	def open(self, mode='r', sliceno=None, encoding=None, errors=None):
		return self.job.open(self.name, mode, sliceno, encoding, errors)
