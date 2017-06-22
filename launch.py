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

from __future__ import print_function
from __future__ import division

import os
import signal
import sys
import jobid as jobid_module
from inspect import getargspec
from collections import defaultdict
import g
from importlib import import_module
from traceback import print_exc, format_tb, format_exception_only
from extras import job_params, ResultIterMagic, DotDict
from time import time, sleep
import json
from compat import pickle, iteritems
from dispatch import JobError
import blob
import status
import dataset


g_allesgut = False
_prof_fd = -1


g_always = {'running',}
assert set(n for n in dir(g) if not n.startswith("__")) == g_always, "Don't put anything in g.py"


def call_analysis(analysis_func, sliceno_, q, preserve_result, parent_pid, **kw):
	try:
		status._start('analysis(%d)' % (sliceno_,), parent_pid, 't')
		os.close(_prof_fd)
		for stupid_inconsistent_name in ('sliceno', 'index'):
			if stupid_inconsistent_name in kw:
				kw[stupid_inconsistent_name] = sliceno_
			setattr(g, stupid_inconsistent_name, sliceno_)
		for dw in dataset._datasetwriters.values():
			if dw._for_single_slice is None:
				dw._set_slice(sliceno_)
		res = analysis_func(**kw)
		if preserve_result:
			# Remove defaultdicts until we find one with a picklable default_factory.
			# (This is what you end up doing manually anyway.)
			def picklable(v):
				try:
					pickle.dumps(v, pickle.HIGHEST_PROTOCOL)
					return True
				except Exception:
					return False
			def fixup(d):
				if isinstance(d, defaultdict) and not picklable(d.default_factory):
					if not d:
						return {}
					v = next(iteritems(d))
					if isinstance(v, defaultdict) and not picklable(v.default_factory):
						return {k: fixup(v) for k, v in iteritems(d)}
					else:
						return dict(d)
				else:
					return d
			def save(item, name):
				blob.save(fixup(item), name, sliceno=sliceno_, temp=True)
			if isinstance(res, tuple):
				if sliceno_ == 0:
					blob.save(len(res), "Analysis.tuple", temp=True)
				for ix, item in enumerate(res):
					save(item, "Analysis.%d." % (ix,))
			else:
				if sliceno_ == 0:
					blob.save(False, "Analysis.tuple", temp=True)
				save(res, "Analysis.")
		from extras import saved_files
		dw_lens = {}
		dw_minmax = {}
		for name, dw in dataset._datasetwriters.items():
			if dw._for_single_slice in (None, sliceno_,):
				dw.close()
				dw_lens[name] = dw._lens
				dw_minmax[name] = dw._minmax
		status._end()
		q.put((sliceno_, time(), saved_files, dw_lens, dw_minmax, None,))
	except:
		status._end()
		q.put((sliceno_, time(), {}, {}, {}, fmt_tb(1),))
		print_exc()
		sleep(5) # give launcher time to report error (and kill us)
		exitfunction()

def fork_analysis(slices, analysis_func, kw, preserve_result):
	from multiprocessing import Process, Queue
	q = Queue()
	children = []
	t = time()
	pid = os.getpid()
	for i in range(slices):
		p = Process(target=call_analysis, args=(analysis_func, i, q, preserve_result, pid), kwargs=kw, name='analysis-%d' % (i,))
		p.start()
		children.append(p)
	per_slice = []
	temp_files = {}
	for p in children:
		s_no, s_t, s_temp_files, s_dw_lens, s_dw_minmax, s_tb = q.get()
		if s_tb:
			data = [{'analysis(%d)' % (s_no,): s_tb}, None]
			os.write(_prof_fd, json.dumps(data).encode('utf-8'))
			exitfunction()
		per_slice.append((s_no, s_t))
		temp_files.update(s_temp_files)
		for name, lens in s_dw_lens.items():
			dataset._datasetwriters[name]._lens.update(lens)
		for name, minmax in s_dw_minmax.items():
			dataset._datasetwriters[name]._minmax.update(minmax)
	for p in children:
		p.join()
	if preserve_result:
		res_seq = ResultIterMagic(slices, reuse_msg="analysis_res is an iterator, don't re-use it")
	else:
		res_seq = None
	return [v - t for k, v in sorted(per_slice)], temp_files, res_seq

def args_for(func):
	kw = {}
	for arg in getargspec(func).args:
		kw[arg] = getattr(g, arg)
	return kw


def fmt_tb(skip_level):
	msg = []
	e_type, e, tb = sys.exc_info()
	tb = format_tb(tb)[skip_level:]
	if isinstance(e, JobError):
		msg.append(e.format_msg())
		tb = tb[:-5] # the five innermost are in automata_common and of no interest.
	msg.append("Traceback (most recent call last):\n")
	msg.extend(tb)
	msg.extend(format_exception_only(e_type, e))
	return ''.join(msg)


def execute_process(workdir, jobid, slices, result_directory, common_directory, source_directory, index=None, workspaces=None, daemon_url=None, subjob_cookie=None, parent_pid=0):
	path = os.path.join(workdir, jobid)
	try:
		os.chdir(path)
	except Exception:
		print("Cannot cd to workdir", path)
		exit(1)

	g.params = params = job_params()
	method_ref = import_module(params.package+'.a_'+params.method)
	g.sliceno = -1

	if workspaces:
		jobid_module.put_workspaces(workspaces)

	def maybe_dataset(v):
		if isinstance(v, list):
			return [maybe_dataset(e) for e in v]
		if not v:
			return ''
		try:
			return dataset.Dataset(v)
		except IOError:
			return v
	datasets = DotDict({k: maybe_dataset(v) for k, v in params.datasets.items()})

	g.options          = params.options
	g.datasets         = datasets
	g.jobids           = params.jobids

	method_ref.options = params.options
	method_ref.datasets= datasets
	method_ref.jobids  = params.jobids

	# compatibility names
	g.SLICES           = slices
	g.JOBID            = jobid
	g.jobid            = jobid
	g.METHOD           = params.method
	g.WORKSPACEPATH    = workdir
	g.CAPTION          = params.caption
	g.PACKAGE          = params.package
	g.RESULT_DIRECTORY = result_directory
	g.COMMON_DIRECTORY = common_directory
	g.SOURCE_DIRECTORY = source_directory
	g.index            = -1

	g.daemon_url       = daemon_url
	g.running          = 'launch'
	status._start('%s %s' % (jobid, params.method,), parent_pid)

	def dummy():
		pass

	prepare_func   = getattr(method_ref, 'prepare'  , dummy)
	analysis_func  = getattr(method_ref, 'analysis' , dummy)
	synthesis_func = getattr(method_ref, 'synthesis', dummy)

	synthesis_needs_analysis = 'analysis_res' in getargspec(synthesis_func).args

	# A chain must be finished from the back, so sort on that.
	sortnum_cache = {}
	def dw_sortnum(name):
		if name not in sortnum_cache:
			dw = dataset._datasetwriters[name]
			if dw.previous and dw.previous.startswith(jobid + '/'):
				pname = dw.previous.split('/')[1]
				num = dw_sortnum(pname) + 1
			else:
				num = 0
			sortnum_cache[name] = num
		return sortnum_cache[name]

	prof = {}
	if prepare_func is dummy:
		prof['prepare'] = 0 # truthish!
	else:
		t = time()
		g.running = 'prepare'
		g.subjob_cookie = subjob_cookie
		with status.status(g.running):
			g.prepare_res = method_ref.prepare(**args_for(method_ref.prepare))
			to_finish = [dw.name for dw in dataset._datasetwriters.values() if dw._started]
			if to_finish:
				with status.status("Finishing datasets"):
					for name in sorted(to_finish, key=dw_sortnum):
						dataset._datasetwriters[name].finish()
		prof['prepare'] = time() - t
	from extras import saved_files
	if analysis_func is dummy:
		prof['per_slice'] = []
		prof['analysis'] = 0
	else:
		t = time()
		g.running = 'analysis'
		g.subjob_cookie = None # subjobs are not allowed from analysis
		with status.status('Waiting for all slices to finish analysis'):
			prof['per_slice'], files, g.analysis_res = fork_analysis(slices, analysis_func, args_for(analysis_func), synthesis_needs_analysis)
		prof['analysis'] = time() - t
		saved_files.update(files)
	t = time()
	g.running = 'synthesis'
	g.subjob_cookie = subjob_cookie
	with status.status(g.running):
		synthesis_res = synthesis_func(**args_for(synthesis_func))
		if synthesis_res is not None:
			blob.save(synthesis_res, temp=False)
		if dataset._datasetwriters:
			with status.status("Finishing datasets"):
				for name in sorted(dataset._datasetwriters, key=dw_sortnum):
					dataset._datasetwriters[name].finish()
	t = time() - t
	prof['synthesis'] = t

	from subjobs import _record
	status._end()
	return None, (prof, saved_files, _record)


def run(workdir, jobid, slices, result_directory, common_directory, source_directory, index=None, workspaces=None, daemon_url=None, subjob_cookie=None, parent_pid=0, prof_fd=-1):
	global g_allesgut, _prof_fd
	_prof_fd = prof_fd
	try:
		data = execute_process(workdir, jobid, slices, result_directory, common_directory, source_directory, index=index, workspaces=workspaces, daemon_url=daemon_url, subjob_cookie=subjob_cookie, parent_pid=parent_pid)
		g_allesgut = True
	except Exception:
		print_exc()
		data = [{g.running: fmt_tb(2)}, None]
	os.write(prof_fd, json.dumps(data).encode('utf-8'))


def exitfunction():
	if not g_allesgut:
		print('LAUNCH:  The deathening!')
		os.killpg(os.getpgid(0), signal.SIGTERM)
