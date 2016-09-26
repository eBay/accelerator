from __future__ import print_function
from __future__ import division

import atexit
import os
import signal
import sys
import getopt
from autoflush import AutoFlush
import jobid as jobid_module
from inspect import getargspec
from collections import defaultdict
import g
from importlib import import_module
from traceback import print_exc, format_tb, format_exception_only
from extras import job_params, ResultIterMagic, DotDict
from setupfile import SetupCompat
from time import time, sleep
import json
from compat import pickle, iteritems
from dispatch import JobError
import blob
import status
import dataset


g_allesgut = False
prof_fd = -1


g_always = {'running',}
assert set(n for n in dir(g) if not n.startswith("__")) == g_always, "Don't put anything in g.py"


def call_analysis(analysis_func, sliceno_, q, preserve_result, parent_pid, **kw):
	try:
		status._start('analysis(%d)' % (sliceno_,), parent_pid, 't')
		os.close(prof_fd)
		for stupid_inconsistent_name in ('sliceno', 'index'):
			if stupid_inconsistent_name in kw:
				kw[stupid_inconsistent_name] = sliceno_
			setattr(g, stupid_inconsistent_name, sliceno_)
		for dw in dataset._datasetwriters.values():
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
			os.write(prof_fd, json.dumps(data))
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


def execute_process(workdir, jobid, slices, result_directory, common_directory, source_directory, index=None, workspaces=None, all=False, analysis=False, synthesis=False, daemon_url=None, subjob_cookie=None, parent_pid=0):
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

	# compatibility names
	g.SLICES           = slices
	g.JOBID            = jobid
	g.jobid            = jobid
	g.METHOD           = params.method
	g.WORKSPACEPATH    = workdir
	g.CAPTION          = params.caption
	g.DEPJOBID         = params.link
	g.PACKAGE          = params.package
	g.RESULT_DIRECTORY = result_directory
	g.COMMON_DIRECTORY = common_directory
	g.SOURCE_DIRECTORY = source_directory
	g.OPTIONS          = params.options
	g.options          = params.options
	g.DATASETS         = datasets
	g.datasets         = datasets
	g.JOBIDS           = params.jobids
	g.jobids           = params.jobids
	g.SETUP            = SetupCompat(params)
	g.index            = -1

	g.daemon_url       = daemon_url
	g.running          = 'launch'
	status._start('%s %s' % (jobid, params.method,), parent_pid)

	for v in dir(g):
		if not v.startswith('__') and v not in g_always:
			setattr(method_ref, v, getattr(g, v))

	def dummy():
		pass

	analysis_func  = getattr(method_ref, 'analysis' , dummy)
	synthesis_func = getattr(method_ref, 'synthesis', dummy)

	analysis_needs_prepare  = 'prepare_res' in getargspec(analysis_func).args
	synthesis_needs_prepare = 'prepare_res' in getargspec(synthesis_func).args
	synthesis_needs_analysis = 'analysis_res' in getargspec(synthesis_func).args

	prof = {}
	if not analysis_needs_prepare and not synthesis_needs_prepare:
		prof['prepare'] = 0 # truthish!
	if hasattr(method_ref, 'prepare') and (all or (analysis and analysis_needs_prepare) or (synthesis and synthesis_needs_prepare)):
		t = time()
		g.running = 'prepare'
		g.subjob_cookie = subjob_cookie
		with status.status(g.running):
			g.prepare_res = method_ref.prepare(**args_for(method_ref.prepare))
			for dw in dataset._datasetwriters.values():
				if dw._started:
					dw.finish()
		prof['prepare'] = time() - t
	from extras import saved_files
	if analysis or all:
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
	if synthesis or all:
		t = time()
		g.running = 'synthesis'
		g.subjob_cookie = subjob_cookie
		with status.status(g.running):
			synthesis_res = synthesis_func(**args_for(synthesis_func))
			if synthesis_res is not None:
				blob.save(synthesis_res, temp=False)
			for dw in dataset._datasetwriters.values():
				dw.finish()
		t = time() - t
		if synthesis_func is dummy:
			t = 0 # truthish!
		prof['synthesis'] = t

	from subjobs import _record
	status._end()
	return None, (prof, saved_files, _record)


def main(argv):
	global g_allesgut, prof_fd
	sys.stdout = AutoFlush(sys.stdout)
	sys.stderr = AutoFlush(sys.stderr)
	opts, rem = getopt.getopt(argv[1:], '', ['index=', 'jobid=', 'workdir=', 'slices=', 'result_directory=', 'common_directory=', 'source_directory=', 'wstr=', 'prof_fd=', 'all', 'analysis', 'synthesis', 'debug', 'daemon_url=', 'subjob_cookie=', 'parent_pid='])
	jobid = None
	index = None
	all = analysis = synthesis = False
	result_directory = ''
	common_directory = ''
	workspaces = {}
	daemon_url = None
	subjob_cookie = None
	parent_pid = 0
	for opt, arg in opts:
		if opt=='--index':
			index = int(arg)
		if opt=='--workdir':
			workdir = arg
		if opt=='--jobid':
			jobid = arg
		if opt=='--slices':
			slices = int(arg)
		if opt=='--result_directory':
			result_directory = arg
		if opt=='--common_directory':
			common_directory = arg
		if opt=='--source_directory':
			source_directory = arg
		if opt=='--all':
			all = True
		if opt=='--analysis':
			analysis = True
		if opt=='--synthesis':
			synthesis = True
		if opt=='--wstr':
			for x in arg.split(','):
				name, path = x.split(':')
				workspaces[name] = path
		if opt=='--prof_fd':
			prof_fd = int(arg)
		if opt=='--daemon_url':
			daemon_url = arg
		if opt=='--subjob_cookie':
			subjob_cookie = arg or None
		if opt=='--parent_pid':
			parent_pid = int(arg or 0)

	assert sum([all, analysis, synthesis]) == 1, 'Specify exactly one of --all --analysis --synthesis'
	try:
		data = execute_process(workdir, jobid, slices, result_directory, common_directory, source_directory, index=index, workspaces=workspaces, all=all, analysis=analysis, synthesis=synthesis, daemon_url=daemon_url, subjob_cookie=subjob_cookie, parent_pid=parent_pid)
		g_allesgut = True
	except Exception:
		print_exc()
		data = [{g.running: fmt_tb(2)}, None]
	os.write(prof_fd, json.dumps(data).encode('utf-8'))






def exitfunction():
	if not g_allesgut:
		print('LAUNCH:  The deathening!')
		os.killpg(os.getpgid(0), signal.SIGTERM)


if __name__ == "__main__":
	atexit.register(exitfunction)
	sys.exit(main(sys.argv))
