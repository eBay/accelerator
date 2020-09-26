############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2020 Carl Drougge                       #
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

import os
import signal
import sys
from collections import defaultdict
from importlib import import_module
from traceback import format_tb, format_exception_only
from time import time, sleep
import json
import ctypes

from accelerator.job import CurrentJob, WORKDIRS
from accelerator.compat import pickle, iteritems, setproctitle, QueueEmpty, getarglist
from accelerator.extras import job_params, ResultIterMagic
from accelerator.build import JobError
from accelerator import g
from accelerator import blob
from accelerator import statmsg
from accelerator import dataset
from accelerator import iowrapper


g_allesgut = False
_prof_fd = -1


g_always = {'running',}
assert set(n for n in dir(g) if not n.startswith("__")) == g_always, "Don't put anything in g.py"


# C libraries may not flush their stdio handles now that
# they are "files", so we do that after each stage.
clib = ctypes.cdll.LoadLibrary(None)
def c_fflush():
	clib.fflush(None)


def writeall(fd, data):
	while data:
		data = data[os.write(fd, data):]


def call_analysis(analysis_func, sliceno_, q, preserve_result, parent_pid, output_fds, **kw):
	try:
		# tell iowrapper our PID, so our output goes to the right status stack.
		# (the pty is not quite a transparent transport ('\n' transforms into
		# '\r\n'), so we use a fairly human readable encoding.)
		writeall(output_fds[sliceno_], b'%16x' % (os.getpid(),))
		# use our iowrapper fd instead of stdout/stderr
		os.dup2(output_fds[sliceno_], 1)
		os.dup2(output_fds[sliceno_], 2)
		for fd in output_fds:
			os.close(fd)
		slicename = 'analysis(%d)' % (sliceno_,)
		statmsg._start(slicename, parent_pid, 't')
		setproctitle(slicename)
		os.close(_prof_fd)
		kw['sliceno'] = g.sliceno = sliceno_
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
		from accelerator.extras import saved_files
		dw_lens = {}
		dw_minmax = {}
		for name, dw in dataset._datasetwriters.items():
			if dw._for_single_slice in (None, sliceno_,):
				dw.close()
				dw_lens[name] = dw._lens
				dw_minmax[name] = dw._minmax
		c_fflush()
		q.put((sliceno_, time(), saved_files, dw_lens, dw_minmax, None,))
	except:
		c_fflush()
		msg = fmt_tb(1)
		print(msg)
		q.put((sliceno_, time(), {}, {}, {}, msg,))
		sleep(5) # give launcher time to report error (and kill us)
		exitfunction()

def fork_analysis(slices, analysis_func, kw, preserve_result, output_fds):
	from multiprocessing import Process, Queue
	import gc
	q = Queue()
	children = []
	t = time()
	pid = os.getpid()
	if hasattr(gc, 'freeze'):
		# See https://bugs.python.org/issue31558
		# (Though we keep the gc disabled by default.)
		gc.freeze()
	for i in range(slices):
		p = Process(target=call_analysis, args=(analysis_func, i, q, preserve_result, pid, output_fds), kwargs=kw, name='analysis-%d' % (i,))
		p.start()
		children.append(p)
	for fd in output_fds:
		os.close(fd)
	per_slice = []
	temp_files = {}
	no_children_no_messages = False
	while len(per_slice) < slices:
		still_alive = []
		for p in children:
			if p.is_alive():
				still_alive.append(p)
			else:
				p.join()
				if p.exitcode:
					raise Exception("%s terminated with exitcode %d" % (p.name, p.exitcode,))
		children = still_alive
		# If a process dies badly we may never get a message here.
		# No need to handle that very quickly though, 10 seconds is fine.
		# (Typically this is caused by running out of memory.)
		try:
			s_no, s_t, s_temp_files, s_dw_lens, s_dw_minmax, s_tb = q.get(timeout=10)
		except QueueEmpty:
			if not children:
				# No children left, so they must have all sent their messages.
				# Still, just to be sure there isn't a race, wait one iteration more.
				if no_children_no_messages:
					raise Exception("All analysis processes exited cleanly, but not all returned a result.")
				else:
					no_children_no_messages = True
			continue
		if s_tb:
			data = [{'analysis(%d)' % (s_no,): s_tb}, None]
			writeall(_prof_fd, json.dumps(data).encode('utf-8'))
			exitfunction()
		per_slice.append((s_no, s_t))
		temp_files.update(s_temp_files)
		for name, lens in s_dw_lens.items():
			dataset._datasetwriters[name]._lens.update(lens)
		for name, minmax in s_dw_minmax.items():
			dataset._datasetwriters[name]._minmax.update(minmax)
	g.update_top_status("Waiting for all slices to finish cleanup")
	for p in children:
		p.join()
	if preserve_result:
		res_seq = ResultIterMagic(slices, reuse_msg="analysis_res is an iterator, don't re-use it")
	else:
		res_seq = None
	return [v - t for k, v in sorted(per_slice)], temp_files, res_seq

def args_for(func):
	kw = {}
	for arg in getarglist(func):
		kw[arg] = getattr(g, arg)
	return kw


def fmt_tb(skip_level):
	msg = []
	e_type, e, tb = sys.exc_info()
	tb = format_tb(tb)[skip_level:]
	if isinstance(e, JobError):
		msg.append(e.format_msg())
		tb = tb[:-5] # the five innermost are in build.py and of no interest.
	msg.append("Traceback (most recent call last):\n")
	msg.extend(tb)
	from accelerator.statmsg import _exc_status
	if len(_exc_status[1]) > 1:
		msg.append("Status when the exception occurred:\n")
		for ix, txt in enumerate(_exc_status[1], 1):
			msg.append("  " * ix)
			msg.append(txt)
			msg.append("\n")
	msg.extend(format_exception_only(e_type, e))
	return ''.join(msg)


def execute_process(workdir, jobid, slices, result_directory, common_directory, input_directory, index=None, workdirs=None, server_url=None, subjob_cookie=None, parent_pid=0):
	WORKDIRS.update(workdirs)

	g.job = jobid
	setproctitle('launch')
	path = os.path.join(workdir, jobid)
	try:
		os.chdir(path)
	except Exception:
		print("Cannot cd to workdir", path)
		exit(1)

	g.params = params = job_params()
	method_ref = import_module(params.package+'.a_'+params.method)
	g.sliceno = -1

	g.job = CurrentJob(jobid, params, result_directory, input_directory)
	g.slices = slices

	g.options          = params.options
	g.datasets         = params.datasets
	g.jobs             = params.jobs

	method_ref.options = params.options
	method_ref.datasets= params.datasets
	method_ref.jobs    = params.jobs

	g.server_url       = server_url
	g.running          = 'launch'
	statmsg._start('%s %s' % (jobid, params.method,), parent_pid)

	def dummy():
		pass

	prepare_func   = getattr(method_ref, 'prepare'  , dummy)
	analysis_func  = getattr(method_ref, 'analysis' , dummy)
	synthesis_func = getattr(method_ref, 'synthesis', dummy)

	synthesis_needs_analysis = 'analysis_res' in getarglist(synthesis_func)

	fd2pid, names, masters, slaves = iowrapper.setup(slices, prepare_func is not dummy, analysis_func is not dummy)
	def switch_output():
		fd = slaves.pop()
		os.dup2(fd, 1)
		os.dup2(fd, 2)
		os.close(fd)
	iowrapper.run_reader(fd2pid, names, masters, slaves)
	for fd in masters:
		os.close(fd)

	# A chain must be finished from the back, so sort on that.
	sortnum_cache = {}
	def dw_sortnum(name):
		if name not in sortnum_cache:
			dw = dataset._datasetwriters.get(name)
			if not dw: # manually .finish()ed
				num = -1
			elif dw.previous and dw.previous.startswith(jobid + '/'):
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
		switch_output()
		g.running = 'prepare'
		g.subjob_cookie = subjob_cookie
		setproctitle(g.running)
		with statmsg.status(g.running):
			g.prepare_res = method_ref.prepare(**args_for(method_ref.prepare))
			to_finish = [dw.name for dw in dataset._datasetwriters.values() if dw._started]
			if to_finish:
				with statmsg.status("Finishing datasets"):
					for name in sorted(to_finish, key=dw_sortnum):
						dataset._datasetwriters[name].finish()
		c_fflush()
		prof['prepare'] = time() - t
	switch_output()
	setproctitle('launch')
	from accelerator.extras import saved_files
	if analysis_func is dummy:
		prof['per_slice'] = []
		prof['analysis'] = 0
	else:
		t = time()
		g.running = 'analysis'
		g.subjob_cookie = None # subjobs are not allowed from analysis
		with statmsg.status('Waiting for all slices to finish analysis') as update:
			g.update_top_status = update
			prof['per_slice'], files, g.analysis_res = fork_analysis(slices, analysis_func, args_for(analysis_func), synthesis_needs_analysis, slaves)
			del g.update_top_status
		prof['analysis'] = time() - t
		saved_files.update(files)
	t = time()
	g.running = 'synthesis'
	g.subjob_cookie = subjob_cookie
	setproctitle(g.running)
	with statmsg.status(g.running):
		synthesis_res = synthesis_func(**args_for(synthesis_func))
		if synthesis_res is not None:
			blob.save(synthesis_res, temp=False)
		if dataset._datasetwriters:
			with statmsg.status("Finishing datasets"):
				for name in sorted(dataset._datasetwriters, key=dw_sortnum):
					dataset._datasetwriters[name].finish()
	if dataset._datasets_written:
		with g.job.open('datasets.txt', 'w', encoding='utf-8') as fh:
			for name in dataset._datasets_written:
				fh.write(name)
				fh.write(u'\n')
	c_fflush()
	t = time() - t
	prof['synthesis'] = t

	from accelerator.subjobs import _record
	return None, (prof, saved_files, _record)


def run(workdir, jobid, slices, result_directory, common_directory, input_directory, index=None, workdirs=None, server_url=None, subjob_cookie=None, parent_pid=0, prof_fd=-1):
	global g_allesgut, _prof_fd
	_prof_fd = prof_fd
	try:
		data = execute_process(workdir, jobid, slices, result_directory, common_directory, input_directory, index=index, workdirs=workdirs, server_url=server_url, subjob_cookie=subjob_cookie, parent_pid=parent_pid)
		g_allesgut = True
	except Exception:
		msg = fmt_tb(2)
		print(msg)
		data = [{g.running: msg}, None]
	writeall(prof_fd, json.dumps(data).encode('utf-8'))


def exitfunction():
	if not g_allesgut:
		print('LAUNCH:  The deathening!')
		os.killpg(os.getpgid(0), signal.SIGTERM)
