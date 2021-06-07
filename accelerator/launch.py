############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2021 Carl Drougge                       #
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
from time import sleep
import json
import ctypes

from accelerator.job import CurrentJob, WORKDIRS
from accelerator.compat import pickle, iteritems, setproctitle, QueueEmpty
from accelerator.compat import getarglist, monotonic
from accelerator.extras import job_params, ResultIterMagic
from accelerator.build import JobError
from accelerator.lockfree_queue import LockFreeQueue
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


def call_analysis(analysis_func, sliceno_, delayed_start, q, preserve_result, parent_pid, output_fds, **kw):
	try:
		q.make_writer()
		# tell iowrapper our PID, so our output goes to the right status stack.
		# (the pty is not quite a transparent transport ('\n' transforms into
		# '\r\n'), so we use a fairly human readable encoding.)
		writeall(output_fds[sliceno_], b'%16x' % (os.getpid(),))
		# use our iowrapper fd instead of stdout/stderr
		os.dup2(output_fds[sliceno_], 1)
		os.dup2(output_fds[sliceno_], 2)
		for fd in output_fds:
			os.close(fd)
		os.close(_prof_fd)
		slicename = 'analysis(%d)' % (sliceno_,)
		setproctitle(slicename)
		if delayed_start:
			os.close(delayed_start[1])
			update = statmsg._start('waiting for concurrency limit (%d)' % (sliceno_,), parent_pid, True)
			if os.read(delayed_start[0], 1) != b'a':
				raise Exception('bad delayed_start, giving up')
			update(slicename)
			os.close(delayed_start[0])
		else:
			statmsg._start(slicename, parent_pid, True)
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
		dw_compressions = {}
		for name, dw in dataset._datasetwriters.items():
			if dw._for_single_slice or sliceno_ == 0:
				dw_compressions[name] = dw._compressions
			if dw._for_single_slice in (None, sliceno_,):
				dw.close()
				dw_lens[name] = dw._lens
				dw_minmax[name] = dw._minmax
		c_fflush()
		q.put((sliceno_, monotonic(), saved_files, dw_lens, dw_minmax, dw_compressions, None,))
		q.close()
	except:
		c_fflush()
		msg = fmt_tb(1)
		print(msg)
		q.put((sliceno_, monotonic(), {}, {}, {}, {}, msg,))
		q.close()
		sleep(5) # give launcher time to report error (and kill us)
		exitfunction()

def fork_analysis(slices, concurrency, analysis_func, kw, preserve_result, output_fds, q):
	from multiprocessing import Process
	import gc
	children = []
	t = monotonic()
	pid = os.getpid()
	if hasattr(gc, 'freeze'):
		# See https://bugs.python.org/issue31558
		# (Though we keep the gc disabled by default.)
		gc.freeze()
	delayed_start = False
	delayed_start_todo = 0
	for i in range(slices):
		if i == concurrency:
			assert concurrency != 0
			# The rest will wait on this queue
			delayed_start = os.pipe()
			delayed_start_todo = slices - i
		p = Process(target=call_analysis, args=(analysis_func, i, delayed_start, q, preserve_result, pid, output_fds), kwargs=kw, name='analysis-%d' % (i,))
		p.start()
		children.append(p)
	for fd in output_fds:
		os.close(fd)
	if delayed_start:
		os.close(delayed_start[0])
	q.make_reader()
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
		# (iowrapper tries to tell us though.)
		# No need to handle that very quickly though, 10 seconds is fine.
		# (Typically this is caused by running out of memory.)
		try:
			msg = q.get(timeout=10)
			if not msg:
				# Notification from iowrapper, so we wake up (quickly) even if
				# the process died badly (e.g. from running out of memory).
				continue
			s_no, s_t, s_temp_files, s_dw_lens, s_dw_minmax, s_dw_compressions, s_tb = msg
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
		if delayed_start_todo:
			# Another analysis is allowed to run now
			os.write(delayed_start[1], b'a')
			delayed_start_todo -= 1
		per_slice.append((s_no, s_t))
		temp_files.update(s_temp_files)
		for name, lens in s_dw_lens.items():
			dataset._datasetwriters[name]._lens.update(lens)
		for name, minmax in s_dw_minmax.items():
			dataset._datasetwriters[name]._minmax.update(minmax)
		for name, compressions in s_dw_compressions.items():
			dataset._datasetwriters[name]._compressions.update(compressions)
	g.update_top_status("Waiting for all slices to finish cleanup")
	q.close()
	if delayed_start:
		os.close(delayed_start[1])
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


def execute_process(workdir, jobid, slices, concurrency, result_directory, common_directory, input_directory, index=None, workdirs=None, server_url=None, subjob_cookie=None, parent_pid=0):
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
	if analysis_func is dummy:
		q = None
	else:
		q = LockFreeQueue()
	iowrapper.run_reader(fd2pid, names, masters, slaves, q=q)
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
		t = monotonic()
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
		prof['prepare'] = monotonic() - t
	switch_output()
	setproctitle('launch')
	from accelerator.extras import saved_files
	if analysis_func is dummy:
		prof['per_slice'] = []
		prof['analysis'] = 0
	else:
		t = monotonic()
		g.running = 'analysis'
		g.subjob_cookie = None # subjobs are not allowed from analysis
		with statmsg.status('Waiting for all slices to finish analysis') as update:
			g.update_top_status = update
			prof['per_slice'], files, g.analysis_res = fork_analysis(slices, concurrency, analysis_func, args_for(analysis_func), synthesis_needs_analysis, slaves, q)
			del g.update_top_status
		prof['analysis'] = monotonic() - t
		saved_files.update(files)
	t = monotonic()
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
		blob.save(dataset._datasets_written, 'DS/LIST', temp=False, _hidden=True)
	c_fflush()
	t = monotonic() - t
	prof['synthesis'] = t

	from accelerator.subjobs import _record
	return None, (prof, saved_files, _record)


def run(workdir, jobid, slices, concurrency, result_directory, common_directory, input_directory, index=None, workdirs=None, server_url=None, subjob_cookie=None, parent_pid=0, prof_fd=-1, debuggable=False):
	global g_allesgut, _prof_fd
	_prof_fd = prof_fd
	try:
		data = execute_process(workdir, jobid, slices, concurrency, result_directory, common_directory, input_directory, index=index, workdirs=workdirs, server_url=server_url, subjob_cookie=subjob_cookie, parent_pid=parent_pid)
		g_allesgut = True
	except Exception:
		msg = fmt_tb(2)
		print(msg)
		data = [{g.running: msg}, None]
		writeall(prof_fd, json.dumps(data).encode('utf-8'))
	finally:
		exitfunction()
	writeall(prof_fd, json.dumps(data).encode('utf-8'))


def exitfunction():
	if not g_allesgut:
		print('LAUNCH:  The deathening!')
		os.killpg(os.getpgid(0), signal.SIGKILL)
