#!/usr/bin/env python3.3
# -*- coding: iso-8859-1 -*-

from glob import glob
from os import stat
from os.path import exists, join
from traceback import print_exc
from lxml import etree
from sys import stderr, exit
from collections import OrderedDict, namedtuple
from itertools import chain

Job = namedtuple("Job", "mtime id method fn caption depends")

class Jobtree:
	def __init__(self, basedir):
		self.all_setup = []
		self.all = OrderedDict()
		metas = glob(basedir + "/*/meta.txt")
		metas = sorted((stat(fn).st_mtime, fn) for fn in metas)
		for mtime, fn in metas:
			fn = fn.replace("meta.txt", "setup.xml")
			try:
				x = etree.parse(open(fn, "rb")).getroot()
				self.all_setup.append((fn.split("/")[-2], mtime, x))
			except Exception:
				print("Failed to read ", fn, ":", file=stderr)
				print_exc()
		for jobid, mtime, x in self.all_setup:
			method = x.find("method").text
			imp = x.find("options/import")
			if imp is None:
				fn = None
			else:
				fn = imp.get("filename").split("/")[-1]
			depends = set()
			for opt in x.find("options") or []:
				depends.update(chain(*[opt.get(a).split(",") for a in opt.attrib if a.startswith("jobid")]))
			for link in x.find("link"):
				depends.add(link.get("jobid"))
			for dep in depends:
				if dep not in self.all:
					print(jobid, "missing dep", dep)
			self.all[jobid] = Job(mtime, jobid, method, fn, x.get("caption") or "NO NAME", depends)
	
	def print_all(self):
		for job in self.all.values():
			self.print_job(job)
	
	def print_job(self, job, level=0):
		print("  " * level, job.id, job.method, job.caption)
		for cjob in self.all.values():
			if cjob.id in job.depends:
				self.print_job(cjob, level + 1)

if __name__ == "__main__":
	from sys import argv
	if len(argv) == 2:
		basedir = argv[1].rstrip("/")
		if exists(join(basedir, "meta.txt")):
			basedir, jobid = basedir.rsplit("/", 1)
		else:
			jobid = None
	elif len(argv) == 3:
		basedir, jobid = argv[1:]
		if exists(join(basedir, "meta.txt")):
			print("Don't specify a jobid with a jobdir", file=stderr)
			exit(1)
	else:
		print("Usage:", argv[0], "basedir_or_jobdir [jobid]", file=stderr)
		exit(1)
	jt = Jobtree(basedir)
	if jobid:
		jt.print_job(jt.all[jobid])
	else:
		jt.print_all()
