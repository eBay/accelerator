from __future__ import print_function
from __future__ import division

import os
import glob
import jobid as jobid_module

SLICES_FILENAME = 'slices.conf'


class WorkSpace:
    """ Handle all access to a single "physical" workdir. """

    def __init__(self, name, path, slices, writeable=False):
        """ name is workspace name, e.g. "churn" or "redlaser".  all jobids are prefixed by name.
            path is simply where all jobids are put.
            slices is number of slices for workdir, for example 12. """           
        self.name   = name
        self.path   = path
        self.slices = int(slices)
        self.valid_jobids = []
        self.ok = self._check_slices(writeable)


    def make_writeable(self):
        self._check_slices(True)


    def _check_slices(self, writeable):
        """ verify or write workdir specific slices file """
        filename = os.path.join(self.path, SLICES_FILENAME)
        ok = True
        try:
            with open(filename) as F:
                file_slices = int(F.read())
            if self.slices != file_slices:
                print("WORKSPACE:  ERROR, workspace has %d slices, but config file stipulates %d!" % (file_slices, self.slices))
                print("WORKSPACE:  Consequence:  ignore config file, use SLICES=%d." % (file_slices))
                self.slices = file_slices
        except Exception:
            if writeable:
                print("WORKSPACE:  create %s in %s." % (SLICES_FILENAME, self.path))
                with open(filename, 'wb') as F:
                    F.write(str(self.slices)+'\n')
            else:
                print("WORKSPACE:  not a workspace \"%s\" at \"%s\"" % (self.name, self.path,))
                ok = False
        if ok:
            print("WORKSPACE:  Set up \"%s\" : \"%s\" : %d" % (self.name, self.path, self.slices))
            return True
        else:
            return False


    def get_slices(self):
        """ return number of slices in workdir """
        return self.slices


    def get_path(self):
        return self.path


    def add_single_jobid(self, jobid):
        self.valid_jobids.append(jobid)

    def update(self, valid=True, parallelism=4):
        """
        read all jobids from disk, two things may happen:
            - valid = True
               valid (i.e. completed jobids) are stored in self.valid_jobids
            - valid = False
               function returns list of all (complete or not) jobids.
               This is used for "allocate_jobs" only.
         """
        globexpression = os.path.join(self.path, jobid_module.globexpression(self.name))
        jobidv = [d.rsplit('/', 1)[1] for d in glob.glob(globexpression)]
        if valid:
            from os.path import exists, join
            from safe_pool import Pool
            from itertools import compress
            pool = Pool(processes=parallelism)
            known = set(self.valid_jobids)
            cand  = set(jobidv)
            new   = cand - known
            good_known = list(known & cand)
            pathv = [join(self.path, j, 'post.json') for j in new]
            jobidv = list(compress(new, pool.map(exists, pathv, chunksize=64))) + good_known
            pool.close()
        jobidv = sorted(jobidv, key = lambda x: jobid_module.Jobid(x).number)
        if valid:
            self.valid_jobids = jobidv
        else:
            return jobidv


    def list_of_jobids(self, valid=True):
        """ return a list of all (valid) jobids in workdir """
        return self.valid_jobids


    def allocate_jobs(self, num_jobs):
        """ create num_jobs directories in self.path with jobid-compliant naming """
        highest = self._get_highest_jobnumber()
#        print('WORKSPACE:  Highest jobid is', highest)
        jobidv = [jobid_module.create(self.name, highest + 1, x) for x in range(num_jobs)]
        for jobid in jobidv:
            fullpath = os.path.join(self.path, jobid)
            print("WORKSPACE:  Allocate_job \"%s\"" % fullpath)
            os.mkdir(fullpath)
        return jobidv


    def _get_highest_jobnumber(self):
        """ get highest current jobid number """
        x = self.update(valid=False)
        if x:
            return jobid_module.Jobid(x[-1]).major
        else:
            return -1
