import time
import configfile
import dependency
import dispatch

import workspace
import database
import methods
from extras import json_save
from setupfile import update_setup
from jobid import resolve_jobid_filename, put_workspaces
from extras import DotDict, Temp

from threading import Thread
from os import unlink
from os.path import join

METHODS_CONFIGFILENAME = 'methods.conf'
#DIRECTORIES      = ['analysis', 'default_analysis', ]
#Methods = methods.SubMethods(DIRECTORIES, CONFIGFILENAME)



class Main:
    """ This is the main controller behind the daemon. """

    def __init__(self, options, daemon_url):
        """
        Setup objects:

          Methods

          WorkSpaces

        """
        self.config = configfile.get_config(options.config, verbose=False)
        self.debug = options.debug
        self.daemon_url = daemon_url
        # check config file
        configfile.sanity_check(self.config)
        self._update_methods()
        # initialise workspaces
        self.workspaces = {}
        for name, data in self.config['workspace'].items():
            path   = data[0]
            slices = data[1]
            w = workspace.WorkSpace(name, path, slices)
            if w.ok:
                # add only if everything whent well in __init__
                self.workspaces[name] = w
            else:
                # hmm, maybe new target workspace
                if name == self.config['main_workspace']:
                    self.workspaces[name] = workspace.WorkSpace(name, path, slices, True)

        put_workspaces({k: v.path for k, v in self.workspaces.iteritems()})
        # set current workspace pointers
        self.set_workspace(self.config['main_workspace'])
        self.set_remote_workspaces(self.config.get('remote_workspaces', ''))
        # and update contents
        self.DataBase = database.DataBase(self)
        self.update_database()
        self.broken = False

        # Collect all valid fds, so we can close them in job processes
        from fcntl import fcntl, F_GETFD
        from resource import getrlimit, RLIMIT_NOFILE
        valid_fds = []
        for fd in range(3, getrlimit(RLIMIT_NOFILE)[0]):
            try:
                fcntl(fd, F_GETFD)
                valid_fds.append(fd)
            except Exception:
                pass
        self.valid_fds = valid_fds

    def _update_methods(self):
        print 'Update methods'
        # initialise methods class looking in method_directories from config file
        method_directories = self.config['method_directories']
        self.Methods = methods.SubMethods(method_directories, METHODS_CONFIGFILENAME)

    def update_methods(self):
        try:
            self._update_methods()
            self.update_database()
            self.broken = False
        except methods.MethodLoadException as e:
            self.broken = e.module_list
            return {'broken': e.module_list}


    def set_workspace(self, workspacename):
        """ set current workspace by name, and clear all remotes, just to be sure """
        self.current_workspace = workspacename
        self.current_remote_workspaces = set()
        self.workspaces[workspacename].make_writeable()

    def set_remote_workspaces(self, workspaces):
        slices = self.workspaces[self.current_workspace].get_slices()
        self.current_remote_workspaces = set()
        for name in workspaces:
            if self.workspaces[name].get_slices() == slices:
                self.current_remote_workspaces.add(name)
            else:
                print "Warning, could not add remote workspace \"%s\", since it has %d slices (and %d required from \"%s\")" % (
                    name, self.workspaces[name].get_slices(), slices, self.current_workspace)


    def get_workspace_details(self):
        """ Some information about main workspace, some parts of config """
        return dict(
            [(key, getattr(self.workspaces[self.current_workspace], key),) for key in ('name', 'path', 'slices',)] +
            [(key, self.config.get(key),) for key in ('source_directory', 'result_directory', 'common_directory', 'urd',)]
        )


    def list_workspaces(self):
        """ Return list of all initiated workspaces """
        return self.workspaces


    def get_current_workspace(self):
        """ return name of current workspace """
        return self.current_workspace


    def get_current_remote_workspaces(self):
        """ return names of current remote workspaces """
        return self.current_remote_workspaces


    def add_single_jobid(self, jobid):
        ws = self.workspaces[jobid.split('-', 1)[0]]
        ws.add_single_jobid(jobid)
        self.DataBase.add_single_jobid(jobid)

    def update_database(self):
        """Insert all new jobids (from all workspaces) in database,
        discard all deleted or with incorrect hash.
        """
        t_l = []
        for name in self.workspaces:
            # Run all updates in parallel. This gets all (sync) listdir calls
            # running at the same time. Then each workspace will spawn processes
            # to do the post.json checking, to keep disk queues effective. But
            # try to run a reasonable total number of post.json checkers.
            parallelism = max(3, int(self.workspaces[name].slices / len(self.workspaces)))
            t = Thread(target=self.workspaces[name].update,
                       kwargs=dict(parallelism=parallelism),
                       name='Update ' + name
                      )
            t.daemon = True
            t.start()
            t_l.append(t)
        for t in t_l:
            t.join()
        # These run one at a time, but they will spawn SLICES workers for
        # reading and parsing files. (So unless workspaces are on different
        # disks this is probably better.)
        for name in [self.current_workspace] + list(self.current_remote_workspaces):
            self.DataBase.update_workspace(self.workspaces[name])
        self.DataBase.update_finish(self.Methods.hash)


    def initialise_jobs(self, setup):
        """ Updata database, check deps, create jobids. """
        return dependency.initialise_jobs(
            setup,
            self.workspaces[self.current_workspace],  # target workspace
            self.DataBase,
            self.Methods)


    def run_job(self, jobid, partial=False, subjob_cookie=None, parent_pid=0):
        """ Run analysis and synthesis for jobid in current workspace from WorkSpaceStorage """
        W = self.workspaces[self.current_workspace]
        #
        active_workspaces = {}
        for name in [self.current_workspace] + list(self.current_remote_workspaces):
            active_workspaces[name] = self.workspaces[name].get_path()
        slices = self.workspaces[self.current_workspace].get_slices()

        launcher = dispatch.launch_all
        if partial:
            print 'RUN PARTIAL:', partial
            if jobid not in W.list_of_jobids(valid=False):
                print "ERROR, cannot update jobid \"%s\" - it does not exist" % jobid
                return
            x = {
                'analysis' : dispatch.launch_analysis,
                'synthesis' : dispatch.launch_synthesis,}
            launcher = x[partial]
        t0 = time.time()
        prof = update_setup(jobid, starttime=t0).profile or DotDict()
        new_prof, files, subjobs = launcher(W.path, jobid, self.config, self.Methods, active_workspaces, slices, self.debug, self.daemon_url, subjob_cookie, self.valid_fds, parent_pid)
        if self.debug:
            delete_from = Temp.TEMP
        else:
            delete_from = Temp.DEBUG
        for filename, temp in files.items():
            if temp >= delete_from:
                unlink(join(W.path, jobid, filename))
                del files[filename]
        prof.update(new_prof)
        prof.total = 0
        prof.total = sum(v for v in prof.itervalues() if isinstance(v, (float, int)))
        data = dict(
                    starttime=t0,
                    endtime=time.time(),
                    profile=prof,
                   )
        update_setup(jobid, **data)
        data['files'] = files
        data['subjobs'] = subjobs
        json_save(data, resolve_jobid_filename(jobid, 'post.json'))


    def get_methods(self):
        return self.Methods.db


    def method_info(self, method):
        return self.Methods.db.get(method, '')

