#!/usr/bin/env python2.7
# -*- coding: iso-8859-1 -*-

from web import ThreadedHTTPServer, ThreadedUnixHTTPServer, BaseWebHandler

import sys
import argparse
import socket
import traceback
import signal
import os
import control
import configfile
import resource
import autoflush
import time
from stat import S_ISSOCK
from threading import Thread, Lock as TLock, Lock as JLock
from string import ascii_letters
import random
import atexit

from extras import json_encode, json_decode, DotDict
from dispatch import JobError
from status import statmsg_sink, children, print_status_stacks, status_stacks_export



DEBUG_WRITE_JSON = False


def gen_cookie(size=16):
    return ''.join(random.choice(ascii_letters) for _ in range(size))

# This contains cookie: {lock, last_error} for all jobs, main jobs have cookie None.
job_tracking = {None: DotDict(lock=JLock(), last_error=None)}


# This needs .ctrl to work. It is set from main()
class XtdHandler(BaseWebHandler):
    server_version = "scx/0.1"
    DEBUG =  not True

    def log_message(self, format, *args):
        return

    def encode_body(self, body):
        if isinstance(body, str):
            return body
        if isinstance(body, unicode):
            return body.encode('utf-8')
        return json_encode(body)

    def handle_req(self, path, args):
        if self.DEBUG:  print >>sys.stderr, "@daemon.py:  Handle_req, path = \"%s\", args = %s" %( path, args )
        try:
            self._handle_req( path, args )
        except Exception:
            traceback.print_exc()
            self.do_response(500, "text/plain", "ERROR")

    def _handle_req(self, path, args):
        if path[0] == 'status':
            data = job_tracking.get(args.get('subjob_cookie') or None)
            if not data:
                self.do_response(500, 'text/plain', 'bad subjob_cookie!\n' )
                return
            timeout = min(float(args.get('timeout', 0)), 128)
            status = DotDict(idle=data.lock.acquire(False))
            deadline = time.time() + timeout
            while not status.idle and time.time() < deadline:
                time.sleep(0.1)
                status.idle = data.lock.acquire(False)
            if status.idle:
                if data.last_error:
                    status.last_error = data.last_error
                    data.last_error = None
                data.lock.release()
            elif path == ['status', 'full']:
                status.status_stacks, status.current = status_stacks_export()
            self.do_response(200, "text/json", status)
            return

        elif path==['list_workspaces']:
            ws = {k: v.path for k, v in self.ctrl.list_workspaces().items()}
            self.do_response(200, "text/json", ws)

        elif path==['config']:
            self.do_response(200, "text/json", self.ctrl.config)

        elif path==['update_methods']:
            self.do_response(200, "text/json", self.ctrl.update_methods())

        elif path==['methods']:
            """ return a json with everything the Method object knows about the methods """
            self.do_response(200, "text/json", self.ctrl.get_methods())

        elif path[0]=='method_info':
            method = path[1]
            self.do_response(200, "text/json", self.ctrl.method_info(method))

        elif path[0]=='set_workspace':
            _ws = path[1]
            if _ws not in self.ctrl.list_workspaces():
                self.do_response(500,'text/plain', 'Undefined workspace \"%s\"\n' % _ws)
            else:
                self.ctrl.set_workspace(_ws)
                self.do_response(200,'text/plain', 'Workspace set to \"%s\"\n' % _ws)

        elif path[0]=='workspace_info':
            self.do_response(200, 'text/json', self.ctrl.get_workspace_details())

        elif path[0] == 'abort':
            tokill = list(children)
            print 'Force abort ', tokill
            for child in tokill:
                os.killpg(child, signal.SIGKILL)
            self.do_response(200, 'text/json', {'killed': len(tokill)})

        elif path[0]=='update':
            if job_tracking[None].lock.acquire(False):
                try:
                    extra = False
                    if len(path)>2:
                        extra = path[2]
                    jobid = path[1]
                    self.do_response(200,'text/plain', 'Updating jobid \"%s\"\n'%jobid)
                    self.ctrl.update_database()
                    self.ctrl.run_job(jobid, partial=extra)
                finally:
                    job_tracking[None].lock.release()
            else:
                self.do_response(200,"text/plain", "Busy doing work for you...\n" )
            return

        elif path==['submit']:
            if self.ctrl.broken:
                self.do_response(500, "text/json", {'broken': self.ctrl.broken, 'error': 'Broken methods: ' + ', '.join(sorted(m.split('.')[-1][2:] for m in self.ctrl.broken))})
            elif args.has_key('xml'):
                self.do_response(500, 'text/plain', 'JSON > XML!\n' )
            elif args.has_key('json'):
                if DEBUG_WRITE_JSON:
                    with open('DEBUG_WRITE.json', 'wb') as fh:
                        fh.write(args['json'])
                setup = json_decode(args['json'])
                data = job_tracking.get(setup.get('subjob_cookie') or None)
                if not data:
                    self.do_response(500, 'text/plain', 'bad subjob_cookie!\n' )
                    return
                if len(job_tracking) - 1 > 5: # max five levels
                    print 'Too deep subjob nesting!'
                    self.do_response(500, 'text/plain', 'Too deep subjob nesting')
                    return
                if data.lock.acquire(False):
                    respond_after = True
                    try:
                        if self.DEBUG:  print >>sys.stderr, '@daemon.py:  Got the lock!'
                        jobidv, job_res = self.ctrl.initialise_jobs(setup)
                        job_res['done'] = False
                        if jobidv:
                            error = []
                            tlock = TLock()
                            link2job = {j['link']: j for j in job_res['jobs'].itervalues()}
                            def run(jobidv, tlock):
                                for jobid in jobidv:
                                    passed_cookie = None
                                    # This is not a race - all higher locks are locked too.
                                    while passed_cookie in job_tracking:
                                        passed_cookie = gen_cookie()
                                    job_tracking[passed_cookie] = DotDict(lock=JLock(), last_error=None)
                                    try:
                                        self.ctrl.run_job(jobid, subjob_cookie=passed_cookie, parent_pid=setup.get('parent_pid', 0))
                                        # update database since a new jobid was just created
                                        self.ctrl.add_single_jobid(jobid)
                                        with tlock:
                                            link2job[jobid]['make'] = 'DONE'
                                    except JobError as e:
                                        error.append([e.jobid, e.method, e.status])
                                        with tlock:
                                            link2job[jobid]['make'] = 'FAIL'
                                        return
                                    finally:
                                        del job_tracking[passed_cookie]
                                # everything was built ok, update symlink
                                try:
                                    dn = self.ctrl.workspaces[self.ctrl.current_workspace].path
                                    ln = os.path.join(dn, "LATEST_")
                                    try:
                                        os.unlink(ln)
                                    except OSError:
                                        pass
                                    os.symlink(jobid, ln)
                                    os.rename(ln, os.path.join(dn, "LATEST"))
                                except Exception:
                                    pass # meh
                            t = Thread(target=run, name="job runner", args=(jobidv, tlock,))
                            t.daemon = True
                            t.start()
                            t.join(2) # give job two seconds to complete
                            with tlock:
                                for j in link2job.itervalues():
                                    if j['make'] in (True, 'FAIL',):
                                        respond_after = False
                                        job_res_json = json_encode(job_res)
                                        break
                            if not respond_after: # not all jobs are done yet, give partial response
                                self.do_response(200, "text/json", job_res_json)
                            t.join() # wait until actually complete
                            del tlock
                            del t
                            # verify that all jobs got built.
                            for j in link2job.itervalues():
                                jobid = j['link']
                                if j['make'] == True:
                                    # Well, crap.
                                    error.append([jobid, "unknown", {"INTERNAL": "Not built"}])
                                    print >>sys.stderr, "INTERNAL ERROR IN JOB BUILDING!"
                            data.last_error = error
                    except Exception as e:
                        if respond_after:
                            self.do_response(500, "text/json", {'error': str(e)})
                        raise
                    finally:
                        data.lock.release()
                    if respond_after:
                        job_res['done'] = True
                        self.do_response(200, "text/json", job_res)
                    if self.DEBUG:  print >>sys.stderr, "@daemon.py:  Process releases lock!" # note: has already done http response
                else:
                    self.do_response(200, 'text/plain', 'Busy doing work for you...\n')
            else:
                self.do_response(500, 'text/plain', 'Missing json input!\n' )
        else:
            self.do_response(500, 'text/plain', 'Unknown path\n' )
            return


def parse_args(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--config', default='../conf/FRAMEWORK_CONFIG', metavar='CONFIG_FILE', help='Configuration file')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--port', type=int, help='Listen on tcp port')
    group.add_argument('--socket', help='Listen on unix socket', default='socket.dir/default')
    return parser.parse_args(argv)


def exitfunction(*a):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print
    print 'The daemon deathening! %d %s' % (os.getpid(), children,)
    print
    for child in children:
        os.killpg(child, signal.SIGKILL)
    os.killpg(os.getpgid(0), signal.SIGKILL)
    os._exit(1) # we really should be dead already

def check_socket(fn):
    dn = os.path.dirname(fn)
    try:
        os.mkdir(dn, 0o750)
    except OSError:
        pass
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(fn)
    except socket.error:
        try:
            assert S_ISSOCK(os.lstat(fn).st_mode), fn + " exists as non-socket"
            os.unlink(fn)
        except OSError:
            pass
        return
    raise Exception("Socket %s already listening" % (fn,))

def siginfo(sig, frame):
    print_status_stacks()

def main(options):

    # all forks belong to the same happy family
    try:
        os.setpgrp()
    except OSError:
        print >>sys.stderr, "Failed to create process group - there is probably already one (daemontools)."

    # increase number of open file per process
    r1, r2 = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (r2, r2))
    print "DAEMON:  Set max number of open files to (%d, %d)" % resource.getrlimit(resource.RLIMIT_NOFILE)

    # setup statmsg sink and tell address using ENV
    statmsg_rd, statmsg_wr = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    os.environ['BD_STATUS_FD'] = str(statmsg_wr.fileno())
    def buf_up(fh, opt):
        sock = socket.fromfd(fh.fileno(), socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, opt, 256 * 1024)
    buf_up(statmsg_wr, socket.SO_SNDBUF)
    buf_up(statmsg_rd, socket.SO_RCVBUF)

    CONFIG = configfile.get_config(options.config)

    t = Thread(target=statmsg_sink, args=(CONFIG['logfilename'], statmsg_rd), name="statmsg sink")
    t.daemon = True
    t.start()

    # do all main-stuff, i.e. run server
    sys.stdout = autoflush.AutoFlush(sys.stdout)
    sys.stderr = autoflush.AutoFlush(sys.stderr)
    atexit.register(exitfunction)
    signal.signal(signal.SIGTERM, exitfunction)
    signal.signal(signal.SIGINT, exitfunction)

    signal.signal(signal.SIGUSR1, siginfo)
    signal.siginterrupt(signal.SIGUSR1, False)
    if hasattr(signal, 'SIGINFO'):
        signal.signal(signal.SIGINFO, siginfo)
        signal.siginterrupt(signal.SIGINFO, False)

    if options.port:
        server = ThreadedHTTPServer(('', options.port), XtdHandler)
        daemon_url = 'http://localhost:%d' % (options.port,)
    else:
        check_socket(options.socket)
        # We want the socket to be world writeable, protect it with dir permissions.
        u = os.umask(0)
        server = ThreadedUnixHTTPServer(options.socket, XtdHandler)
        os.umask(u)
        daemon_url = configfile.resolve_socket_url(options.socket)

    ctrl = control.Main(options, daemon_url)
    print "DAEMON:  Available workspaces"
    for x in ctrl.list_workspaces():
        print "DAEMON:    %s" % x
    print "DAEMON:  Current workspace is          \"%s\"" % ctrl.get_current_workspace()
    print "DAEMON:  Current remote workspaces are %s" % ', '.join(['\"' + x + '\"' for x in ctrl.get_current_remote_workspaces()])

    XtdHandler.ctrl = ctrl

    print >>sys.stderr, "Start serving on port %s." % (options.port or options.socket,)
    print '-' * 79
    print
    server.serve_forever()



if __name__ == "__main__":
    from warnings import filterwarnings
    # Re-importing methods that use cffi should always be harmless here, don't bug the user.
    filterwarnings("ignore", message=r"reimporting '_cffi[^']*' might overwrite older definitions", category=UserWarning)
    options = parse_args(sys.argv[1:])
    main(options)
