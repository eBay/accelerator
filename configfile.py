import string
import re
import os
from urllib import quote_plus



def get_config( filename, verbose=True ):
    with open(filename) as F:
        c = parse_config(F.read(), filename)
    if verbose:
        print_config(c)
    return c


def print_config(config):
    print '-'*79
    X = {
        'method_directories' : lambda x : ', '.join('\"%s\"' % t for t in x),
        'workspace'          : lambda x : ''.join(['\n  -    %12s %40s %2d' %(
                    string.ljust(s,12), string.ljust(t[0],40), t[1]) for s, t in x.items()]), 
        }
    for x, y in config.items():
        print "  %30s :" % string.ljust(x, 30),
        if x in X:
            print X[x](y)
        else:
            print y
    print '-'*79


_re_var = re.compile(r'\$\{([^\}=]*)(?:=([^\}]*))?\}')
def _interpolate(s):
    """Replace ${FOO=BAR} with os.environ.get('FOO', 'BAR')
    (just ${FOO} is of course also supported, but not $FOO)"""
    return _re_var.subn(lambda m: os.environ.get(m.group(1), m.group(2)), s)[0]


def resolve_socket_url(path):
    if '://' in path:
        return path
    else:
        return 'unixhttp://' + quote_plus(os.path.realpath(path))

def parse_config(string, filename=None):
    ret = {}
    for line in string.split('\n'):
        line = line.split('#')[0].strip()
        if len(line)==0:
            continue
        try:
            key, val = line.split('=', 1)
            val = _interpolate(val)
            if key =='workspace':
                # create a dict {name : (path, slices), ...}
                ret.setdefault(key, {})
                val = val.split(':')
                name = val[0]
                path = val[1]
                if len(val)==2:
                    # there is no slice information
                    slices = -1
                else:
                    slices = val[2]
                ret[key][name] = (path, int(slices))
            elif key in ('remote_workspaces', 'method_directories',):
                # create a set of (name, ...)
                ret.setdefault(key, set())
                ret[key].update(val.split(','))
            elif key == 'urd':
                ret[key] = resolve_socket_url(val)
            else:
                ret[key] = val
        except:
            print "Error parsing config %s: \"%s\"" % (filename, line,)
    if not ret.has_key('workspace'):
        raise Exception("Error, missing workspace in config " + filename)
    return ret


def sanity_check(config_dict):
    ok = True
    if 'main_workspace' not in config_dict:
        print "# Error in configfile, must specify main_workspace."
        ok = False
    if 'workspace' not in config_dict:
        print "# Error in configfile, must specify at least one workspace."
        ok = False


    if not ok:
        exit(1)
