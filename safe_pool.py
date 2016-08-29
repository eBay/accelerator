# The normal multiprocessing.Pool isn't safe because we set signal handlers,
# so we clear our SIGTERM handler for the workers here.

from multiprocessing import Pool as PyPool
from signal import signal, SIGTERM, SIG_DFL

def Pool(processes=None):
    return PyPool(processes=processes, initializer=signal, initargs=(SIGTERM, SIG_DFL,))
