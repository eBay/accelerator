from distutils.core import setup, Extension

gzlinesmodule = Extension("gzlines", sources = ["gzlinesmodule.c"])

setup(name="gzlines", version="1.0", description="Read lines from gz files", ext_modules=[gzlinesmodule])
