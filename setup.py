from distutils.core import setup, Extension

gzlinesmodule = Extension("gzlines", sources = ["gzlinesmodule.c"], libraries=["z"])

setup(name="gzlines", version="1.5.3", description="Read lines from gz files", ext_modules=[gzlinesmodule])
