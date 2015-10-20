from distutils.core import setup, Extension

gzutilmodule = Extension("gzutil", sources = ["gzutilmodule.c"], libraries=["z"])

setup(name="gzutil", version="2.0.0", description="Read/write values from/to gz files", ext_modules=[gzutilmodule])
