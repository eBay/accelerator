from distutils.core import setup, Extension

gzutilmodule = Extension("gzutil", sources = ["gzutilmodule.c"], libraries=["z"], extra_compile_args=['-std=c99'])

setup(name="gzutil", version="2.0.5", description="Read/write values from/to gz files", ext_modules=[gzutilmodule])
