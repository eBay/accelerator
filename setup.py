from distutils.core import setup, Extension

gzutilmodule = Extension("gzutil", sources = ["siphash24.c", "gzutilmodule.c"], libraries=["z"], extra_compile_args=['-std=c99', '-O3'])

setup(name="gzutil", version="2.3.1", description="Read/write values from/to gz files", ext_modules=[gzutilmodule])
