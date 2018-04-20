
Helper library used by the [Accelerator](https://github.com/eBay/accelerator).



Use and Purpose
===============

This is one of three repositories that are intended to be used together:
1.    https://github.com/eBay/accelerator-project_skeleton
2.    https://github.com/eBay/accelerator-gzutil
3.    https://github.com/eBay/accelerator

The purpose of the Accelerator project is to allow for fast data processing with big data. Extensive documentation on the purpose and how to use the Accelerator projects is covered in the reference manual found here:

[Reference Manual](https://berkeman.github.io/pdf/acc_manual.pdf) \
[Installation Manual](https://berkeman.github.io/pdf/acc_install.pdf)



Build and Runtime Environment
=============================

The Accelerator projects has been built, tested, and runs on:
 - Ubuntu16.04 and Debian 9,
 - FreeBSD 11.1

but is in no way limited to these systems or versions.



Installation
============

1. Clone the https://github.com/eBay/accelerator-project_skeleton repository.
2. Install dependencies.  On Debian and Ubuntu

    ```sudo apt-get install build-essential python-dev python3-dev zlib1g-dev git virtualenv```

3. Run the setup script
    ```
    cd accelerator-project_skeleton
    ./init.py
    ```
    Please read and modify this script according to your needs.
4. Done.  The Accelerator is now ready for use.

The init.py script will clone both the accelerator-gzutil and the main accelerator repositories.  The gzutil library will be set up in virtual environments for Python2 as well as Python3, and the Accelerator will be set up as a git submodule to the project_skeleton repository.



License
=======

Copyright 2017-2018 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
