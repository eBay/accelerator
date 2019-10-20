############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#  http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
#                                                                          #
############################################################################

# The normal multiprocessing.Pool isn't safe because we set signal handlers,
# so we clear our SIGTERM handler for the workers here.

from multiprocessing import Pool as PyPool
from signal import signal, SIGTERM, SIG_DFL

def Pool(processes=None):
	return PyPool(processes=processes, initializer=signal, initargs=(SIGTERM, SIG_DFL,))
