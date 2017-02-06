#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from setuptools import setup

_VERSION = "2.1.4"   # NOTE: update __init__.py too!

# I hack, therefore I am (productive) Some distros (which will not be named)
# don't use setup.py to install the proton python module.  In this case, pip
# will not think proton is installed, and will attempt to install it,
# overwriting the distro's installation.  To prevent this, don't set the
# 'install_requires' if the proton python module is already installed
#
_dependencies = []
try:
    import proton
except ImportError:
    # this version of proton will download and install the proton shared
    # library as well:
    _dependencies = ['python-qpid-proton>=0.9,<0.17']


setup(name="pyngus",
      version=_VERSION,
      author="kgiusti",
      author_email="kgiusti@apache.org",
      packages=["pyngus"],
      package_dir={"pyngus": "pyngus"},
      description="Callback API implemented over Proton",
      url="https://github.com/kgiusti/pyngus",
      license="Apache Software License",
      install_requires=_dependencies,
      classifiers=["License :: OSI Approved :: Apache Software License",
                   "Intended Audience :: Developers",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python"])
