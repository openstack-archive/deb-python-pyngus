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

_VERSION = "1.3.0"   # NOTE: update __init__.py too!

try:
    # NOTE(flaper87): Hold your breath, don't kill any kittens
    # (or do it, that's fine) but certainly don't chase the author
    # of this patch. The reason we're doing this is because the proposed
    # patch that will (hopefully) land in master[0] targets the 0.10 release
    # of the library but the current stable version is 0.10. As soon as
    # the patch lands and 0.10 is out, this will be removed and we'll
    # all be back to our happy and ideal world where kgiusti has a blue Tesla.
    # [0] https://issues.apache.org/jira/browse/PROTON-885
    _qpid_proton = "-egit+https://github.com/FlaPer87/qpid-proton.git@0.9.x#egg=python-qpid-proton&subdirectory=proton-c/bindings/python"

    import subprocess
    subprocess.Popen(['pip', 'install', _qpid_proton]).wait()
except Exception:
    pass


setup(name="pyngus",
      version=_VERSION,
      author="kgiusti",
      author_email="kgiusti@apache.org",
      packages=["pyngus"],
      package_dir={"pyngus": "pyngus"},
      description="Callback API implemented over Proton",
      url="https://github.com/kgiusti/pyngus",
      license="Apache Software License",
      install_requires=['python-qpid-proton>=0.9,<0.10'],
      classifiers=["License :: OSI Approved :: Apache Software License",
                   "Intended Audience :: Developers",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python"])
