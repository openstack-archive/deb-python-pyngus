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
#


class Test(object):

    def __init__(self, name):
        self.name = name

    def configure(self, config):
        self.config = config

    def default(self, name, value, **profiles):
        default = value
        profile = self.config.defines.get("profile")
        if profile:
            default = profiles.get(profile, default)
        return self.config.defines.get(name, default)

    @property
    def delay(self):
        return float(self.default("delay", "1", fast="0.1"))

    @property
    def timeout(self):
        return float(self.default("timeout", "60", fast="10"))

    @property
    def verbose(self):
        return int(self.default("verbose", 0))


class Skipped(Exception):
    """Throw to skip a test without generating a test failure."""
    skipped = True
