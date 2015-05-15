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
from . import common
import gc

import pyngus


class APITest(common.Test):

    def test_create_destroy(self):
        gc.enable()
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)
        container = pyngus.Container("My-Container")
        assert container.name == "My-Container"
        container.destroy()

    def test_create_connection(self):
        gc.enable()
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)
        container = pyngus.Container("A123")
        container.create_connection("c1")
        container.create_connection("c2")
        c1 = container.get_connection("c1")
        assert c1 and c1.name == "c1", "Connection not found!"
        c3 = container.get_connection("c3")
        assert not c3, "Bad connection returned!"
        container.destroy()
        del c1

    def test_cleanup(self):
        gc.enable()
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)
        container = pyngus.Container("abc")
        c1 = container.create_connection("c1")
        c2 = container.create_connection("c2")
        assert c2
        del c2
        gc.collect()
        c2 = container.get_connection("c2")
        assert c2
        c1 = container.get_connection("c1")
        assert c1
        c1.create_receiver("r1")
        c1.create_sender("s1")
        del c1
        del c2
        container.destroy()
        del container
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)

    def test_need_processing(self):
        gc.enable()
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)
        container = pyngus.Container("abc")
        c1 = container.create_connection("c1")
        c2 = container.create_connection("c2")
        props = {"idle-time-out": 10}
        c3 = container.create_connection("c3", properties=props)
        c4 = container.create_connection("c4")
        c1.open()
        c2.open()
        c3.open()
        c4.open()
        # every connection should need both input and output,
        # but no timers yet (connection's not up)
        r, w, t = container.need_processing()
        assert c1 in r and c1 in w
        assert c2 in r and c2 in w
        assert c3 in r and c3 in w
        assert c4 in r and c4 in w
        assert not t
        common.process_connections(c1, c2)
        common.process_connections(c3, c4)
        # After connections come up, no connection should need to do I/O, and
        # only c4 and c4 should be in the timer list:
        r, w, t = container.need_processing()
        assert c1 in r
        assert c2 in r
        assert c3 in r
        assert c4 in r
        assert not w
        assert len(t) == 2 and c3 in t and c4 in t
        container.destroy()
