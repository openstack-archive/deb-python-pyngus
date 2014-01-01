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
import heapq, logging
from connection import Connection

LOG = logging.getLogger(__name__)

#
# An implementation of an AMQP 1.0 container
#

class ContainerEventHandler(object):
    # @todo - ContainerEventHandler
    pass


class Container(object):
    def __init__(self, name, eventHandler=None, properties={}):
        self._name = name
        self._connections = {}
        self._timer_heap = []  # (next_tick, connection)
        self._need_processing = set()
        self._handler = eventHandler
        self._properties = properties

    @property
    def name(self):
        return self._name

    def create_connection(self, name, eventHandler=None, properties={}):
        if name in self._connections:
            raise KeyError("connection '%s' already exists" % str(name))
        conn = Connection(self, name, eventHandler, properties)
        if conn:
            self._connections[name] = conn
        return conn

    def need_processing(self):
        """A utility to help determine which connections need
        processing. Returns a triple of lists containing those connections that
        0) need to read from the network, 1) need to write to the network, 2)
        waiting for pending timers to expire.  The timer list is sorted with
        the connection next expiring at index 0.
        """
        readers = []
        writers = []
        timer_heap = []
        for c in self._connections.itervalues():
            if c.needs_input > 0:
                readers.append(c)
            if c.has_output > 0:
                writers.append(c)
            if c.next_tick:
                heapq.heappush(timer_heap, (c.next_tick, c))
        timers = []
        while timer_heap:
            x = heapq.heappop(timer_heap)
            timers.append(x[1])

        return (readers, writers, timers)

    def resolve_sender(self, target_address):
        pass

    def resolve_receiver(self, source_address):
        pass

    def get_connection(self, name):
        return self._connections.get(name, None)

    def _remove_connection(self, name):
        if name in self._connections:
            del self._connections[name]

__all__ = [
    "ContainerEventHandler",
    "Container"
    ]
