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


class Container(object):
    def __init__(self, name, properties={}):
        self._connections = {}
        self._timer_heap = []  # (next_tick, connection)

    def connect(self, name, properties={}):
        pass

    def disconnect(self, name):
        pass

    def need_io(self):
        """Return a pair of lists containing those connections that are
        read-blocked and write-ready.
        """
        readfd = []
        writefd = []
        for st in self._connections.itervalues():
            if st.need_read:
                readfd.append(st)
            if st.need_write:
                writefd.append(st)
        return (readfd, writefd)

    def get_next_tick(self):
        """Returns the next pending timeout timestamp in seconds since Epoch,
        or 0 if no pending timeouts.  process_io() must be called at least
        once prior to the timeout.
        """
        if not self._timer_heap:
            return 0
        return self._timer_heap[0][0]

    def process_io(self, readable, writable):
        """Does all I/O and timer related processing.  readble is a sequence of
        sockettransports whose sockets are readable.  'writable' is a sequence
        of sockettransports whose sockets are writable. This method returns a
        set of all the SocketTransport in the container that have been
        processed (this includes the arguments plus any that had timers
        processed).  Use get_next_tick() to determine when process_io() must
        be invoked again.
        """

        work_list = set()
        for st in readable:
            st.read_input()
            # check if process_read set a timer
            if st.next_tick == 0:
                st.tick(time.time())
            if st.next_tick:
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        # process any expired transport ticks
        while self._timer_heap and self._timer_heap[0][0] <= time.time():
            st = heapq.heappop(self._timer_heap)
            st.tick(time.time())
            if st.next_tick:   # timer reset
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        for st in writable:
            st.write_output()
            # check if process_write set a timer
            if st.next_tick == 0:
                st.tick(time.time())
            if st.next_tick:
                heapq.heappush(self._timer_heap, (st.next_tick, st))
            work_list.add(st)

        return work_list
