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

import socket, errno, time, heapq, select
from proton import Transport

class ConnectionEventHandler(object):
    def sasl_done(self, connection, result):
        # Not sure if this is enough for servers....
        pass
    def connection_closed(self, connection, reason):
        pass
    def link_pending(self, connection, link):
        # return True to accept new link,
        # @todo False to reject
        return True


class Connection(object):
    """Provides network I/O for a Proton connection via a socket-like object.
    """
    def __init__(self, socket, eventHandler, name=None):
        """socket - Python socket. Expected to be configured and connected.
        name - optional name for this SocketTransport
        """
        self._name = name
        self._socket = socket
        self._pn_transport = Transport()
        self._pn_connection = Connection()
        self._pn_transport.bind(self._pn_connection)
        self._pn_transport.trace(proton.Transport.TRACE_FRM)
        self._handler = eventHandler
        self._read_done = False
        self._write_done = False
        self._next_tick = 0

        self._sasl_done = False
        self._pn_connection.open()


    def fileno(self):
        """Allows use of a Connection by the python select() call.
        """
        return self._socket.fileno()

    @property
    # @todo - hopefully remove
    def transport(self):
        return self._pn_transport

    @property
    # @todo - hopefully remove
    def connection(self):
        return self._pn_connection

    @property
    def socket(self):
        return self._socket

    @property
    def name(self):
        return self._name

    @property
    # @todo - think about server side use of this!
    def sasl(self):
        return self._pn_transport.sasl()

    def tick(self, now):
        """Invoke the transport's tick method.  'now' is seconds since Epoch.
        Returns the timestamp for the next tick, or zero if no next tick.
        """
        self._next_tick = self._pn_transport.tick(now)
        return self._next_tick

    @property
    def next_tick(self):
        """Timestamp for next call to tick()
        """
        return self._next_tick

    @property
    def need_read(self):
        """True when the Transport requires data from the network layer.
        """
        return (not self._read_done) and self._pn_transport.capacity() > 0

    @property
    def need_write(self):
        """True when the Transport has data to write to the network layer.
        """
        return (not self._write_done) and self._pn_transport.pending() > 0

    def read_input(self):
        """Read from the network layer and processes all data read.  Can
        support both blocking and non-blocking sockets.
        """
        if self._read_done:
            return None

        c = self._pn_transport.capacity()
        if c < 0:
            try:
                self._socket.shutdown(socket.SHUT_RD)
            except:
                pass
            self._read_done = True
            return None

        if c > 0:
            try:
                buf = self._socket.recv(c)
                if buf:
                    self._pn_transport.push(buf)
                    return len(buf)
                # else socket closed
                self._pn_transport.close_tail()
                self._read_done = True
                return None
            except socket.timeout, e:
                raise  # let the caller handle this
            except socket.error, e:
                err = e.args[0]
                if (err != errno.EAGAIN and
                    err != errno.EWOULDBLOCK and
                    err != errno.EINTR):
                    # otherwise, unrecoverable:
                    self._pn_transport.close_tail()
                    self._read_done = True
                raise
            except:  # beats me...
                self._pn_transport.close_tail()
                self._read_done = True
                raise
        return 0

    def write_output(self):
        """Write data to the network layer.  Can support both blocking and
        non-blocking sockets.
        """
        # @todo - KAG: need to explicitly document error return codes/exceptions!!!
        if self._write_done:
            return None

        c = self._pn_transport.pending()
        if c < 0:  # output done
            try:
                self._socket.shutdown(socket.SHUT_WR)
            except:
                pass
            self._write_done = True
            return None

        if c > 0:
            buf = self._pn_transport.peek(c)
            try:
                rc = self._socket.send(buf)
                if rc > 0:
                    self._pn_transport.pop(rc)
                    return rc
                # else socket closed
                self._pn_transport.close_head()
                self._write_done = True
                return None
            except socket.timeout, e:
                raise # let the caller handle this
            except socket.error, e:
                err = e.args[0]
                if (err != errno.EAGAIN and
                    err != errno.EWOULDBLOCK and
                    err != errno.EINTR):
                    # otherwise, unrecoverable
                    self._pn_transport.close_head()
                    self._write_done = True
                raise
            except:
                self._pn_transport.close_tail()
                self._write_done = True
                # @todo - invoke callback (?)
                raise
        return 0

    @property
    def done(self):
        return self._write_done and self._read_done

    def process_endpoints(self):

        # wait until SASL has authenticated
        if not self._sasl_done:
            if self.sasl.state not in (SASL.STATE_PASS,
                                       SASL.STATE_FAIL):
                print("SASL wait.")
                return

            self._handler.sasl_done(self, self.sasl.state)
            self._sasl_done = True

        ssn = self._pn_connection.session_head(_NEED_INIT)
        while ssn:
            ssn.open()
            ssn = ssn.next(_NEED_INIT)

        link = self._pn_connection.link_head(_NEED_INIT)
        while link:
            # @todo - wrap link in the proper abstraction
            if self._handler.link_pending(self, link):
                link.open()
            link = link.next(_NEED_INIT)

        # @todo: any need for ACTIVE callback?
        # process the work queue

        delivery = self._pn_connection.work_head
        while delivery:
            !!! call back via the Link, no connection!!
            self._endpoint_cb.delivery_update(delivery)
            delivery = delivery.work_next

        # close all endpoints closed by remotes

        link = self._pn_connection.link_head(_NEED_CLOSE)
        while link:
            !!! Need link callback
            self._endpoint_cb.link_remote_closed(link)
            link = link.next(_NEED_CLOSE)

        ssn = self._pn_connection.session_head(_NEED_CLOSE)
        while ssn:
            ssn.close()
            ssn = ssn.next(_NEED_CLOSE)

        if self._pn_connection.state == (_NEED_CLOSE):
            # @todo determine reason, pass along
            self._handler.connection_closed(self, None)


    def create_sender(self, target_address, name=None, properties={}):
        """Factory for Sender links"""
        pass

    def create_receiver(self, source_address, eventHandler, name=None,
                        properties={}):
        """Factor for Receive links"""
        pass


__all__ = [
    "ConnectionEventHandler",
    "Connection"
    ]
