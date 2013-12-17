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

import proton
from link import SenderLink, ReceiverLink

#
# An implementation of an AMQP 1.0 Connection
#

class ConnectionEventHandler(object):
    """
    """
    def connection_closed(self, connection, reason):
        pass

    def link_pending(self, connection, link):
        # return True to accept new link,
        # @todo False to reject
        return True

    def sasl_done(self, connection, result):
        # @todo sasl server support
        pass

class Connection(object):
    """
    """

    EOS = -1   # indicates 'I/O stream closed'

    def __init__(self, container, name, eventHandler=None, properties={}):
        """
        """
        self._name = name
        self._container = container
        self._pn_connection = proton.Connection()
        self._pn_connection.container = container.name
        if 'hostname' in properties:
            self._pn_connection.hostname = properties['hostname']

        self._pn_transport = proton.Transport()
        self._pn_transport.bind(self._pn_connection)
        # @todo - logging??
        self._pn_transport.trace(proton.Transport.TRACE_FRM)
        self._handler = eventHandler

        self._sender_links = {}
        self._receiver_links = {}
        self._read_done = False
        self._write_done = False
        self._next_tick = 0
        self._user_context = None

        # @todo sasl configuration and handling
        self._sasl = None
        self._sasl_done = False

        self._pn_connection.open()
        self._pn_session = self._pn_connection.session()
        self._pn_session.open()

    @property
    # @todo - hopefully remove
    def transport(self):
        return self._pn_transport

    @property
    # @todo - hopefully remove
    def connection(self):
        return self._pn_connection

    @property
    def name(self):
        return self._name

    @property
    # @todo - think about server side use of sasl!
    def sasl(self):
        if not self._sasl:
            self._sasl = self._pn_transport.sasl()
        return self._sasl

    def _get_user_context(self):
        return self._user_context

    def _set_user_context(self, ctxt):
        self._user_context = ctxt

    user_context = property(_get_user_context, _set_user_context,
                            doc="""
Associate an arbitrary user object with this Connection.
""")

    _NEED_INIT = proton.Endpoint.LOCAL_UNINIT
    _NEED_CLOSE = (proton.Endpoint.LOCAL_ACTIVE|proton.Endpoint.REMOTE_CLOSED)

    def process(self, now):
        """
        """
        self._next_tick = self._pn_transport.tick(now)

        # wait until SASL has authenticated
        # @todo Server-side SASL
        if self._sasl:
            if self._sasl.state not in (proton.SASL.STATE_PASS,
                                        proton.SASL.STATE_FAIL):
                print("SASL wait.")
                return

            self._handler.sasl_done(self, self.sasl.outcome)
            self._sasl = None

        if self._pn_connection.state & self._NEED_INIT:
            assert False, "Connection always opened() on create"

        ssn = self._pn_connection.session_head(self._NEED_INIT)
        while ssn:
            print "Opening remotely initiated session"
            ssn.open()
            ssn = ssn.next(self._NEED_INIT)

        link = self._pn_connection.link_head(self._NEED_INIT)
        while link:
            print "Link needs init"
            # @todo support remote link initiation
            # self._handler.link_pending(self, xxx)
            link = link.next(self._NEED_INIT)

        # ?any need for ACTIVE callback?

        # process the work queue

        delivery = self._pn_connection.work_head
        while delivery:
            print "Delivery updated!"
            if delivery.link.is_sender:
                sender_link = delivery.link.context
                sender_link._delivery_updated(delivery)
            else:
                receiver_link = delivery.link.context
                receiver_link._delivery_updated(delivery)
            delivery = delivery.work_next

        # close all endpoints closed by remotes

        link = self._pn_connection.link_head(self._NEED_CLOSE)
        while link:
            print "Link needs close"
            # @todo - find link, invoke callback
            link = link.next(self._NEED_CLOSE)

        ssn = self._pn_connection.session_head(self._NEED_CLOSE)
        while ssn:
            print "Session closed remotely"
            ssn.close()
            ssn = ssn.next(self._NEED_CLOSE)

        if self._pn_connection.state == (self._NEED_CLOSE):
            print "Connection remotely closed"
            # @todo - think about handling this wrt links!
            cond = self._pn_connection.remote_condition()
            self._pn_connection.close()
            self._handler.connection_closed(self, cond)

        return self._next_tick

    @property
    def next_tick(self):
        """Timestamp for next call to tick()
        """
        return self._next_tick

    @property
    def needs_input(self):
        """
        """
        if self._read_done:
            return self.EOS
        capacity = self._pn_transport.capacity()
        if capacity >= 0:
            return capacity
        self._read_done = True
        return self.EOS

    def process_input(self, in_data):
        """
        """
        c = self.needs_input
        if c <= 0:
            return c

        rc = self._pn_transport.push(in_data[:c])
        if rc:  # error
            self._read_done = True
            return self.EOS
        return c

    def close_input(self, reason=None):
        if not self._read_done:
            self._pn_transport.close_tail()
        self._read_done = True

    @property
    def has_output(self):
        """
        """
        if self._write_done:
            return self.EOS

        pending = self._pn_transport.pending()
        if pending >= 0:
            return pending
        self._write_done = True
        return self.EOS

    def output_data(self):
        """
        """
        c = self.has_output
        if c <= 0:
            return None

        buf = self._pn_transport.peek(c)
        return buf

    def output_written(self, count):
        self._pn_transport.pop(count)

    def close_output(self, reason=None):
        if not self._write_done:
            self._pn_transport.close_head()
        self._write_done = True

    @property
    def closed(self):
        return self._write_done and self._read_done

    def create_sender(self, source_address, target_address=None,
                      eventHandler=None, name=None, properties={}):
        """Factory for Sender links"""
        ident = name or str(source_address)
        if ident in self._sender_links:
            raise KeyError("Sender %s already exists!" % ident)

        pn_link = self._pn_session.sender(ident)
        if pn_link:
            s = SenderLink(self, pn_link, ident,
                           source_address, target_address,
                           eventHandler, properties)
            if s:
                self._sender_links[ident] = s
                return s
        return None

    def accept_sender(self, XXX):
        # @todo accept_sender(self, XXX):
        pass

    def reject_sender(self, reason, XXX):
        # @todo reject_sender(self, XXX):
        pass

    def create_receiver(self, target_address, source_address,
                        eventHandler, name=None, properties={}):
        """Factory for Receive links"""
        ident = name or str(target_address)
        if ident in self._receiver_links:
            raise KeyError("Receiver %s already exists!" % ident)

        pn_link = self._pn_session.receiver(ident)
        if pn_link:
            r = ReceiverLink(self, pn_link, ident, target_address,
                             source_address, eventHandler, properties)
            if r:
                self._receiver_links[ident] = r
                return r
        return None

    def accept_receiver(self, XXX):
        # @todo accept_receiver(self, XXX):
        pass

    def reject_receiver(self, reason, XXX):
        # @todo reject_receiver(self, XXX):
        pass

    def destroy(self, error=None):
        """
        """
        for l in self._sender_links.itervalues():
            l.destroy(error)
        self._sender_links = {}
        for l in self._receiver_links.itervalues():
            l.destroy(error)
        self._receiver_links = {}
        self._pn_session.close()
        self._pn_session = None
        self._pn_connection.close()
        self._pn_connection = None
        self._pn_transport = None
        self._user_context = None

    def _remove_sender(self, name):
        if name in self._sender_links:
            del self._sender_links[name]

    def _remove_receiver(self, name):
        if name in self._receiver_links:
            del self._receiver_links[name]


__all__ = [
    "ConnectionEventHandler",
    "Connection"
    ]
