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

__all__ = [
    "ConnectionEventHandler",
    "Connection"
    ]

import logging

import proton
from link import SenderLink, ReceiverLink

LOG = logging.getLogger(__name__)

#
# An implementation of an AMQP 1.0 Connection
#

class ConnectionEventHandler(object):
    """
    """
    def connection_active(self, connection):
        """connection handshake completed"""
        LOG.debug("connection_active (ignored)")

    def connection_remote_closed(self, connection, error=None):
        LOG.debug("connection_remote_closed (ignored)")

    def connection_closed(self, connection):
        LOG.debug("connection_closed (ignored)")

    def sender_requested(self, connection, link_handle,
                         name, requested_source,
                         properties={}):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        LOG.debug("sender_requested (ignored)")

    def receiver_requested(self, connection, link_handle,
                           name, requested_target,
                           properties={}):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        LOG.debug("receiver_requested (ignored)")

    # @todo cleaner sasl support, esp. server side
    def sasl_step(self, connection, pn_sasl):
        LOG.debug("sasl_step (ignored)")

    def sasl_done(self, connection, result):
        LOG.debug("sasl_done (ignored)")


class Connection(object):
    """
    """

    EOS = -1   # indicates 'I/O stream closed'

    def __init__(self, container, name, eventHandler=None, properties={}):
        """
        """
        self._name = name
        self._container = container
        self._handler = eventHandler

        self._pn_connection = proton.Connection()
        self._pn_connection.container = container.name
        if 'hostname' in properties:
            self._pn_connection.hostname = properties['hostname']

        self._pn_transport = proton.Transport()
        self._pn_transport.bind(self._pn_connection)
        if properties.get("trace"):
            self._pn_transport.trace(proton.Transport.TRACE_FRM)

        # indexed by link-name
        self._sender_links = {}    # SenderLink or pn_link if pending
        self._receiver_links = {}  # ReceiverLink or pn_link if pending

        self._read_done = False
        self._write_done = False
        self._next_tick = 0
        self._user_context = None
        self._active = False

        # @todo sasl configuration and handling
        self._pn_sasl = None
        self._sasl_done = False

    @property
    def container(self):
        return self._container

    @property
    # @todo - hopefully remove
    def pn_transport(self):
        return self._pn_transport

    @property
    # @todo - hopefully remove
    def pn_connection(self):
        return self._pn_connection

    @property
    def name(self):
        return self._name

    @property
    def remote_container(self):
        """Return the name of the remote container. Should be present once the
        connection is active.
        """
        return self._pn_connection.remote_container

    @property
    # @todo - think about server side use of sasl!
    def sasl(self):
        if not self._pn_sasl:
            self._pn_sasl = self._pn_transport.sasl()
        return self._pn_sasl

    def _get_user_context(self):
        return self._user_context

    def _set_user_context(self, ctxt):
        self._user_context = ctxt

    user_context = property(_get_user_context, _set_user_context,
                            doc="""
Associate an arbitrary user object with this Connection.
""")

    def open(self):
        """
        """
        self._pn_connection.open()
        self._pn_session = self._pn_connection.session()
        self._pn_session.open()

    def close(self, error=None):
        """
        """
        for l in self._sender_links.itervalues():
            l.close(error)
        for l in self._receiver_links.itervalues():
            l.close(error)
        self._pn_session.close()
        self._pn_connection.close()

    @property
    def closed(self):
        #return self._write_done and self._read_done
        state = self._pn_connection.state
        return state == (proton.Endpoint.LOCAL_CLOSED
                         | proton.Endpoint.REMOTE_CLOSED)

    def destroy(self):
        """
        """
        self._sender_links.clear()
        self._receiver_links.clear()
        self._container._remove_connection(self._name)
        self._container = None
        self._pn_connection = None
        self._pn_transport = None
        self._user_context = None

    def _link_requested(self, pn_link):
        if pn_link.is_sender and pn_link.name not in self._sender_links:
            LOG.debug("Remotely initiated Sender needs init")
            self._sender_links[pn_link.name] = pn_link
            pn_link.context = None # @todo: update proton.py
            req_source = ""
            if pn_link.remote_source.dynamic:
                req_source = None
            elif pn_link.remote_source.address:
                req_source = pn_link.remote_source.address
            self._handler.sender_requested(self, pn_link.name,
                                           pn_link.name, req_source,
                                           {"target-address":
                                            pn_link.remote_target.address})
        elif pn_link.is_receiver and pn_link.name not in self._receiver_links:
            LOG.debug("Remotely initiated Receiver needs init")
            self._receiver_links[pn_link.name] = pn_link
            pn_link.context = None # @todo: update proton.py
            req_target = ""
            if pn_link.remote_target.dynamic:
                req_target = None
            elif pn_link.remote_target.address:
                req_target = pn_link.remote_target.address
            self._handler.receiver_requested(self, pn_link.name,
                                             pn_link.name, req_target,
                                             {"source-address":
                                              pn_link.remote_source.address})

    _REMOTE_REQ = (proton.Endpoint.LOCAL_UNINIT|proton.Endpoint.REMOTE_ACTIVE)
    _REMOTE_CLOSE = (proton.Endpoint.LOCAL_ACTIVE|proton.Endpoint.REMOTE_CLOSED)
    _ACTIVE = (proton.Endpoint.LOCAL_ACTIVE|proton.Endpoint.REMOTE_ACTIVE)
    _CLOSED = (proton.Endpoint.LOCAL_CLOSED|proton.Endpoint.REMOTE_CLOSED)

    def process(self, now):
        """
        """
        self._next_tick = self._pn_transport.tick(now)

        # wait until SASL has authenticated
        # @todo Server-side SASL
        if self._pn_sasl:
            if self._pn_sasl.state not in (proton.SASL.STATE_PASS,
                                        proton.SASL.STATE_FAIL):
                LOG.debug("SASL in progress. State=%s", str(self._pn_sasl.state))
                self._handler.sasl_step(self, self._pn_sasl)
                return

            self._handler.sasl_done(self, self._pn_sasl.outcome)
            self._pn_sasl = None

        # do endpoint up handling:

        if self._pn_connection.state == self._ACTIVE:
            if not self._active:
                self._active = True
                self._handler.connection_active(self)

        pn_session = self._pn_connection.session_head(proton.Endpoint.LOCAL_UNINIT)
        while pn_session:
            LOG.debug("Opening remotely initiated session")
            pn_session.open()
            pn_session = pn_session.next(proton.Endpoint.LOCAL_UNINIT)

        pn_link = self._pn_connection.link_head(self._REMOTE_REQ)
        while pn_link:
            next_link = pn_link.next(proton.Endpoint.LOCAL_UNINIT)

            if pn_link.state == self._REMOTE_REQ:
                self._link_requested(pn_link)

            pn_link = next_link

        # @todo: won't scale?
        pn_link = self._pn_connection.link_head(self._ACTIVE)
        while pn_link:
            next_link = pn_link.next(self._ACTIVE)

            if pn_link.context and not pn_link.context._active:
                LOG.debug("Link is up")
                pn_link.context._active = True
                if pn_link.is_sender:
                    sender_link = pn_link.context
                    assert isinstance(sender_link, SenderLink)
                    sender_link._handler.sender_active(sender_link)
                else:
                    receiver_link = pn_link.context
                    assert isinstance(receiver_link, ReceiverLink)
                    receiver_link._handler.receiver_active(receiver_link)
            pn_link = next_link

        # process the work queue

        pn_delivery = self._pn_connection.work_head
        while pn_delivery:
            LOG.debug("Delivery updated!")
            next_delivery = pn_delivery.work_next
            if pn_delivery.link.context:
                if pn_delivery.link.is_sender:
                    sender_link = pn_delivery.link.context
                    sender_link._delivery_updated(pn_delivery)
                else:
                    receiver_link = pn_delivery.link.context
                    receiver_link._delivery_updated(pn_delivery)
            pn_delivery = next_delivery

        # do endpoint down handling:

        pn_link = self._pn_connection.link_head(self._REMOTE_CLOSE)
        while pn_link:
            LOG.debug("Link closed remotely")
            next_link = pn_link.next(self._REMOTE_CLOSE)
            # @todo: error reporting
            if pn_link.context:
                if pn_link.is_sender:
                    sender_link = pn_link.context
                    sender_link._handler.sender_remote_closed(sender_link, None)
                else:
                    receiver_link = pn_link.context
                    receiver_link._handler.receiver_remote_closed(receiver_link,
                                                                  None)
            pn_link = next_link

        pn_link = self._pn_connection.link_head(self._CLOSED)
        while pn_link:
            next_link = pn_link.next(self._CLOSED)
            if pn_link.context and pn_link.context._active:
                LOG.debug("Link close completed")
                pn_link.context._active = False
                if pn_link.is_sender:
                    sender_link = pn_link.context
                    sender_link._handler.sender_closed(sender_link)
                else:
                    receiver_link = pn_link.context
                    receiver_link._handler.receiver_closed(receiver_link)
            pn_link = next_link

        pn_session = self._pn_connection.session_head(self._REMOTE_CLOSE)
        while pn_session:
            LOG.debug("Session closed remotely")
            pn_session.close()
            pn_session = pn_session.next(self._REMOTE_CLOSE)

        if self._pn_connection.state == self._REMOTE_CLOSE:
            LOG.debug("Connection remotely closed")
            self._handler.connection_remote_closed(self, None)
        elif self._pn_connection.state == self._CLOSED:
            LOG.debug("Connection close complete")
            self._handler.connection_closed(self)

        # DEBUG LINK "LEAK"
        # count = 0
        # link = self._pn_connection.link_head(0)
        # while link:
        #     count += 1
        #     link = link.next(0)
        # print "Link Count %d" % count

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

    def create_sender(self, source_address, target_address=None,
                      eventHandler=None, name=None, properties={}):
        """Factory for Sender links"""
        ident = name or str(source_address)
        if ident in self._sender_links:
            raise KeyError("Sender %s already exists!" % ident)

        pn_link = self._pn_session.sender(ident)
        if pn_link:
            s = SenderLink(self, pn_link,
                           source_address, target_address,
                           eventHandler, properties)
            self._sender_links[ident] = s
            return s
        return None

    def accept_sender(self, link_handle, source_override=None,
                      event_handler=None, properties={}):
        pn_link = self._sender_links.get(link_handle)
        if not pn_link or not isinstance(pn_link, proton.Sender):
            raise Exception("Invalid link_handle: %s" % link_handle)
        if pn_link.remote_source.dynamic and not source_override:
            raise Exception("A source address must be supplied!")
        source_addr = source_override or pn_link.remote_source.address
        self._sender_links[link_handle] = SenderLink(self, pn_link,
                                                     source_addr,
                                                     pn_link.remote_target.address,
                                                     event_handler, properties)
        return self._sender_links[link_handle]

    def reject_sender(self, link_handle, reason):
        pn_link = self._sender_links.get(link_handle)
        if not pn_link or not isinstance(pn_link, proton.Sender):
            raise Exception("Invalid link_handle: %s" % link_handle)
        del self._sender_links[link_handle]
        # @todo support reason for close
        pn_link.close()

    def create_receiver(self, target_address, source_address=None,
                        eventHandler=None, name=None, properties={}):
        """Factory for Receive links"""
        ident = name or str(target_address)
        if ident in self._receiver_links:
            raise KeyError("Receiver %s already exists!" % ident)

        pn_link = self._pn_session.receiver(ident)
        if pn_link:
            r = ReceiverLink(self, pn_link, target_address,
                             source_address, eventHandler, properties)
            self._receiver_links[ident] = r
            return r
        return None

    def accept_receiver(self, link_handle, target_override=None,
                        event_handler=None, properties={}):
        pn_link = self._receiver_links.get(link_handle)
        if not pn_link or not isinstance(pn_link, proton.Receiver):
            raise Exception("Invalid link_handle: %s" % link_handle)
        if pn_link.remote_target.dynamic and not target_override:
            raise Exception("A target address must be supplied!")
        target_addr = target_override or pn_link.remote_target.address
        self._receiver_links[link_handle] = ReceiverLink(self, pn_link,
                                                         target_addr,
                                                         pn_link.remote_source.address,
                                                         event_handler, properties)
        return self._receiver_links[link_handle]

    def reject_receiver(self, link_handle, reason):
        pn_link = self._receiver_links.get(link_handle)
        if not pn_link or not isinstance(pn_link, proton.Receiver):
            raise Exception("Invalid link_handle: %s" % link_handle)
        del self._receiver_links[link_handle]
        # @todo support reason for close
        pn_link.close()


    def _remove_sender(self, name):
        if name in self._sender_links:
            del self._sender_links[name]

    def _remove_receiver(self, name):
        if name in self._receiver_links:
            del self._receiver_links[name]

