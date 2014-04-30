#    Licensed to the Apache Software Foundation (ASF) under one
#    or more contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

__all__ = [
    "ConnectionEventHandler",
    "Connection"
]

import heapq
import logging
import proton
import time
import warnings

from pyngus.endpoint import Endpoint
from pyngus.link import _SessionProxy

LOG = logging.getLogger(__name__)


class ConnectionEventHandler(object):
    """An implementation of an AMQP 1.0 Connection."""
    def connection_active(self, connection):
        """Connection handshake has completed."""
        LOG.debug("connection_active (ignored)")

    def connection_failed(self, connection, error):
        """Connection's transport has failed in some way."""
        LOG.warn("connection_failed, error=%s (ignored)", str(error))

    def connection_remote_closed(self, connection, pn_condition):
        """Peer has closed its end of the connection."""
        LOG.debug("connection_remote_closed (ignored)")

    def connection_closed(self, connection):
        """The connection has cleanly closed."""
        LOG.debug("connection_closed (ignored)")

    def sender_requested(self, connection, link_handle,
                         name, requested_source,
                         properties):
        """Peer has requested a SenderLink be created."""
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        LOG.debug("sender_requested (ignored)")

    def receiver_requested(self, connection, link_handle,
                           name, requested_target,
                           properties):
        """Peer has requested a ReceiverLink be created."""
        # call accept_receiver to accept new link,
        # reject_receiver to reject it.
        LOG.debug("receiver_requested (ignored)")

    # TODO(kgiusti) cleaner sasl support, esp. server side
    def sasl_step(self, connection, pn_sasl):
        """SASL exchange occurred."""
        LOG.debug("sasl_step (ignored)")

    def sasl_done(self, connection, pn_sasl, result):
        """SASL exchange complete."""
        LOG.debug("sasl_done (ignored)")


class Connection(Endpoint):
    """A Connection to a peer."""
    EOS = -1   # indicates 'I/O stream closed'

    def __init__(self, container, name, event_handler=None, properties=None):
        """Create a new connection from the Container

        The following AMQP connection properties are supported:

        hostname: string, target host name sent in Open frame.

        idle-time-out: float, time in seconds before an idle link will be
        closed.

        properties: map, connection properties sent to the peer.

        max-frame-size: int, maximum acceptable frame size in bytes.

        The following custom connection properties are supported:

        x-trace-protocol: boolean, if true, dump sent and received frames to
        stdout.

        x-ssl-server: boolean, if True connection acts as a SSL server (default
        False - use Client mode)

        x-ssl-identity: tuple, contains identifying certificate information
        which will be presented to the peer.  The first item in the tuple is
        the path to the certificate file (PEM format).  The second item is the
        path to a file containing the private key used to sign the certificate
        (PEM format, optional if private key is stored in the certificate
        itself). The last item is the password used to encrypt the private key
        (string, not required if private key is not encrypted)

        x-ssl-ca-file: string, path to a file containing the certificates of
        the trusted Certificate Authorities that will be used to check the
        signature of the peer's certificate.

        x-ssl-verify-mode: string, configure the level of security provided by
        SSL.  Possible values:
            "verify-peer" (default) - most secure, requires peer to supply a
            certificate signed by a valid CA (see x-ssl-ca-file), and check
            the CN or SAN entry in the certificate against the expected
            peer hostname (see x-ssl-peer-name)
            "verify-cert" (default if no x-ssl-peer-name given) - like
            verify-peer, but skips the check of the peer hostname.
             Vulnerable to man-in-the-middle attack.
            "no-verify" - do not require the peer to provide a certificate.
            Results in a weaker encryption stream, and other vulnerabilities.

        x-ssl-peer-name: string, DNS name of peer.  Required to authenticate
        peer's certificate (see x-ssl-verify-mode).

        x-ssl-allow-cleartext: boolean, Allows clients to connect without using
        SSL (eg, plain TCP). Used by a server that will accept clients
        requesting either trusted or untrusted connections.
        """
        super(Connection, self).__init__(name)
        self._container = container
        self._handler = event_handler

        self._pn_connection = proton.Connection()
        self._pn_connection.container = container.name
        self._pn_transport = proton.Transport()
        self._pn_transport.bind(self._pn_connection)
        self._pn_collector = proton.Collector()
        self._pn_connection.collect(self._pn_collector)

        if properties:
            if 'hostname' in properties:
                self._pn_connection.hostname = properties['hostname']
            secs = properties.get("idle-time-out")
            if secs:
                self._pn_transport.idle_timeout = secs
            max_frame = properties.get("max-frame-size")
            if max_frame:
                self._pn_transport.max_frame_size = max_frame
            if 'properties' in properties:
                self._pn_connection.properties = properties["properties"]
            if properties.get("x-trace-protocol"):
                self._pn_transport.trace(proton.Transport.TRACE_FRM)

        # indexed by link-name
        self._sender_links = {}    # SenderLink
        self._receiver_links = {}  # ReceiverLink

        self._timers = {}  # indexed by expiration date
        self._timers_heap = []  # sorted by expiration date

        self._read_done = False
        self._write_done = False
        self._error = None
        self._next_deadline = 0
        self._user_context = None
        self._in_process = False
        self._remote_session_id = 0

        self._pn_ssl = self._configure_ssl(properties)

        # TODO(kgiusti) sasl configuration and handling
        self._pn_sasl = None
        self._sasl_done = False

    @property
    def container(self):
        return self._container

    @property
    # TODO(kgiusti) - hopefully remove
    def pn_transport(self):
        return self._pn_transport

    @property
    # TODO(kgiusti) - hopefully remove
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
    def remote_hostname(self):
        """Return the hostname advertised by the remote, if present."""
        if self._pn_connection:
            return self._pn_connection.remote_hostname
        return None

    @property
    def remote_properties(self):
        """Properties provided by the peer."""
        if self._pn_connection:
            return self._pn_connection.remote_properties
        return None

    # TODO(kgiusti) - think about server side use of sasl!
    @property
    def pn_sasl(self):
        if not self._pn_sasl:
            self._pn_sasl = self._pn_transport.sasl()
        return self._pn_sasl

    @property
    def sasl(self):
        text = "sasl deprecated, use pn_sasl instead"
        warnings.warn(DeprecationWarning(text))
        return self.pn_sasl

    def pn_ssl(self):
        """Return the Proton SSL context for this Connection."""
        return self._pn_ssl

    def _get_user_context(self):
        return self._user_context

    def _set_user_context(self, ctxt):
        self._user_context = ctxt

    _uc_docstr = """Associate an arbitrary user object with this Connection."""
    user_context = property(_get_user_context, _set_user_context,
                            doc=_uc_docstr)

    def open(self):
        self._pn_connection.open()

    def close(self, pn_condition=None):
        for link in self._sender_links.itervalues():
            link.close(pn_condition)
        for link in self._receiver_links.itervalues():
            link.close(pn_condition)
        if pn_condition:
            self._pn_connection.condition = pn_condition
        self._pn_connection.close()

    @property
    def active(self):
        """Return True if both ends of the Connection are open."""
        return self._endpoint_state == self._ACTIVE

    @property
    def closed(self):
        """Return True if the Connection has finished closing."""
        return (self._write_done and self._read_done)

    def destroy(self):
        self._sender_links.clear()
        self._receiver_links.clear()
        self._container.remove_connection(self._name)
        self._container = None
        self._pn_collector = None
        self._pn_connection = None
        self._pn_transport = None
        self._user_context = None

    _REMOTE_REQ = (proton.Endpoint.LOCAL_UNINIT
                   | proton.Endpoint.REMOTE_ACTIVE)
    _CLOSED = (proton.Endpoint.LOCAL_CLOSED | proton.Endpoint.REMOTE_CLOSED)
    _ACTIVE = (proton.Endpoint.LOCAL_ACTIVE | proton.Endpoint.REMOTE_ACTIVE)

    def process(self, now):
        """Perform connection state processing."""
        if self._in_process:
            raise RuntimeError("Connection.process() is not re-entrant!")
        self._in_process = True
        try:
            # if the connection has hit an unrecoverable error,
            # nag the application until connection is destroyed
            if self._error:
                if self._handler:
                    self._handler.connection_failed(self, self._error)
                # nag application until connection is destroyed
                self._next_deadline = now
                return now

            # do nothing until the connection has been opened
            if self._pn_connection.state & proton.Endpoint.LOCAL_UNINIT:
                return 0

            # wait until SASL has authenticated
            # TODO(kgiusti) Server-side SASL
            if self._pn_sasl:
                if self._pn_sasl.state not in (proton.SASL.STATE_PASS,
                                               proton.SASL.STATE_FAIL):
                    LOG.debug("SASL in progress. State=%s",
                              str(self._pn_sasl.state))
                    if self._handler:
                        self._handler.sasl_step(self, self._pn_sasl)
                    return self._next_deadline

                if self._handler:
                    self._handler.sasl_done(self, self._pn_sasl,
                                            self._pn_sasl.outcome)
                self._pn_sasl = None

            # process timer events:
            timer_deadline = self._expire_timers(now)
            transport_deadline = self._pn_transport.tick(now)
            if timer_deadline and transport_deadline:
                self._next_deadline = min(timer_deadline, transport_deadline)
            else:
                self._next_deadline = timer_deadline or transport_deadline

            # process events from proton:
            pn_event = self._pn_collector.peek()
            while pn_event:
                if pn_event.type == proton.Event.CONNECTION_REMOTE_STATE:
                    self.process_remote_state()

                elif pn_event.type == proton.Event.CONNECTION_LOCAL_STATE:
                    self.process_local_state()

                elif pn_event.type == proton.Event.SESSION_REMOTE_STATE:
                    pn_session = pn_event.session
                    # create a new session if requested by remote:
                    if (not hasattr(pn_session, 'context')
                            or pn_session.context is None):
                        LOG.debug("Opening remotely initiated session")
                        name = "session-%d" % self._remote_session_id
                        self._remote_session_id += 1
                        session = _SessionProxy(name, self, pn_session)
                    pn_session.context.process_remote_state()

                elif pn_event.type == proton.Event.SESSION_LOCAL_STATE:
                    pn_session = pn_event.session
                    pn_session.context.process_local_state()

                elif pn_event.type == proton.Event.LINK_REMOTE_STATE:
                    pn_link = pn_event.link
                    # create a new link if requested by remote:
                    if (not hasattr(pn_link, 'context')
                            or pn_link.context is None):
                        session = pn_link.session.context
                        if (pn_link.is_sender and
                                pn_link.name not in self._sender_links):
                            LOG.debug("Remotely initiated Sender needs init")
                            link = session.request_sender(pn_link)
                            self._sender_links[pn_link.name] = link
                        elif (pn_link.is_receiver and
                              pn_link.name not in self._receiver_links):
                            LOG.debug("Remotely initiated Receiver needs init")
                            link = session.request_receiver(pn_link)
                            self._receiver_links[pn_link.name] = link
                    pn_link.context.process_remote_state()

                elif pn_event.type == proton.Event.LINK_LOCAL_STATE:
                    pn_link = pn_event.link
                    pn_link.context.process_local_state()

                elif pn_event.type == proton.Event.DELIVERY:
                    link = pn_event.link.context
                    pn_delivery = pn_event.delivery
                    link._process_delivery(pn_delivery)

                elif pn_event.type == proton.Event.LINK_FLOW:
                    link = pn_event.link.context
                    link._process_credit()

                self._pn_collector.pop()
                pn_event = self._pn_collector.peek()

            # invoked closed callback after endpoint has fully closed and all
            # pending I/O has completed:
            if (self._endpoint_state == self._CLOSED and
                    self._read_done and self._write_done):
                if self._handler:
                    self._handler.connection_closed(self)

            return self._next_deadline

        finally:
            self._in_process = False

    @property
    def next_tick(self):
        text = "next_tick deprecated, use deadline instead"
        warnings.warn(DeprecationWarning(text))
        return self.deadline

    @property
    def deadline(self):
        """Must invoke process() on or before this timestamp."""
        return self._next_deadline

    @property
    def needs_input(self):
        if self._read_done:
            return self.EOS
        try:
            # TODO(grs): can this actually throw?
            capacity = self._pn_transport.capacity()
        except Exception as e:
            return self._connection_failed(str(e))
        if capacity >= 0:
            return capacity
        self._read_done = True
        return self.EOS

    def process_input(self, in_data):
        c = min(self.needs_input, len(in_data))
        if c <= 0:
            return c
        try:
            rc = self._pn_transport.push(in_data[:c])
        except Exception as e:
            return self._connection_failed(str(e))
        if rc:  # error?
            self._read_done = True
            return self.EOS
        # hack: check if this was the last input needed by the connection.
        # If so, this will set the _read_done flag and the 'connection closed'
        # callback can be issued on the next call to process()
        self.needs_input
        return c

    def close_input(self, reason=None):
        if not self._read_done:
            try:
                self._pn_transport.close_tail()
            except Exception as e:
                self._connection_failed(str(e))
            self._read_done = True

    @property
    def has_output(self):
        if self._write_done:
            return self.EOS
        try:
            pending = self._pn_transport.pending()
        except Exception as e:
            return self._connection_failed(str(e))
        if pending >= 0:
            return pending
        self._write_done = True
        return self.EOS

    def output_data(self):
        """Get a buffer of data that needs to be written to the network.
        """
        c = self.has_output
        if c <= 0:
            return None
        try:
            buf = self._pn_transport.peek(c)
        except Exception as e:
            self._connection_failed(str(e))
            return None
        return buf

    def output_written(self, count):
        try:
            self._pn_transport.pop(count)
        except Exception as e:
            return self._connection_failed(str(e))
        # hack: check if this was the last output from the connection.  If so,
        # this will set the _write_done flag and the 'connection closed'
        # callback can be issued on the next call to process()
        self.has_output

    def close_output(self, reason=None):
        if not self._write_done:
            try:
                self._pn_transport.close_head()
            except Exception as e:
                self._connection_failed(str(e))
            self._write_done = True

    def create_sender(self, source_address, target_address=None,
                      event_handler=None, name=None, properties=None):
        """Factory method for Sender links."""
        ident = name or str(source_address)
        if ident in self._sender_links:
            raise KeyError("Sender %s already exists!" % ident)

        session = _SessionProxy("session-%s" % ident, self)
        session.open()
        sl = session.new_sender(ident)
        sl.configure(target_address, source_address, event_handler, properties)
        self._sender_links[ident] = sl
        return sl

    def accept_sender(self, link_handle, source_override=None,
                      event_handler=None, properties=None):
        link = self._sender_links.get(link_handle)
        if not link:
            raise Exception("Invalid link_handle: %s" % link_handle)
        pn_link = link._pn_link
        if pn_link.remote_source.dynamic and not source_override:
            raise Exception("A source address must be supplied!")
        source_addr = source_override or pn_link.remote_source.address
        link.configure(pn_link.remote_target.address,
                       source_addr,
                       event_handler, properties)
        return link

    def reject_sender(self, link_handle, pn_condition=None):
        """Rejects the SenderLink, and destroys the handle."""
        link = self._sender_links.get(link_handle)
        if not link:
            raise Exception("Invalid link_handle: %s" % link_handle)
        link.reject(pn_condition)
        link.destroy()

    def create_receiver(self, target_address, source_address=None,
                        event_handler=None, name=None, properties=None):
        """Factory method for creating Receive links."""
        ident = name or str(target_address)
        if ident in self._receiver_links:
            raise KeyError("Receiver %s already exists!" % ident)

        session = _SessionProxy("session-%s" % ident, self)
        session.open()
        rl = session.new_receiver(ident)
        rl.configure(target_address, source_address, event_handler, properties)
        self._receiver_links[ident] = rl
        return rl

    def accept_receiver(self, link_handle, target_override=None,
                        event_handler=None, properties=None):
        link = self._receiver_links.get(link_handle)
        if not link:
            raise Exception("Invalid link_handle: %s" % link_handle)
        pn_link = link._pn_link
        if pn_link.remote_target.dynamic and not target_override:
            raise Exception("A target address must be supplied!")
        target_addr = target_override or pn_link.remote_target.address
        link.configure(target_addr,
                       pn_link.remote_source.address,
                       event_handler, properties)
        return link

    def reject_receiver(self, link_handle, pn_condition=None):
        link = self._receiver_links.get(link_handle)
        if not link:
            raise Exception("Invalid link_handle: %s" % link_handle)
        link.reject(pn_condition)
        link.destroy()

    @property
    def _endpoint_state(self):
        return self._pn_connection.state

    def _remove_sender(self, name):
        if name in self._sender_links:
            del self._sender_links[name]

    def _remove_receiver(self, name):
        if name in self._receiver_links:
            del self._receiver_links[name]

    def _connection_failed(self, error):
        """Clean up after connection failure detected."""
        if not self._error:
            LOG.error("Connection failed: %s", str(error))
            self._read_done = True
            self._write_done = True
            self._error = error
            # report error during the next call to process()
            self._next_deadline = time.time()
        return self.EOS

    def _configure_ssl(self, properties):
        if not properties:
            return None
        verify_modes = {'verify-peer': proton.SSLDomain.VERIFY_PEER_NAME,
                        'verify-cert': proton.SSLDomain.VERIFY_PEER,
                        'no-verify': proton.SSLDomain.ANONYMOUS_PEER}

        mode = proton.SSLDomain.MODE_CLIENT
        if properties.get('x-ssl-server'):
            mode = proton.SSLDomain.MODE_SERVER

        identity = properties.get('x-ssl-identity')
        ca_file = properties.get('x-ssl-ca-file')

        if not identity and not ca_file:
            return None  # SSL not configured

        hostname = None
        # This will throw proton.SSLUnavailable if SSL support is not installed
        domain = proton.SSLDomain(mode)
        if identity:
            # our identity:
            domain.set_credentials(identity[0], identity[1], identity[2])
        if ca_file:
            # how we verify peers:
            domain.set_trusted_ca_db(ca_file)
            hostname = properties.get('x-ssl-peer-name')
            vdefault = 'verify-peer' if hostname else 'verify-cert'
            vmode = verify_modes.get(properties.get('x-ssl-verify-mode',
                                                    vdefault))
            # check for configuration error
            if not vmode:
                raise proton.SSLException("bad value for x-ssl-verify-mode")
            if vmode == proton.SSLDomain.VERIFY_PEER_NAME and not hostname:
                raise proton.SSLException("verify-peer needs x-ssl-peer-name")
            domain.set_peer_authentication(vmode, ca_file)
        if mode == proton.SSLDomain.MODE_SERVER:
            if properties.get('x-ssl-allow-cleartext'):
                domain.allow_unsecured_client()
        pn_ssl = proton.SSL(self._pn_transport, domain)
        if hostname:
            pn_ssl.peer_hostname = hostname
        LOG.debug("SSL configured for connection %s", self._name)
        return pn_ssl

    def _add_timer(self, deadline, callback):
        callbacks = self._timers.get(deadline)
        if callbacks:
            callbacks.add(callback)
        else:
            callbacks = set()
            callbacks.add(callback)
            self._timers[deadline] = callbacks
            heapq.heappush(self._timers_heap, deadline)
            if deadline < self._next_deadline:
                self._next_deadline = deadline

    def _cancel_timer(self, deadline, callback):
        callbacks = self._timers.get(deadline)
        if callbacks:
            callbacks.discard(callback)
        # next expire will discard empty deadlines

    def _expire_timers(self, now):
        while (self._timers_heap and
               self._timers_heap[0] <= now):
            deadline = heapq.heappop(self._timers_heap)
            callbacks = self._timers.get(deadline)
            if callbacks:
                del self._timers[deadline]
                for cb in callbacks:
                    cb()
        return self._timers_heap[0] if self._timers_heap else 0

    # endpoint state machine actions:

    def _ep_active(self):
        """Both ends of the Endpoint have become active."""
        LOG.debug("Connection is up")
        if self._handler:
            self._handler.connection_active(self)

    def _ep_need_close(self):
        """The remote has closed its end of the endpoint."""
        LOG.debug("Connection remotely closed")
        if self._handler:
            cond = self._pn_connection.remote_condition
            self._handler.connection_remote_closed(self, cond)

    def _ep_error(self):
        """The endpoint state machine failed due to protocol error."""
        super(Connection, self)._ep_error()
        self._connection_failed("Protocol error occurred.")
