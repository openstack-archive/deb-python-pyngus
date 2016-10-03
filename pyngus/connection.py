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
import warnings

from pyngus.endpoint import Endpoint
from pyngus.link import _Link
from pyngus.link import _SessionProxy

LOG = logging.getLogger(__name__)

_PROTON_VERSION = (int(getattr(proton, "VERSION_MAJOR", 0)),
                   int(getattr(proton, "VERSION_MINOR", 0)))


class _CallbackLock(object):
    """A utility class for detecting when a callback invokes a non-reentrant
    Pyngus method.
    """
    def __init__(self):
        super(_CallbackLock, self).__init__()
        self.in_callback = 0

    def __enter__(self):
        self.in_callback += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.in_callback -= 1
        # if a call is made to a non-reentrant method while this context is
        # held, then the method will raise a RuntimeError().  Return false to
        # propagate the exception to the caller
        return False


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

    # No longer supported by proton >= 0.10, so this method is deprecated
    def sasl_step(self, connection, pn_sasl):
        """DEPRECATED"""
        LOG.debug("sasl_step (ignored)")

    def sasl_done(self, connection, pn_sasl, result):
        """SASL exchange complete."""
        LOG.debug("sasl_done (ignored)")


class Connection(Endpoint):
    """A Connection to a peer."""
    EOS = -1   # indicates 'I/O stream closed'

    # set of all SASL connection configuration properties
    _SASL_PROPS = set(['x-username', 'x-password', 'x-require-auth',
                       'x-sasl-mechs', 'x-sasl-config-dir',
                       'x-sasl-config-name', 'x-force-sasl'])

    def _not_reentrant(func):
        """Decorator that prevents callbacks from calling into methods that are
        not reentrant
        """
        def wrap(self, *args, **kws):
            if self._callback_lock and self._callback_lock.in_callback:
                m = "Connection %s cannot be invoked from a callback!" % func
                raise RuntimeError(m)
            return func(self, *args, **kws)
        return wrap

    def __init__(self, container, name, event_handler=None, properties=None):
        """Create a new connection from the Container

        properties: map, properties of the new connection. The following keys
        and values are supported:

        idle-time-out: float, time in seconds before an idle link will be
        closed.

        hostname: string, the name of the host to which this connection is
        being made, sent in the Open frame.

        max-frame-size: int, maximum acceptable frame size in bytes.

        properties: map, proton connection properties sent to the peer.

        The following custom connection properties are supported:

        x-server: boolean, set this to True to configure the connection as a
        server side connection.  This should be set True if the connection was
        remotely initiated (e.g. accept on a listening socket).  If the
        connection was locally initiated (e.g. by calling connect()), then this
        value should be set to False.  This setting is used by authentication
        and encryption to configure the connection's role.  The default value
        is False for client mode.

        x-username: string, the client's username to use when authenticating
        with a server.

        x-password: string, the client's password, used for authentication.

        x-require-auth: boolean, reject remotely-initiated client connections
        that fail to provide valid credentials for authentication.

        x-sasl-mechs: string, a space-separated list of mechanisms
        that are allowed for authentication.  Defaults to "ANONYMOUS"

        x-sasl-config-dir: string, path to the directory containing the Cyrus
        SASL server configuration.

        x-sasl-config-name: string, name of the Cyrus SASL configuration file
        contained in the x-sasl-config-dir (without the '.conf' suffix)

        x-force-sasl: by default SASL authentication is disabled.  SASL will be
        enabled if any of the above x-sasl-* options are set. For clients using
        GSSAPI it is likely none of these options will be set.  In order for
        these clients to authenticate this flag must be set true.  The value of
        this property is ignored if any of the other SASL related properties
        are set.

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
            peer hostname (see hostname and x-ssl-peer-name properties)
            "verify-cert" (default if no x-ssl-peer-name given) - like
            verify-peer, but skips the check of the peer hostname.
             Vulnerable to man-in-the-middle attack.
            "no-verify" - do not require the peer to provide a certificate.
            Results in a weaker encryption stream, and other vulnerabilities.

        x-ssl-peer-name: string, DNS name of peer.  Override the hostname used
        to authenticate peer's certificate (see x-ssl-verify-mode).  The value
        of the 'hostname' property is used if this property is not supplied.

        x-ssl-allow-cleartext: boolean, Allows clients to connect without using
        SSL (eg, plain TCP). Used by a server that will accept clients
        requesting either trusted or untrusted connections.

        x-trace-protocol: boolean, if true, dump sent and received frames to
        stdout.
        """
        super(Connection, self).__init__(name)
        self._transport_bound = False
        self._container = container
        self._handler = event_handler
        self._properties = properties or {}
        old_flag = self._properties.get('x-ssl-server', False)
        self._server = self._properties.get('x-server', old_flag)

        self._pn_connection = proton.Connection()
        self._pn_connection.container = container.name
        if (_PROTON_VERSION < (0, 9)):
            self._pn_transport = proton.Transport()
        else:
            if self._server:
                mode = proton.Transport.SERVER
            else:
                mode = proton.Transport.CLIENT
            self._pn_transport = proton.Transport(mode)
        self._pn_collector = proton.Collector()
        self._pn_connection.collect(self._pn_collector)

        if 'hostname' in self._properties:
            self._pn_connection.hostname = self._properties['hostname']
        secs = self._properties.get("idle-time-out")
        if secs:
            self._pn_transport.idle_timeout = secs
        max_frame = self._properties.get("max-frame-size")
        if max_frame:
            self._pn_transport.max_frame_size = max_frame
        if 'properties' in self._properties:
            self._pn_connection.properties = self._properties["properties"]
        if self._properties.get("x-trace-protocol"):
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
        self._remote_session_id = 0
        self._callback_lock = _CallbackLock()

        self._pn_sasl = None
        self._sasl_done = False

        # if x-force-sasl is false remove it so it does not trigger the SASL
        # configuration logic below
        if not self._properties.get('x-force-sasl', True):
            del self._properties['x-force-sasl']

        if self._SASL_PROPS.intersection(set(self._properties.keys())):
            # SASL config specified, need to enable SASL
            if (_PROTON_VERSION < (0, 10)):
                # best effort map of 0.10 sasl config to pre-0.10 sasl
                if self._server:
                    self.pn_sasl.server()
                    if 'x-require-auth' in self._properties:
                        if not self._properties['x-require-auth']:
                            if _PROTON_VERSION >= (0, 8):
                                self.pn_sasl.allow_skip()
                else:
                    if 'x-username' in self._properties:
                        self.pn_sasl.plain(self._properties['x-username'],
                                           self._properties.get('x-password',
                                                                ''))
                    else:
                        self.pn_sasl.client()
                mechs = self._properties.get('x-sasl-mechs')
                if mechs:
                    self.pn_sasl.mechanisms(mechs)
            else:
                # new Proton SASL configuration:
                # maintain old behavior: allow PLAIN and ANONYMOUS
                # authentication.  Override this using x-sasl-mechs below:
                self.pn_sasl.allow_insecure_mechs = True
                if 'x-require-auth' in self._properties:
                    ra = self._properties['x-require-auth']
                    self._pn_transport.require_auth(ra)
                if 'x-username' in self._properties:
                    self._pn_connection.user = self._properties['x-username']
                if 'x-password' in self._properties:
                    self._pn_connection.password = \
                        self._properties['x-password']
                if 'x-sasl-mechs' in self._properties:
                    mechs = self._properties['x-sasl-mechs'].upper()
                    self.pn_sasl.allowed_mechs(mechs)
                    if 'PLAIN' not in mechs and 'ANONYMOUS' not in mechs:
                        self.pn_sasl.allow_insecure_mechs = False
                if 'x-sasl-config-dir' in self._properties:
                    self.pn_sasl.config_path(
                        self._properties['x-sasl-config-dir'])
                if 'x-sasl-config-name' in self._properties:
                    self.pn_sasl.config_name(
                        self._properties['x-sasl-config-name'])

        # intercept any SSL failures and cleanup resources before propagating
        # the exception:
        try:
            self._pn_ssl = self._configure_ssl(properties)
        except:
            self.destroy()
            raise

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

    @property
    def pn_sasl(self):
        if not self._pn_sasl:
            self._pn_sasl = self._pn_transport.sasl()
        return self._pn_sasl

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
        if not self._transport_bound:
            self._pn_transport.bind(self._pn_connection)
            self._transport_bound = True
        if self._pn_connection.state & proton.Endpoint.LOCAL_UNINIT:
            self._pn_connection.open()

    def close(self, pn_condition=None):
        for link in list(self._sender_links.values()):
            link.close(pn_condition)
        for link in list(self._receiver_links.values()):
            link.close(pn_condition)
        if pn_condition:
            self._pn_connection.condition = pn_condition
        if self._pn_connection.state & proton.Endpoint.LOCAL_ACTIVE:
            self._pn_connection.close()

    @property
    def active(self):
        """Return True if both ends of the Connection are open."""
        return self._endpoint_state == self._ACTIVE

    @property
    def closed(self):
        """Return True if the Connection has finished closing."""
        return (self._write_done and self._read_done)

    @_not_reentrant
    def destroy(self):
        # if a connection is destroyed without flushing pending output,
        # the remote will see an unclean shutdown (framing error)
        if self.has_output > 0:
            LOG.debug("Connection with buffered output destroyed")
        self._error = "Destroyed by the application"
        self._handler = None
        self._properties = None
        tmp = self._sender_links.copy()
        for l in tmp.values():
            l.destroy()
        assert(len(self._sender_links) == 0)
        tmp = self._receiver_links.copy()
        for l in tmp.values():
            l.destroy()
        assert(len(self._receiver_links) == 0)
        self._timers.clear()
        self._timers_heap = None
        self._container.remove_connection(self._name)
        self._container = None
        self._user_context = None
        self._callback_lock = None
        if self._transport_bound:
            self._pn_transport.unbind()
        self._pn_transport = None
        self._pn_connection.free()
        self._pn_connection = None
        if _PROTON_VERSION < (0, 8):
            # memory leak: drain the collector before releasing it
            while self._pn_collector.peek():
                self._pn_collector.pop()
        self._pn_collector = None
        self._pn_sasl = None
        self._pn_ssl = None

    _CLOSED = (proton.Endpoint.LOCAL_CLOSED | proton.Endpoint.REMOTE_CLOSED)
    _ACTIVE = (proton.Endpoint.LOCAL_ACTIVE | proton.Endpoint.REMOTE_ACTIVE)

    @_not_reentrant
    def process(self, now):
        """Perform connection state processing."""
        if self._pn_connection is None:
            LOG.error("Connection.process() called on destroyed connection!")
            return 0

        # do nothing until the connection has been opened
        if self._pn_connection.state & proton.Endpoint.LOCAL_UNINIT:
            return 0

        if self._pn_sasl and not self._sasl_done:
            # wait until SASL has authenticated
            if (_PROTON_VERSION < (0, 10)):
                if self._pn_sasl.state not in (proton.SASL.STATE_PASS,
                                               proton.SASL.STATE_FAIL):
                    LOG.debug("SASL in progress. State=%s",
                              str(self._pn_sasl.state))
                    if self._handler:
                        with self._callback_lock:
                            self._handler.sasl_step(self, self._pn_sasl)
                    return self._next_deadline

                self._sasl_done = True
                if self._handler:
                    with self._callback_lock:
                        self._handler.sasl_done(self, self._pn_sasl,
                                                self._pn_sasl.outcome)
            else:
                if self._pn_sasl.outcome is not None:
                    self._sasl_done = True
                    if self._handler:
                        with self._callback_lock:
                            self._handler.sasl_done(self, self._pn_sasl,
                                                    self._pn_sasl.outcome)

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
            # LOG.debug("pn_event: %s received", pn_event.type)
            if _Link._handle_proton_event(pn_event, self):
                pass
            elif self._handle_proton_event(pn_event):
                pass
            elif _SessionProxy._handle_proton_event(pn_event, self):
                pass
            self._pn_collector.pop()
            pn_event = self._pn_collector.peek()

        # check for connection failure after processing all pending
        # engine events:
        if self._error:
            if self._handler:
                # nag application until connection is destroyed
                self._next_deadline = now
                with self._callback_lock:
                    self._handler.connection_failed(self, self._error)
        elif (self._endpoint_state == self._CLOSED and
              self._read_done and self._write_done):
            # invoke closed callback after endpoint has fully closed and
            # all pending I/O has completed:
            if self._handler:
                with self._callback_lock:
                    self._handler.connection_closed(self)

        return self._next_deadline

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
            LOG.debug("needs_input EOS")
            return self.EOS
        try:
            capacity = self._pn_transport.capacity()
        except Exception as e:
            self._read_done = True
            self._connection_failed(str(e))
            return self.EOS
        if capacity >= 0:
            return capacity
        LOG.debug("needs_input read done")
        self._read_done = True
        return self.EOS

    def process_input(self, in_data):
        c = min(self.needs_input, len(in_data))
        if c <= 0:
            return c
        try:
            rc = self._pn_transport.push(in_data[:c])
        except Exception as e:
            self._read_done = True
            self._connection_failed(str(e))
            return self.EOS
        if rc:  # error?
            LOG.debug("process_input read done")
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
            LOG.debug("close_input read done")
            self._read_done = True

    @property
    def has_output(self):
        if self._write_done:
            LOG.debug("has output EOS")
            return self.EOS
        try:
            pending = self._pn_transport.pending()
        except Exception as e:
            self._write_done = True
            self._connection_failed(str(e))
            return self.EOS
        if pending >= 0:
            return pending
        LOG.debug("has output write_done")
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
            self._write_done = True
            self._connection_failed(str(e))
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
            LOG.debug("close output write done")
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
        # note: normally, link.destroy() cannot be called from a callback,
        # but this link was never made available to the application so this
        # link is only referenced by the connection
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
        # note: normally, link.destroy() cannot be called from a callback,
        # but this link was never made available to the application so this
        # link is only referenced by the connection
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

    def _connection_failed(self, error="Error not specified!"):
        """Clean up after connection failure detected."""
        if not self._error:
            LOG.error("Connection failed: %s", str(error))
            self._error = error

    def _configure_ssl(self, properties):
        if not properties:
            return None
        verify_modes = {'verify-peer': proton.SSLDomain.VERIFY_PEER_NAME,
                        'verify-cert': proton.SSLDomain.VERIFY_PEER,
                        'no-verify': proton.SSLDomain.ANONYMOUS_PEER}

        mode = proton.SSLDomain.MODE_CLIENT
        if properties.get('x-ssl-server', properties.get('x-server')):
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
            hostname = properties.get('x-ssl-peer-name',
                                      properties.get('hostname'))
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
        if callbacks is None:
            callbacks = set()
            self._timers[deadline] = callbacks
            heapq.heappush(self._timers_heap, deadline)
            if deadline < self._next_deadline:
                self._next_deadline = deadline
        callbacks.add(callback)

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
            while callbacks:
                callbacks.pop()()
            del self._timers[deadline]

        return self._timers_heap[0] if self._timers_heap else 0

    # Proton's event model was changed after 0.7
    if (_PROTON_VERSION >= (0, 8)):
        _endpoint_event_map = {
            proton.Event.CONNECTION_REMOTE_OPEN: Endpoint.REMOTE_OPENED,
            proton.Event.CONNECTION_REMOTE_CLOSE: Endpoint.REMOTE_CLOSED,
            proton.Event.CONNECTION_LOCAL_OPEN: Endpoint.LOCAL_OPENED,
            proton.Event.CONNECTION_LOCAL_CLOSE: Endpoint.LOCAL_CLOSED}

        def _handle_proton_event(self, pn_event):
            ep_event = Connection._endpoint_event_map.get(pn_event.type)
            if ep_event is not None:
                self._process_endpoint_event(ep_event)
            elif pn_event.type == proton.Event.CONNECTION_INIT:
                LOG.debug("Connection created: %s", pn_event.context)
            elif pn_event.type == proton.Event.CONNECTION_FINAL:
                LOG.debug("Connection finalized: %s", pn_event.context)
            elif pn_event.type == proton.Event.TRANSPORT_ERROR:
                self._connection_failed(str(self._pn_transport.condition))
            else:
                return False  # unknown
            return True  # handled
    elif hasattr(proton.Event, "CONNECTION_LOCAL_STATE"):
        # 0.7 proton event model
        def _handle_proton_event(self, pn_event):
            if pn_event.type == proton.Event.CONNECTION_LOCAL_STATE:
                self._process_local_state()
            elif pn_event.type == proton.Event.CONNECTION_REMOTE_STATE:
                self._process_remote_state()
            else:
                return False  # unknown
            return True  # handled
    else:
        raise Exception("The installed version of Proton is not supported.")

    # endpoint state machine actions:

    def _ep_active(self):
        """Both ends of the Endpoint have become active."""
        LOG.debug("Connection is up")
        if self._handler:
            with self._callback_lock:
                self._handler.connection_active(self)

    def _ep_need_close(self):
        """The remote has closed its end of the endpoint."""
        LOG.debug("Connection remotely closed")
        if self._handler:
            cond = self._pn_connection.remote_condition
            with self._callback_lock:
                self._handler.connection_remote_closed(self, cond)

    def _ep_error(self, error):
        """The endpoint state machine failed due to protocol error."""
        super(Connection, self)._ep_error(error)
        self._connection_failed("Protocol error occurred.")

    # order by name

    def __lt__(self, other):
        return self.name < other.name

    def __le__(self, other):
        return self < other or self.name == other.name

    def __gt__(self, other):
        return self.name > other.name

    def __ge__(self, other):
        return self > other or self.name == other.name
