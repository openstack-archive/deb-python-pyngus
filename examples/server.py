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
#
"""A simple server that consumes and produces messages."""

import logging
import optparse
import select
import sys
import time
import uuid

from proton import Message
import pyngus

from utils import get_host_port
from utils import server_socket

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())


class SocketConnection(pyngus.ConnectionEventHandler):
    """Associates a pyngus Connection with a python network socket"""

    def __init__(self, container, socket_, name, properties):
        """Create a Connection using socket_."""
        self.socket = socket_
        self.connection = container.create_connection(name,
                                                      self,  # handler
                                                      properties)
        self.connection.user_context = self
        self.connection.open()

        self.sender_links = set()
        self.receiver_links = set()

    def destroy(self):
        for link in self.sender_links.copy():
            link.destroy()
        for link in self.receiver_links.copy():
            link.destroy()
        if self.connection:
            self.connection.destroy()
            self.connection = None
        if self.socket:
            self.socket.close()
            self.socket = None

    @property
    def closed(self):
        return self.connection is None or self.connection.closed

    def fileno(self):
        """Allows use of a SocketConnection in a select() call."""
        return self.socket.fileno()

    def process_input(self):
        """Called when socket is read-ready"""
        try:
            pyngus.read_socket_input(self.connection, self.socket)
        except Exception as e:
            LOG.error("Exception on socket read: %s", str(e))
            self.connection.close_input()
            self.connection.close()
        self.connection.process(time.time())

    def send_output(self):
        """Called when socket is write-ready"""
        try:
            pyngus.write_socket_output(self.connection,
                                       self.socket)
        except Exception as e:
            LOG.error("Exception on socket write: %s", str(e))
            self.connection.close_output()
            self.connection.close()
        self.connection.process(time.time())

    # ConnectionEventHandler callbacks:

    def connection_remote_closed(self, connection, reason):
        LOG.debug("Connection: remote closed!")
        # The remote has closed its end of the Connection.  Close my end to
        # complete the close of the Connection:
        self.connection.close()

    def connection_closed(self, connection):
        LOG.debug("Connection: closed.")

    def connection_failed(self, connection, error):
        LOG.error("Connection: failed! error=%s", str(error))
        self.connection.close()

    def sender_requested(self, connection, link_handle,
                         name, requested_source, properties):
        LOG.debug("Connection: sender requested")
        if requested_source is None:
            # the peer has requested us to create a source node. Pretend we do
            # this, and supply a dummy name
            requested_source = uuid.uuid4().hex
        sender = MySenderLink(self, link_handle, requested_source)
        self.sender_links.add(sender)

    def receiver_requested(self, connection, link_handle,
                           name, requested_target, properties):
        LOG.debug("receiver requested callback")
        if requested_target is None:
            # the peer has requested us to create a target node. Pretend we do
            # this, and supply a dummy name
            requested_target = uuid.uuid4().hex
        receiver = MyReceiverLink(self, link_handle, requested_target)
        self.receiver_links.add(receiver)

    # SASL callbacks:

    def sasl_step(self, connection, pn_sasl):
        LOG.debug("SASL step callback")
        # Unconditionally accept the client:
        pn_sasl.done(pn_sasl.OK)

    def sasl_done(self, connection, pn_sasl, result):
        LOG.debug("SASL done callback, result=%s", str(result))


class MySenderLink(pyngus.SenderEventHandler):
    """Send messages until credit runs out."""
    def __init__(self, socket_conn, handle, src_addr=None):
        self.socket_conn = socket_conn
        sl = socket_conn.connection.accept_sender(handle,
                                                  source_override=src_addr,
                                                  event_handler=self)
        self.sender_link = sl
        self.sender_link.open()
        print("New Sender link created, name=%s" % sl.name)

    @property
    def closed(self):
        return self.sender_link.closed

    def destroy(self):
        print("Sender link destroyed, name=%s" % self.sender_link.name)
        self.socket_conn.sender_links.discard(self)
        self.socket_conn = None
        self.sender_link.destroy()
        self.sender_link = None

    def send_message(self):
        msg = Message()
        msg.body = "Hi There!"
        LOG.debug("Sender: Sending message...")
        self.sender_link.send(msg, self)

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        LOG.debug("Sender: Active")
        if sender_link.credit > 0:
            self.send_message()

    def sender_remote_closed(self, sender_link, error):
        LOG.debug("Sender: Remote closed")
        self.sender_link.close()

    def credit_granted(self, sender_link):
        LOG.debug("Sender: credit granted")
        # Send a single message:
        if sender_link.credit > 0:
            self.send_message()

    # 'message sent' callback:
    def __call__(self, sender, handle, status, error=None):
        print("Message sent on Sender link %s, status=%s" %
              (self.sender_link.name, status))
        if self.sender_link.credit > 0:
            # send another message:
            self.send_message()


class MyReceiverLink(pyngus.ReceiverEventHandler):
    """Receive messages, and drop them."""
    def __init__(self, socket_conn, handle, rx_addr=None):
        self.socket_conn = socket_conn
        rl = socket_conn.connection.accept_receiver(handle,
                                                    target_override=rx_addr,
                                                    event_handler=self)
        self.receiver_link = rl
        self.receiver_link.open()
        self.receiver_link.add_capacity(1)
        print("New Receiver link created, name=%s" % rl.name)

    @property
    def closed(self):
        return self.receiver_link.closed

    def destroy(self):
        print("Receiver link destroyed, name=%s" % self.receiver_link.name)
        self.socket_conn.receiver_links.discard(self)
        self.socket_conn = None
        self.receiver_link.destroy()
        self.receiver_link = None

    # ReceiverEventHandler callbacks:

    def receiver_active(self, receiver_link):
        LOG.debug("Receiver: Active")

    def receiver_remote_closed(self, receiver_link, error):
        LOG.debug("Receiver: Remote closed")
        self.receiver_link.close()

    def message_received(self, receiver_link, message, handle):
        self.receiver_link.message_accepted(handle)
        print("Message received on Receiver link %s, message=%s"
              % (self.receiver_link.name, str(message)))
        if receiver_link.capacity < 1:
            receiver_link.add_capacity(1)


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="address", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="Server address [amqp://0.0.0.0:5672]")
    parser.add_option("--idle", dest="idle_timeout", type="float",
                      help="timeout for an idle link, in seconds")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")
    parser.add_option("--debug", dest="debug", action="store_true",
                      help="enable debug logging")
    parser.add_option("--cert",
                      help="PEM File containing the server's certificate")
    parser.add_option("--key",
                      help="PEM File containing the server's private key")
    parser.add_option("--keypass",
                      help="Password used to decrypt key file")
    parser.add_option("--require-auth", action="store_true",
                      help="Require clients to authenticate")
    parser.add_option("--sasl-mechs", type="string",
                      help="The list of acceptable SASL mechs")
    parser.add_option("--sasl-cfg-name", type="string",
                      help="name of SASL config file (no suffix)")
    parser.add_option("--sasl-cfg-dir", type="string",
                      help="Path to the SASL config file")

    opts, arguments = parser.parse_args(args=argv)
    if opts.debug:
        LOG.setLevel(logging.DEBUG)

    # Create a socket for inbound connections
    #
    host, port = get_host_port(opts.address)
    my_socket = server_socket(host, port)

    # create an AMQP container that will 'provide' the Server service
    #
    container = pyngus.Container("Server")
    socket_connections = set()

    # Main loop: process I/O and timer events:
    #
    while True:
        readers, writers, timers = container.need_processing()

        # map pyngus Connections back to my SocketConnections:
        readfd = [c.user_context for c in readers]
        writefd = [c.user_context for c in writers]

        timeout = None
        if timers:
            deadline = timers[0].next_tick  # [0] == next expiring timer
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        LOG.debug("select() start (t=%s)", str(timeout))
        readfd.append(my_socket)
        readable, writable, ignore = select.select(readfd, writefd,
                                                   [], timeout)
        LOG.debug("select() returned")

        worked = set()
        for r in readable:
            if r is my_socket:
                # new inbound connection request received,
                # create a new SocketConnection for it:
                client_socket, client_address = my_socket.accept()
                # name = uuid.uuid4().hex
                name = str(client_address)
                conn_properties = {'x-server': True}
                if opts.require_auth:
                    conn_properties['x-require-auth'] = True
                if opts.sasl_mechs:
                    conn_properties['x-sasl-mechs'] = opts.sasl_mechs
                if opts.sasl_cfg_name:
                    conn_properties['x-sasl-config-name'] = opts.sasl_cfg_name
                if opts.sasl_cfg_dir:
                    conn_properties['x-sasl-config-dir'] = opts.sasl_cfg_dir
                if opts.idle_timeout:
                    conn_properties["idle-time-out"] = opts.idle_timeout
                if opts.trace:
                    conn_properties["x-trace-protocol"] = True
                if opts.cert:
                    conn_properties["x-ssl-server"] = True
                    identity = (opts.cert, opts.key, opts.keypass)
                    conn_properties["x-ssl-identity"] = identity
                sconn = SocketConnection(container,
                                         client_socket,
                                         name,
                                         conn_properties)
                socket_connections.add(sconn)
                LOG.debug("new connection created name=%s", name)

            else:
                assert isinstance(r, SocketConnection)
                r.process_input()
                worked.add(r)

        for t in timers:
            now = time.time()
            if t.next_tick > now:
                break
            t.process(now)
            sc = t.user_context
            assert isinstance(sc, SocketConnection)
            worked.add(sc)

        for w in writable:
            assert isinstance(w, SocketConnection)
            w.send_output()
            worked.add(w)

        closed = False
        while worked:
            sc = worked.pop()
            # nuke any completed connections:
            if sc.closed:
                socket_connections.discard(sc)
                sc.destroy()
                closed = True
            else:
                # can free any closed links now (optional):
                for link in sc.sender_links | sc.receiver_links:
                    if link.closed:
                        link.destroy()

        if closed:
            LOG.debug("%d active connections present", len(socket_connections))

    return 0


if __name__ == "__main__":
    sys.exit(main())
