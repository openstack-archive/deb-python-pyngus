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
"""
This module implements a simple RPC server that can be used with the example
RPC client.  The client sends a 'method call'
to the server, and waits for a response.  The method call is a map of the form:
{'method': '<name of method on server>',
 'args': {<map of name=value arguments for the call}
}
The server replies to the client using a map that contains a copy of the method
map sent in the request.
"""

import errno
import logging
import optparse
import re
import socket
import select
import sys
import time
import uuid
# import gc

# from guppy import hpy
# hp = hpy()

from proton import Message, Condition
import pyngus

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())

# Maps of outgoing and incoming links.  These are indexed by
# (remote-container-name, link-name)
sender_links = {}
receiver_links = {}

# links that have closed and need to be destroyed:
dead_links = set()

# Map reply-to address to the proper sending link (indexed by address)
reply_senders = {}

# database of all active SocketConnections
socket_connections = {}  # indexed by name


class SocketConnection(pyngus.ConnectionEventHandler):
    """Associates a pyngus Connection with a python network socket"""

    def __init__(self, name, socket_, container, conn_properties):
        self.name = name
        self.socket = socket_
        self.connection = container.create_connection(name, self,
                                                      conn_properties)
        self.connection.user_context = self
        self.connection.open()
        self.done = False

    def destroy(self):
        self.done = True
        if self.connection:
            self.connection.destroy()
            self.connection = None
        if self.socket:
            self.socket.close()
            self.socket = None

    def fileno(self):
        """Allows use of a SocketConnection in a select() call.
        """
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

    def connection_active(self, connection):
        LOG.debug("Connection active callback")

    def connection_remote_closed(self, connection, reason):
        LOG.debug("Connection remote closed callback")
        assert self.connection is connection
        self.connection.close()

    def connection_closed(self, connection):
        LOG.debug("connection closed.")
        # main loop will destroy
        self.done = True

    def connection_failed(self, connection, error):
        LOG.error("connection failed! error=%s", str(error))
        self.connection_closed(connection)

    def sender_requested(self, connection, link_handle,
                         name, requested_source, properties):
        LOG.debug("sender requested callback")
        global sender_links
        global reply_senders

        # reject if name conflict
        remote_container = connection.remote_container
        ident = (remote_container, name)
        if ident in sender_links:
            connection.reject_sender(link_handle, "link name in use")
            return

        # allow for requested_source address if it doesn't conflict with an
        # existing address, otherwise override
        if not requested_source or requested_source in reply_senders:
            requested_source = uuid.uuid4().hex
            assert requested_source not in reply_senders

        sender = MySenderLink(ident, connection, link_handle, requested_source)
        sender_links[ident] = sender
        reply_senders[requested_source] = sender
        print("New Sender link created, source=%s" % requested_source)

    def receiver_requested(self, connection, link_handle,
                           name, requested_target, properties):
        LOG.debug("receiver requested callback")
        global receiver_links

        # reject if name conflict
        remote_container = connection.remote_container
        ident = (remote_container, name)
        if ident in receiver_links:
            connection.reject_sender(link_handle, "link name in use")
            return

        # I don't use the target address, but supply one if necessary
        if not requested_target:
            requested_target = uuid.uuid4().hex

        receiver = MyReceiverLink(ident, connection,
                                  link_handle, requested_target)
        receiver_links[ident] = receiver
        print("New Receiver link created, target=%s" % requested_target)

    # SASL callbacks:

    def sasl_step(self, connection, pn_sasl):
        LOG.debug("SASL step callback")
        pn_sasl.done(pn_sasl.OK)

    def sasl_done(self, connection, pn_sasl, result):
        LOG.debug("SASL done callback, result=%s", str(result))


class MySenderLink(pyngus.SenderEventHandler):
    """Link for sending RPC replies."""
    def __init__(self, ident, connection, link_handle,
                 source_address, properties=None):

        self._ident = ident
        self._source_address = source_address
        self.sender_link = connection.accept_sender(link_handle,
                                                    source_address,
                                                    self,
                                                    properties)
        self.sender_link.open()

    @property
    def closed(self):
        if self.sender_link:
            return self.sender_link.closed
        return True

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        LOG.debug("sender active callback")

    def sender_remote_closed(self, sender_link, error):
        LOG.debug("sender remote closed callback")
        self.sender_link.close()

    def sender_closed(self, sender_link):
        LOG.debug("sender closed callback")
        global sender_links
        global reply_senders
        global dead_links

        if self._ident in sender_links:
            del sender_links[self._ident]
        if self._source_address in reply_senders:
            del reply_senders[self._source_address]

        dead_links.add(self.sender_link)
        self.sender_link = None

    # 'message sent' callback:

    def __call__(self, sender, handle, status, error=None):
        LOG.debug("message sent callback, status=%s", str(status))


class MyReceiverLink(pyngus.ReceiverEventHandler):
    """
    """
    def __init__(self, ident, connection, link_handle, target_address,
                 properties=None):
        self._ident = ident
        self._target_address = target_address
        self._link = connection.accept_receiver(link_handle,
                                                target_address,
                                                self,
                                                properties)
        self._link.open()

    @property
    def closed(self):
        if self._link:
            return self._link.closed
        return True

    # ReceiverEventHandler callbacks:
    def receiver_active(self, receiver_link):
        LOG.debug("receiver active callback")
        self._link.add_capacity(5)

    def receiver_remote_closed(self, receiver_link, error):
        LOG.debug("receiver remote closed callback")
        self._link.close()

    def receiver_closed(self, receiver_link):
        LOG.debug("receiver closed callback")
        global receiver_links
        global dead_links

        if self._ident in receiver_links:
            del receiver_links[self._ident]

        dead_links.add(self._link)
        self._link = None

    def message_received(self, receiver_link, message, handle):
        LOG.debug("message received callback")

        global reply_senders

        # extract to reply-to, correlation id
        reply_to = message.reply_to
        if not reply_to or reply_to not in reply_senders:
            LOG.error("sender for reply-to not found, reply-to=%s",
                      str(reply_to))
            info = Condition("not-found",
                             "Bad reply-to address: %s" % str(reply_to))
            self._link.message_rejected(handle, info)
        else:
            my_sender = reply_senders[reply_to]
            correlation_id = message.correlation_id
            method_map = message.body
            if (not isinstance(method_map, dict) or
                    'method' not in method_map):
                LOG.error("no method given, map=%s", str(method_map))
                info = Condition("invalid-field",
                                 "no method given, map=%s" % str(method_map))
                self._link.message_rejected(handle, info)
            else:
                response = Message()
                response.address = reply_to
                response.subject = message.subject
                response.correlation_id = correlation_id
                response.body = {"response": method_map}

                print("RPC request received, msg=%s" % str(method_map))
                print("  to address=%s" % str(message.address))
                print("  replying to=%s" % str(reply_to))
                link = my_sender.sender_link
                # @todo send timeouts
                # link.send( response, my_sender,
                #            message, time.time() + 5.0)
                link.send(response, my_sender, message)

                self._link.message_accepted(handle)

        if self._link.capacity == 0:
            LOG.debug("increasing credit...")
            self._link.add_capacity(5)


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="address", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="""The socket address this server will listen on
 [amqp://0.0.0.0:5672]""")
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

    opts, arguments = parser.parse_args(args=argv)
    if opts.debug:
        LOG.setLevel(logging.DEBUG)

    # Create a socket for inbound connections
    #
    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    LOG.debug("Listening on %s", opts.address)
    x = regex.match(opts.address)
    if not x:
        raise Exception("Bad address syntax: %s" % opts.address)
    matches = x.groups()
    host = matches[0]
    port = int(matches[2]) if matches[2] else None
    addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addr:
        raise Exception("Could not translate address '%s'" % opts.address)
    my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
    my_socket.setblocking(0)  # 0=non-blocking
    try:
        my_socket.bind((host, port))
        my_socket.listen(10)
    except socket.error as e:
        if e.errno != errno.EINPROGRESS:
            raise

    # create an AMQP container that will 'provide' the RPC service
    #
    container = pyngus.Container("example RPC service")
    global socket_connections
    global dead_links

    while True:

        #
        # Poll for I/O & timers
        #

        readfd = [my_socket]
        writefd = []
        readers, writers, timers = container.need_processing()

        # map pyngus Connections back to my SocketConnections
        for c in readers:
            sc = c.user_context
            assert sc and isinstance(sc, SocketConnection)
            readfd.append(sc)
        for c in writers:
            sc = c.user_context
            assert sc and isinstance(sc, SocketConnection)
            writefd.append(sc)

        timeout = None
        if timers:
            deadline = timers[0].next_tick  # 0 == next expiring timer
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        LOG.debug("select() start (t=%s)", str(timeout))
        readable, writable, ignore = select.select(readfd,
                                                   writefd,
                                                   [],
                                                   timeout)
        LOG.debug("select() returned")

        worked = []
        for r in readable:
            if r is my_socket:
                # new inbound connection request
                client_socket, client_address = my_socket.accept()
                name = uuid.uuid4().hex
                assert name not in socket_connections
                conn_properties = {'x-server': True}
                if opts.idle_timeout:
                    conn_properties["idle-time-out"] = opts.idle_timeout
                if opts.trace:
                    conn_properties["x-trace-protocol"] = True
                if opts.cert:
                    conn_properties["x-ssl-server"] = True
                    identity = (opts.cert, opts.key, opts.keypass)
                    conn_properties["x-ssl-identity"] = identity
                socket_connections[name] = SocketConnection(name,
                                                            client_socket,
                                                            container,
                                                            conn_properties)
                LOG.debug("new connection created name=%s", name)

            else:
                assert isinstance(r, SocketConnection)
                r.process_input()
                worked.append(r)

        for t in timers:
            now = time.time()
            if t.next_tick > now:
                break
            t.process(now)
            sc = t.user_context
            assert isinstance(sc, SocketConnection)
            worked.append(sc)

        for w in writable:
            assert isinstance(w, SocketConnection)
            w.send_output()
            worked.append(w)

        # first, free any closed links:
        while dead_links:
            dead_links.pop().destroy()

        # then nuke any completed connections:
        closed = False
        while worked:
            sc = worked.pop()
            if sc.done:
                if sc.name in socket_connections:
                    del socket_connections[sc.name]
                sc.destroy()
                closed = True
        if closed:
            LOG.debug("%d active connections present", len(socket_connections))

    return 0


if __name__ == "__main__":
    sys.exit(main())
