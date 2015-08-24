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
""" This module implements a simple RPC client.  The client sends a 'method
call' to a server, and waits for a response.  The method call is a map of the
form: {'method': '<name of method on server>', 'args': {<map of name=value
arguments for the call} }
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

from proton import Message
import pyngus

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())


class MyConnection(pyngus.ConnectionEventHandler):

    def __init__(self, name, container, properties):
        self.name = name
        self.container = container
        self.properties = properties
        self.socket = None
        self.caller = None
        self.connection = None

    def reset(self):
        if self.caller:
            self.caller.reset()
        if self.connection:
            self.connection.user_context = None
            self.connection.destroy()
            self.connection = None
        if self.socket:
            self.socket.close()
            self.socket = None

    def connect(self, socket_):
        self.reset()
        self.socket = socket_
        self.connection = self.container.create_connection(self.name,
                                                           self,
                                                           self.properties)
        self.connection.user_context = self
        self.connection.open()

    def process(self):
        """ Do connection-based processing (I/O and timers) """
        readfd = []
        writefd = []
        if self.connection.needs_input > 0:
            readfd = [self.socket]
        if self.connection.has_output > 0:
            writefd = [self.socket]

        timeout = None
        deadline = self.connection.next_tick
        if deadline:
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        LOG.debug("select() start (t=%s)", str(timeout))
        readable, writable, ignore = select.select(readfd,
                                                   writefd,
                                                   [],
                                                   timeout)
        LOG.debug("select() returned")

        if readable:
            try:
                pyngus.read_socket_input(self.connection,
                                         self.socket)
            except Exception as e:
                LOG.error("Exception on socket read: %s", str(e))
                self.connection.close_input()
                self.connection.close()

        self.connection.process(time.time())
        if writable:
            try:
                pyngus.write_socket_output(self.connection,
                                           self.socket)
            except Exception as e:
                LOG.error("Exception on socket write: %s", str(e))
                self.connection.close_output()
                self.connection.close()

    def close(self, error=None):
        self.connection.close(error)

    @property
    def closed(self):
        return self.connection.closed

    def destroy(self, error=None):
        self.reset()
        self.caller.destroy()
        self.caller = None
        self.container = None

    def create_caller(self, method_map, source_addr, target_addr,
                      receiver_properties, sender_properties):
        """ Caller factory
        """
        if self.caller:
            self.caller.destroy()
        self.caller = MyCaller(method_map, self, source_addr, target_addr,
                               receiver_properties, sender_properties)
        return self.caller

    # Connection callbacks:

    def connection_active(self, connection):
        """connection handshake completed"""
        LOG.debug("Connection active callback")

    def connection_remote_closed(self, connection, reason):
        LOG.debug("Connection remote closed callback")
        connection.close()

    def connection_closed(self, connection):
        LOG.debug("Connection closed callback")

    def connection_failed(self, connection, error):
        LOG.error("Connection failed! error=%s", str(error))
        raise Exception("Connection failure: %s" % str(error))

    def sender_requested(self, connection, link_handle, name,
                         requested_source, properties):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        assert False, "Not expected"

    def receiver_requested(self, connection, link_handle, name,
                           requested_target, properties):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        assert False, "Not expected"

    def sasl_done(self, connection, pn_sasl, result):
        LOG.debug("SASL done, result=%s", str(result))


class MyCaller(pyngus.SenderEventHandler,
               pyngus.ReceiverEventHandler):
    """Implements state for a single RPC call."""

    def __init__(self, method_map, my_connection,
                 my_source, my_target,
                 receiver_properties, sender_properties):
        # self._name = uuid.uuid4().hex
        self._my_connection = my_connection
        self._source_addr = my_source
        self._target_addr = my_target
        self._receiver_properties = receiver_properties
        self._sender_properties = sender_properties
        self._method = method_map
        self._sender = None
        self._receiver = None
        self.reset()

    def reset(self):
        LOG.debug("Resetting my-caller")
        # TODO(kgiusti: for now, use new name as engine isn't cleaning up link
        # state properly...

        self._name = uuid.uuid4().hex
        self._reply_to = None
        self._to = None
        self._send_completed = False
        self._response_received = False

        if self._sender:
            self._sender.destroy()
            self._sender = None

        if self._receiver:
            self._receiver.destroy()
            self._receiver = None

    def connect(self):
        self.reset()
        LOG.debug("Connecting my-caller")
        conn = self._my_connection.connection
        self._sender = conn.create_sender(
            self._source_addr,
            target_address=None,
            event_handler=self,
            # name=self._source_addr,
            name=self._name,
            properties=self._sender_properties)
        self._receiver = conn.create_receiver(
            self._target_addr,
            source_address=None,
            event_handler=self,
            # name=self._target_addr,
            name=self._name,
            properties=self._receiver_properties)
        self._sender.open()
        self._receiver.add_capacity(1)
        self._receiver.open()

    def done(self):
        return self._send_completed and self._response_received

    def close(self):
        LOG.debug("Closing my-caller")
        self._sender.close(None)
        self._receiver.close(None)

    def closed(self):
        return self._sender.closed and self._receiver.closed

    def destroy(self):
        LOG.debug("Destroying my-caller")
        self.reset()
        self._my_connection = None

    def _send_request(self):
        """Send a message containing the RPC method call
        """
        msg = Message()
        msg.subject = "An RPC call!"
        msg.address = self._to
        msg.reply_to = self._reply_to
        msg.body = self._method
        msg.correlation_id = 5  # whatever...

        print("sending RPC call request: %s" % str(self._method))
        # @todo send timeout self._sender.send(msg, self, None, time.time() +
        # 10)
        self._sender.send(msg, self)

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        LOG.debug("Sender active callback")
        assert sender_link is self._sender
        self._to = sender_link.target_address
        assert self._to, "Expected a target address!!!"
        if self._reply_to:
            self._send_request()

    def sender_remote_closed(self, sender_link, error):
        LOG.debug("Sender remote closed callback")
        assert sender_link is self._sender
        self._sender.close()

    def sender_closed(self, sender_link):
        LOG.debug("Sender closed callback")
        assert sender_link is self._sender
        self._send_completed = True

    # send complete callback:

    def __call__(self, sender_link, handle, status, error):
        LOG.debug("Sender message sent callback, status=%s", str(status))
        self._send_completed = True

    # ReceiverEventHandler callbacks:

    def receiver_active(self, receiver_link):
        LOG.debug("Receiver active callback")
        assert receiver_link is self._receiver
        self._reply_to = receiver_link.source_address
        assert self._reply_to, "Expected a source address!!!"
        if self._to:
            self._send_request()

    def receiver_remote_closed(self, receiver_link, error):
        LOG.debug("receiver remote closed callback")
        assert receiver_link is self._receiver
        self._receiver.close()

    def receiver_closed(self, receiver_link):
        LOG.debug("Receiver closed callback")
        assert receiver_link is self._receiver
        self._response_received = True

    def message_received(self, receiver_link, message, handle):
        LOG.debug("Receiver message received callback")
        assert receiver_link is self._receiver
        print("RPC call response received: %s" % str(message))
        receiver_link.message_accepted(handle)
        self._response_received = True


def main(argv=None):

    _usage = """Usage: %prog [options] method arg1 value1 [arg2 value2] ..."""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="server", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="The address of the server [amqp://0.0.0.0:5672]")
    parser.add_option("-t", "--timeout", dest="timeout", type="int",
                      help="timeout used when waiting for reply, in seconds")
    parser.add_option("--repeat", dest="repeat", type="int",
                      default=1,
                      help="Repeat the RPC call REPEAT times (0 == forever)")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")
    parser.add_option("--debug", dest="debug", action="store_true",
                      help="enable debug logging")
    parser.add_option("--ca",
                      help="Certificate Authority PEM file")

    opts, method_info = parser.parse_args(args=argv)
    if not method_info:
        assert False, "No method info specified!"
    if len(method_info) % 2 != 1:
        assert False, "An even number of method arguments are required!"

    if opts.debug:
        LOG.setLevel(logging.DEBUG)

    # Create a socket connection to the server
    #
    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    LOG.debug("Connecting to %s", opts.server)
    x = regex.match(opts.server)
    if not x:
        raise Exception("Bad address syntax: %s" % opts.server)
    matches = x.groups()
    host = matches[0]
    port = int(matches[2]) if matches[2] else None
    addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addr:
        raise Exception("Could not translate address '%s'" % opts.server)
    my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
    my_socket.setblocking(0)  # 0=non-blocking
    try:
        my_socket.connect(addr[0][4])
    except socket.error as e:
        if e.errno != errno.EINPROGRESS:
            raise

    # create AMQP container, connection, sender and receiver
    #
    container = pyngus.Container(uuid.uuid4().hex)
    conn_properties = {}
    if opts.trace:
        conn_properties["x-trace-protocol"] = True
    if opts.ca:
        conn_properties["x-ssl-ca-file"] = opts.ca

    my_connection = MyConnection("to-server", container, conn_properties)

    # Create the RPC caller
    method = {'method': method_info[0],
              'args': dict([(method_info[i], method_info[i+1])
                            for i in range(1, len(method_info), 2)])}
    my_caller = my_connection.create_caller(method,
                                            "my-source-address",
                                            "my-target-address",
                                            receiver_properties={},
                                            sender_properties={})
    try:
        my_connection.connect(my_socket)
        repeat = 0
        while opts.repeat == 0 or repeat < opts.repeat:

            LOG.debug("Requesting RPC...")

            my_caller.connect()
            while not my_caller.done():
                my_connection.process()

            LOG.debug("RPC completed!  Closing caller...")

            my_caller.close()

            while not my_caller.closed():
                my_connection.process()

            LOG.debug("Caller closed cleanly!")

            repeat += 1

        print("Closing connections")
        my_connection.close()
        while not my_connection.closed:
            my_connection.process()

        LOG.debug("Connection closed")
        my_caller.destroy()
        my_connection.destroy()
    except Exception as e:
        LOG.error("Unexpected exception occured: %s", str(e))
        return -1

    return 0


if __name__ == "__main__":
    sys.exit(main())
