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
"""Tool to gauge message passing throughput and latencies"""

import logging
import optparse
import time
import uuid

import pyngus
from proton import Message

from utils import connect_socket
from utils import get_host_port
from utils import process_connection

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())


class ConnectionEventHandler(pyngus.ConnectionEventHandler):
    def __init__(self):
        super(ConnectionEventHandler, self).__init__()

    def connection_failed(self, connection, error):
        """Connection has failed in some way."""
        LOG.warn("Connection failed callback: %s", error)

    def connection_remote_closed(self, connection, pn_condition):
        """Peer has closed its end of the connection."""
        LOG.debug("connection_remote_closed condition=%s", pn_condition)
        connection.close()


class SenderHandler(pyngus.SenderEventHandler):
    def __init__(self, count):
        self._count = count
        self._msg = Message()
        self.calls = 0
        self.total_ack_latency = 0.0
        self.stop_time = None
        self.start_time = None

    def credit_granted(self, sender_link):
        if self.start_time is None:
            self.start_time = time.time()
            self._send_message(sender_link)

    def _send_message(self, link):
        now = time.time()
        self._msg.body = {'tx-timestamp': now}
        self._last_send = now
        link.send(self._msg, self)

    def __call__(self, link, handle, status, error):
        now = time.time()
        self.total_ack_latency += now - self._last_send
        self.calls += 1
        if self._count:
            self._count -= 1
            if self._count == 0:
                self.stop_time = now
                link.close()
                return
        self._send_message(link)

    def sender_remote_closed(self, sender_link, pn_condition):
        LOG.debug("Sender peer_closed condition=%s", pn_condition)
        sender_link.close()

    def sender_failed(self, sender_link, error):
        """Protocol error occurred."""
        LOG.debug("Sender failed error=%s", error)
        sender_link.close()


class ReceiverHandler(pyngus.ReceiverEventHandler):
    def __init__(self, count, capacity):
        self._count = count
        self._capacity = capacity
        self._msg = Message()
        self.receives = 0
        self.tx_total_latency = 0.0

    def receiver_active(self, receiver_link):
        receiver_link.add_capacity(self._capacity)

    def receiver_remote_closed(self, receiver_link, pn_condition):
        """Peer has closed its end of the link."""
        LOG.debug("receiver_remote_closed condition=%s", pn_condition)
        receiver_link.close()

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.warn("receiver_failed error=%s", error)
        receiver_link.close()

    def message_received(self, receiver, message, handle):
        now = time.time()
        receiver.message_accepted(handle)
        self.tx_total_latency += now - message.body['tx-timestamp']
        self.receives += 1
        if self._count:
            self._count -= 1
            if self._count == 0:
                receiver.close()
                return
        lc = receiver.capacity
        cap = self._capacity
        if lc < (cap / 2):
            receiver.add_capacity(cap - lc)


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="server", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="The address of the server [amqp://0.0.0.0:5672]")
    parser.add_option("--node", type='string', default='amq.topic',
                      help='Name of source/target node')
    parser.add_option("--count", type='int', default=100,
                      help='Send N messages (send forever if N==0)')
    parser.add_option("--debug", dest="debug", action="store_true",
                      help="enable debug logging")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")

    opts, _ = parser.parse_args(args=argv)
    if opts.debug:
        LOG.setLevel(logging.DEBUG)
    host, port = get_host_port(opts.server)
    my_socket = connect_socket(host, port)

    # create AMQP Container, Connection, and SenderLink
    #
    container = pyngus.Container(uuid.uuid4().hex)
    conn_properties = {'hostname': host,
                       'x-server': False}
    if opts.trace:
        conn_properties["x-trace-protocol"] = True

    c_handler = ConnectionEventHandler()
    connection = container.create_connection("perf_tool",
                                             c_handler,
                                             conn_properties)

    r_handler = ReceiverHandler(opts.count, opts.count or 1000)
    receiver = connection.create_receiver(opts.node, opts.node, r_handler)

    s_handler = SenderHandler(opts.count)
    sender = connection.create_sender(opts.node, opts.node, s_handler)

    connection.open()
    receiver.open()
    while not receiver.active:
        process_connection(connection, my_socket)

    sender.open()

    # Run until all messages transfered
    while not sender.closed or not receiver.closed:
        process_connection(connection, my_socket)
    connection.close()
    while not connection.closed:
        process_connection(connection, my_socket)

    duration = s_handler.stop_time - s_handler.start_time
    thru = s_handler.calls / duration
    permsg = duration / s_handler.calls
    ack = s_handler.total_ack_latency / s_handler.calls
    lat = r_handler.tx_total_latency / r_handler.receives
    print("Stats:\n"
          " TX Avg Calls/Sec: %f Per Call: %f Ack Latency %f\n"
          " RX Latency: %f" % (thru, permsg, ack, lat))

    sender.destroy()
    receiver.destroy()
    connection.destroy()
    container.destroy()
    my_socket.close()
    return 0


if __name__ == "__main__":
    main()
