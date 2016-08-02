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
"""A benchmarking utility."""

import optparse
import sys
import time
import uuid

from proton import Message
from proton import VERSION as PN_VERSION
import pyngus


class PerfConnection(pyngus.ConnectionEventHandler):
    def __init__(self, name, container, properties):
        self.name = name
        self.connection = container.create_connection(name, self,
                                                      properties)
        self.connection.context = self

    def connection_failed(self, connection, error):
        """Connection's transport has failed in some way."""
        assert False


class PerfSendConnection(PerfConnection):
    def __init__(self, name, container, properties, msg_count, link_count):
        super(PerfSendConnection, self).__init__(name, container, properties)
        self._msg_count = msg_count
        self._link_count = link_count
        self.senders = set()
        self.connection.open()

    def connection_active(self, connection):
        for i in range(self._link_count):
            PerfSender("sender-%d" % i, self, self._msg_count)


class PerfReceiveConnection(PerfConnection):
    def __init__(self, name, container, properties,
                 msg_count, credit_window):
        super(PerfReceiveConnection, self).__init__(name, container,
                                                    properties)
        self._msg_count = msg_count
        self._credit_window = credit_window
        self.latency = 0
        self.latency_min = 100000000
        self.latency_max = 0
        self.connection.open()
        self.receivers = set()

    def sasl_step(self, connection, pn_sasl):
        # Unconditionally accept the client:
        pn_sasl.done(pn_sasl.OK)

    def connection_remote_closed(self, connection, pn_condition):
        """All senders have finished and closed, test over."""
        assert len(self.receivers) == 0
        self.connection.close()

    def receiver_requested(self, connection, link_handle,
                           name, requested_target,
                           properties):
        PerfReceiver("receiver-%d" % len(self.receivers),
                     link_handle, self,
                     self._msg_count, self._credit_window)


class PerfSender(pyngus.SenderEventHandler):
    def __init__(self, address, perf_send_conn, msg_count):
        self.msg = Message()
        self.sent = 0
        self.acked = 0
        self.msg_count = msg_count
        self.address = address
        self.perf_conn = perf_send_conn
        self.perf_conn.senders.add(self)
        connection = perf_send_conn.connection
        self.link = connection.create_sender(address, event_handler=self)
        self.link.context = self
        self.link.open()

    def sender_active(self, sender_link):
        self._send_msgs()

    def credit_granted(self, sender_link):
        self._send_msgs()

    def _send_msgs(self):
        # Send as many messages as credit allows
        while self.link.credit > 0 and self.sent < self.msg_count:
            self.msg.body = {"timestamp": time.time()}
            self.link.send(self.msg, self._send_complete)
            self.sent += 1

    def _send_complete(self, link, handle, result, info):
        self.acked += 1
        if self.acked == self.msg_count:
            # test done, shutdown
            self.link.close()
            self.perf_conn.senders.discard(self)
            if len(self.perf_conn.senders) == 0:
                self.link.connection.close()


class PerfReceiver(pyngus.ReceiverEventHandler):
    def __init__(self, address, handle, perf_receive_conn,
                 msg_count, credit_window):
        self.msg_count = msg_count
        self.received = 0
        self.credit_window = credit_window if credit_window else msg_count
        self.credit_low = (credit_window + 1) / 2
        self.address = address
        self.perf_conn = perf_receive_conn
        self.perf_conn.receivers.add(self)
        connection = perf_receive_conn.connection
        self.link = connection.accept_receiver(handle,
                                               target_override=address,
                                               event_handler=self)
        self.link.context = self
        self.link.add_capacity(self.credit_window)
        self.link.open()

    def message_received(self, receiver_link, message, handle):
        # Acknowledge receipt, grant more credit if needed
        self.link.message_accepted(handle)
        self.received += 1
        timestamp = message.body["timestamp"]
        latency = time.time() - timestamp
        self.perf_conn.latency += latency
        self.perf_conn.latency_min = min(latency, self.perf_conn.latency_min)
        self.perf_conn.latency_max = max(latency, self.perf_conn.latency_max)
        if self.link.capacity < self.credit_low and \
           self.received < self.msg_count:
            self.link.add_capacity(self.credit_window - self.link.capacity)
        elif self.received == self.msg_count:
            # link done
            self.link.close()
            self.perf_conn.receivers.discard(self)


def process_connections(c1, c2):
    # Transfer I/O, then process each connection
    def _do_io(src, dst):
        count = min(src.has_output, dst.needs_input)
        if count > 0:
            count = dst.process_input(src.output_data())
            if count > 0:
                src.output_written(count)

    _do_io(c1, c2)
    _do_io(c2, c1)
    c1.process(time.time())
    c2.process(time.time())


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("--count", dest="count", type="int",
                      default=10000,
                      help="# of messages to transfer per link.")
    parser.add_option("--links", dest="link_count", type="int",
                      default=20,
                      help="# of link pairs.")
    parser.add_option("--send-batch", dest="send_batch", type="int",
                      default=10,
                      help="DEPRECATED")
    parser.add_option("--credit", dest="credit_window", type="int",
                      default=10,
                      help="Credit window issued by receiver.")
    parser.add_option("--ca",
                      help="Certificate Authority PEM file")
    parser.add_option("--cert",
                      help="PEM File containing the server's certificate")
    parser.add_option("--key",
                      help="PEM File containing the server's private key")
    parser.add_option("--keypass",
                      help="Password used to decrypt key file")

    opts, extra = parser.parse_args(args=argv)

    container = pyngus.Container(uuid.uuid4().hex)

    # sender acts like SSL client
    conn_properties = {'hostname': "test.server.com",
                       'x-trace-protocol': False,
                       'x-sasl-mechs': "ANONYMOUS"}
    if opts.ca:
        conn_properties["x-ssl-ca-file"] = opts.ca
    sender_conn = PerfSendConnection("send-conn",
                                     container,
                                     conn_properties,
                                     opts.count,
                                     opts.link_count)

    # receiver acts as SSL server
    conn_properties = {'hostname': "my.client.com",
                       'x-server': True,
                       'x-sasl-mechs': "ANONYMOUS"}
    if opts.cert:
        identity = (opts.cert, opts.key, opts.keypass)
        conn_properties["x-ssl-identity"] = identity
    receiver_conn = PerfReceiveConnection("recv-conn",
                                          container,
                                          conn_properties,
                                          opts.count,
                                          opts.credit_window)

    # process connections until finished:
    start = time.time()
    while ((not sender_conn.connection.closed) or
           (not receiver_conn.connection.closed)):
        process_connections(sender_conn.connection, receiver_conn.connection)

    sender_conn.connection.destroy()
    receiver_conn.connection.destroy()
    container.destroy()
    delta = time.time() - start
    total = opts.count * opts.link_count
    print("Total: %s messages; credit window: %s; proton %s"
          % (total, opts.credit_window, PN_VERSION))
    print("%d Messages/second; Latency avg: %.3fms min: %.3fms max: %.3fms"
          % (total / delta, (receiver_conn.latency / total) * 1000.0,
             receiver_conn.latency_min * 1000.0,
             receiver_conn.latency_max * 1000.0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
