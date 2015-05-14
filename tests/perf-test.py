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
    def __init__(self, name, container, properties,
                 msg_count, batch_size, link_count):
        super(PerfSendConnection, self).__init__(name, container,
                                                 properties)
        self._msg_count = msg_count
        self._batch_size = batch_size
        self._link_count = link_count
        self.connection.pn_sasl.mechanisms("ANONYMOUS")
        self.connection.pn_sasl.client()
        self.senders = set()
        self.connection.open()

    def connection_active(self, connection):
        for i in range(self._link_count):
            PerfSender("sender-%d" % i,
                       self,
                       self._msg_count,
                       self._batch_size)


class PerfReceiveConnection(PerfConnection):
    def __init__(self, name, container, properties,
                 msg_count, credit_window):
        super(PerfReceiveConnection, self).__init__(name, container,
                                                    properties)
        self._msg_count = msg_count
        self._credit_window = credit_window
        self.connection.pn_sasl.mechanisms("ANONYMOUS")
        self.connection.pn_sasl.server()
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
    def __init__(self, address, perf_send_conn,
                 msg_count, batch_size):
        self.msg = Message()
        self.msg.body = "HELLO"
        self.sent = 0
        self.acked = 0
        self.msg_count = msg_count
        self.batch_size = batch_size
        self.address = address
        self.perf_conn = perf_send_conn
        self.perf_conn.senders.add(self)
        connection = perf_send_conn.connection
        self.link = connection.create_sender(address,
                                             event_handler=self)
        self.link.context = self
        self.link.open()

    def _send_msgs(self):
        """Send up to a batch of messages."""
        batch = min(self.msg_count - self.sent, self.batch_size)
        target = self.sent + batch
        while self.sent < target:
            self.link.send(self.msg, self.send_complete)
            self.sent += 1

    def sender_active(self, sender_link):
        self._send_msgs()

    def send_complete(self, link, handle, result, info):
        self.acked += 1
        if self.acked == self.msg_count:
            # test done, shutdown
            self.link.close()
            self.perf_conn.senders.discard(self)
            if len(self.perf_conn.senders) == 0:
                self.link.connection.close()
        elif self.acked == self.sent:
            # send next batch
            self._send_msgs()


class PerfReceiver(pyngus.ReceiverEventHandler):
    def __init__(self, address, handle, perf_receive_conn,
                 msg_count, credit_window):
        self.msg_count = msg_count
        self.received = 0
        self.credit_window = credit_window if credit_window else msg_count
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
        """Acknowledge receipt, grant more credit if needed."""
        self.link.message_accepted(handle)
        self.received += 1
        if self.received < self.msg_count:
            if self.link.capacity == 0:
                self.link.add_capacity(self.credit_window)
        else:
            # link done
            self.link.close()
            self.perf_conn.receivers.discard(self)


def process_connections(c1, c2):
    """Transfer I/O, then process each connection."""
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
                      help="# of messages to transfer.")
    parser.add_option("--links", dest="link_count", type="int",
                      default=100,
                      help="# of link pairs.")
    parser.add_option("--send-batch", dest="send_batch", type="int",
                      default=10,
                      help="# of msgs sender queues at once.")
    parser.add_option("--credit-batch", dest="credit_batch", type="int",
                      default=5,
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
                       'x-trace-protocol': False}
    if opts.ca:
        conn_properties["x-ssl-ca-file"] = opts.ca
    sender_conn = PerfSendConnection("send-conn",
                                     container,
                                     conn_properties,
                                     opts.count, opts.send_batch,
                                     opts.link_count)

    # receiver acts as SSL server
    conn_properties = {'hostname': "my.client.com"}
    if opts.cert:
        conn_properties["x-ssl-server"] = True
        identity = (opts.cert, opts.key, opts.keypass)
        conn_properties["x-ssl-identity"] = identity
    receiver_conn = PerfReceiveConnection("recv-conn",
                                          container,
                                          conn_properties,
                                          opts.count,
                                          opts.credit_batch)

    # process connections until finished:
    while ((not sender_conn.connection.closed) or
           (not receiver_conn.connection.closed)):
        process_connections(sender_conn.connection,
                            receiver_conn.connection)

    sender_conn.connection.destroy()
    receiver_conn.connection.destroy()
    container.destroy()
    return 0


if __name__ == "__main__":
    sys.exit(main())
