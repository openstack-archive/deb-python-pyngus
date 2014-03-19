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
""" Minimal message send example code."""

import optparse
import sys
import uuid

from proton import Message
import fusion
from utils import connect_socket
from utils import get_host_port
from utils import process_connection
from utils import SEND_STATUS


def main(argv=None):

    _usage = """Usage: %prog [options] [message content string]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="server", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="The address of the server [amqp://0.0.0.0:5672]")
    parser.add_option("--idle", dest="idle_timeout", type="int",
                      default=0,
                      help="Idle timeout for connection (seconds).")
    parser.add_option("--source", dest="source_addr", type="string",
                      help="Address for link source.")
    parser.add_option("--target", dest="target_addr", type="string",
                      help="Address for link target.")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")
    parser.add_option("--ca",
                      help="Certificate Authority PEM file")

    opts, payload = parser.parse_args(args=argv)
    if not payload:
        payload = "Hi There!"

    host, port = get_host_port(opts.server)
    my_socket = connect_socket(host, port)

    # create AMQP Container, Connection, and SenderLink
    #
    container = fusion.Container(uuid.uuid4().hex)
    conn_properties = {'hostname': host}
    if opts.trace:
        conn_properties["x-trace-protocol"] = True
    if opts.ca:
        conn_properties["x-ssl-ca-file"] = opts.ca
    if opts.idle_timeout:
        conn_properties["idle-time-out"] = opts.idle_timeout

    connection = container.create_connection("sender",
                                             None,  # no events
                                             conn_properties)
    connection.pn_sasl.mechanisms("ANONYMOUS")
    connection.pn_sasl.client()
    connection.open()

    source_address = opts.source_addr or uuid.uuid4().hex
    sender = connection.create_sender(source_address,
                                      opts.target_addr)
    sender.open()

    # Send a single message:
    msg = Message()
    msg.body = str(payload)

    class SendCallback(object):
        def __init__(self):
            self.done = False
            self.status = None

        def __call__(self, link, handle, status, error):
            self.done = True
            self.status = status

    cb = SendCallback()
    sender.send(msg, cb)

    # Poll connection until SendCallback is invoked:
    while not cb.done:
        process_connection(connection, my_socket)

    print("Send done, status=%s" % SEND_STATUS.get(cb.status,
                                                   "???"))
    sender.close()
    connection.close()

    # Poll connection until close completes:
    while not connection.closed:
        process_connection(connection, my_socket)

    sender.destroy()
    connection.destroy()
    container.destroy()
    my_socket.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
