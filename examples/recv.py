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
""" Minimal message receive example code."""

import logging
import optparse
import sys
import uuid

import pyngus
from utils import connect_socket
from utils import get_host_port
from utils import process_connection

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())


class ConnectionEventHandler(pyngus.ConnectionEventHandler):
    def connection_failed(self, connection, error):
        """Connection has failed in some way."""
        LOG.warn("Connection failed: %s", error)
        connection.close()

    def connection_remote_closed(self, connection, pn_condition):
        """Peer has closed its end of the connection."""
        LOG.debug("connection_remote_closed condition=%s", pn_condition)
        connection.close()


class ReceiverEventHandler(pyngus.ReceiverEventHandler):
    def __init__(self):
        self.done = False
        self.message = None
        self.handle = None

    def receiver_remote_closed(self, receiver_link, pn_condition):
        """Peer has closed its end of the link."""
        LOG.debug("receiver_remote_closed condition=%s", pn_condition)
        receiver_link.close()
        self.done = True

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.warn("receiver_failed error=%s", error)
        receiver_link.close()
        self.done = True

    def message_received(self, receiver, message, handle):
        self.done = True
        self.message = message
        self.handle = handle


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="server", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="The address of the server [amqp://0.0.0.0:5672]")
    parser.add_option("--idle", dest="idle_timeout", type="int",
                      default=0,
                      help="Idle timeout for connection (seconds).")
    parser.add_option("--debug", dest="debug", action="store_true",
                      help="enable debug logging")
    parser.add_option("--source", dest="source_addr", type="string",
                      help="Address for link source.")
    parser.add_option("--target", dest="target_addr", type="string",
                      help="Address for link target.")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")
    parser.add_option("--ca",
                      help="Certificate Authority PEM file")
    parser.add_option("--ssl-cert-file",
                      help="Self-identifying certificate (PEM file)")
    parser.add_option("--ssl-key-file",
                      help="Key for self-identifying certificate (PEM file)")
    parser.add_option("--ssl-key-password",
                      help="Password to unlock SSL key file")
    parser.add_option("--username", type="string",
                      help="User Id for authentication")
    parser.add_option("--password", type="string",
                      help="User password for authentication")
    parser.add_option("--sasl-mechs", type="string",
                      help="The list of acceptable SASL mechs")
    parser.add_option("--sasl-config-dir", type="string",
                      help="Path to directory containing sasl config")
    parser.add_option("--sasl-config-name", type="string",
                      help="Name of the sasl config file (without '.config')")

    opts, extra = parser.parse_args(args=argv)
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
    if opts.ca:
        conn_properties["x-ssl-ca-file"] = opts.ca
    if opts.ssl_cert_file:
        conn_properties["x-ssl-identity"] = (opts.ssl_cert_file,
                                             opts.ssl_key_file,
                                             opts.ssl_key_password)
    if opts.idle_timeout:
        conn_properties["idle-time-out"] = opts.idle_timeout
    if opts.username:
        conn_properties['x-username'] = opts.username
    if opts.password:
        conn_properties['x-password'] = opts.password
    if opts.sasl_mechs:
        conn_properties['x-sasl-mechs'] = opts.sasl_mechs
    if opts.sasl_config_dir:
        conn_properties["x-sasl-config-dir"] = opts.sasl_config_dir
    if opts.sasl_config_name:
        conn_properties["x-sasl-config-name"] = opts.sasl_config_name

    c_handler = ConnectionEventHandler()
    connection = container.create_connection("receiver",
                                             c_handler,
                                             conn_properties)
    connection.open()

    target_address = opts.target_addr or uuid.uuid4().hex
    cb = ReceiverEventHandler()
    receiver = connection.create_receiver(target_address,
                                          opts.source_addr,
                                          cb)
    receiver.add_capacity(1)
    receiver.open()

    # Poll connection until something arrives
    while not cb.done and not connection.closed:
        process_connection(connection, my_socket)

    if cb.done:
        print("Receive done, message=%s" % str(cb.message) if cb.message
              else "ERROR: no message received")
        if cb.handle:
            receiver.message_accepted(cb.handle)
    else:
        print("Receive failed due to connection failure!")

    # flush any remaining output before closing (optional)
    while connection.has_output > 0:
        process_connection(connection, my_socket)

    receiver.close()
    connection.close()

    # Poll connection until close completes:
    while not connection.closed:
        process_connection(connection, my_socket)

    receiver.destroy()
    connection.destroy()
    container.destroy()
    my_socket.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
