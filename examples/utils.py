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
"""Utilities used by the Examples"""

import errno
import logging
import re
import socket
import select
import time

import pyngus

LOG = logging.getLogger()


def get_host_port(server_address):
    """Parse the hostname and port out of the server_address."""
    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    x = regex.match(server_address)
    if not x:
        raise Exception("Bad address syntax: %s" % server_address)
    matches = x.groups()
    host = matches[0]
    port = int(matches[2]) if matches[2] else None
    return host, port


def connect_socket(host, port, blocking=True):
    """Create a TCP connection to the server."""
    addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addr:
        raise Exception("Could not translate address '%s:%s'"
                        % (host, str(port)))
    my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
    if not blocking:
        my_socket.setblocking(0)
    try:
        my_socket.connect(addr[0][4])
    except socket.error as e:
        if e.errno != errno.EINPROGRESS:
            raise
    return my_socket


def server_socket(host, port, backlog=10):
    """Create a TCP listening socket for a server."""
    addr = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addr:
        raise Exception("Could not translate address '%s:%s'"
                        % (host, str(port)))
    my_socket = socket.socket(addr[0][0], addr[0][1], addr[0][2])
    my_socket.setblocking(0)  # 0=non-blocking
    try:
        my_socket.bind(addr[0][4])
        my_socket.listen(backlog)
    except socket.error as e:
        if e.errno != errno.EINPROGRESS:
            raise
    return my_socket


def process_connection(connection, my_socket):
    """Handle I/O and Timers on a single Connection."""
    if connection.closed:
        return False

    work = False
    readfd = []
    writefd = []
    if connection.needs_input > 0:
        readfd = [my_socket]
        work = True
    if connection.has_output > 0:
        writefd = [my_socket]
        work = True

    timeout = None
    deadline = connection.next_tick
    if deadline:
        work = True
        now = time.time()
        timeout = 0 if deadline <= now else deadline - now

    if not work:
        return False

    readable, writable, ignore = select.select(readfd,
                                               writefd,
                                               [],
                                               timeout)
    if readable:
        try:
            pyngus.read_socket_input(connection, my_socket)
        except Exception as e:
            # treat any socket error as
            LOG.error("Socket error on read: %s", str(e))
            connection.close_input()
            # make an attempt to cleanly close
            connection.close()

    connection.process(time.time())
    if writable:
        try:
            pyngus.write_socket_output(connection, my_socket)
        except Exception as e:
            LOG.error("Socket error on write %s", str(e))
            connection.close_output()
            # this may not help, but it won't hurt:
            connection.close()
    return True

# Map the send callback status to a string
SEND_STATUS = {
    pyngus.SenderLink.ABORTED: "Aborted",
    pyngus.SenderLink.TIMED_OUT: "Timed-out",
    pyngus.SenderLink.UNKNOWN: "Unknown",
    pyngus.SenderLink.ACCEPTED: "Accepted",
    pyngus.SenderLink.REJECTED: "REJECTED",
    pyngus.SenderLink.RELEASED: "RELEASED",
    pyngus.SenderLink.MODIFIED: "MODIFIED"
}
