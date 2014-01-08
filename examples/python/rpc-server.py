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
import optparse, sys, time, uuid
import re, socket, select, errno
import logging

from proton import Message
import fusion

LOG = logging.getLogger()
LOG.addHandler(logging.StreamHandler())

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


# Maps of outgoing and incoming links
sender_links = {}  # indexed by Source address
receiver_links = {} # indexed by Target address


class SocketConnection(fusion.ConnectionEventHandler):
    """Associates a fusion Connection with a python network socket"""

    def __init__(self, name, socket, container, conn_properties):
        self.name = name
        self.socket = socket
        self.connection = container.create_connection(name, self,
                                                      conn_properties)
        self.connection.user_context = self
        self.connection.sasl.mechanisms("ANONYMOUS")
        self.connection.sasl.server()
        self.connection.open()

    def fileno(self):
        """Allows use of a SocketConnection in a select() call.
        """
        return self.socket.fileno()

    def process_input(self):
        """Called when socket is read-ready"""
        rc = fusion.read_socket_input(self.connection,
                                      self.socket)
        self.connection.process(time.time())
        return rc

    def send_output(self):
        """Called when socket is write-ready"""
        rc = fusion.write_socket_output(self.connection,
                                        self.socket)
        self.connection.process(time.time())
        return rc

    # ConnectionEventHandler callbacks:

    def connection_active(self, connection):
        LOG.debug("Connection active callback")

    def connection_closed(self, connection, reason):
        LOG.debug("Connection closed callback")

    def sender_requested(self, connection, link_handle,
                         requested_source, properties):
        LOG.debug("sender requested callback")
        global sender_links

        name = uuid.uuid4().hex
        # allow for requested_source address if it doesn't conflict with an
        # existing address
        if not requested_source or requested_source in sender_links:
            requested_source = "/%s/%s" % (connection.container.name,
                                           name)
            assert requested_source not in sender_links

        sender = MySenderLink(connection, link_handle, requested_source,
                              name, {})
        sender_links[requested_source] = sender
        print("New Sender link created, source=%s" % requested_source)

    def receiver_requested(self, connection, link_handle,
                           requested_target, properties):
        LOG.debug("receiver requested callback")
        # allow for requested_source address if it doesn't conflict with an
        # existing address
        global receiver_links

        name = uuid.uuid4().hex
        if not requested_target or requested_target in receiver_links:
            requested_target = "/%s/%s" % (connection.container.name,
                                           name)
            assert requested_target not in receiver_links

        receiver = MyReceiverLink(connection, link_handle,
                                  requested_target, name)
        receiver_links[requested_target] = receiver
        print("New Receiver link created, target=%s" % requested_target)

    # SASL callbacks:

    def sasl_step(self, connection, pn_sasl):
        LOG.debug("SASL step callback")
        pn_sasl.done(pn_sasl.OK)

    def sasl_done(self, connection, result):
        LOG.debug("SASL done callback, result=%s", str(result))


class MySenderLink(fusion.SenderEventHandler):
    """
    """
    def __init__(self, connection, link_handle, source_address,
                 name, properties):

        self.sender_link = connection.accept_sender(link_handle,
                                                    source_address,
                                                    self,
                                                    name,
                                                    properties)
        self.sender_link.open()

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        LOG.debug("sender active callback")

    def sender_closed(self, sender_link, error):
        LOG.debug("sender closed callback")

    # 'message sent' callback:

    def __call__(self, sender, handle, status, error=None):
        LOG.debug("message sent callback, status=%s", str(status))


class MyReceiverLink(fusion.ReceiverEventHandler):
    """
    """
    def __init__(self, connection, link_handle, target_address,
                 name, properties={}):
        self._link = connection.accept_receiver(link_handle,
                                                target_address,
                                                self,
                                                name,
                                                properties)
        self._link.open()

    # ReceiverEventHandler callbacks:
    def receiver_active(self, receiver_link):
        LOG.debug("receiver active callback")
        self._link.add_capacity(5)

    def receiver_closed(self, receiver_link, error):
        LOG.debug("receiver closed callback")

    def message_received(self, receiver_link, message, handle):
        LOG.debug("message received callback")

        global sender_links

        # extract to reply-to, correlation id
        reply_to = message.reply_to
        if not reply_to or reply_to not in sender_links:
            LOG.error("sender for reply-to not found, reply-to=%s",
                      str(reply_to))
            self._link.message_rejected(handle, "Bad reply-to address")
        else:
            my_sender = sender_links[reply_to]
            correlation_id = message.correlation_id
            method_map = message.body
            if (not isinstance(method_map, dict) or
                'method' not in method_map):
                LOG.error("no method given, map=%s", str(method_map))
                self._link.message_rejected(handle, "Bad format")
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
                #link.send( response, my_sender,
                #           message, time.time() + 5.0)
                link.send(response, my_sender, message)

                self._link.message_accepted(handle)

        if self._link.capacity == 0:
            LOG.debug("increasing credit...")
            self._link.add_capacity( 5 )


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="address", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="""The socket address this server will listen on
 [amqp://0.0.0.0:5672]""")
    parser.add_option("--trace", dest="trace", action="store_true",
                      help="enable protocol tracing")
    parser.add_option("--debug", dest="debug", action="store_true",
                      help="enable debug logging")

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
    my_socket.setblocking(0) # 0=non-blocking
    try:
        my_socket.bind((host, port))
        my_socket.listen(10)
    except socket.error, e:
        if e[0] != errno.EINPROGRESS:
            raise

    # create an AMQP container that will 'provide' the RPC service
    #
    container = fusion.Container("example RPC service")
    socket_connections = {} # indexed by name (uuid)

    while True:

        #
        # Poll for I/O & timers
        #

        readfd = [my_socket]
        writefd = []
        readers,writers,timers = container.need_processing()

        # map fusion Connections back to my SocketConnections
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
            deadline = timers[0].next_tick # 0 == next expiring timer
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        LOG.debug("select() start (t=%s)", str(timeout))
        readable,writable,ignore = select.select(readfd,writefd,[],timeout)
        LOG.debug("select() returned")

        for r in readable:
            if r is my_socket:
                # new inbound connection request
                client_socket, client_address = my_socket.accept()
                name = uuid.uuid4().hex
                assert name not in socket_connections
                conn_properties = {}
                if opts.trace:
                    conn_properties["trace"] = True
                socket_connections[name] = SocketConnection(name,
                                                            client_socket,
                                                            container,
                                                            conn_properties)
            else:
                assert isinstance(r, SocketConnection)
                rc = r.process_input()

        for t in timers:
            now = time.time()
            if t.next_tick > now:
                break
            t.process(now)

        for w in writable:
            assert isinstance(w, SocketConnection)
            rc = w.send_output()

    return 0


if __name__ == "__main__":
    sys.exit(main())

