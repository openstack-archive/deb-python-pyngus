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

from proton import Message

# @todo stop the madness:
import container as fusion_container
import connection as fusion_connection
import link as fusion_link


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


class SocketConnection(fusion_connection.ConnectionEventHandler):
    """Associates a fusion Connection with a python network socket"""

    def __init__(self, name, socket, container, conn_properties):
        self.name = name
        self.socket = socket
        self.connection = container.create_connection(name, self,
                                                      conn_properties)
        self.connection.user_context = self

    def fileno(self):
        """Allows use of a SocketConnection in a select() call.
        """
        return self.socket.fileno()

    # ConnectionEventHandler callbacks:
    def connection_active(self, connection):
        print "APP: CONN ACTIVE"

    def connection_closed(self, connection, reason):
        print "APP: CONN CLOSED"

    def sender_requested(self, connection, link_handle,
                         requested_source, properties):
        print "APP: SENDER LINK REQUESTED"
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
        print "APP: NEW SENDER LINK CREATED, source=%s" % requested_source

    def receiver_requested(self, connection, link_handle,
                           requested_target, properties):
        print "APP: RECEIVER LINK REQUESTED"
        # allow for requested_source address if it doesn't conflict with an
        # existing address
        global receiver_links

        name = uuid.uuid4().hex
        if not requested_target or requested_target in receiver_links:
            requested_target = "/%s/%s" % (connection.container.name,
                                           name)
            assert requested_target not in receiver_links

        receiver = MyReceiverLink(connection, link_handle,
                                   requested_target, name,
                                  {"capacity": 3})
        receiver_links[requested_target] = receiver
        print "APP: NEW RECEIVER LINK CREATED, target=%s" % requested_target


    def sasl_done(self, connection, result):
        print "APP: SASL DONE"
        print result


class MySenderLink(fusion_link.SenderEventHandler):
    """
    """
    def __init__(self, connection, link_handle, source_address,
                 name, properties):

        self.sender_link = connection.accept_sender(link_handle,
                                                    source_address,
                                                    self,
                                                    name,
                                                    properties)

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        print "APP: SENDER ACTIVE"

    def sender_closed(self, sender_link, error):
        print "APP: SENDER CLOSED"

    # 'message sent' callback:

    def __call__(self, sender, handle, status, error=None):
        print "APP: MESSAGE SENT CALLBACK %s" % status


class MyReceiverLink(fusion_link.ReceiverEventHandler):
    """
    """
    def __init__(self, connection, link_handle, target_address,
                 name, properties):
        self._link = connection.accept_receiver(link_handle,
                                                target_address,
                                                self,
                                                name,
                                                properties)

    # ReceiverEventHandler callbacks:
    def receiver_active(self, receiver_link):
        print "APP: RECEIVER ACTIVE"

    def receiver_closed(self, receiver_link, error):
        print "APP: RECEIVER CLOSED"

    def message_received(self, receiver_link, message, handle):
        print "APP: MESSAGE RECEIVED"

        global sender_links

        # extract to reply-to, correlation id
        reply_to = message.reply_to
        if not reply_to or reply_to not in sender_links:
            self._link.message_rejected(handle, "Bad reply-to address")
        else:
            my_sender = sender_links[reply_to]
            correlation_id = message.correlation_id
            method_map = message.body
            if (not isinstance(method_map, dict) or
                'method' not in method_map):
                self._link.message_rejected(handle, "Bad format")
            else:
                print "Echoing back message map"
                response = Message()
                response.address = reply_to
                response.subject = message.subject
                response.correlation_id = correlation_id
                response.body = {"response": method_map}

                link = my_sender.sender_link
                link.send( response, my_sender,
                           message, time.time() + 5.0)

        if self._link.capacity == 0:
            self._link.add_capacity( 5 )


def main(argv=None):

    _usage = """Usage: %prog [options]"""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="address", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="""The socket address this server will listen on
 [amqp://0.0.0.0:5672]""")

    opts, arguments = parser.parse_args(args=argv)

    # Create a socket for inbound connections
    #
    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    print("Listening on %s" % opts.address)
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
    container = fusion_container.Container("example RPC service")
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

        print("select start (t=%s)" % str(timeout))
        readable,writable,ignore = select.select(readfd,writefd,[],timeout)
        print("select return")

        for r in readable:
            if r is my_socket:
                # new inbound connection request
                client_socket, client_address = my_socket.accept()
                name = uuid.uuid4().hex
                assert name not in socket_connections
                socket_connections[name] = SocketConnection(name,
                                                            client_socket,
                                                            container, {})
            else:
                assert isinstance(r, SocketConnection)
                count = r.connection.needs_input
                if count > 0:
                    try:
                        sock_data = r.socket.recv(count)
                        if sock_data:
                            r.connection.process_input( sock_data )
                        else:
                            # closed?
                            r.connection.close_input()
                    except socket.timeout, e:
                        raise  # I don't expect this
                    except socket.error, e:
                        err = e.args[0]
                        # ignore non-fatal errors
                        if (err != errno.EAGAIN and
                            err != errno.EWOULDBLOCK and
                            err != errno.EINTR):
                            # otherwise, unrecoverable:
                            r.connection.close_input()
                            raise
                    except:  # beats me...
                        r.connection.close_input()
                        raise
                    r.connection.process(time.time())

        for t in timers:
            now = time.time()
            if t.next_tick > now:
                break
            t.process(now)

        for w in writable:
            assert isinstance(w, SocketConnection)
            data = w.connection.output_data()
            if data:
                try:
                    rc = w.socket.send(data)
                    if rc > 0:
                        w.connection.output_written(rc)
                    else:
                        # else socket closed
                        w.connection.close_output()
                except socket.timeout, e:
                    raise # I don't expect this
                except socket.error, e:
                    err = e.args[0]
                    # ignore non-fatal errors
                    if (err != errno.EAGAIN and
                        err != errno.EWOULDBLOCK and
                        err != errno.EINTR):
                        # otherwise, unrecoverable
                        w.connection.close_output()
                        raise
                except:  # beats me...
                    w.connection.close_output()
                    raise
                w.connection.process(time.time())

    return 0


if __name__ == "__main__":
    sys.exit(main())

