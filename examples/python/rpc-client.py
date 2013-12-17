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
This module implements a simple RPC client.  The client sends a 'method call'
to a server, and waits for a response.  The method call is a map of the form:
{'method': '<name of method on server>',
 'args': {<map of name=value arguments for the call}
}
"""


class MyConnectionEventHandler(fusion_connection.ConnectionEventHandler):

    def sasl_done(self, connection, result):
        print "APP: SASL DONE"
        print result

    def connection_closed(self, connection, reason):
        print "APP: CONN CLOSED"

    def link_pending(self, connection, link):
        print "APP: LINK PENDING"
        return True


class MySenderEventHandler(fusion_link.SenderEventHandler):
    """
    """
    def sender_active(self, sender_link):
        print "APP: SENDER ACTIVE"

    def sender_closed(self, sender_link, error):
        print "APP: SENDER CLOSED"


class MyReceiverEventHandler(fusion_link.ReceiverEventHandler):

    def receiver_active(self, receiver_link):
        print "APP: RECEIVER ACTIVE"

    def receiver_closed(self, receiver_link, error):
        print "APP: RECEIVER CLOSED"

    def message_received(self, receiver_link, message, handle):
        print "APP: MESSAGE RECEIVED"


def send_callback(sender, handle, status, error=None):
    print "APP: MESSAGE SENT CALLBACK"


def main(argv=None):

    _usage = """Usage: %prog [options] method arg1 value1 [arg2 value2] ..."""
    parser = optparse.OptionParser(usage=_usage)
    parser.add_option("-a", dest="server", type="string",
                      default="amqp://0.0.0.0:5672",
                      help="The address of the server [amqp://0.0.0.0:5672]")
    parser.add_option("-t", "--timeout", dest="timeout", type="int",
                      help="timeout used when waiting for reply, in seconds")

    opts, method_info = parser.parse_args(args=argv)
    if not method_info:
        assert False, "No method info specified!"
    if len(method_info) % 2 != 1:
        assert False, "An even number of method arguments is required!"


    method = {'method': method_info[0],
              'args': dict([(method_info[i], method_info[i+1])
                            for i in range(1, len(method_info), 2)])}

    print "method=%s" % str(method)


    # Create a socket connection to the server
    #
    regex = re.compile(r"^amqp://([a-zA-Z0-9.]+)(:([\d]+))?$")
    print("Connecting to %s" % opts.server)
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
    my_socket.setblocking(0) # 0=non-blocking
    try:
        my_socket.connect(addr[0][4])
    except socket.error, e:
        if e[0] != errno.EINPROGRESS:
            raise

    # create AMQP container, connection, sender and receiver
    #
    conn_handler = MyConnectionEventHandler()
    send_handler = MySenderEventHandler()
    recv_handler = MyReceiverEventHandler()

    container = fusion_container.Container(uuid.uuid4().hex)
    connection = container.create_connection("server",
                                             conn_handler)
    # @todo: need better sasl + server
    connection.sasl.mechanisms("ANONYMOUS")
    connection.sasl.client()

    sender = connection.create_sender( "rpc-client-src",
                                       "rpc-server-tgt",
                                       send_handler )

    receiver = connection.create_receiver( "rpc-client-tgt",
                                           "rpc-server-src",
                                           recv_handler )

    # send a message containing the RPC method call
    #
    msg = Message()
    msg.address = "rpc-server-address"
    msg.subject = "An RPC call!"
    msg.reply_to = "rpc-client-tgt"
    msg.body = method

    sender.send( msg, send_callback, "my-handle", opts.timeout )

    while not connection.closed:

        #
        # Poll for I/O & timers
        #

        readfd = []
        writefd = []
        if connection.needs_input > 0:
            readfd = [my_socket]
        if connection.has_output > 0:
            writefd = [my_socket]

        timeout = None
        deadline = connection.next_tick
        if deadline:
            now = time.time()
            timeout = 0 if deadline <= now else deadline - now

        print("select start (t=%s)" % str(timeout))
        readable,writable,ignore = select.select(readfd,writefd,[],timeout)
        print("select return")

        if readable:
            count = connection.needs_input
            if count > 0:
                try:
                    sock_data = my_socket.recv(count)
                    if sock_data:
                        connection.process_input( sock_data )
                    else:
                        # closed?
                        connection.close_input()
                except socket.timeout, e:
                    raise  # I don't expect this
                except socket.error, e:
                    err = e.args[0]
                    # ignore non-fatal errors
                    if (err != errno.EAGAIN and
                        err != errno.EWOULDBLOCK and
                        err != errno.EINTR):
                        # otherwise, unrecoverable:
                        connection.close_input()
                        raise
                except:  # beats me...
                    connection.close_input()
                    raise

        if writable:
            data = connection.output_data()
            if data:
                try:
                    rc = my_socket.send(data)
                    if rc > 0:
                        connection.output_written(rc)
                    else:
                        # else socket closed
                        connection.close_output()
                except socket.timeout, e:
                    raise # I don't expect this
                except socket.error, e:
                    err = e.args[0]
                    # ignore non-fatal errors
                    if (err != errno.EAGAIN and
                        err != errno.EWOULDBLOCK and
                        err != errno.EINTR):
                        # otherwise, unrecoverable
                        connection.close_output()
                        raise
                except:  # beats me...
                    connection.close_output()
                    raise

        connection.process(time.time())

    print "DONE"

    return 0


if __name__ == "__main__":
    sys.exit(main())

