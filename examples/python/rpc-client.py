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
import fusion


"""
This module implements a simple RPC client.  The client sends a 'method call'
to a server, and waits for a response.  The method call is a map of the form:
{'method': '<name of method on server>',
 'args': {<map of name=value arguments for the call}
}
"""


class MyConnection(fusion.ConnectionEventHandler):

    def __init__(self, name, socket, container, properties):
        self.name = name
        self.socket = socket
        self.connection = container.create_connection(name, self,
                                                      properties)
        self.connection.user_context = self

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

        print("select start (t=%s)" % str(timeout))
        readable,writable,ignore = select.select(readfd,writefd,[],timeout)
        print("select return")

        if readable:
            count = self.connection.needs_input
            if count > 0:
                try:
                    sock_data = self.socket.recv(count)
                    if sock_data:
                        self.connection.process_input( sock_data )
                    else:
                        # closed?
                        self.connection.close_input()
                except socket.timeout, e:
                    raise  # I don't expect this
                except socket.error, e:
                    err = e.args[0]
                    # ignore non-fatal errors
                    if (err != errno.EAGAIN and
                        err != errno.EWOULDBLOCK and
                        err != errno.EINTR):
                        # otherwise, unrecoverable:
                        self.connection.close_input()
                        raise
                except:  # beats me...
                    self.connection.close_input()
                    raise

        if writable:
            data = self.connection.output_data()
            if data:
                try:
                    rc = self.socket.send(data)
                    if rc > 0:
                        self.connection.output_written(rc)
                    else:
                        # else socket closed
                        self.connection.close_output()
                except socket.timeout, e:
                    raise # I don't expect this
                except socket.error, e:
                    err = e.args[0]
                    # ignore non-fatal errors
                    if (err != errno.EAGAIN and
                        err != errno.EWOULDBLOCK and
                        err != errno.EINTR):
                        # otherwise, unrecoverable
                        self.connection.close_output()
                        raise
                except:  # beats me...
                    self.connection.close_output()
                    raise

        self.connection.process(time.time())

    def close(self, error=None):
        self.connection.close(error)

    @property
    def closed(self):
        return self.connection.closed

    def destroy(self, error=None):
        self.connection.user_context = None
        self.connection.destroy()
        self.connection = None
        self.socket.close()
        self.socket = None

    # Connection callbacks:

    def connection_active(self, connection):
        """connection handshake completed"""
        print "APP: CONN ACTIVE"

    def connection_closed(self, connection, reason):
        print "APP: CONN CLOSED"

    def sender_requested(self, connection, link_handle,
                         requested_source, properties={}):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        assert False, "Not expected"

    def receiver_requested(self, connection, link_handle,
                           requested_target, properties={}):
        # call accept_sender to accept new link,
        # reject_sender to reject it.
        assert False, "Not expected"

    def sasl_done(self, connection, result):
        print "APP: SASL DONE"
        print result


class MyCaller(fusion.SenderEventHandler,
               fusion.ReceiverEventHandler):
    """
    """

    def __init__(self, method_map, my_connection,
                 my_source, my_target,
                 receiver_properties, sender_properties):
        conn = my_connection.connection
        self._sender = conn.create_sender(my_source, target_address=None,
                                          eventHandler=self, name=my_source,
                                          properties=sender_properties)
        self._receiver = conn.create_receiver(my_target, source_address=None,
                                              eventHandler=self, name=my_target,
                                              properties=receiver_properties)
        self._method = method_map
        self._reply_to = None
        self._to = None

        self._send_completed = False
        self._response_received = False

    def done(self):
        return self._send_completed and self._response_received

    def close(self):
        self._sender.close(None)
        self._receiver.close(None)

    def destroy(self):
        if self._sender:
            self._sender.destroy()
        if self._receiver:
            self._receiver.destroy()

    def _send_request(self):
        """Send a message containing the RPC method call
        """
        msg = Message()
        msg.subject = "An RPC call!"
        msg.address = self._to
        msg.reply_to = self._reply_to
        msg.body = self._method
        msg.correlation_id = 5  # whatever...

        self._sender.send(msg, self, None, time.time() + 10)

    # SenderEventHandler callbacks:

    def sender_active(self, sender_link):
        print "APP: SENDER ACTIVE"
        assert sender_link is self._sender
        self._to = sender_link.target_address
        assert self._to, "Expected a target address!!!"
        if self._reply_to:
            self._send_request()

    def sender_closed(self, sender_link, error):
        print "APP: SENDER CLOSED"
        assert sender_link is self._sender
        self._send_completed = True

    # send complete callback:

    def __call__(self, sender_link, handle, status, error):
        print "APP: MESSAGE SENT CALLBACK %s" % status
        self._send_completed = True

    # ReceiverEventHandler callbacks:

    def receiver_active(self, receiver_link):
        print "APP: RECEIVER ACTIVE"
        assert receiver_link is self._receiver
        self._reply_to = receiver_link.source_address
        assert self._reply_to, "Expected a source address!!!"
        if self._to:
            self._send_request()

    def receiver_closed(self, receiver_link, error):
        print "APP: RECEIVER CLOSED"
        assert receiver_link is self._receiver
        self._response_received = True

    def message_received(self, receiver_link, message, handle):
        print "APP: MESSAGE RECEIVED"
        assert receiver_link is self._receiver
        print "Response received: %s" % str(message)
        receiver_link.message_accepted(handle)
        self._response_received = True

        #self._sender.destroy()
        #del self._sender
        #self._receiver.destroy()
        #del self._receiver


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
        assert False, "An even number of method arguments are required!"

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
    container = fusion.Container(uuid.uuid4().hex)
    my_connection = MyConnection( "to-server", my_socket,
                                  container, {})

    # @todo: need better sasl + server
    my_connection.connection.sasl.mechanisms("ANONYMOUS")
    my_connection.connection.sasl.client()

    # Create the RPC caller
    method = {'method': method_info[0],
              'args': dict([(method_info[i], method_info[i+1])
                            for i in range(1, len(method_info), 2)])}
    my_caller = MyCaller( method,
                          my_connection,
                          "my-source-address",
                          "my-target-address",
                          receiver_properties={"capacity": 1},
                          sender_properties={})

    while not my_caller.done():
        my_connection.process()

    print "DONE"

    my_caller.close()
    my_connection.close()
    while not my_connection.closed:
        my_connection.process()

    print "CLOSED"
    my_caller.destroy()
    my_connection.destroy()

    return 0


if __name__ == "__main__":
    sys.exit(main())

