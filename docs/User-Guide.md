# Pyngus #

A callback-based messaging framework built around the QPID Proton
engine.

# Purpose #

This framework is meant to ease the integration of AMQP 1.0 messaging
into applications that use a callback-based logic flow.  It provides a
very basic, connection-oriented messaging model that should meet the
needs of most applications.

The framework has been designed with the following goals in mind:

* provide a callback-based messaging API

* simplify the user model exported by the Proton engine - you should
not have to be an expert in AMQP to use this framework!

* give the application control of the I/O implementation where
  possible

* limit the functionality provided by Proton to a subset that
should be adequate for 79% of all messaging use-cases [1]

All actions are designed to be non-blocking,
leveraging callbacks where asynchronous behavior is modeled.

There is no threading architecture assumed or locking performed by
this framework.  Locking is assumed to be handled outside of this
framework by the application - all processing provided by this
framework is assumed to be single-threaded [2].

[1] Unlike the Proton Engine, this framework does not intend to
support all functionality and features defined by the AMQP 1.0
protocol. It is only intended to provide basic message passing
functionality.

[2] Not entirely true - it may be possible to multithread as long as
connections are not shared across threads.  TBD

## What this framework doesn't do ##

* __Message management__ - All messages are assumed to be Proton
  Messages.  Creating and parsing Messages is left to the application.

* __Routing__ - This framework basically ignores the "to" or "reply-to"
  contained in the message.  It leaves these fields under the control
  and interpretation of the application.  This means that the
  application determines the proper Link over which to send an
  outgoing message.  In addition, it assumes the application can
  dispatch messages arriving on a link to the proper handler [3].

* __Connection management__ - It is expected that your application will
  manage the creation and configuration of sockets. Whether those
  sockets are created by initiating a connection or accepting an
  inbound connection is irrelevant to this framework.  It is also
  assumed that, if desired, your application will be responsible for
  monitoring the sockets for I/O activity (e.g. call poll()).  The
  framework will support both blocking and non-blocking sockets,
  however it may block when doing I/O over a blocking socket.  Note
  well: reconnect and failover must also be handled by the application
  [3].

* __Flow control__ - It is assumed the application will control the
  number of messages that can be accepted by a receiving link
  (capacity).  Sent messages will be queued locally until credit is
  made available for the message(s) to be transmitted.  The framework's
  API allows the application to monitor the amount of credit available
  on an outgoing link [3].

[3] All these features are provided by the QPID Messenger API.  The
Messenger API may be a better match for your application if any of
these features are required.  See the Messenger section of the [Apache
QPID website](http://qpid.apache.org/components/messenger/index.html
"Messenger") for more detail.

# Theory of Operations #

This framework defines the following set of objects:

 * __Container__ - an implementation of the container concept defined
      by AMQP 1.0.  This object is a factory for Connections.

 * __Connection__ - an implementation of the connection concept defined by
       AMQP 1.0.  You can think of this as a data pipe between two
       Containers.  This object is a factory for links.

 * __Links__ - A uni-directional pipe for messages traveling between
       resources (nodes) within a container.  A link exists within a
       Connection, and uses the Connection as its data path to the
       remote.  There are two sub-classes of Links: *SenderLinks* and
       *ReceiverLinks*.  SenderLinks produce messages from a particular
       node.  ReceiverLinks consume messages on behalf of a local
       node.

An application creates one or more Containers, which represents a
domain for a set of message-oriented **nodes**.  Nodes are those
components within an application that are the source or sink of a
message flow.  Example nodes would include message queues, message
topics, a database, an event logger, etc.  A node is identified by its
name, which must uniquely identify the node within its container.

To pass messages between nodes, the application must first set up a
Connection between the two Containers that hold the nodes.  To do
this, the application creates a network connection to the system that
holds the remote Container.  How this network connection is created is
determined by the application's design and purpose.  For example, the
application may proactively initiate these network connections
(eg. call connect()), or passively listen for connection requests
coming from remote systems (eg. listen()/accept()).  The method used
by the application to determine which systems it should connect to in
order to access particular resources (eg. nodes) is left to the
application designers.

The application must then create a Connection object to manage the
network connection it has set up.  A Connection object is allocated
from the Container, and represents the data pipe between the local and
remote Containers. The Connection consumes data arriving from, and
produces data for, its network connection. It is the responsiblity of
the application to transfer network data between the Connection object
and its corresponding network connection.

To transfer messages between the connected Container's nodes, a *link*
must be created.  A link is uni-directional; from the application's
perspective, it either sends messages to a remote node, or consumes
messages from a remote node.  The framework provides two distinct link
classes - one for sending messages to a node, the other for receiving
messages from a node.

To send messages to a node on the remote Container, the application
allocates a *SenderLink* from the Connection that attaches to that
remote Container.  The application assigns a local name to the
SenderLink which identifies the node that is the source of the
messages sent byit.  This is the *Source node address*, and is made
available to the remote so it may classify the origin of the message
stream.  The application may also supply the address of the node to
which it is sending.  This is the *Target node address*.  The Target
address supplied by the application is merely a hint for the peer -
the peer may override the supplied address and provide the actual
Target address in use by the peer.  If no Target address is given the
remote may allocate one on behalf of the sending application.  The
SenderLink's final Target address is made available to the sending
application once the link has completed setup.

When sending a message, an application can choose whether or not it
needs to know about the arrival status of the message at the remote
node.  The application may send the message as *best effort* if it
doesn't care about the arrival status.  This *send-and-forget* service
provides no feedback on the delivery of the message.  Otherwise the
application may register a callback that is invoked when the delivery
status of the message is determined by the framework.

All messages are sent using *at-most-once* semantics: this framework
does not attempt to re-send messages on behalf of the application.  If
reliable messaging is required the application must implement its own
retry logic.

If the application needs to consume messages from a node on the remote
Container, it allocates a *ReceiverLink* from the Connection that
attaches to that remote Container.  The application assigns a local
name to the ReceiverLink that identifies the local node that is the
consumer of all the messages that arrive on the link.  This is the
*Target node address*, and is made available to the remote so it may
identify the destination of the message stream.  The application may
also supply the address of the remote node from which it is consuming.
This is the *Source node address*.  The Source address supplied by the
receiver is merely a hint for the remote application - the remote may
override this address and provide the actual Source address used by
the remote. If no Source address is given the remote may allocate one
on behalf of the receiving application.  The ReceiverLink's final
Source address is made available to the receiving application once the
link has completed setup.

## Callback Events ##

This framework uses a callback model to notify the application when
messaging-related events have occurred.  Each of the object types
provided by the framework define a set of events that may be of
interest to the application.  To receive these events, the application
must register callback handlers with each object that it manages.  See
the API section for details regarding each class's event handlers.

----------

# API #

## The Container Class ##

The Container class provides a named repository for nodes.  It is
identified by a name, which must uniquely identify the Container
across all Containers in the messaging domain.

TBD: locking - allow thread-per-connection functionality, would need
locking for container management of connections!!

### Container Methods ###

`Container(name, properties)`

Construct a container.  Parameters:

* __name__ - string, an identifier for the new container, __MUST__ be
unique across the entire messaging domain.
* __properties__ - map, contents TBD


`Container.create_connection(name, ConnectionEventHandler, properties)`

The factory for Connection objects. Use this to create a connection to
a peer Container.  Your application must create a unique Connection
object for each network connection it makes (eg. per-socket, in the
case of TCP).  Parameters:

* __name__ - string, name of this Connection.  The name __MUST__ uniquely
  identify this connection within the Container - no two Connections
  within a Container can share the same name.
* __ConnectionEventHandler__ - object, provides callback handlers for the
  new Connection (see below).
* __properties__ - map containing the following optional connection
  attributes:
   * "hostname" - string, DNS name of __remote__ host (ie. the host
     that is being connected to).  This name will also be used by the
     SSL layer to check the CommonName/SAN contained in the
     certificate provided by the peer.
   * "idle-time-out" - integer, time in seconds before the Connection
     is closed due to lack of traffic.  Setting this may enable
     heartbeat generation by the peer, if supported.
   * "x-trace-protocol" - boolean, if True, enable debug dumps of the
     AMQP wire traffic.
   * "x-server" - boolean, set this to True to configure the
     connection as a server side connection.  This should be set True
     if the connection was remotely initiated (e.g. accept on a
     listening socket).  If the connection was locally initiated
     (e.g. by calling connect()), then this value should be set to
     False.  This setting is used by authentication and encryption to
     configure the connection's role.  The default value is False for
     client mode.
   * "x-username" - string, the client's username to use when
     authenticating with a server.
   * "x-password" - string, the client's password, used for
     authentication.
   * "x-require-auth" - boolean, reject remotely-initiated client
     connections that fail to provide valid credentials for
     authentication.
   * "x-sasl-mechs" - string, a space-separated list of mechanisms
     that are allowed for authentication.  Defaults to "ANONYMOUS"
   * "x-ssl-ca-file" - string, path to a PEM file containing the
     certificates of the trusted Certificate Authorities that will be
     used to check the signature of the peer's certificate.
   * "x-ssl-server" - __DEPRECATED__ use x-server instead.
   * "x-ssl-identity" - tuple, contains self-identifying certificate
     information which will be presented to the peer.  The first item
     in the tuple is the path to the certificate file (PEM format).
     The second item is the path to a file containing the private key
     used to sign the certificate (PEM format, optional if private key
     is stored in the certificate itself). The last item is the
     password used to encrypt the private key (string, not required if
     private key is not encrypted)
   * "x-ssl-verify-mode" - string, configure the level of security
     provided by SSL.  Possible values:
         * "verify-peer" (default) - most secure, requires peer to
           supply a certificate signed by a valid CA (see
           x-ssl-ca-file), and checks the CN or SAN entry in the
           certificate against the expected peer hostname (see
           x-ssl-peer-name)
         * "verify-cert" (default if no hostname or x-ssl-peer-name
           given) - like verify-peer, but skips the check of the peer
           hostname.  Vulnerable to man-in-the-middle attack.
         * "no-verify" - do not require the peer to provide a
           certificate.  Results in a weaker encryption stream, and
           other vulnerabilities.
   * "x-ssl-peer-name" - string, DNS name of peer.  Can be used to
     override the value passed in by the "hostname" option, if
     necessary.  A DNS host name is required to authenticate peer's
     certificate (see x-ssl-verify-mode).
   * "x-ssl-allow-cleartext" - boolean, allows clients to connect
     without using SSL (eg, plain TCP). Used by a server that will
     accept clients requesting either trusted or untrusted
     connections.

`Container.name()`

Returns the name of the Container.

`Container.need_processing()`

A utility to help determine which Connections need processing. Returns
a triple of lists containing those connections that:

 * need to read from the network (index 0)
 * need to write to the network (index 1)
 * waiting for their *next-tick* timer to expire (see *Connection.process()*). (index 2)

The timer list is sorted with the Connection next expiring at index 0.

`Container.get_connection(name)`

Returns the Connection instance identified by *name*.

## The Connection Class ##

A Connection is created from the Container that it is going to
'connect'.  See the *create_connection()* Container method.

`Connection.name()`

Returns the name of the Connection.

`Connection.remote_container()`

Returns the name of the remote container.  This name is only available
once the Connection has become Active.

`Connection.user_context`

A opaque handle that can be set by the application for its own
per-Connection data.

`Connection.open()`

Initiate the connection to the remote peer.  This must be called in
order to transfer data over the Connection.  The Connection is
considered active once both peers have opened it.

`Connection.close()`

Terminate the connection to the remote peer.  This should be called
when the application is done with the Connection and wants to perform
a clean close.  The Connection is considered closed when both peers
have closed it.

`Connection.closed()`

True when both peers have completed closing the Connection.

`Connection.destroy()`

This releases the Connection and all links that use the connection.
This must be called to release the resources used by the Connection.
Once called the Connection is no longer present - the application
should drop all references to the destroyed Connection.

`Connection.process(now)`

This causes the Connection to run the AMQP protocol state machine.
This method must be called periodically (see *Connection.next_tick*)
and whenever network I/O has been done on the connection (see below).
Event callbacks may occur when this method is invoked.  The *now*
parameter is the current time (format determined by the platform).
This method returns a timestamp which is the maximum time interval the
application can wait before it must call Connection.process() again.  If
Connection.process() is not called at or before this deadline the
Connection may fail.

`Connection.next_tick()`

Returns the deadline for the next call to Connection.process().  This
is the same value that was returned by the last call to

`Connection.needs_input()`

Returns the number of bytes of inbound network data this Connection is
capable of consuming.  Returns zero if no input can be processed at
this time.  Returns `EOS` when the input pipe has been closed.

`Connection.process_input(data)`

Process data read from the network.  Returns the number of bytes from
`data` that have been processed, which will be no less than the last
value returned from `Connection.need_input()`.  Returns EOS if the
input pipe has been closed.  The application should call
Connection.process() after calling this method.

`Connection.input_closed(reason)`

The application must call this method when the inbound network data
source has closed.  This indicates that no more data arrive for this
Connection.  The application should call Connection.process() after
calling this method.

`Connection.has_output()`

Returns the number of bytes of output data the Connection has
buffered.  This data needs to be written to the network.  Returns zero
when no pending output is available.  Returns EOS when the output pipe
has been closed.  The application should call Connection.process()
prior to calling this method.

`Connection.output_data()`

Returns a buffer containing data that needs to be written to the
network.  Returns None if no data or the output pipe has been closed.

`Connection.output_written(N)`

The application must call this to notify the framework that N bytes of
output data (as given by `Connection.output_data()`) has been written
to the network.  This will cause the framework to release the first N
bytes from the buffer output data.

`Connection.output_closed()`

The application must call this method when the outbound network data
pipe has closed.  This indicates that no more data can be written to
the network.

`Connection.create_sender(source_address, target_address,
                           SenderEventHandler, name, properties)`

Construct a SenderLink over this Connection which will send messages
to the node identified by *target_address* on the remote.  If
*target_address* is None the remote may create a node for this link.
The target address of a dynamically-created node will be made
available via the SenderLink once the link is active.  Parameters:

* **source_address** - string, address of the local node that is
  generating the messages sent on this link.
* **target_address** - string or None, address of the destination node
  that is to consume the sent messages. May be None if the remote can
  dynamically allocate a node for consuming the messages.
* __SenderEventHandler__ - a set of callbacks for monitoring the state of
  the link.  See below.
* __name__ - string, optional name for the created link, __MUST__ be
  unique across all SenderLinks on this Connection.
* __properties__ - map of optional properties to apply to the link:
   * "distribution-mode" - informs the receiver of the distribution
     mode of messages supplied by this link (see the AMQP 1.0
     specification for details).  Values:
   * "move" - the message will not be available for other consumers
     once it has been accepted by the remote.  This implies that a
     message will be consumed by only one consumer.
   * "copy" - the message will continue to be available for other
     consumers after it has been accepted by the peer.  This implies
     that multiple consumers may get a copy of the same message.

`Connection.accept_sender(handle, source_override, SenderEventHandler, properties)`

This constructs a SenderLink in response to a request from the peer to
consume from a node.  When a peer wants to consume messages from a
local node  it will request that the application create a SenderLink
on its behalf.  This SenderLink will be used to transfer the messages
to the peer.  The application is notified of such a request via the
*sender_requested* Connection callback (see below).  After receiving
this notification the application may grant the request by calling
this method.  Parameters:

* __handle__ - opaque handle provided by the
  Connection.sender_requested() callback.  This handle is used by the
  framework to correlate the created SenderLink with the request from
  the peer.
* **source_override** - string, the address of the source node.  This
  allows the application to supply (or override) the source address
  requested by the peer.
* __SenderEventHandler__ - object containing callbacks for events
  generated by this SenderLink (see below).
* __properties__ - map of properties used by the SenderLink.  Same
  values as supplied to the *create_sender* method.  Values in this
  map will override any properties requested by the peer.

`Connection.reject_sender(handle, reason)`

Called by the application to reject a request from the peer to create
a SenderLink.  This should be called instead of *accept_sender()* if
the application wants to deny the peer's request.  The application is
notified of such a request via the *sender_requested* Connection
callback (see below).  Parameters:

* __handle__ - the opaque handle provided by the
  *Connection.sender_requested()* callback.
* __reason__ - **TBD**


`Connection.create_receiver( target_address, source_address,
                             ReceiverEventHandler, name, properties)`

Construct a ReceiverLink over this Connection which will consume
messages from the node identified by *source_address* on the remote.
If *source_address* is None the remote may create a node for this
link.  The source address of the dynamically-created node will be made
available via the ReceiverLink once the link is active.  Parameters:

* **target_address** - string address of the local node that is
  consuming the messages arriving on the link.
* **source_address** - string or None, address of the remote node that is
  to supply the messages arriving on the link. May be None if the
  remote can dynamically allocate a node for the source.
* __receiverEventHandler__ - object containing a set of callbacks for
  receiving messages and monitoring the state of the link.  See below.
* __properties__ - map of optional properties to apply to the link:
   * "distribution-mode" - Requests the distribution mode that the
     peer's SenderLink should use when supplying messages.  This is
     merely a request and can be overridden by the peer.  Values are
     the same as given for *Connection.create_sender()*


`Connection.accept_receiver(handle, target_override, ReceiverEventHandler,
                            properties)`

This constructs a ReceiverLink in response to a request from the peer
to send messages to a node.  When a peer wants to send messages to a
local node it will request that the application create a ReceiverLink
on its behalf.  This ReceiverLink will be used to consume the messages
sent by the peer.  The application is notified of such a request via
the *receiver_requested* Connection callback (see below).  After
receiving this notification the application may grant the request by
calling this method.  Parameters:

* __handle__ - opaque handle provided by the
  Connection.receiver_requested() callback.  This handle is used by the
  framework to correlate the created ReceiverLink with the request from
  the peer.
* __target_override__ - string, the address of the target node.  This
  allows the application to supply (or override) the target address
  requested by the peer.
* __ReceiverEventHandler__ - object containing callbacks for events
  generated by this ReceiverLink (see below).
* __properties__ - map of properties used by the ReceiverLink.  **TBD**

`Connection.reject_receiver(handle, reason)`

Called by the application to reject a request from the peer to create
a ReceiverLink.  This should be called instead of *accept_receiver()*
if the application wants to deny the peer's request.  The application
is notified of such a request via the *receiver_requested* Connection
callback (see below).  Parameters:

* __handle__ - the opaque handle provided by the
  *Connection.receiver_requested()* callback.
* __reason__ - **TBD**

### Connection Events ###

Callbacks can be registered with an instance of a Connection object.
These callbacks are invoked on the following events:

* The Connection becomes active.
* The peer has requested that the Connection be closed.
* The Connection has closed.
* The Connection has experienced a failure.
* The peer wants to consume from a local node.
* The peer wants to send messages to a local node.
* SASL authentication

Callbacks for these events are provided by the application via the
ConnectionEventHandler object.  The following callbacks are defined
for a Connection:

`connection_active(Connection)`

The Connection has transitioned to the Active state.  This means that
both ends of the Connection can send and receive data.

`connection_closed(Connection)`

The Connection has closed cleanly.  This event is generated as a
result of both ends of the Connection being closed via the *close()*
method.

`connection_remote_closed(Connection, error)`

Indicates that the peer has issued a *close()* on the connection.  **TBD** error

`connection_failed(Connection, error)`

Indicates that the connection has failed.  No further message passing
is possible and all contained links are dead.  At this point the
application has no choice but to destroy the Connection.  **TBD** error

`sender_requested(Connection, handle, name, requested_source, properties)`

The peer has requested that a SenderLink be created so it can consume
messages from a node in the local Container.  The *requested_source* is
the address of the (source) node that the peer wishes to consume
messages from.  The application may accept the request by calling
*Connection.accept_sender()* or deny the request by calling
*Connection.reject_sender()*

`receiver_requested(Connection, handle, name, requested_target, properties)`

The peer needs to send messages to a node in the local container and
is asking your application to create a ReceiverLink on its behalf.
The intent of this ReceiverLink is to consume the incoming messages
from the peer. The *requested_target* is the address of the (target)
node that the peer wishes to send messages to. The application may
accept the request by calling *Connection.accept_receiver()* or deny
the request by calling *Connection.reject_receiver()*

`sasl_step(Connection, pn_sasl)`

Invoked each time the SASL negotiation transfers information.  See the
Proton API for more detail.

`sasl_done(Connection, result)`

Invoked on completion of the SASL handshake.  See the Proton API for
more detail.

## The SenderLink Class ##

A SenderLink is created from the Connection that connects to remote
Container that holdes the target node. See the
*Connection.create_sender()* and *Connection.accept_sender()* methods.

`SenderLink.name()`

Returns the name of the SenderLink.

`SenderLink.user_context`

A opaque handle that can be set by the application for its own
per-SenderLink data.

`SenderLink.open()`

A SenderLink must be opened before it will entry the Active state and
messages can be sent.

`SenderLink.source_address()`

Returns the address of the local node that is the source of the sent messages.

`SenderLink.target_address()`

Returns the address of the node in the peer's Container that will
accept the sent messages.  Note that this address is not final until
the SenderLink has reached the Active state.

`SenderLink.close()`

Initiate a close of the SenderLink.

`SenderLink.closed()`

Returns True when the SenderLink has closed.

`SenderLink.destroy()`

This releases the SenderLink.  This must be called to release the
resources used by the SenderLink.  Once called the SenderLink is no
longer present - the application should drop all references to the
destroyed SenderLink.

`SenderLink.send(message, delivery_callback, handle, deadline)`

Queue a message for sending over the link.  *Message* is a Proton
Message object.  If there is no need to know the delivery status of
the message at the peer then *delivery_callback*, *handle*, and *deadline*
should not be provided.  In this case, the message will be sent
*pre-settled*.  To get notification on the delivery status of the
message a *delivery_callback* and *handle* must be supplied.  The
*deadline* is optional in this case.  This method returns 0 if the
message was queued successfully (and the *delivery_callback*, if
supplied, is guaranteed to be invoked).  Otherwise an error occurred
and the message was not queued and no callback will be made.
Parameters:

* __message__ - a complete Proton Message object
* __handle__ - opaque object supplied by application.  Passed to
  the *delivery_callback* method.
* __deadline__ - future timestamp when the send should be aborted if
  it has not completed. In seconds since Epoch.
* **`delivery_callback`** - an optional method that will be invoked when
  the delivery status of the message has reached a terminal state.
  The callback accepts the following parameters:
    * __SenderLink__ - the link over which the message was sent.
    * __handle__ - the handle provided in the *send()* call.
    * __status__ - the status of the delivery. It will be one of the
      following values:
       * `TIMED_OUT` - the delivery did not reach a terminal state
         before the deadline expired. Whether or not the message was
         actually received is unknown.
       * `ACCEPTED` - the remote has received and accepted the message. See `ReceiverLink.message_accepted()`
       * `REJECTED` - the remote has received but has rejected the message. See `ReceiverLink.message_rejected()`
       * `RELEASED` - the remote has received but will not accept the message. See `ReceiverLink.message_released()`
       * `ABORTED` - Connection or SenderLink has been
         closed/destroyed/failed, etc.
       * `UNKNOWN` - the remote did not provide a delivery status.
       * `MODIFIED` - **RESERVED** **TBD**
    * __error__ - if status is ABORTED, an error code is provided **TBD**
    * __outcome__ - **RESERVED**  **TBD**

`SenderLink.pending()`

Returns the number of outging messages in the process of being sent.

`SenderLink.credit()`

Returns the number of messages the remote ReceiverLink has permitted
the SenderLink to send.

`SenderLink.flushed()`  **TBD**

### SenderLink Events ###

Callbacks can be registered with an instance of a SenderLink object.
These callbacks are invoked on the following events:

* The link becomes active.
* The peer has initiated the close of the link.
* The link has closed.
* The peer has made credit available to the sender.

Callbacks for these events are provided by the application via the
SenderEventHandler objects. The following callback methods are defined
for a SenderLink:

`sender_active(SenderLink)`

Called when the link open has completed and the SenderLink is active.

`sender_closed(SenderLink)`

Called when SenderLink has closed.

`sender_remote_closed(SenderLink, error)`

Indicates that the peer has issued a *close()* on the link.  **TBD** error

`credit_granted(SenderLink)`

Indicates that the peer has made credit available.  The current credit
value can be determined via the *SenderLink.credit()* method.  **TBD**
- invoked only when credit transitions from <= 0 to > 0???

`flush(SenderLink)`  **TBD**

## The ReceiverLink Class ##

A ReceiverLink is created from the Connection that connects to the
remote Container which holds the source node.  See the
*Connection.create_receiver()* and *Connection.accept_receiver()*
methods.

`ReceiverLink.name()`

Returns the name of the ReceiverLink.

`ReceiverLink.user_context`

A opaque handle that can be set by the application for its own
per-ReceiverLink data.

`ReceiverLink.open()`

A ReceiverLink must be opened before it will entry the Active state and
messages can be sent.

`ReceiverLink.source_address()`

Returns the address of the node in the peer's Container that is the
source of received messages.  Note that this address is not final
until the ReceiverLink has reached the Active state.

`ReceiverLink.target_address()`

Returns the address of the local node that will accept the received
messages.

`ReceiverLink.close()`

Initiate a close of the ReceiverLink.

`ReceiverLink.closed()`

Returns True when the ReceiverLink has closed.

`ReceiverLink.destroy()`

This releases the ReceiverLink.  This must be called to release the
resources used by the ReceiverLink.  Once called the ReceiverLink is no
longer present - the application should drop all references to the
destroyed ReceiverLink.

`ReceiverLink.capacity()`

Returns the number of messages the ReceiverLink is able to queue
locally before back-pressuring the sender.  Capacity decreases by one
each time a message arrives.  Capacity is initialized to zero when the
ReceiverLink is first created - the application must call
*add_capacity()* in order to allow the peer to send messages.

`ReceiverLink.add_capacity(N)`

Increases the credit made available to the peer by N messages.  Must
be called by application to replenish the sender's credit as messages
arrive.

`ReceiverLink.flush()`  **TBD**

`ReceiverLink.message_accepted( handle )`

Indicate to the remote that the message identified by *handle* has
been successfully processed by the application. See the
*message_received* callback below, as well as the *SenderLink.send()*
method.

`ReceiverLink.message_rejected( handle, outcome )`

Indicate to the remote that the message identified by *handle* is
considered invalid and cannot be processed by the application. See the
*message_received* callback below, as well as the *SenderLink.send()*
method.  Outcome **TBD**

`ReceiverLink.message_released( handle )`

Indicate to the remote that the message identified by *handle* will
not be processed by the application and should be made available for
other consumers. See the *message_received* callback below, as well as
the *SenderLink.send()* method.

`ReceiverLink.message_modified( handle, outcome )`

Indicate to the remote that the message identified by *handle* was
modified by the application but not processed. See the
*message_received* callback below, as well as the *SenderLink.send()*
method.  Outcome **TBD**

### ReceiverLink Events ###

Callbacks can be registered with an instance of a ReceiverLink object.
These callbacks are invoked on the following events:

* The link has become active.
* The peer has initiated the close of the link.
* The link has closed.
* A message has arrived from the sender.

Callbacks for these events are provided by the application via the
ReceiverEventHandler objects. The following callback methods are
defined for a ReceiverLink:

`receiver_active(ReceiverLink)`

Called when the link open has completed and the ReceiverLink is
active.

`receiver_closed(ReceiverLink)`

Called when ReceiverLink has closed.

`receiver_remote_closed(ReceiverLink, error)`

Indicates that the peer has issued a *close()* on the link.  **TBD** error

`message_received(ReceiverLink, Message, handle)`

Called when a Proton Message has arrived on the link.  Use *handle* to
indicate whether the message has been accepted or not by calling the
appropriate method (*message_accepted*, *message_rejected*, etc) The
capacity of the link will be decremented by one on return from this
callback. Parameters:

* __ReceiverLink__ - link which received the Message
* __Message__ - a complete Proton Message
* __handle__ - opaque handle used by framework to coordinate the
  message's receive status.

`remote_flushed(ReceiverLink)`  **TBD**



