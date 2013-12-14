# fusion #

A connection oriented messaging framework built around the QPID Proton engine.

# Purpose #

This framework is meant to ease the integration of AMQP 1.0 messaging
into existing applications.  It provides a very basic,
connection-oriented messaging model that should meet the needs of most
applications.

The framework has been designed with the following goals in mind:

* simplifying the user model exported by the Proton engine - you
should not have to be an expert in AMQP to use this framework!

* application control of the I/O implementation where possible

* limit the functionality provided by Proton to a subset that
should be adequate for 79% of all messaging use-cases [1]


All actions are designed to be non-blocking,
leveraging callbacks where asynchronous behavior is modeled.

There is currently no threading architecture or locking performed by
this framework.  Locking is assumed to be handled outside of this
framework by the application - all processing provided by this
framework are assumed to be single-threaded.

[1] If I don't understand it, it won't be provided. [2]
[2] Even if I do understand it, it may not be provided [3]
[3] Ask yourself: Is this feature *critical* for the simplest messaging task?

## What this framework doesn't do ##

* Message management.  All messages are assumed to be Proton Messages.
  Creating and parsing Messages is left to the application.

* Routing. This framework assumes link-based addressing.  What does
  that mean?  It means that this infrastructure basically ignores the
  "to" or "reply-to" contained in the message.  It leaves these fields
  under the control and interpretation of the application.  This
  infrastructure requires that the application determines the proper
  Link over which to send an outgoing message.  In addition, it assumes
  the application can correlate messages arriving over a link.

* Connection management.  It is expected that your application will
  manage the creation of sockets. Whether those sockets are created by
  initiating a connection or accepting an inbound connection is
  irrelevant to the framework.  It is also assumed that, if desired,
  your application will be responsible for monitoring the sockets for
  I/O activity (e.g. call poll()).  The framework will support both
  blocking and non-blocking sockets, however it may block when doing
  I/O over a blocking socket.  Note well: reconnect/failover must also
  be handled by the application.

* Flow control. It is assumed the application will determine the
  maximum number of messages to allow a receiving link to accept
  (capacity).  Sent messages will be queued locally until credit is
  made availble for the message(s) to be transmitted.  The framework's
  API allows these levels to be configured and/or monitored.


# Theory of Operations #

This framework defines the following set of objects:

 * Container - a manifestation of the container concept from AMQP 1.0.
      An instance must have a name that is unique across the entire
      messaging domain, as it is used as part of the address.  This
      object is a factory for Connections.

 * Connection - a manifestation of the connection concept from AMQP
       1.0.  You can think of this as a pipe between two Containers.
       When creating a Connection, your application must provide a
       socket (or socket-like object).  That socket will be
       responsible for handling data travelling over the Connection.

 * ResourceAddress - an identifier for a resource provided by a
       Container.  Messages may be consumed from, or sent to, a
       resource.

 * Links - A uni-directional pipe for messages travelling between
       resources.  There are two sub-classes of Links: SenderLinks and
       ReceiverLinks.  SenderLinks produce messages from a particular
       resource.  ReceiverLinks consume messages from a particular
       resource.

And application creates one or more Containers, which represents a
domain for a set of message-oriented resources (queues, publishers,
consumers, etc) offered by the application.  The application then
forms network connections with other systems that offer their own
containers.  The application may initiate these network connections
(eg. call connect()), or listen for connection requests from remote
systems (eg. listen()/accept()) - this is determined by the
application's design and purpose.

The method used by the application to determine which systems it
should connect to in order to access particular resources and
Containers is left to the application designers.

Once these network connections are initiated, the application
allocates a Connection object from the local Container to represent
the connection between the local and remote Containers.  The
application provides a network socket for the framework to use for
communicating over the Connection.

If the application needs to send messages to a resource on the remote
Container, it allocates a SenderLink from the Connection to the remote
Container.  The application assigns a local name to the SenderLink
that identifies the resource that is the source of the sent messages.
This is the Source resource address, and is made available to the
remote so it may classify the origin of the message stream.  The
application may also supply the address of the resource to which it is
sending.  This is the Target resource address.  The Target resource
address may be overridden by the remote.  If no Target address is
given, the remote may allocate one on behalf of the sending
application.  The SenderLink's final Target address is made available
to the sending application once the link has completed setup.

If the application needs to consume messages from a resource on the
remote Container, it allocates a ReceiverLink from the Connection to
the remote Container.  The application assigns a local name to the
ReceiverLink that identifies the local resource that is the consumer
of all the messages that arrive on the link.  This is the Target
resource address, and is made available to the remote so it may
classify the destination of the message stream.  The application may
also supply the address of the remote resource from which it is
consuming.  This is the Source resource address.  The Source resource
address may be overridden by the remote.  If no Source address is
given, the remote may allocate one on behalf of the receiving
application.  The ReceiverLink's final Source address is made
available to the receiving application once the link has completed
setup.

# API #

## Container ##

    Container( name, ContainerEventHandler, properties={} ): creates a new Container instance.
      name - identifier for the new container, MUST BE UNIQUE across the entire messaging domain.
      eventHandler - callbacks for container events (see below)

    Container.create_connection( name, socket, ConnectionEventHandler, properties={}...): factory for Connection objects.
      name - uniquely identifies this connection within the Container
      socket: the socket (like) object provided by your application.  It is assumed that your application has sucessfully initialized this object (including issuing connect() in the case of an outbound socket).
      properties - map containing the following optional connection attributes:
       "remote-hostname" - DNS name of remote host.
       "idle-time-out" - time in milliseconds before connection is closed due to lack of traffic.  Setting this may enable heartbeat generation by the peer, if implemented.
       "sasl" - map of sasl configuration stuff (need callbacks, TBD)
       "ssl"  - ditto, future-feature

    Container.need_io(): return a pair of lists containing those connections that are read blocked and write-ready.

    Container.next_tick(): return the timestamp of the next pending timer event to expire. In seconds since Epoch.  Your application must call Container.process_io at least once at or before this timestamp!

    Container.process_io_(readable, writeable): Does all I/O and timer
      related processing for all of the Connections held by this
      Container.  Invoke Container.next_tick() after making this call to
      determine the deadline for the next timer event.

      readable [input]: a list of those Connections whose sockets are read-able.
      writable [input]: a list of those Connections whose socket are write-able.
      returns: a list of all Connections that have been serviced.  This
        will include all readable and writable connections passed in, plus
        those additional connections that have processed expired timers.

    Container.resolve_sender(target-address) - find the Sender link that sends to the remote target-address
    Container.resolve_receiver(source-address) - find the Receiver link that consumes from the remote source-address
    Container.get_connection(connection-name) - lookup

### ContainerEventHandler ###

The ContainerEventHandler passed on container construction has the following callback methods that your application can register:

TBD


## Connection ##

No public constructor - use Container.create_connection().

    Connection.tick(now) - update any protocol timers running in the connection.
      now: seconds since Epoch
      returns: a timestamp, which is the deadline for the expiring timer in seconds since Epoch.  Zero if no active timers.

    Connection.next_tick() - last return value from tick() call.

    Connection.need_read() - True when protocol engine can accept network data from the socket.  Use this to know if a call to Connection.read_input() should be made.

    Connection.read_input() - read from the socket and process the input.  Return the # of bytes read, or None if the read stream has closed.  Will re-throw any exceptions/errors returned from the socket read call (eg, EAGAIN, timeout, etc).

    Connection.need_write - True when the protocol engine has data that needs to be written to the socket.

    Connection.write_output() - write protocol engine data to the socket.  Return the # of bytes written, or None if the write stream has closed.  Will re-throw any exceptions/errors returned from the socket write call (eg, EAGAIN, timeout, etc).

    Connection.destroy(error) - close the connection, and force any in progress message transfers to abort.  Delete the connection (and all underlying Links) from the Container.
      error - optional error, supplied by application if closing due to an unrecoverable error.

    Connection.create_sender(target-address, SenderEventHandler, properties={}) - construct a Sender over this connection which will send messages to the <target-address> on the remote.  If target-address is None, the remote may generate one. 
      properties - map of optional properties:
        "source-address": address of local resource that is generating the messages sent on this link.
        "backlog": maximum number of outgoing messages this sender can buffer.

    Connection.accept_sender(sender-info, see create_sender) - accept a remotely-requested Sender (see ConnectionEventHandler.sender_request) and construct it.

    Connection.reject_sender(sender-info, reason) - reject a remotely-requested Sender (see ConnectionEventHandler.sender_request)

    Connection.create_receiver(source-address, ReceiverEventHandler, properties={}) - construct a Receiver over this connection which will consume messages from the <source-address> on the remote.  If source-address is None, the remote may generate one. 
      properties - map of optional properties:
        "target-address": address of local resource that is consuming these messages
        "capacity": maximum number of incoming messages this receiver can buffer.

    Connection.accept_receiver(receiver-info, see create_receiver) - accept a remotely-requested Receiver (see ConnectionEventHandler) and construct it.

    Connection.reject_receiver(receiver-info, reason)


### ConnectionEventHandler ###

The ConnectionEventHandler passed on connection construction has the following callback methods that your application can register:

    ConnectionEventHandler.connection_active(connection): called when the connection transitions to up.

    ConnectionEventHandler.connection_closed(connection, error-code): called when connection has been closed by peer.  Error-code provided by peer (optional).

**TBD - what about SASL result?**
**TBD - what about SSL (if not already provided with socket)?**

    ConnectionEventHandler.receiver_request(connection, target-address, receiver-info):  the peer is attempting to create a new link so it can send messages to a resource in the local container.  This resource is identified by <target-address>.  Your application must accept or reject this request.  If target-address is None, the peer is requesting your application generate a resource - your application must provide the address of this resource should you accept this Receiver.

    ConnectionEventHandler.sender_request(connection, source-address, sender-info): the peer is attempting to create a new link so it can consume messages from a resource in the local container.  This resource is identified by <source-address>.  Your application must accept or reject this request.  If source-address is None, the peer is requesting your application generate a resource - your application must provide the address of this resource should you accept this Sender.


## SenderLink ##

No public constructor - use Connector.create_sender() for creating a local sender or Connector.accept_sender() to establish a remotely-requested one.

    SenderLink.send( Message, timeout, DeliveryCallback, handle, flags) - queue a message for sending over the link.  Message is a Proton Message object.  timeout is optional, time in seconds before send is aborted (not necessarily the TTL).  DeliveryCallback, handle - optional callback and callback argument (see below).  Flags - optional, if passed as FLAGS_ACKED, the callback will be invoked after the remote has communicated its delivery status.  If no flags, the callback is made when the message has been accepted by the engine.
      Returns: 0 on success - guarantees callback will be (or has been) made.
      Throws OverflowError when the attempt to send overflows the SenderLink's backlog.

    DeliveryCallback( SenderLink, handle, status ) - optional, invoked when the send operation completes.
      status - one of:
        TIMED_OUT - send did not complete before timeout hit, send aborted
        IN_PROGRESS - message passed into engine (flags != FLAGS_ACKED)
        ACCEPTED - remote has received and accepted the message
        REJECTED - remote has received but has rejected the message
        ABORTED  - connection or sender has been forced closed/destroyed, etc.

    SenderLink.pending() - returns number of outging messages in the process of being sent.
    SenderLink.credit() - returns the number of messages the remote Receiver has permitted the SenderLink to send.
    SenderLink.destroy(error) - close the link, and force any in progress message transfers to abort.  Delete the link from the Container.
      error - optional error, supplied by application if closing due to an unrecoverable error.


### SenderEventHandler ###

Passed to SenderLink constructor (optional)

    SenderEventHandler.sender_active(SenderLink) - called when the link protocol has completed and the SenderLink is active.
    SenderEventHandler.sender_closed(SenderLink, error-code) - called when connection has been closed by peer, or due to close of the owning Connection.  Error-code provided by peer (optional)


## ReceiverLink ##

No public constructor - use Connector.create_receiver() for creating a local receiver or Connector.accept_receiver() to establish a remotely-requested one.

    ReceiverLink.capacity() - returns the number of messages the is able to accept.
    ReceiverLink.add_capacity(N) - increases capacity by N messages.  Must be called by application to replenish credit as messages arrive.
    ReceiverLink.accept_message( msg-handle ) - indicate to the remote that the message identified by msg-handle has been accepted.
    ReceiverLink.reject_message( msg-handle, reason ) - indicate to the remote that the message identified by msg-handle has been rejected.


### ReceiverEventHandler ###

Passed to ReceiverLink constructor

    ReceiverEventHandler.receiver_active(ReceiverLink) - called when the link protocol has completed and the ReceiverLink is active.
    ReceiverEventHandler.receiver_closed(ReceiverLink, error-code) - called when connection has been closed by peer, or due to close of the owning Connection.  Error-code provided by peer (optional)
    ReceiverEventHandler.message_received(ReceiverLink, Message, msg-handle) - called when a Proton Message has arrived on the link.  Use msg-handle to indicate whether the message has been accepted or rejected.


## ResourceAddress ##

**TBD: this needs more thought**

The address for a resource is comprised of four parts:

* transport address - identifies the host of the Container on the network.  Typically this is the DNS hostname, with optional port.
* container identifier - the identifier of the Container as described above
* resource identifier - the identifer of the resource within the Container.
* property map - an optional map of address properties TBD

The ResourceAddress can be represented in a string using the following syntax:

*amqp://[user:password@]<transport-address>/<container-id>/<resource-id>[; {property-map}*

where:

    [user:password@] - TBD
    <transport-address> is a DNS hostname.  Largely ignored by the framework, as socket configuration is provided by the application.
    <container-id> - string, not containing '/' character
    <resource-id> - string, not containing ';' character
    property-map - TBD

Example:
    "amqp://localhost.localdomain:5672/my-container/queue-A ; {mode: browse}"

For the most part, <transport-address> is ignored by the infrastructure.  <container-id> is used to identify the remote container, and thus resolve an address to the proper connection.  <resource-id> is used to differentate links within a container.

These address strings will be set as the Target and Source addresses in the AMQP 1.0 links.

