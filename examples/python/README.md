# Examples #

`send.py`: An example of a minimal sending client.  It sends one
message, waits for the send callback, then closes the connection.

`recv.py`: An example of a minimal receiving client.  It waits for the
arrival of a single message, accepts it, then closes the connection.

`server.py`: A simple server.  It passively waits for clients to
connect, accepting receiver and sender links.  Each message that
arrives on a receiving link is accepted and dropped.  A simple message
is generated and sent on each sender link that has credit.  Only one
message is outstanding for each sender link.  The server waits for the
clients to close the links and connections - it does not initiate
closing links or connections.  It can be used as a server for the
**send.py** and **recv.py** examples.

`rpc-server.py`: An example RPC server.  Doesn't actually do any RPC
operations.  It does map incoming messages to outgoing links by using
the Source and Target addresses of the links.  A bit more complicated
that the simple **server.py** example.

`rpc-client.py: An sample client to use with the **rpc-server.py**
example.  Sends an "RPC" message, and waits for a reply from the
server.

`utils.py`: Common code used by the examples.

