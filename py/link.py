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

class _Link(object):
    def __init__(self, connection, address, name, properties={}):
        self._connection = connection
        self._address = target_address
        self._name = name
        self._properties = properties


class Sender(_Link):
    !!! need to accept a pre-available pn_sender!!!
    def __init__(self, connection, target_address, name=None, properties={}):
        super(Sender, self).__init__(connection, target_address,
                                     name, properties)
        self._pending_send = []
        self._pending_ack = []

    FLAGS_NONE=0
    FLAGS_ACKED=1
    def send(message, callback=None, timeout=None, flags=FLAGS_NONE):
        if callback:
            if flags & FLAGS_ACKED:
                # invoke callback with final disposition on delivery
                # eg. REJECTED, ACCEPTED, etc (send un-settled)
                pass
            else:
                # invoke callback with IN_PROGRESS when credit has become
                # available and message written. (send pre-settled)
                pass
        # need to invoke cb with ABORTED on close of connection!

    def close(self):
        # @todo remove from owning connection
        pass

    def _handle_delivery(self, delivery):
        pass

class ReceiveEventHandler(object):

    def message_received(self, receiver, msg):
        # return ACCEPTED or REJECTED
        pass

    def closed(self, receiver, reason):
        pass

class Receiver(object):

    def __init__(self, connection, target_address, eventHandler,
                 name=None, properties={}):
        super(Receiver, self).__init__(connection, target_address,
                                       name, properties)
        self._credit = properties.get("capacity", 10)

        self._handler = eventHandler

    def _handle_delivery(self, delivery):
        pass

    
