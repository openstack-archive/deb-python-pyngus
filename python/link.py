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

__all__ = [
    "SenderEventHandler",
    "SenderLink",
    "ReceiverEventHandler",
    "ReceiverLink"
    ]

import collections, logging
import proton

LOG = logging.getLogger(__name__)

class _Link(object):
    """Generic Link base class"""
    def __init__(self, connection, pn_link,
                 target_address, source_address,
                 handler, properties):
        self._connection = connection
        self._name = pn_link.name
        self._handler = handler
        self._properties = properties
        self._user_context = None
        self._active = False
        # @todo: raise jira to add 'context' attr to api
        self._pn_link = pn_link
        pn_link.context = self

        if target_address is None:
            assert pn_link.is_sender, "Dynamic target not allowed"
            self._pn_link.target.dynamic = True
        elif target_address:
            self._pn_link.target.address = target_address

        if source_address is None:
            assert pn_link.is_receiver, "Dynamic source not allowed"
            self._pn_link.source.dynamic = True
        elif source_address:
            self._pn_link.source.address = source_address

        desired_mode = properties.get("distribution-mode")
        if desired_mode:
            if desired_mode == "copy":
                self._pn_link.source.distribution_mode = \
                    proton.Terminus.DIST_MODE_COPY
            elif desired_mode == "move":
                self._pn_link.source.distribution_mode = \
                    proton.Terminus.DIST_MODE_MOVE
            else:
                raise Exception("Unknown distribution mode: %s" %
                                str(desired_mode))

    @property
    def name(self):
        return self._name

    def open(self):
        """
        """
        self._pn_link.open()

    def _get_user_context(self):
        return self._user_context

    def _set_user_context(self, ctxt):
        self._user_context = ctxt

    user_context = property(_get_user_context, _set_user_context,
                            doc="""
Associate an arbitrary application object with this link.
""")

    @property
    def source_address(self):
        """If link is a sender, source is determined by the local value, else
        use the remote
        """
        if self._pn_link.is_sender:
            return self._pn_link.source.address
        else:
            return self._pn_link.remote_source.address

    @property
    def target_address(self):
        """If link is a receiver, target is determined by the local value, else
        use the remote
        """
        if self._pn_link.is_receiver:
            return self._pn_link.target.address
        else:
            return self._pn_link.remote_target.address

    def close(self, error=None):
        self._pn_link.close()

    @property
    def closed(self):
        state = self._pn_link.state
        return state == (proton.Endpoint.LOCAL_CLOSED
                         | proton.Endpoint.REMOTE_CLOSED)

    def destroy(self):
        LOG.debug("link destroyed %s" % str(self._pn_link))
        self._user_context = None
        self._pn_link.context = None
        self._pn_link = None

class SenderEventHandler(object):
    """
    """
    def sender_active(self, sender_link):
        LOG.debug("sender_active (ignored)")

    def sender_remote_closed(self, sender_link, error=None):
        LOG.debug("sender_remote_closed (ignored)")

    def sender_closed(self, sender_link):
        LOG.debug("sender_closed (ignored)")


class SenderLink(_Link):

    # Status for message send callback
    #
    ABORTED = -2
    TIMED_OUT = -1
    UNKNOWN = 0
    ACCEPTED = 1
    REJECTED = 2
    RELEASED = 3
    MODIFIED = 4

    def __init__(self, connection, pn_link, source_address,
                 target_address, eventHandler, properties):
        super(SenderLink, self).__init__(connection, pn_link,
                                         target_address, source_address,
                                         eventHandler, properties)
        self._pending_sends = collections.deque()
        self._pending_acks = {}
        self._next_deadline = 0
        self._next_tag = 0

        # @todo - think about send-settle-mode configuration

    def send(self, message, delivery_callback=None, handle=None, deadline=None):
        """
        """
        self._pending_sends.append( (message, delivery_callback, handle,
                                     deadline) )
        # @todo deadline not supported yet
        assert not deadline, "send timeout not supported yet!"
        if deadline and (self._next_deadline == 0 or
                         self._next_deadline > deadline):
            self._next_deadline = deadline

        delivery = self._pn_link.delivery( "tag-%x" % self._next_tag )
        self._next_tag += 1

        if delivery.writable:
            send_req = self._pending_sends.popleft()
            self._write_msg( delivery, send_req )

        return 0

    def pending(self):
        return len(self._pending_sends) + len(self._pending_acks)

    def credit(self):
        return self._pn_link.credit()

    def close(self, error=None):
        while self._pending_sends:
            i = self._pending_sends.popleft()
            cb = i[1]
            if cb:
                handle = i[2]
                cb(self, handle, self.ABORTED, error)
        for i in self._pending_acks.itervalues():
            cb = i[1]
            handle = i[2]
            cb(self, handle, self.ABORTED, error)
        self._pending_acks.clear()
        super(SenderLink, self).close()

    def destroy(self):
        self._connection._remove_sender(self._name)
        self._connection = None
        super(SenderLink, self).destroy()

    def _delivery_updated(self, delivery):
        # A delivery has changed state.
        # Do we need to know?
        _disposition_state_map = {
            proton.Disposition.ACCEPTED: SenderLink.ACCEPTED,
            proton.Disposition.REJECTED: SenderLink.REJECTED,
            proton.Disposition.RELEASED: SenderLink.RELEASED,
            proton.Disposition.MODIFIED: SenderLink.MODIFIED,
            }

        if delivery.tag in self._pending_acks:
            if delivery.settled:  # remote has finished
                LOG.debug("delivery updated, remote state=%s", str(delivery.remote_state))

                send_req = self._pending_acks.pop(delivery.tag)
                state = _disposition_state_map.get(delivery.remote_state,
                                                   self.UNKNOWN)
                cb = send_req[1]
                handle = send_req[2]

                cb(self, handle, state, None)
                delivery.settle()
        else:
            # not for a sent msg, use it to send the next
            if delivery.writable and self._pending_sends:
                send_req = self._pending_sends.popleft()
                self._write_msg(delivery, send_req)
            else:
                # what else is there???
                delivery.settle()

    def _write_msg(self, delivery, send_req):
        # given a writable delivery, send a message
        # send_req = (msg, cb, handle, deadline)
        msg = send_req[0]
        cb = send_req[1]
        self._pn_link.send( msg.encode() )
        self._pn_link.advance()
        if cb:  # delivery callback given
            assert delivery.tag not in self._pending_acks
            self._pending_acks[delivery.tag] = send_req
        else:
            # no status required, so settle it now.
            delivery.settle()


class ReceiverEventHandler(object):

    def receiver_active(self, receiver_link):
        LOG.debug("receiver_active (ignored)")

    def receiver_remote_closed(self, receiver_link, error=None):
        LOG.debug("receiver_remote_closed (ignored)")

    def receiver_closed(self, receiver_link):
        LOG.debug("receiver_closed (ignored)")

    def message_received(self, receiver_link, message, handle):
        LOG.debug("message_received (ignored)")


class ReceiverLink(_Link):
    def __init__(self, connection, pn_link, target_address,
                 source_address, eventHandler, properties):
        super(ReceiverLink, self).__init__(connection, pn_link,
                                           target_address, source_address,
                                           eventHandler, properties)
        self._next_handle = 0
        self._unsettled_deliveries = {} # indexed by handle

        # @todo - think about receiver-settle-mode configuration

    def capacity(self):
        return self._pn_link.credit()

    def add_capacity(self, amount):
        self._pn_link.flow(amount)

    def message_accepted(self, handle):
        self._settle_delivery(handle, proton.Delivery.ACCEPTED)

    def message_rejected(self, handle, reason=None):
        # @todo: how to deal with 'reason'
        self._settle_delivery(handle, proton.Delivery.REJECTED)

    def message_released(self, handle):
        self._settle_delivery(handle, proton.Delivery.RELEASED)

    def message_modified(self, handle):
        self._settle_delivery(handle, proton.Delivery.MODIFIED)

    def destroy(self, error=None):
        self._connection._remove_receiver(self._name)
        self._connection = None
        super(ReceiverLink, self).destroy()

    def _delivery_updated(self, delivery):
        # a receive delivery changed state
        # @todo: multi-frame message transfer
        LOG.debug("Receive delivery updated")
        if delivery.readable:
            data = self._pn_link.recv(delivery.pending)
            msg = proton.Message()
            msg.decode(data)
            self._pn_link.advance()

            if self._handler:
                handle = "rmsg-%s:%x" % (self._name, self._next_handle)
                self._next_handle += 1
                self._unsettled_deliveries[handle] = delivery
                self._handler.message_received(self, msg, handle)
            else:
                # @todo: is it ok to assume Delivery.REJECTED?
                delivery.settle()

    def _settle_delivery(self, handle, result):
        # settle delivery associated with a handle
        if handle not in self._unsettled_deliveries:
            raise Exception("Invalid message handle: %s" % str(handle))
        delivery = self._unsettled_deliveries.pop(handle)
        delivery.update(result)
        delivery.settle()

