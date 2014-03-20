#    Licensed to the Apache Software Foundation (ASF) under one
#    or more contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

__all__ = [
    "SenderEventHandler",
    "SenderLink",
    "ReceiverEventHandler",
    "ReceiverLink"
]

import collections
import logging
import proton

LOG = logging.getLogger(__name__)


class _Link(object):
    """A generic Link base class."""

    def __init__(self, connection, pn_link):
        self._state = _Link._STATE_UNINIT
        # last known endpoint state:
        self._ep_state = (proton.Endpoint.LOCAL_UNINIT |
                          proton.Endpoint.REMOTE_UNINIT)
        self._connection = connection
        self._name = pn_link.name
        self._handler = None
        self._properties = None
        self._user_context = None
        # TODO(kgiusti): raise jira to add 'context' attr to api
        self._pn_link = pn_link
        pn_link.context = self

    def configure(self, target_address, source_address, handler, properties):
        """Assign addresses, properties, etc."""
        self._handler = handler
        self._properties = properties

        if target_address is None:
            if not self._pn_link.is_sender:
                raise Exception("Dynamic target not allowed")
            self._pn_link.target.dynamic = True
        elif target_address:
            self._pn_link.target.address = target_address

        if source_address is None:
            if not self._pn_link.is_receiver:
                raise Exception("Dynamic source not allowed")
            self._pn_link.source.dynamic = True
        elif source_address:
            self._pn_link.source.address = source_address

        if properties:
            desired_mode = properties.get("distribution-mode")
            if desired_mode:
                if desired_mode == "copy":
                    mode = proton.Terminus.DIST_MODE_COPY
                elif desired_mode == "move":
                    mode = proton.Terminus.DIST_MODE_MOVE
                else:
                    raise Exception("Unknown distribution mode: %s" %
                                    str(desired_mode))
                self._pn_link.source.distribution_mode = mode

    @property
    def name(self):
        return self._name

    def open(self):
        LOG.debug("Opening the link.")
        self._pn_link.open()

    def _get_user_context(self):
        return self._user_context

    def _set_user_context(self, ctxt):
        self._user_context = ctxt

    _uc_docstr = """Arbitrary application object associated with this link."""
    user_context = property(_get_user_context, _set_user_context,
                            doc=_uc_docstr)

    @property
    def source_address(self):
        """Return the authorative source of the link."""
        # If link is a sender, source is determined by the local
        # value, else use the remote.
        if self._pn_link.is_sender:
            return self._pn_link.source.address
        else:
            return self._pn_link.remote_source.address

    @property
    def target_address(self):
        """Return the authorative target of the link."""
        # If link is a receiver, target is determined by the local
        # value, else use the remote.
        if self._pn_link.is_receiver:
            return self._pn_link.target.address
        else:
            return self._pn_link.remote_target.address

    def close(self, error=None):
        LOG.debug("Closing the link.")
        self._pn_link.close()

    @property
    def closed(self):
        state = self._pn_link.state
        return state == (proton.Endpoint.LOCAL_CLOSED
                         | proton.Endpoint.REMOTE_CLOSED)

    def destroy(self):
        LOG.debug("link destroyed %s", str(self._pn_link))
        self._user_context = None
        self._connection = None
        if self._pn_link:
            session = self._pn_link.session.context
            self._pn_link.context = None
            self._pn_link.free()
            self._pn_link = None
            session.link_destroyed(self)  # destroy session _after_ link

    ## Link State Machine

    # Link States:  (note: keep in sync with _STATE_MAP below!)
    _STATE_UNINIT = 0  # initial state
    _STATE_PENDING = 1  # waiting for remote to open
    _STATE_REQUESTED = 2  # waiting for local to open
    _STATE_CANCELLED = 3  # remote closed requested link before accepted
    _STATE_ACTIVE = 4
    _STATE_NEED_CLOSE = 5  # remote initiated close
    _STATE_CLOSING = 6  # locally closed, pending remote
    _STATE_CLOSED = 7  # terminal state
    _STATE_REJECTED = 8  # terminal state, automatic link cleanup

    # Events:
    _LOCAL_ACTIVE = 1
    _LOCAL_CLOSED = 2
    _REMOTE_ACTIVE = 3
    _REMOTE_CLOSED = 4

    # state entry actions - link type specific:

    @staticmethod
    def _fsm_active(link):
        """Both ends of the link have become active."""
        link._do_active()

    @staticmethod
    def _fsm_need_close(link):
        """The remote has closed its end of the link."""
        link._do_need_close()

    @staticmethod
    def _fsm_closed(link):
        """Both ends of the link have closed."""
        link._do_closed()

    @staticmethod
    def _fsm_requested(link):
        """Remote has created a new link."""
        link._do_requested()

    def _process_endpoints(self):
        """Process any changes in link endpoint state."""
        LOCAL_MASK = (proton.Endpoint.LOCAL_UNINIT |
                      proton.Endpoint.LOCAL_ACTIVE |
                      proton.Endpoint.LOCAL_CLOSED)
        REMOTE_MASK = (proton.Endpoint.REMOTE_UNINIT |
                       proton.Endpoint.REMOTE_ACTIVE |
                       proton.Endpoint.REMOTE_CLOSED)
        new_state = self._pn_link.state
        old_state = self._ep_state
        fsm = _Link._STATE_MAP[self._state]
        if ((new_state & LOCAL_MASK) != (old_state & LOCAL_MASK)):
            event = None
            if new_state & proton.Endpoint.LOCAL_ACTIVE:
                event = _Link._LOCAL_ACTIVE
            elif new_state & proton.Endpoint.LOCAL_CLOSED:
                event = _Link._LOCAL_CLOSED
            if event:
                entry = fsm.get(event)
                if entry:
                    LOG.debug("Old State: %d Event %d New State: %d",
                              self._state, event, entry[0])
                    self._state = entry[0]
                    if entry[1]:
                        entry[1](self)

        if ((new_state & REMOTE_MASK) != (old_state & REMOTE_MASK)):
            event = None
            if new_state & proton.Endpoint.REMOTE_ACTIVE:
                event = _Link._REMOTE_ACTIVE
            elif new_state & proton.Endpoint.REMOTE_CLOSED:
                event = _Link._REMOTE_CLOSED
            if event:
                entry = fsm.get(event)
                if entry:
                    LOG.debug("Old State: %d Event %d New State: %d",
                              self._state, event, entry[0])
                    self._state = entry[0]
                    if entry[1]:
                        entry[1](self)
        self._ep_state = new_state

    def _process_delivery(self, pn_delivery):
        raise NotImplementedError("Must Override")

    def _process_credit(self):
        raise NotImplementedError("Must Override")

    def _session_closed(self):
        """Remote has closed the session used by this link."""
        fsm = _Link._STATE_MAP[self._state]
        entry = fsm.get(_Link._REMOTE_CLOSED)
        if entry:
            LOG.debug("Old State: %d Event %d New State: %d",
                      self._state, _Link._REMOTE_CLOSED, entry[0])
            self._state = entry[0]
            if entry[1]:
                entry[1](self)

_Link._STATE_MAP = [  # {event: (next-state, action), ...}
    # _STATE_UNINIT:
    {_Link._LOCAL_ACTIVE:  (_Link._STATE_PENDING,    None),
     _Link._REMOTE_ACTIVE: (_Link._STATE_REQUESTED,  _Link._fsm_requested),
     _Link._REMOTE_CLOSED: (_Link._STATE_NEED_CLOSE, _Link._fsm_need_close)},
    # _STATE_PENDING:
    {_Link._LOCAL_CLOSED:  (_Link._STATE_CLOSING,    None),
     _Link._REMOTE_ACTIVE: (_Link._STATE_ACTIVE,     _Link._fsm_active),
     _Link._REMOTE_CLOSED: (_Link._STATE_NEED_CLOSE, _Link._fsm_need_close)},
    # _STATE_REQUESTED:
    {_Link._LOCAL_CLOSED:  (_Link._STATE_REJECTED,   None),
     _Link._LOCAL_ACTIVE:  (_Link._STATE_ACTIVE,     _Link._fsm_active),
     _Link._REMOTE_CLOSED: (_Link._STATE_CANCELLED,  None)},
    #_STATE_CANCELLED:
    {_Link._LOCAL_CLOSED:  (_Link._STATE_REJECTED,   None),
     _Link._LOCAL_ACTIVE:  (_Link._STATE_NEED_CLOSE, _Link._fsm_need_close)},
    #_STATE_ACTIVE:
    {_Link._LOCAL_CLOSED:  (_Link._STATE_CLOSING,    None),
     _Link._REMOTE_CLOSED: (_Link._STATE_NEED_CLOSE, _Link._fsm_need_close)},
    #_STATE_NEED_CLOSE:
    {_Link._LOCAL_CLOSED:  (_Link._STATE_CLOSED,     _Link._fsm_closed)},
    #_STATE_CLOSING:
    {_Link._REMOTE_CLOSED: (_Link._STATE_CLOSED,     _Link._fsm_closed)},
    #_STATE_CLOSED:
    {},
    #_STATE_REJECTED:
    {}]


class SenderEventHandler(object):
    def sender_active(self, sender_link):
        LOG.debug("sender_active (ignored)")

    def sender_remote_closed(self, sender_link, error=None):
        LOG.debug("sender_remote_closed (ignored)")

    def sender_closed(self, sender_link):
        LOG.debug("sender_closed (ignored)")

    def credit_granted(self, sender_link):
        LOG.debug("credit_granted (ignored)")


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

    def __init__(self, connection, pn_link):
        super(SenderLink, self).__init__(connection, pn_link)
        self._pending_sends = collections.deque()
        self._pending_acks = {}
        self._next_deadline = 0
        self._next_tag = 0
        self._last_credit = 0

        # TODO(kgiusti) - think about send-settle-mode configuration

    def send(self, message, delivery_callback=None,
             handle=None, deadline=None):
        self._pending_sends.append((message, delivery_callback, handle,
                                   deadline))
        # TODO(kgiusti) deadline not supported yet
        if deadline:
            raise NotImplementedError("send timeout not supported yet!")
        if deadline and (self._next_deadline == 0 or
                         self._next_deadline > deadline):
            self._next_deadline = deadline

        pn_delivery = self._pn_link.delivery("tag-%x" % self._next_tag)
        self._next_tag += 1

        if pn_delivery.writable:
            send_req = self._pending_sends.popleft()
            self._write_msg(pn_delivery, send_req)

        return 0

    def pending(self):
        return len(self._pending_sends) + len(self._pending_acks)

    def credit(self):
        return self._pn_link.credit

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

    def reject(self, reason):
        """See Link Reject, AMQP1.0 spec."""
        # TODO(kgiusti) support reason for close
        self._pn_link.source.type = proton.Terminus.UNSPECIFIED
        self._pn_link.open()
        self._pn_link.close()

    def destroy(self):
        self._connection._remove_sender(self._name)
        self._connection = None
        super(SenderLink, self).destroy()

    def _process_delivery(self, pn_delivery):
        """Check if the delivery can be processed."""
        _disposition_state_map = {
            proton.Disposition.ACCEPTED: SenderLink.ACCEPTED,
            proton.Disposition.REJECTED: SenderLink.REJECTED,
            proton.Disposition.RELEASED: SenderLink.RELEASED,
            proton.Disposition.MODIFIED: SenderLink.MODIFIED,
        }

        if pn_delivery.tag in self._pending_acks:
            if pn_delivery.settled:  # remote has finished
                LOG.debug("Remote has settled a sent msg")
                send_req = self._pending_acks.pop(pn_delivery.tag)
                state = _disposition_state_map.get(pn_delivery.remote_state,
                                                   self.UNKNOWN)
                cb = send_req[1]
                handle = send_req[2]

                cb(self, handle, state, None)
                pn_delivery.settle()
        else:
            # not for a sent msg, use it to send the next
            if pn_delivery.writable and self._pending_sends:
                send_req = self._pending_sends.popleft()
                self._write_msg(pn_delivery, send_req)
            else:
                # what else is there???
                pn_delivery.settle()

    def _process_credit(self):
        # Alert if credit has become available
        if self._handler:
            new_credit = self._pn_link.credit()
            if self._last_credit <= 0 and new_credit > 0:
                self._handler.credit_granted(self)
            self._last_credit = new_credit

    def _write_msg(self, pn_delivery, send_req):
        # given a writable delivery, send a message
        # send_req = (msg, cb, handle, deadline)
        LOG.debug("Sending a pending message")
        msg = send_req[0]
        cb = send_req[1]
        self._pn_link.send(msg.encode())
        self._pn_link.advance()
        if cb:  # delivery callback given
            if pn_delivery.tag in self._pending_acks:
                raise Exception("Duplicate delivery tag?")
            self._pending_acks[pn_delivery.tag] = send_req
        else:
            # no status required, so settle it now.
            pn_delivery.settle()

    # state machine actions:

    def _do_active(self):
        LOG.debug("Link is up")
        if self._handler:
            self._handler.sender_active(self)

    def _do_need_close(self):
        # TODO(kgiusti) error reporting
        LOG.debug("Link remote closed")
        if self._handler:
            self._handler.sender_remote_closed(self, None)

    def _do_closed(self):
        LOG.debug("Link close completed")
        if self._handler:
            self._handler.sender_closed(self)

    def _do_requested(self):
        LOG.debug("Remote has initiated a link")
        pn_link = self._pn_link
        # has the remote requested a source address?
        req_source = ""
        if pn_link.remote_source.dynamic:
            req_source = None
        elif pn_link.remote_source.address:
            req_source = pn_link.remote_source.address
        handler = self._connection._handler
        if handler:
            handler.sender_requested(self._connection,
                                     pn_link.name,  # handle
                                     pn_link.name,
                                     req_source,
                                     {"target-address":
                                      pn_link.remote_target.address})


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
    def __init__(self, connection, pn_link):
        super(ReceiverLink, self).__init__(connection, pn_link)
        self._next_handle = 0
        self._unsettled_deliveries = {}  # indexed by handle

        # TODO(kgiusti) - think about receiver-settle-mode configuration

    def capacity(self):
        return self._pn_link.credit()

    def add_capacity(self, amount):
        self._pn_link.flow(amount)

    def message_accepted(self, handle):
        self._settle_delivery(handle, proton.Delivery.ACCEPTED)

    def message_rejected(self, handle, reason=None):
        # TODO(kgiusti): how to deal with 'reason'
        self._settle_delivery(handle, proton.Delivery.REJECTED)

    def message_released(self, handle):
        self._settle_delivery(handle, proton.Delivery.RELEASED)

    def message_modified(self, handle):
        self._settle_delivery(handle, proton.Delivery.MODIFIED)

    def reject(self, reason):
        """See Link Reject, AMQP1.0 spec."""
        # TODO(kgiusti) support reason for close
        self._pn_link.target.type = proton.Terminus.UNSPECIFIED
        self._pn_link.open()
        self._pn_link.close()

    def destroy(self):
        self._connection._remove_receiver(self._name)
        self._connection = None
        super(ReceiverLink, self).destroy()

    def _process_delivery(self, pn_delivery):
        """Check if the delivery can be processed."""
        # TODO(kgiusti): multi-frame message transfer
        if pn_delivery.readable:
            LOG.debug("Receive delivery readable")
            data = self._pn_link.recv(pn_delivery.pending)
            msg = proton.Message()
            msg.decode(data)
            self._pn_link.advance()

            if self._handler:
                handle = "rmsg-%s:%x" % (self._name, self._next_handle)
                self._next_handle += 1
                self._unsettled_deliveries[handle] = pn_delivery
                self._handler.message_received(self, msg, handle)
            else:
                # TODO(kgiusti): is it ok to assume Delivery.REJECTED?
                pn_delivery.settle()

    def _settle_delivery(self, handle, result):
        # settle delivery associated with a handle
        if handle not in self._unsettled_deliveries:
            raise Exception("Invalid message handle: %s" % str(handle))
        pn_delivery = self._unsettled_deliveries.pop(handle)
        pn_delivery.update(result)
        pn_delivery.settle()

    def _process_credit(self):
        # Only used by SenderLink
        pass

    # state machine actions:

    def _do_active(self):
        LOG.debug("Link is up")
        if self._handler:
            self._handler.receiver_active(self)

    def _do_need_close(self):
        LOG.debug("Link remote closed")
        if self._handler:
            self._handler.receiver_remote_closed(self, None)

    def _do_closed(self):
        LOG.debug("Link close completed")
        if self._handler:
            self._handler.receiver_closed(self)

    def _do_requested(self):
        LOG.debug("Remote has initiated a ReceiverLink")
        pn_link = self._pn_link
        # has the remote requested a target address?
        req_target = ""
        if pn_link.remote_target.dynamic:
            req_target = None
        elif pn_link.remote_target.address:
            req_target = pn_link.remote_target.address
        handler = self._connection._handler
        if handler:
            handler.receiver_requested(self._connection,
                                       pn_link.name,  # handle
                                       pn_link.name,
                                       req_target,
                                       {"source-address":
                                        pn_link.remote_source.address})


class _SessionProxy(object):
    """Corresponds to a Proton Session object."""
    def __init__(self, connection, pn_session=None):
        self._locally_initiated = not pn_session
        self._connection = connection
        if not pn_session:
            pn_session = connection._pn_connection.session()
        self._pn_session = pn_session
        self._links = set()
        pn_session.context = self

    def open(self):
        self._pn_session.open()

    def new_sender(self, name):
        """Create a new sender link."""
        pn_link = self._pn_session.sender(name)
        return self.request_sender(pn_link)

    def request_sender(self, pn_link):
        """Create link from request for a sender."""
        sl = SenderLink(self._connection, pn_link)
        self._links.add(sl)
        return sl

    def new_receiver(self, name):
        """Create a new receiver link."""
        pn_link = self._pn_session.receiver(name)
        return self.request_receiver(pn_link)

    def request_receiver(self, pn_link):
        """Create link from request for a receiver."""
        rl = ReceiverLink(self._connection, pn_link)
        self._links.add(rl)
        return rl

    def link_destroyed(self, link):
        """Link has been destroyed"""
        self._links.discard(link)
        if not self._links:
            # no more links
            LOG.debug("destroying unneeded session")
            self._pn_session.close()
            self._pn_session.free()
            self._pn_session = None
            self._connection = None

    def remote_closed(self):
        """Peer has closed its end of the session."""
        links = self._links.copy()  # may modify _links
        for link in links:
            link._session_closed()
