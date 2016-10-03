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

from pyngus.endpoint import Endpoint

LOG = logging.getLogger(__name__)

_PROTON_VERSION = (int(getattr(proton, "VERSION_MAJOR", 0)),
                   int(getattr(proton, "VERSION_MINOR", 0)))

# map property names to proton values:
_dist_modes = {"copy": proton.Terminus.DIST_MODE_COPY,
               "move": proton.Terminus.DIST_MODE_MOVE}
_snd_settle_modes = {"settled": proton.Link.SND_SETTLED,
                     "unsettled": proton.Link.SND_UNSETTLED,
                     "mixed": proton.Link.SND_MIXED}
_rcv_settle_modes = {"first": proton.Link.RCV_FIRST,
                     "second": proton.Link.RCV_SECOND}


# TODO(kgiusti): this is duplicated in connection.py, put in common file
class _CallbackLock(object):
    """A utility class for detecting when a callback invokes a non-reentrant
    Pyngus method.
    """
    def __init__(self, link):
        super(_CallbackLock, self).__init__()
        self._link = link
        self.in_callback = 0

    def __enter__(self):
        # manually lock parent - can't enter its non-reentrant methods
        self._link._connection._callback_lock.__enter__()
        self.in_callback += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.in_callback -= 1
        self._link._connection._callback_lock.__exit__(None, None, None)
        # if a call is made to a non-reentrant method while this context is
        # held, then the method will raise a RuntimeError().  Return false to
        # propagate the exception to the caller
        return False


def _not_reentrant(func):
    """Decorator that prevents callbacks from calling into link methods that
    are not reentrant """
    def wrap(*args, **kws):
        link = args[0]
        if link._callback_lock.in_callback:
            m = "Link %s cannot be invoked from a callback!" % func
            raise RuntimeError(m)
        return func(*args, **kws)
    return wrap


class _Link(Endpoint):
    """A generic Link base class."""

    def __init__(self, connection, pn_link):
        super(_Link, self).__init__(pn_link.name)
        self._connection = connection
        self._handler = None
        self._properties = None
        self._user_context = None
        self._rejected = False  # requested link was refused
        self._failed = False  # protocol error occurred
        self._callback_lock = _CallbackLock(self)
        # TODO(kgiusti): raise jira to add 'context' attr to api
        self._pn_link = pn_link
        pn_link.context = self

    def configure(self, target_address, source_address, handler, properties):
        """Assign addresses, properties, etc."""
        self._handler = handler
        self._properties = properties

        dynamic_props = None
        if properties:
            dynamic_props = properties.get("dynamic-node-properties")
            mode = _dist_modes.get(properties.get("distribution-mode"))
            if mode is not None:
                self._pn_link.source.distribution_mode = mode
            mode = _snd_settle_modes.get(properties.get("snd-settle-mode"))
            if mode is not None:
                self._pn_link.snd_settle_mode = mode
            mode = _rcv_settle_modes.get(properties.get("rcv-settle-mode"))
            if mode is not None:
                self._pn_link.rcv_settle_mode = mode

        if target_address is None:
            if not self._pn_link.is_sender:
                raise Exception("Dynamic target not allowed")
            self._pn_link.target.dynamic = True
            if dynamic_props:
                self._pn_link.target.properties.clear()
                self._pn_link.target.properties.put_dict(dynamic_props)
        elif target_address:
            self._pn_link.target.address = target_address

        if source_address is None:
            if not self._pn_link.is_receiver:
                raise Exception("Dynamic source not allowed")
            self._pn_link.source.dynamic = True
            if dynamic_props:
                self._pn_link.source.properties.clear()
                self._pn_link.source.properties.put_dict(dynamic_props)
        elif source_address:
            self._pn_link.source.address = source_address

    @property
    def name(self):
        return self._name

    @property
    def connection(self):
        return self._connection

    def open(self):
        if self._pn_link.state & proton.Endpoint.LOCAL_UNINIT:
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

    def close(self, pn_condition=None):
        if self._pn_link.state & proton.Endpoint.LOCAL_ACTIVE:
            LOG.debug("Closing the link.")
            if pn_condition:
                self._pn_link.condition = pn_condition
            self._pn_link.close()

    @property
    def active(self):
        state = self._pn_link.state
        return (not self._failed and
                state == (proton.Endpoint.LOCAL_ACTIVE |
                          proton.Endpoint.REMOTE_ACTIVE))

    @property
    def closed(self):
        state = self._pn_link.state
        return (self._failed or
                state == (proton.Endpoint.LOCAL_CLOSED |
                          proton.Endpoint.REMOTE_CLOSED))

    def reject(self, pn_condition):
        self._rejected = True  # prevent 'active' callback!
        self._pn_link.open()
        if pn_condition:
            self._pn_link.condition = pn_condition
        self._pn_link.close()

    def destroy(self):
        LOG.debug("link destroyed %s", str(self._pn_link))
        self._user_context = None
        self._connection = None
        self._handler = None
        self._callback_lock = None
        if self._pn_link:
            session = self._pn_link.session.context
            self._pn_link.context = None
            self._pn_link.free()
            self._pn_link = None
            session.link_destroyed(self)  # destroy session _after_ link

    def _process_delivery(self, pn_delivery):
        raise NotImplementedError("Must Override")

    def _process_credit(self):
        raise NotImplementedError("Must Override")

    def _link_failed(self, error):
        raise NotImplementedError("Must Override")

    def _session_closed(self):
        """Remote has closed the session used by this link."""
        # if link not already closed:
        if self._endpoint_state & proton.Endpoint.REMOTE_ACTIVE:
            # simulate close received
            self._process_remote_state()
        elif self._endpoint_state & proton.Endpoint.REMOTE_UNINIT:
            # locally created link, will never come up
            self._failed = True
            self._link_failed("Parent session closed.")

    # Proton's event model was changed after 0.7
    if (_PROTON_VERSION >= (0, 8)):
        _endpoint_event_map = {
            proton.Event.LINK_REMOTE_OPEN: Endpoint.REMOTE_OPENED,
            proton.Event.LINK_REMOTE_CLOSE: Endpoint.REMOTE_CLOSED,
            proton.Event.LINK_LOCAL_OPEN: Endpoint.LOCAL_OPENED,
            proton.Event.LINK_LOCAL_CLOSE: Endpoint.LOCAL_CLOSED}

        @staticmethod
        def _handle_proton_event(pn_event, connection):
            etype = pn_event.type
            if etype == proton.Event.DELIVERY:
                pn_link = pn_event.link
                pn_link.context and \
                    pn_link.context._process_delivery(pn_event.delivery)
                return True

            if etype == proton.Event.LINK_FLOW:
                pn_link = pn_event.link
                pn_link.context and pn_link.context._process_credit()
                return True

            ep_event = _Link._endpoint_event_map.get(etype)
            if ep_event is not None:
                pn_link = pn_event.link
                pn_link.context and \
                    pn_link.context._process_endpoint_event(ep_event)
                return True

            if etype == proton.Event.LINK_INIT:
                pn_link = pn_event.link
                # create a new link if requested by remote:
                c = hasattr(pn_link, 'context') and pn_link.context
                if not c:
                    session = pn_link.session.context
                    if (pn_link.is_sender and
                            pn_link.name not in connection._sender_links):
                        LOG.debug("Remotely initiated Sender needs init")
                        link = session.request_sender(pn_link)
                        connection._sender_links[pn_link.name] = link
                    elif (pn_link.is_receiver and
                          pn_link.name not in connection._receiver_links):
                        LOG.debug("Remotely initiated Receiver needs init")
                        link = session.request_receiver(pn_link)
                        connection._receiver_links[pn_link.name] = link
                return True

            if etype == proton.Event.LINK_FINAL:
                LOG.debug("link finalized: %s", pn_event.context)
                return True

            return False  # event not handled

    elif hasattr(proton.Event, "LINK_REMOTE_STATE"):
        # 0.7 proton event model
        @staticmethod
        def _handle_proton_event(pn_event, connection):
            if pn_event.type == proton.Event.LINK_REMOTE_STATE:
                pn_link = pn_event.link
                # create a new link if requested by remote:
                c = hasattr(pn_link, 'context') and pn_link.context
                if ((not c) and
                        (pn_link.state & proton.Endpoint.LOCAL_UNINIT)):
                    session = pn_link.session.context
                    if (pn_link.is_sender and
                            pn_link.name not in connection._sender_links):
                        LOG.debug("Remotely initiated Sender needs init")
                        link = session.request_sender(pn_link)
                        connection._sender_links[pn_link.name] = link
                    elif (pn_link.is_receiver and
                          pn_link.name not in connection._receiver_links):
                        LOG.debug("Remotely initiated Receiver needs init")
                        link = session.request_receiver(pn_link)
                        connection._receiver_links[pn_link.name] = link
                pn_link.context._process_remote_state()
                return True
            elif pn_event.type == proton.Event.LINK_LOCAL_STATE:
                pn_link = pn_event.link
                pn_link.context._process_local_state()
            elif pn_event.type == proton.Event.LINK_FLOW:
                pn_link = pn_event.link
                pn_link.context._process_credit()
            elif pn_event.type == proton.Event.DELIVERY:
                pn_link = pn_event.link
                pn_delivery = pn_event.delivery
                pn_link.context._process_delivery(pn_delivery)
            else:
                return False  # unknown
            return True

    # endpoint methods:
    @property
    def _endpoint_state(self):
        return self._pn_link.state

    def _ep_error(self, error):
        super(_Link, self)._ep_error(error)
        self._failed = True
        self._link_failed("Endpoint protocol error: %s" % error)


def _get_remote_settle_modes(pn_link):
    """Return a map containing the settle modes as provided by the remote.
    Skip any default value.
    """
    modes = {}
    snd = pn_link.remote_snd_settle_mode
    if snd == proton.Link.SND_UNSETTLED:
        modes['snd-settle-mode'] = 'unsettled'
    elif snd == proton.Link.SND_SETTLED:
        modes['snd-settle-mode'] = 'settled'
    if pn_link.remote_rcv_settle_mode == proton.Link.RCV_SECOND:
        modes['rcv-settle-mode'] = 'second'
    return modes


class SenderEventHandler(object):
    def sender_active(self, sender_link):
        LOG.debug("sender_active (ignored)")

    def sender_remote_closed(self, sender_link, pn_condition):
        LOG.debug("sender_remote_closed condition=%s (ignored)",
                  pn_condition)

    def sender_closed(self, sender_link):
        LOG.debug("sender_closed (ignored)")

    def credit_granted(self, sender_link):
        LOG.debug("credit_granted (ignored)")

    def sender_failed(self, sender_link, error):
        """Protocol error occurred."""
        LOG.debug("sender_failed error=%s (ignored)", error)


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

    _DISPOSITION_STATE_MAP = {
        proton.Disposition.ACCEPTED: ACCEPTED,
        proton.Disposition.REJECTED: REJECTED,
        proton.Disposition.RELEASED: RELEASED,
        proton.Disposition.MODIFIED: MODIFIED,
    }

    class _SendRequest(object):
        """Tracks sending a single message."""
        def __init__(self, link, tag, message, callback, handle, deadline):
            self.link = link
            self.tag = tag
            self.message = message
            self.callback = callback
            self.handle = handle
            self.deadline = deadline
            self.link._send_requests[self.tag] = self
            if self.deadline:
                self.link._connection._add_timer(self.deadline, self)

        def __call__(self):
            """Invoked by Connection on timeout (now <= deadline)."""
            self.link._send_expired(self)

        def destroy(self, state, info):
            """Invoked on final completion of send."""
            if self.deadline:
                self.link._connection._cancel_timer(self.deadline, self)
            if self.tag in self.link._send_requests:
                del self.link._send_requests[self.tag]
            if self.callback:
                with self.link._callback_lock:
                    self.callback(self.link, self.handle, state, info)

    def __init__(self, connection, pn_link):
        super(SenderLink, self).__init__(connection, pn_link)
        self._send_requests = {}  # indexed by tag
        self._pending_sends = collections.deque()  # tags in order sent
        self._next_deadline = 0
        self._next_tag = 0
        self._last_credit = 0

        # TODO(kgiusti) - think about send-settle-mode configuration

    def send(self, message, delivery_callback=None,
             handle=None, deadline=None):
        tag = "pyngus-tag-%s" % self._next_tag
        self._next_tag += 1
        send_req = SenderLink._SendRequest(self, tag, message,
                                           delivery_callback, handle,
                                           deadline)
        self._pn_link.delivery(tag)

        pn_delivery = self._pn_link.current
        if pn_delivery and pn_delivery.writable:
            # send oldest pending:
            if self._pending_sends:
                self._pending_sends.append(tag)
                tag = self._pending_sends.popleft()
                send_req = self._send_requests[tag]
            self._write_msg(pn_delivery, send_req)
        else:
            LOG.debug("Send is pending for credit, tag=%s", tag)
            self._pending_sends.append(tag)

        return 0

    @property
    def pending(self):
        return len(self._send_requests)

    @property
    def credit(self):
        return self._pn_link.credit

    def reject(self, pn_condition=None):
        """See Link Reject, AMQP1.0 spec."""
        self._pn_link.source.type = proton.Terminus.UNSPECIFIED
        super(SenderLink, self).reject(pn_condition)

    @_not_reentrant
    def destroy(self):
        self._connection._remove_sender(self._name)
        self._connection = None
        super(SenderLink, self).destroy()

    def _process_delivery(self, pn_delivery):
        """Check if the delivery can be processed."""
        if pn_delivery.tag in self._send_requests:
            if pn_delivery.settled or pn_delivery.remote_state:
                # remote has reached a 'terminal state'
                outcome = pn_delivery.remote_state
                state = SenderLink._DISPOSITION_STATE_MAP.get(outcome,
                                                              self.UNKNOWN)
                pn_disposition = pn_delivery.remote
                info = {}
                if state == SenderLink.REJECTED:
                    if pn_disposition.condition:
                        info["condition"] = pn_disposition.condition
                elif state == SenderLink.MODIFIED:
                    info["delivery-failed"] = pn_disposition.failed
                    info["undeliverable-here"] = pn_disposition.undeliverable
                    annotations = pn_disposition.annotations
                    if annotations:
                        info["message-annotations"] = annotations
                send_req = self._send_requests.pop(pn_delivery.tag)
                send_req.destroy(state, info)
                pn_delivery.settle()
            elif pn_delivery.writable:
                # we can now send on this delivery
                if self._pending_sends:
                    tag = self._pending_sends.popleft()
                    send_req = self._send_requests[tag]
                    self._write_msg(pn_delivery, send_req)
        else:
            # tag no longer valid, expired or canceled send?
            LOG.debug("Delivery ignored, tag=%s", str(pn_delivery.tag))
            pn_delivery.settle()

    def _process_credit(self):
        # check if any pending deliveries are now writable:
        pn_delivery = self._pn_link.current
        while (self._pending_sends and
               pn_delivery and pn_delivery.writable):
            self._process_delivery(pn_delivery)
            pn_delivery = self._pn_link.current

        # Alert if credit has become available
        if self._handler and not self._rejected:
            if 0 < self._pn_link.credit > self._last_credit:
                with self._callback_lock:
                    self._handler.credit_granted(self)
        self._last_credit = self._pn_link.credit

    def _write_msg(self, pn_delivery, send_req):
        # given a writable delivery, send a message
        self._pn_link.send(send_req.message.encode())
        self._pn_link.advance()
        self._last_credit = self._pn_link.credit
        if not send_req.callback:
            # no disposition callback, so we can discard the send request and
            # settle the delivery immediately
            send_req.destroy(SenderLink.UNKNOWN, {})
            pn_delivery.settle()

    def _send_expired(self, send_req):
        LOG.debug("Send request timed-out, tag=%s", send_req.tag)
        try:
            self._pending_sends.remove(send_req.tag)
        except ValueError:
            pass
        send_req.destroy(SenderLink.TIMED_OUT, None)

    def _link_failed(self, error):
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.sender_failed(self, error)

    # endpoint state machine actions:

    def _ep_active(self):
        LOG.debug("SenderLink is up")
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.sender_active(self)

    def _ep_need_close(self):
        LOG.debug("SenderLink remote closed")
        if self._handler and not self._rejected:
            cond = self._pn_link.remote_condition
            with self._callback_lock:
                self._handler.sender_remote_closed(self, cond)

    def _ep_closed(self):
        LOG.debug("SenderLink close completed")
        # abort any pending sends
        self._pending_sends.clear()
        pn_condition = self._pn_link.condition
        info = {"condition": pn_condition} if pn_condition else None
        while self._send_requests:
            key, send_req = self._send_requests.popitem()
            send_req.destroy(SenderLink.ABORTED, info)
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.sender_closed(self)

    def _ep_requested(self):
        LOG.debug("Remote has requested a SenderLink")
        handler = self._connection._handler
        if handler:
            pn_link = self._pn_link
            props = _get_remote_settle_modes(pn_link)
            # has the remote requested a source address?
            req_source = ""
            if pn_link.remote_source.dynamic:
                req_source = None
                req_props = pn_link.remote_source.properties
                if req_props and req_props.next() == proton.Data.MAP:
                    props["dynamic-node-properties"] = req_props.get_dict()
            elif pn_link.remote_source.address:
                req_source = pn_link.remote_source.address

            props["target-address"] = pn_link.remote_target.address
            dist_mode = pn_link.remote_source.distribution_mode
            if (dist_mode == proton.Terminus.DIST_MODE_COPY):
                props["distribution-mode"] = "copy"
            elif (dist_mode == proton.Terminus.DIST_MODE_MOVE):
                props["distribution-mode"] = "move"

            with self._connection._callback_lock:
                handler.sender_requested(self._connection,
                                         pn_link.name,  # handle
                                         pn_link.name,
                                         req_source,
                                         props)


class ReceiverEventHandler(object):

    def receiver_active(self, receiver_link):
        LOG.debug("receiver_active (ignored)")

    def receiver_remote_closed(self, receiver_link, pn_condition):
        LOG.debug("receiver_remote_closed condition=%s (ignored)",
                  pn_condition)

    def receiver_closed(self, receiver_link):
        LOG.debug("receiver_closed (ignored)")

    def receiver_failed(self, receiver_link, error):
        """Protocol error occurred."""
        LOG.debug("receiver_failed error=%s (ignored)", error)

    def message_received(self, receiver_link, message, handle):
        LOG.debug("message_received (ignored)")


class ReceiverLink(_Link):
    def __init__(self, connection, pn_link):
        super(ReceiverLink, self).__init__(connection, pn_link)
        self._next_handle = 0
        self._unsettled_deliveries = {}  # indexed by handle

        # TODO(kgiusti) - think about receiver-settle-mode configuration

    @property
    def capacity(self):
        return self._pn_link.credit

    def add_capacity(self, amount):
        self._pn_link.flow(amount)

    def _settle_delivery(self, handle, state):
        pn_delivery = self._unsettled_deliveries.pop(handle, None)
        if pn_delivery is None:
            raise Exception("Invalid message handle: %s" % str(handle))
        pn_delivery.update(state)
        pn_delivery.settle()

    def message_accepted(self, handle):
        self._settle_delivery(handle, proton.Delivery.ACCEPTED)

    def message_released(self, handle):
        self._settle_delivery(handle, proton.Delivery.RELEASED)

    def message_rejected(self, handle, pn_condition=None):
        pn_delivery = self._unsettled_deliveries.pop(handle, None)
        if pn_delivery is None:
            raise Exception("Invalid message handle: %s" % str(handle))
        if pn_condition:
            pn_delivery.local.condition = pn_condition
        pn_delivery.update(proton.Delivery.REJECTED)
        pn_delivery.settle()

    def message_modified(self, handle, delivery_failed, undeliverable,
                         annotations):
        pn_delivery = self._unsettled_deliveries.pop(handle, None)
        if pn_delivery is None:
            raise Exception("Invalid message handle: %s" % str(handle))
        pn_delivery.local.failed = delivery_failed
        pn_delivery.local.undeliverable = undeliverable
        if annotations:
            pn_delivery.local.annotations = annotations
        pn_delivery.update(proton.Delivery.MODIFIED)
        pn_delivery.settle()

    def reject(self, pn_condition=None):
        """See Link Reject, AMQP1.0 spec."""
        self._pn_link.target.type = proton.Terminus.UNSPECIFIED
        super(ReceiverLink, self).reject(pn_condition)

    @_not_reentrant
    def destroy(self):
        self._connection._remove_receiver(self._name)
        self._connection = None
        super(ReceiverLink, self).destroy()

    def _process_delivery(self, pn_delivery):
        """Check if the delivery can be processed."""
        if pn_delivery.readable and not pn_delivery.partial:
            data = self._pn_link.recv(pn_delivery.pending)
            msg = proton.Message()
            msg.decode(data)
            self._pn_link.advance()

            if self._handler:
                handle = "rmsg-%s:%x" % (self._name, self._next_handle)
                self._next_handle += 1
                self._unsettled_deliveries[handle] = pn_delivery
                with self._callback_lock:
                    self._handler.message_received(self, msg, handle)
            else:
                # TODO(kgiusti): is it ok to assume Delivery.REJECTED?
                pn_delivery.settle()

    def _process_credit(self):
        # Only used by SenderLink
        pass

    def _link_failed(self, error):
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.receiver_failed(self, error)

    # endpoint state machine actions:

    def _ep_active(self):
        LOG.debug("ReceiverLink is up")
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.receiver_active(self)

    def _ep_need_close(self):
        LOG.debug("ReceiverLink remote closed")
        if self._handler and not self._rejected:
            cond = self._pn_link.remote_condition
            with self._callback_lock:
                self._handler.receiver_remote_closed(self, cond)

    def _ep_closed(self):
        LOG.debug("ReceiverLink close completed")
        if self._handler and not self._rejected:
            with self._callback_lock:
                self._handler.receiver_closed(self)

    def _ep_requested(self):
        LOG.debug("Remote has initiated a ReceiverLink")
        handler = self._connection._handler
        if handler:
            pn_link = self._pn_link
            props = _get_remote_settle_modes(pn_link)
            # has the remote requested a target address?
            req_target = ""
            if pn_link.remote_target.dynamic:
                req_target = None
                req_props = pn_link.remote_target.properties
                if req_props and req_props.next() == proton.Data.MAP:
                    props["dynamic-node-properties"] = req_props.get_dict()
            elif pn_link.remote_target.address:
                req_target = pn_link.remote_target.address

            props["source-address"] = pn_link.remote_source.address
            dist_mode = pn_link.remote_source.distribution_mode
            if (dist_mode == proton.Terminus.DIST_MODE_COPY):
                props["distribution-mode"] = "copy"
            elif (dist_mode == proton.Terminus.DIST_MODE_MOVE):
                props["distribution-mode"] = "move"

            with self._connection._callback_lock:
                handler.receiver_requested(self._connection,
                                           pn_link.name,  # handle
                                           pn_link.name,
                                           req_target,
                                           props)


class _SessionProxy(Endpoint):
    """Corresponds to a Proton Session object."""
    def __init__(self, name, connection, pn_session=None):
        super(_SessionProxy, self).__init__(name)
        self._locally_initiated = not pn_session
        self._connection = connection
        if not pn_session:
            pn_session = connection._pn_connection.session()
        self._pn_session = pn_session
        self._links = set()
        pn_session.context = self

    def open(self):
        if self._pn_session.state & proton.Endpoint.LOCAL_UNINIT:
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
        """Link has been destroyed."""
        self._links.discard(link)
        if not self._links:
            # no more links
            LOG.debug("destroying unneeded session")
            self._pn_session.close()
            self._pn_session.free()
            self._pn_session = None
            self._connection = None

    # Proton's event model was changed after 0.7
    if (_PROTON_VERSION >= (0, 8)):
        _endpoint_event_map = {
            proton.Event.SESSION_REMOTE_OPEN: Endpoint.REMOTE_OPENED,
            proton.Event.SESSION_REMOTE_CLOSE: Endpoint.REMOTE_CLOSED,
            proton.Event.SESSION_LOCAL_OPEN: Endpoint.LOCAL_OPENED,
            proton.Event.SESSION_LOCAL_CLOSE: Endpoint.LOCAL_CLOSED}

        @staticmethod
        def _handle_proton_event(pn_event, connection):
            ep_event = _SessionProxy._endpoint_event_map.get(pn_event.type)
            if ep_event is not None:
                pn_session = pn_event.context
                pn_session.context._process_endpoint_event(ep_event)
            elif pn_event.type == proton.Event.SESSION_INIT:
                # create a new session if requested by remote:
                pn_session = pn_event.context
                c = hasattr(pn_session, 'context') and pn_session.context
                if not c:
                    LOG.debug("Opening remotely initiated session")
                    name = "session-%d" % connection._remote_session_id
                    connection._remote_session_id += 1
                    _SessionProxy(name, connection, pn_session)
            elif pn_event.type == proton.Event.SESSION_FINAL:
                LOG.debug("Session finalized: %s", pn_event.context)
            else:
                return False  # unknown
            return True  # handled
    elif hasattr(proton.Event, "SESSION_REMOTE_STATE"):
        # 0.7 proton event model
        @staticmethod
        def _handle_proton_event(pn_event, connection):
            if pn_event.type == proton.Event.SESSION_REMOTE_STATE:
                pn_session = pn_event.session
                # create a new session if requested by remote:
                c = hasattr(pn_session, 'context') and pn_session.context
                if not c:
                    LOG.debug("Opening remotely initiated session")
                    name = "session-%d" % connection._remote_session_id
                    connection._remote_session_id += 1
                    _SessionProxy(name, connection, pn_session)
                pn_session.context._process_remote_state()
            elif pn_event.type == proton.Event.SESSION_LOCAL_STATE:
                pn_session = pn_event.session
                pn_session.context._process_local_state()
            else:
                return False  # unknown
            return True  # handled

    @property
    def _endpoint_state(self):
        return self._pn_session.state

    # endpoint state machine actions:

    def _ep_requested(self):
        """Peer has requested a new session."""
        LOG.debug("Session %s requested - opening...",
                  self._name)
        self.open()

    def _ep_active(self):
        """Both ends of the Endpoint have become active."""
        LOG.debug("Session %s active", self._name)

    def _ep_need_close(self):
        """Peer has closed its end of the session."""
        LOG.debug("Session %s close requested - closing...",
                  self._name)
        links = self._links.copy()  # may modify _links
        for link in links:
            link._session_closed()

    def _ep_closed(self):
        """Both ends of the endpoint have closed."""
        LOG.debug("Session %s closed", self._name)
