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

import logging
import proton

LOG = logging.getLogger(__name__)


class Endpoint(object):
    """AMQP Endpoint state machine."""

    # Endpoint States:  (note: keep in sync with _fsm below!)
    STATE_UNINIT = 0  # initial state
    STATE_PENDING = 1  # waiting for remote to open
    STATE_REQUESTED = 2  # waiting for local to open
    STATE_CANCELLED = 3  # remote closed endpoint before local open
    STATE_ACTIVE = 4
    STATE_NEED_CLOSE = 5  # remote initiated close
    STATE_CLOSING = 6  # locally closed, pending remote
    STATE_CLOSED = 7  # terminal state
    STATE_REJECTED = 8  # terminal state, automatic cleanup

    state_names = ["STATE_UNINIT", "STATE_PENDING", "STATE_REQUESTED",
                   "STATE_CANCELLED", "STATE_ACTIVE", "STATE_NEED_CLOSE",
                   "STATE_CLOSING", "STATE_CLOSED", "STATE_REJECTED"]

    # Events - state has transitioned to:
    LOCAL_ACTIVE = 1
    LOCAL_CLOSED = 2
    REMOTE_ACTIVE = 3
    REMOTE_CLOSED = 4

    event_names = ["UNKNOWN", "LOCAL_ACTIVE", "LOCAL_CLOSED",
                   "REMOTE_ACTIVE", "REMOTE_CLOSED"]

    def __init__(self, name):
        self._name = name
        self._state = Endpoint.STATE_UNINIT
        self._fsm = [  # {event: (next-state, action), ...}
            # STATE_UNINIT:
            {Endpoint.LOCAL_ACTIVE: (Endpoint.STATE_PENDING, None),
             Endpoint.REMOTE_ACTIVE: (Endpoint.STATE_REQUESTED,
                                      self._ep_requested),
             Endpoint.REMOTE_CLOSED: (Endpoint.STATE_NEED_CLOSE,
                                      self._ep_need_close)},
            # STATE_PENDING:
            {Endpoint.LOCAL_CLOSED:  (Endpoint.STATE_CLOSING, None),
             Endpoint.REMOTE_ACTIVE: (Endpoint.STATE_ACTIVE,
                                      self._ep_active),
             Endpoint.REMOTE_CLOSED: (Endpoint.STATE_NEED_CLOSE,
                                      self._ep_need_close)},
            # STATE_REQUESTED:
            {Endpoint.LOCAL_CLOSED:  (Endpoint.STATE_REJECTED, None),
             Endpoint.LOCAL_ACTIVE:  (Endpoint.STATE_ACTIVE,
                                      self._ep_active),
             Endpoint.REMOTE_CLOSED: (Endpoint.STATE_CANCELLED, None)},
            # STATE_CANCELLED:
            {Endpoint.LOCAL_CLOSED:  (Endpoint.STATE_REJECTED, None),
             Endpoint.LOCAL_ACTIVE:  (Endpoint.STATE_NEED_CLOSE,
                                      self._ep_need_close)},
            # STATE_ACTIVE:
            {Endpoint.LOCAL_CLOSED:  (Endpoint.STATE_CLOSING, None),
             Endpoint.REMOTE_CLOSED: (Endpoint.STATE_NEED_CLOSE,
                                      self._ep_need_close)},
            # STATE_NEED_CLOSE:
            {Endpoint.LOCAL_CLOSED:  (Endpoint.STATE_CLOSED,
                                      self._ep_closed)},
            # STATE_CLOSING:
            {Endpoint.REMOTE_CLOSED: (Endpoint.STATE_CLOSED,
                                      self._ep_closed)},
            # STATE_CLOSED:
            {},
            # STATE_REJECTED:
            {}]

    def process_remote_state(self):
        """Call when remote endpoint state changes."""
        state = self._endpoint_state
        if (state & proton.Endpoint.REMOTE_ACTIVE):
            self._dispatch_event(Endpoint.REMOTE_ACTIVE)
        elif (state & proton.Endpoint.REMOTE_CLOSED):
            self._dispatch_event(Endpoint.REMOTE_CLOSED)

    def process_local_state(self):
        """Call when local endpoint state changes."""
        state = self._endpoint_state
        if (state & proton.Endpoint.LOCAL_ACTIVE):
            self._dispatch_event(Endpoint.LOCAL_ACTIVE)
        elif (state & proton.Endpoint.LOCAL_CLOSED):
            self._dispatch_event(Endpoint.LOCAL_CLOSED)

    @property
    def _endpoint_state(self):
        """Returns the current endpoint state."""
        raise NotImplementedError("Must Override")

    def _dispatch_event(self, event):
        LOG.debug("Endpoint %s event: %s",
                  self._name, Endpoint.event_names[event])
        fsm = self._fsm[self._state]
        entry = fsm.get(event)
        if entry:
            LOG.debug("Endpoint %s Old State: %s New State: %s",
                      self._name,
                      Endpoint.state_names[self._state],
                      Endpoint.state_names[entry[0]])
            self._state = entry[0]
            if entry[1]:
                entry[1]()

    # state entry actions - overridden by endpoint subclass:

    def _ep_requested(self):
        """Remote has activated a new endpoint."""
        LOG.debug("endpoint_requested - ignored")

    def _ep_active(self):
        """Both ends of the Endpoint have become active."""
        LOG.debug("endpoint_active - ignored")

    def _ep_need_close(self):
        """The remote has closed its end of the endpoint."""
        LOG.debug("endpoint_need_close - ignored")

    def _ep_closed(self):
        """Both ends of the endpoint have closed."""
        LOG.debug("endpoint_closed - ignored")
