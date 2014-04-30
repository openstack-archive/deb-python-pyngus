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

LOG = logging.getLogger(__name__)


class Endpoint(object):
    """AMQP Endpoint state machine."""

    # Endpoint States:  (note: keep in sync with _fsm below!)
    STATE_UNINIT = 0  # initial state
    STATE_PENDING = 1  # local opened, waiting for remote to open
    STATE_REQUESTED = 2  # remote opened, waiting for local to open
    STATE_CANCELLED = 3  # local closed before remote opened
    STATE_ABANDONED = 4  # remote closed before local opened
    STATE_ACTIVE = 5
    STATE_NEED_CLOSE = 6  # remote closed, waiting for local close
    STATE_CLOSING = 7  # locally closed, pending remote close
    STATE_CLOSED = 8  # terminal state
    STATE_ERROR = 9  # unexpected state transition

    state_names = ["STATE_UNINIT", "STATE_PENDING", "STATE_REQUESTED",
                   "STATE_CANCELLED", "STATE_ABANDONED", "STATE_ACTIVE",
                   "STATE_NEED_CLOSE", "STATE_CLOSING", "STATE_CLOSED",
                   "STATE_ERROR"]

    # Events - (which endpoint state has changed)
    # Endpoint state transitions are fixed to the following sequence:
    # UNINIT --> ACTIVE --> CLOSED
    LOCAL_UPDATE = 0
    REMOTE_UPDATE = 1

    event_names = ["LOCAL_UPDATE", "REMOTE_UPDATE"]

    def __init__(self, name):
        self._name = name
        self._state = Endpoint.STATE_UNINIT
        # Finite State Machine:
        # Indexed by current state, each entry is a list indexed by the event
        # received.  Contains a tuple of (next-state, action)
        self._fsm = [
            # STATE_UNINIT:
            [(Endpoint.STATE_PENDING, None),  # L(open)
             (Endpoint.STATE_REQUESTED, self._ep_requested)],  # R(open)
            # STATE_PENDING:
            [(Endpoint.STATE_CANCELLED, None),  # L(close)
             (Endpoint.STATE_ACTIVE, self._ep_active)],  # R(open)
            # STATE_REQUESTED:
            [(Endpoint.STATE_ACTIVE, self._ep_active),  # L(open)
             (Endpoint.STATE_ABANDONED, None)],  # R(close)
            # STATE_CANCELLED:
            [None,
             (Endpoint.STATE_CLOSING, None)],  # R(open)
            # STATE_ABANDONED:
            [(Endpoint.STATE_NEED_CLOSE, self._ep_need_close),  # L(open)
             (Endpoint.STATE_ERROR, self._ep_error)],
            # STATE_ACTIVE:
            [(Endpoint.STATE_CLOSING, None),  # L(close)
             (Endpoint.STATE_NEED_CLOSE, self._ep_need_close)],  # R(close)
            # STATE_NEED_CLOSE:
            [(Endpoint.STATE_CLOSED, self._ep_closed),  # L(close)
             (Endpoint.STATE_ERROR, self._ep_error)],
            # STATE_CLOSING:
            [None,
             (Endpoint.STATE_CLOSED, self._ep_closed)],  # R(close)
            # STATE_CLOSED:
            [None, None],
            # STATE_ERROR:
            [None, None],
        ]

    def process_remote_state(self):
        """Call when remote endpoint state changes."""
        self._dispatch_event(Endpoint.REMOTE_UPDATE)

    def process_local_state(self):
        """Call when local endpoint state changes."""
        self._dispatch_event(Endpoint.LOCAL_UPDATE)

    @property
    def _endpoint_state(self):
        """Returns the current endpoint state."""
        raise NotImplementedError("Must Override")

    def _dispatch_event(self, event):
        LOG.debug("Endpoint %s event: %s",
                  self._name, Endpoint.event_names[event])
        fsm = self._fsm[self._state]
        entry = fsm[event]
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

    def _ep_error(self):
        """Unanticipated/illegal state change."""
        LOG.error("Endpoint state error: %s, %s",
                  self._name, Endpoint.state_names[self._state])
