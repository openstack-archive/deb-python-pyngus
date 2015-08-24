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

import gc
import time

try:
    from proton import VERSION_MAJOR, VERSION_MINOR
except ImportError:
    # older proton did not export version info
    VERSION_MAJOR = 0
    VERSION_MINOR = 0

import pyngus


class Test(object):

    PROTON_VERSION = (int(VERSION_MAJOR), int(VERSION_MINOR))

    def __init__(self, name):
        self.name = name

    def configure(self, config):
        self.config = config

    def default(self, name, value, **profiles):
        default = value
        profile = self.config.defines.get("profile")
        if profile:
            default = profiles.get(profile, default)
        return self.config.defines.get(name, default)

    def setup(self):
        gc.enable()
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)

    def teardown(self):
        gc.collect()
        assert not gc.garbage, "Object leak: %s" % str(gc.garbage)

    @property
    def delay(self):
        return float(self.default("delay", "1", fast="0.1"))

    @property
    def timeout(self):
        return float(self.default("timeout", "60", fast="10"))

    @property
    def verbose(self):
        return int(self.default("verbose", 0))


class Skipped(Exception):
    """Throw to skip a test without generating a test failure."""
    skipped = True


def do_connection_io(c1, c2):
    """Transfer data between two Connection objects, return True if I/O
    occured."""
    def _do_io(src, dst):
        count = min(src.has_output, dst.needs_input)
        if count > 0:
            count = dst.process_input(src.output_data())
            if count > 0:
                src.output_written(count)
        return count

    o1 = _do_io(c1, c2)
    o2 = _do_io(c2, c1)
    return o1 > 0 or o2 > 0


def process_connections(c1, c2, timestamp=None):
    """Do I/O and protocol processing on two connected Connections until no
    further data is transferred."""
    if timestamp is None:
        timestamp = time.time()
    c1.process(timestamp)
    c2.process(timestamp)
    while do_connection_io(c1, c2):
        c1.process(timestamp)
        c2.process(timestamp)


def _validate_conn_callback(connection):
    """Callbacks must only occur when holding the Connection callback lock"""
    assert connection._callback_lock.in_callback, \
        connection._callback_lock.in_callback


def _validate_link_callback(link):
    """Callbacks must only occur when holding the Link callback lock."""
    assert link._callback_lock.in_callback, link._callback_lock.in_callback


class ConnCallback(pyngus.ConnectionEventHandler):
    """Caches the callback parameters for processing by a test."""
    class RequestArgs(object):
        def __init__(self, handle, name, source, target, props):
            self.link_handle = handle
            self.name = name
            self.requested_source = source
            self.requested_target = target
            self.properties = props

    def __init__(self):
        self.active_ct = 0
        self.failed_ct = 0
        self.failed_error = None
        self.remote_closed_ct = 0
        self.remote_closed_error = None
        self.closed_ct = 0
        self.sender_requested_ct = 0
        self.sender_requested_args = []
        self.receiver_requested_ct = 0
        self.receiver_requested_args = []

        self.sasl_step_ct = 0
        self.sasl_done_ct = 0
        self.sasl_done_outcome = None

    def connection_active(self, connection):
        _validate_conn_callback(connection)
        self.active_ct += 1

    def connection_failed(self, connection, error):
        _validate_conn_callback(connection)
        self.failed_ct += 1
        self.failed_error = error

    def connection_remote_closed(self, connection, error=None):
        _validate_conn_callback(connection)
        self.remote_closed_ct += 1
        self.remote_closed_error = error

    def connection_closed(self, connection):
        _validate_conn_callback(connection)
        self.closed_ct += 1

    def sender_requested(self, connection, link_handle,
                         name, requested_source,
                         properties):
        _validate_conn_callback(connection)
        self.sender_requested_ct += 1
        args = ConnCallback.RequestArgs(link_handle, name,
                                        requested_source, None,
                                        properties)
        self.sender_requested_args.append(args)

    def receiver_requested(self, connection, link_handle,
                           name, requested_target,
                           properties):
        _validate_conn_callback(connection)
        self.receiver_requested_ct += 1
        args = ConnCallback.RequestArgs(link_handle, name,
                                        None, requested_target,
                                        properties)
        self.receiver_requested_args.append(args)

    def sasl_step(self, connection, pn_sasl):
        _validate_conn_callback(connection)
        self.sasl_step_ct += 1

    def sasl_done(self, connection, pn_sasl, result):
        _validate_conn_callback(connection)
        self.sasl_done_ct += 1
        self.sasl_done_outcome = result


class SenderCallback(pyngus.SenderEventHandler):
    def __init__(self):
        self.active_ct = 0
        self.remote_closed_ct = 0
        self.remote_closed_error = None
        self.closed_ct = 0
        self.credit_granted_ct = 0

    def sender_active(self, sender_link):
        _validate_link_callback(sender_link)
        self.active_ct += 1

    def sender_remote_closed(self, sender_link, error=None):
        _validate_link_callback(sender_link)
        self.remote_closed_ct += 1
        self.remote_closed_error = error

    def sender_closed(self, sender_link):
        _validate_link_callback(sender_link)
        self.closed_ct += 1

    def credit_granted(self, sender_link):
        _validate_link_callback(sender_link)
        self.credit_granted_ct += 1


class DeliveryCallback(object):
    """Capture the message delivery callback for a sent message."""
    def __init__(self):
        self.link = None
        self.handle = None
        self.status = None
        self.info = None
        self.count = 0

    def __call__(self, link, handle, status, info):
        _validate_link_callback(link)
        self.link = link
        self.handle = handle
        self.status = status
        self.info = info
        self.count += 1


class ReceiverCallback(pyngus.ReceiverEventHandler):
    def __init__(self):
        self.active_ct = 0
        self.remote_closed_ct = 0
        self.remote_closed_error = None
        self.closed_ct = 0
        self.message_received_ct = 0
        self.received_messages = []

    def receiver_active(self, receiver_link):
        _validate_link_callback(receiver_link)
        _validate_conn_callback(receiver_link.connection)
        self.active_ct += 1

    def receiver_remote_closed(self, receiver_link, error=None):
        _validate_link_callback(receiver_link)
        _validate_conn_callback(receiver_link.connection)
        self.remote_closed_ct += 1
        self.remote_closed_error = error

    def receiver_closed(self, receiver_link):
        _validate_link_callback(receiver_link)
        _validate_conn_callback(receiver_link.connection)
        self.closed_ct += 1

    def message_received(self, receiver_link, message, handle):
        _validate_link_callback(receiver_link)
        _validate_conn_callback(receiver_link.connection)
        self.message_received_ct += 1
        self.received_messages.append((message, handle))
