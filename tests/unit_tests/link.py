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
from . import common
# import logging
import time

from proton import Condition
from proton import Message
from proton import symbol

import pyngus


class APITest(common.Test):

    def setup(self, conn1_props=None, conn2_props=None):
        super(APITest, self).setup()
        # logging.getLogger("pyngus").setLevel(logging.DEBUG)
        self.container1 = pyngus.Container("test-container-1")
        self.conn1_handler = common.ConnCallback()
        if conn1_props is None:
            # props = {"x-trace-protocol": True}
            conn1_props = {"x-trace-protocol": False}
        self.conn1 = self.container1.create_connection("conn1",
                                                       self.conn1_handler,
                                                       conn1_props)
        self.conn1.open()

        self.container2 = pyngus.Container("test-container-2")
        self.conn2_handler = common.ConnCallback()
        self.conn2 = self.container2.create_connection("conn2",
                                                       self.conn2_handler,
                                                       conn2_props)
        self.conn2.open()

    def process_connections(self, timestamp=None):
        common.process_connections(self.conn1, self.conn2, timestamp)

    def teardown(self):
        if self.conn1:
            self.conn1.destroy()
        if self.container1:
            self.container1.destroy()
        if self.conn2:
            self.conn2.destroy()
        if self.container2:
            self.container2.destroy()
        super(APITest, self).teardown()

    def _setup_sender_sync(self):
        """Create links, initiated by sender."""
        sl_handler = common.SenderCallback()
        sender = self.conn1.create_sender("src", "tgt", sl_handler)
        sender.user_context = sl_handler
        sender.open()
        self.process_connections()

        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        rl_handler = common.ReceiverCallback()
        receiver = self.conn2.accept_receiver(args.link_handle,
                                              event_handler=rl_handler)
        receiver.user_context = rl_handler
        receiver.open()
        self.process_connections()
        assert receiver.active and rl_handler.active_ct > 0
        assert sender.active and sl_handler.active_ct > 0
        return (sender, receiver)

    def _setup_receiver_sync(self):
        """Create links, initiated by receiver."""
        rl_handler = common.ReceiverCallback()
        receiver = self.conn2.create_receiver("tgt", "src", rl_handler)
        receiver.user_context = rl_handler
        receiver.open()
        self.process_connections()

        assert self.conn1_handler.sender_requested_ct == 1
        args = self.conn1_handler.sender_requested_args[0]
        sl_handler = common.SenderCallback()
        sender = self.conn1.accept_sender(args.link_handle,
                                          event_handler=sl_handler)
        sender.user_context = sl_handler
        sender.open()
        self.process_connections()
        assert sender.active and sl_handler.active_ct > 0
        assert receiver.active and rl_handler.active_ct > 0
        return (sender, receiver)

    def test_create_destroy(self):
        sender = self.conn1.create_sender("source-addr", "target-addr",
                                          name="my-name")
        sender.user_context = "whatever"
        assert sender.name == "my-name"
        assert sender.source_address == "source-addr"
        assert sender.target_address is None

        receiver = self.conn2.create_receiver("target-addr", "source-addr",
                                              name="other-name")
        receiver.user_context = "meh"
        assert receiver.name == "other-name"
        assert receiver.target_address == "target-addr"
        assert receiver.source_address is None
        sender.destroy()
        receiver.destroy()

    def test_send_abort(self):
        sl_handler = common.SenderCallback()
        cb = common.DeliveryCallback()
        sender = self.conn1.create_sender("saddr", "taddr",
                                          sl_handler)
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, None)
        sender.open()
        sender.close()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        rl_handler = common.ReceiverCallback()
        receiver1 = self.conn2.accept_receiver(args.link_handle,
                                               event_handler=rl_handler)
        receiver1.open()
        self.process_connections()
        assert rl_handler.remote_closed_ct
        receiver1.close()
        self.process_connections()
        assert sl_handler.closed_ct
        assert sl_handler.active_ct == 0
        assert cb.count
        assert cb.status == pyngus.SenderLink.ABORTED

    def test_receiver_abort(self):
        rl_handler = common.ReceiverCallback()
        receiver = self.conn1.create_receiver("taddr", "saddr",
                                              rl_handler)
        receiver.add_capacity(1)
        receiver.open()
        receiver.close()
        self.process_connections()
        assert self.conn2_handler.sender_requested_ct == 1
        args = self.conn2_handler.sender_requested_args[0]
        sl_handler = common.SenderCallback()
        sender = self.conn2.accept_sender(args.link_handle,
                                          event_handler=sl_handler)
        sender.open()
        self.process_connections()
        assert sl_handler.remote_closed_ct
        sender.close()
        self.process_connections()
        assert rl_handler.closed_ct
        assert rl_handler.active_ct == 0

    def test_sender_setup_sync(self):
        sender, receiver = self._setup_sender_sync()
        sl_handler = sender.user_context
        rl_handler = receiver.user_context
        sender.close()
        self.process_connections()
        assert sl_handler.closed_ct == 0
        assert rl_handler.remote_closed_ct == 1
        assert rl_handler.remote_closed_error is None
        receiver.close()
        self.process_connections()
        assert sl_handler.closed_ct == 1
        assert sl_handler.remote_closed_ct == 0
        assert rl_handler.closed_ct == 1

    def test_pipeline_close(self):
        sender, receiver = self._setup_sender_sync()
        sl_handler = sender.user_context
        rl_handler = receiver.user_context
        sender.close()
        self.conn1.close()
        self.process_connections()
        receiver.close()
        self.conn2.close()
        self.process_connections()
        assert sl_handler.closed_ct == 1
        assert rl_handler.closed_ct == 1

    def test_sender_close_cond_sync(self):
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        cond = Condition("bad", "hate you",
                         {"yo-mama": "wears army boots"})
        sender.close(cond)
        self.process_connections()
        assert rl_handler.remote_closed_ct == 1
        assert rl_handler.remote_closed_error
        r_cond = rl_handler.remote_closed_error
        assert r_cond.name == "bad"
        assert r_cond.description == "hate you"
        assert r_cond.info.get("yo-mama") == "wears army boots"

    def test_receiver_setup_sync(self):
        sender, receiver = self._setup_receiver_sync()
        sl_handler = sender.user_context
        rl_handler = receiver.user_context
        receiver.close()
        self.process_connections()
        assert sl_handler.remote_closed_ct == 1
        assert sl_handler.remote_closed_error is None
        sender.close()
        self.process_connections()
        assert rl_handler.closed_ct == 1
        assert rl_handler.remote_closed_ct == 0
        assert sl_handler.closed_ct == 1

    def test_receiver_close_cond_sync(self):
        sender, receiver = self._setup_receiver_sync()
        sl_handler = sender.user_context
        cond = Condition("meh", "blah",
                         {"dog": "cat"})
        receiver.close(cond)
        self.process_connections()
        assert sl_handler.remote_closed_ct == 1
        assert sl_handler.remote_closed_error
        r_cond = sl_handler.remote_closed_error
        assert r_cond.name == "meh"
        assert r_cond.description == "blah"
        assert r_cond.info.get("dog") == "cat"

    def test_credit_sync(self):
        sender, receiver = self._setup_sender_sync()
        sl_handler = sender.user_context
        rl_handler = receiver.user_context
        assert receiver.capacity == 0
        receiver.add_capacity(3)
        assert receiver.capacity == 3
        assert sender.credit == 0
        assert sl_handler.credit_granted_ct == 0
        self.process_connections()
        # verify credit is sent to sender:
        assert sender.credit == 3
        assert sl_handler.credit_granted_ct == 1
        receiver.add_capacity(1)
        self.process_connections()
        assert receiver.capacity == 4
        assert sender.credit == 4
        # callback only occurs when credit is no longer zero:
        assert sl_handler.credit_granted_ct == 1
        assert sender.pending == 0
        msg = Message()
        msg.body = "Hi"
        sender.send(msg)
        # none pending because credit was consumed
        assert sender.credit == 3
        assert sender.pending == 0
        self.process_connections()
        # verify receiver's capacity decreases on send:
        assert receiver.capacity == 3
        assert rl_handler.message_received_ct == 1
        assert sender.credit == 3
        assert sender.pending == 0
        while sender.credit != 0:
            sender.send(msg)
            self.process_connections()
        assert receiver.capacity == 0
        assert rl_handler.message_received_ct == 4
        # verify no msgs sent if no credit:
        sender.send(msg)
        sender.send(msg)
        self.process_connections()
        assert sender.pending == 2
        assert sl_handler.credit_granted_ct == 1
        receiver.add_capacity(1)
        self.process_connections()
        assert receiver.capacity == 0
        assert rl_handler.message_received_ct == 5
        assert sender.credit == 0
        assert sender.pending == 1
        assert sl_handler.credit_granted_ct == 1

        receiver.add_capacity(1)
        self.process_connections()
        assert sender.credit == 0
        assert sender.pending == 0
        assert sl_handler.credit_granted_ct == 1

        # verify new credit becomes available:
        receiver.add_capacity(1)
        self.process_connections()
        assert sender.credit == 1
        assert sl_handler.credit_granted_ct == 2

    def test_send_presettled(self):
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg)
        receiver.add_capacity(1)
        self.process_connections()
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        assert msg2.body == "Hi"
        receiver.message_accepted(handle)

    def test_send_accepted(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle")
        self.process_connections()
        assert rl_handler.message_received_ct == 0
        receiver.add_capacity(1)
        self.process_connections()
        assert cb.link is None  # not acknowledged yet
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        receiver.message_accepted(handle)
        self.process_connections()
        assert cb.link == sender
        assert cb.handle == "my-handle"
        assert cb.status == pyngus.SenderLink.ACCEPTED

    def test_send_released(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle")
        receiver.add_capacity(1)
        self.process_connections()
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        receiver.message_released(handle)
        self.process_connections()
        assert cb.link == sender
        assert cb.handle == "my-handle"
        assert cb.status == pyngus.SenderLink.RELEASED

    def test_send_rejected(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle")
        receiver.add_capacity(1)
        self.process_connections()
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        cond = Condition("itchy", "Needs scratching",
                         {"bath": True})
        receiver.message_rejected(handle, cond)
        self.process_connections()
        assert cb.link == sender
        assert cb.handle == "my-handle"
        assert cb.status == pyngus.SenderLink.REJECTED
        r_cond = cb.info.get("condition")
        assert r_cond and r_cond.name == "itchy"

    def test_send_modified(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle")
        receiver.add_capacity(1)
        self.process_connections()
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        annotations = {"dog": 1, "cat": False}
        receiver.message_modified(handle, False, True, annotations)
        self.process_connections()
        assert cb.link == sender
        assert cb.handle == "my-handle"
        assert cb.status == pyngus.SenderLink.MODIFIED
        assert cb.info.get("delivery-failed") is False
        assert cb.info.get("undeliverable-here") is True
        info = cb.info.get("message-annotations")
        assert info and info["dog"] == 1

    def test_send_expired_no_credit(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_receiver_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle", deadline=10)
        # receiver.add_capacity(1)
        self.process_connections(timestamp=9)
        assert rl_handler.message_received_ct == 0
        assert sender.pending == 1
        assert cb.status is None
        self.process_connections(timestamp=10)
        assert sender.pending == 0
        assert cb.status == pyngus.SenderLink.TIMED_OUT

    def test_send_expired_late_reply(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        receiver.add_capacity(1)
        self.process_connections(timestamp=1)
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle", deadline=10)
        self.process_connections(timestamp=9)
        assert rl_handler.message_received_ct == 1
        assert sender.pending == 1
        assert sender.credit == 0
        assert cb.status is None
        self.process_connections(timestamp=10)
        assert rl_handler.message_received_ct == 1
        assert sender.pending == 0
        assert cb.status == pyngus.SenderLink.TIMED_OUT
        # late reply:
        assert cb.count == 1
        msg2, handle = rl_handler.received_messages[0]
        receiver.message_accepted(handle)
        self.process_connections(timestamp=15)
        assert cb.count == 1

    def test_send_expired_no_reply(self):
        cb = common.DeliveryCallback()
        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, cb, "my-handle", deadline=10)
        self.process_connections(timestamp=1)
        assert rl_handler.message_received_ct == 0
        assert sender.pending == 1
        assert sender.credit == 0
        assert cb.count == 0
        receiver.add_capacity(1)
        self.process_connections(timestamp=2)
        assert rl_handler.message_received_ct == 1
        assert sender.pending == 1
        assert sender.credit == 0
        assert cb.count == 0
        self.process_connections(timestamp=12)
        assert sender.pending == 0
        assert cb.count == 1
        assert cb.status == pyngus.SenderLink.TIMED_OUT

    def test_send_expired_no_callback(self):
        sender, receiver = self._setup_receiver_sync()
        rl_handler = receiver.user_context
        msg = Message()
        msg.body = "Hi"
        sender.send(msg, deadline=10)
        assert sender.pending == 1
        self.process_connections(timestamp=12)
        assert rl_handler.message_received_ct == 0
        assert sender.pending == 0

    def test_send_deadline_idle(self):
        """Validate the connection's deadline processing."""

        self.teardown()
        self.setup(conn1_props={"idle-time-out": 99})

        sender1 = self.conn1.create_sender("src1", "tgt1")
        sender1.open()
        self.process_connections(timestamp=1)
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        receiver1 = self.conn2.accept_receiver(args.link_handle)
        receiver1.open()

        sender2 = self.conn1.create_sender("src2", "tgt2")
        sender2.open()
        self.process_connections(timestamp=1)
        assert self.conn2_handler.receiver_requested_ct == 2
        args = self.conn2_handler.receiver_requested_args[1]
        receiver2 = self.conn2.accept_receiver(args.link_handle)
        receiver2.open()

        self.process_connections(timestamp=1)
        assert self.conn1.deadline == 100.0

        msg = Message()
        msg.body = "Hi"
        sender1.send(msg, deadline=11)
        assert self.conn1.deadline == 11
        self.process_connections(timestamp=2)
        assert self.conn1.deadline == 11

        sender2.send(msg, deadline=7)
        assert self.conn1.deadline == 7
        self.process_connections(timestamp=7)
        assert self.conn1.deadline == 11
        self.process_connections(timestamp=11)
        assert self.conn1.deadline == 100

        # next send timeout after the idle keepalive:
        sender1.send(msg, deadline=101)
        self.process_connections(timestamp=11)
        assert self.conn1.deadline == 100

        # have remote send idle, next deadline should
        # be the pending send:
        self.process_connections(timestamp=self.conn2.deadline)
        assert self.conn1.deadline == 101

    def test_send_close_on_ack(self):
        """Verify that the sender can close itself when delivery complete."""
        class SendDoneCallback(common.DeliveryCallback):
            def __call__(self, link, handle, status, info):
                super(SendDoneCallback, self).__call__(link, handle,
                                                       status, info)
                if self.count == 1:
                    # verify that we can safely close ourself, even if there is
                    # a send that has not completed:
                    assert status == pyngus.SenderLink.ACCEPTED
                    cond = Condition("indigestion", "old sushi",
                                     {"smoked eel": "yummy"})
                    link.close(cond)
                else:
                    # the unsent message is aborted prior
                    # to invoking closed callback:
                    assert status == pyngus.SenderLink.ABORTED
                    sl_handler = link.user_context
                    assert sl_handler.closed_ct == 0

        sender, receiver = self._setup_sender_sync()
        rl_handler = receiver.user_context
        sl_handler = sender.user_context
        receiver.add_capacity(1)
        msg = Message()
        msg.body = "Hi"
        cb = SendDoneCallback()
        sender.send(msg, cb, "my-handle")
        # no credit - this one won't get sent:
        sender.send(msg, cb, "my-handle")
        self.process_connections()
        assert sender.active
        assert rl_handler.message_received_ct == 1
        msg2, handle = rl_handler.received_messages[0]
        receiver.message_accepted(handle)
        self.process_connections()
        assert not sender.active
        assert not sender.closed
        # pending messages are aborted when close completes:
        assert cb.count == 1
        assert sl_handler.closed_ct == 0
        receiver.close()
        self.process_connections()
        assert sender.closed
        assert sl_handler.closed_ct
        assert cb.count == 2
        # last callback should be abort:
        cond = cb.info.get('condition')
        assert cond
        assert cond.name == "indigestion"
        assert cb.status == pyngus.SenderLink.ABORTED

    def test_multi_frame_message(self):
        """Verify multi-frame message send/receive."""

        self.teardown()
        props = {"max-frame-size": 512}
        self.setup(conn1_props=props, conn2_props=props)

        sender1 = self.conn1.create_sender("src1", "tgt1")
        sender1.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        rl_handler = common.ReceiverCallback()
        receiver1 = self.conn2.accept_receiver(args.link_handle,
                                               event_handler=rl_handler)
        receiver1.add_capacity(1)
        receiver1.open()
        self.process_connections()

        msg = Message()
        msg.body = "Hi!" * 512  # > max frame size
        cb = common.DeliveryCallback()
        sender1.send(msg, cb)
        # manually transfer output from conn1 to conn2 in small batches,
        # forcing conn2 to process a partial delivery:
        self.conn1.process(time.time())
        while self.conn1.has_output:
            count = min(self.conn1.has_output, 512)
            part = self.conn1.output_data()[:count]
            count = self.conn2.process_input(part)
            self.conn2.process(time.time())
            self.conn1.output_written(count)
        assert rl_handler.message_received_ct == 1
        rmsg, handle = rl_handler.received_messages[0]
        assert rmsg.body == msg.body
        receiver1.message_accepted(handle)
        self.process_connections()
        assert cb.count
        assert cb.status == pyngus.SenderLink.ACCEPTED

    def test_dynamic_receiver_props(self):
        """Verify dynamic-node-properties can be requested."""
        # dynamic receive link:
        DELETE_ON_CLOSE = 0x000000000000002b
        desired_props = {symbol("lifetime-policy"): DELETE_ON_CLOSE}
        props = {"dynamic-node-properties": desired_props}
        sender = self.conn1.create_sender("saddr",
                                          target_address=None,
                                          properties=props)
        sender.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        dnp = args.properties.get('dynamic-node-properties')
        assert dnp and dnp == desired_props

        # dynamic send link:
        desired_props = {symbol("supported-dist-modes"):
                         [symbol("move"), symbol("copy")]}
        props = {"dynamic-node-properties": desired_props}
        receiver = self.conn1.create_receiver("taddr",
                                              source_address=None,
                                              properties=props)
        receiver.open()
        self.process_connections()
        assert self.conn2_handler.sender_requested_ct == 1
        args = self.conn2_handler.sender_requested_args[0]
        dnp = args.properties.get('dynamic-node-properties')
        assert dnp and dnp == desired_props

    def test_use_after_free(self):
        """Causes proton library to segfault!!!"""
        if self.PROTON_VERSION < (0, 8):
            raise common.Skipped("Skipping test - causes segfault in proton!")
        sender = self.conn1.create_sender("src1", "tgt1")
        sender.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        assert args.properties.get('snd-settle-mode') is None
        self.conn2.reject_receiver(args.link_handle)
        self.process_connections()
        sender.destroy()

        props = {"snd-settle-mode": "settled"}
        sender = self.conn1.create_sender("src1", "tgt1",
                                          properties=props)
        sender.open()
        self.process_connections()

    def test_settle_modes(self):
        """Verify the configured settlement modes are visible to the peer.
        """
        # defaults (none given)
        sender = self.conn1.create_sender("src1", "tgt1")
        sender.open()
        receiver = self.conn1.create_receiver("tgta", "srca")
        receiver.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 1
        args = self.conn2_handler.receiver_requested_args[0]
        assert args.properties.get('snd-settle-mode') is None
        assert args.properties.get('rcv-settle-mode') is None
        assert self.conn2_handler.sender_requested_ct == 1
        args = self.conn2_handler.sender_requested_args[0]
        assert args.properties.get('snd-settle-mode') is None
        assert args.properties.get('rcv-settle-mode') is None

        # settled
        props = {"snd-settle-mode": "settled",
                 "rcv-settle-mode": "second"}
        sender = self.conn1.create_sender("src2", "tgt2",
                                          properties=props)
        sender.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct == 2
        args = self.conn2_handler.receiver_requested_args[1]
        mode = args.properties.get('snd-settle-mode')
        assert mode and mode == 'settled'
        mode = args.properties.get('rcv-settle-mode')
        assert mode and mode == 'second'

        # unsettled
        props = {"snd-settle-mode": "unsettled",
                 "rcv-settle-mode": "second"}
        recver = self.conn1.create_receiver("tgt3", "src3",
                                            properties=props)
        recver.open()
        self.process_connections()
        assert self.conn2_handler.sender_requested_ct == 2
        args = self.conn2_handler.sender_requested_args[1]
        mode = args.properties.get('snd-settle-mode')
        assert mode and mode == 'unsettled'
        mode = args.properties.get('rcv-settle-mode')
        assert mode and mode == 'second'

    def test_non_reentrant_callback(self):
        class SenderBadCallback(common.SenderCallback):
            def sender_active(self, sender):
                # Illegal:
                sender.destroy()

        class ReceiverBadCallback(common.ReceiverCallback):
            def receiver_active(self, receiver):
                # Illegal:
                receiver.destroy()

        class ReceiverBadCallback2(common.ReceiverCallback):
            def receiver_active(self, receiver):
                # Illegal:
                receiver.connection.destroy()

        sc = SenderBadCallback()
        sender = self.conn1.create_sender("src1", "tgt1",
                                          event_handler=sc)
        sender.open()
        self.process_connections()
        assert self.conn2_handler.receiver_requested_ct
        args = self.conn2_handler.receiver_requested_args[-1]
        receiver = self.conn2.accept_receiver(args.link_handle)
        receiver.open()
        try:
            self.process_connections()
            assert False, "RuntimeError expected!"
        except RuntimeError:
            pass

        sender = self.conn1.create_sender("src2", "tgt2")
        sender.open()
        self.process_connections()
        args = self.conn2_handler.receiver_requested_args[-1]
        rc = ReceiverBadCallback()
        receiver = self.conn2.accept_receiver(args.link_handle,
                                              event_handler=rc)
        receiver.open()
        try:
            self.process_connections()
            assert False, "RuntimeError expected!"
        except RuntimeError:
            pass

        sender = self.conn1.create_sender("src3", "tgt3")
        sender.open()
        self.process_connections()
        args = self.conn2_handler.receiver_requested_args[-1]
        rc = ReceiverBadCallback2()
        receiver = self.conn2.accept_receiver(args.link_handle,
                                              event_handler=rc)
        receiver.open()
        try:
            self.process_connections()
            assert False, "RuntimeError expected!"
        except RuntimeError:
            pass
