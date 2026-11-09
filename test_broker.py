#!/usr/bin/env python3
import contextlib
from pathlib import Path
import sqlite3
import socket
import tempfile
import unittest
import uuid
import broker

class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "messages.sqlite3"
        broker.initialize(self.db)
    def tearDown(self):
        self.tmp.cleanup()
    def msg(self, **extra):
        return {"v": 1, "op": "send", "id": str(uuid.uuid4()), "conversation_id": "test",
                "reply_to": None, "kind": "message", "agreement": None, "text": "hello", **extra}
    def send(self, uid=1000, **extra):
        req = self.msg(**extra)
        return req, broker.process(self.db, uid, req)
    def receive(self, uid, after=0):
        return broker.process(self.db, uid, {"v":1,"op":"receive","after":after,"limit":100})
    def test_retry_conflict_restart_and_isolation(self):
        req, accepted = self.send()
        self.assertEqual(self.receive(1000)["messages"], [])
        got = self.receive(996)
        self.assertEqual(got["messages"][0]["sender"], "robin")
        self.assertEqual(got["messages"][0]["seq"], accepted["seq"])
        # A fresh DB connection after reinitialization simulates broker reload.
        broker.initialize(self.db)
        retry = broker.process(self.db, 1000, req)
        self.assertTrue(retry["duplicate"])
        self.assertEqual(retry["seq"], accepted["seq"])
        self.assertEqual(len(self.receive(996)["messages"]), 1)
        self.assertEqual(self.receive(996, got["next_cursor"])["messages"], [])
        with self.assertRaises(broker.Invalid):
            broker.process(self.db, 996, req)
        with self.assertRaises(broker.Invalid):
            broker.process(self.db, 1000, {**req,"text":"changed"})
    def test_ack_fin_and_closure(self):
        req, _ = self.send()
        self.send(996, kind="ack", agreement="agreed", reply_to=req["id"])
        fin, _ = self.send(kind="fin", text="Agreed outcome; no open items")
        _, closed = self.send(996, kind="fin_ack", agreement="agreed", reply_to=fin["id"])
        with contextlib.closing(broker.connect(self.db)) as db:
            self.assertEqual(db.execute("SELECT state FROM conversations").fetchone()[0], "closed")
        with self.assertRaises(broker.Invalid):
            self.send(text="late substantive change")
        ack = self.receive(1000)["messages"][-1]
        retry = {"op":"send", **{k:ack[k] for k in broker.FIELDS if k not in {"sender","recipient"}}}
        self.assertEqual(broker.process(self.db,996,retry)["seq"],closed["seq"])
    def test_changes_keep_topic_open_and_stale_fin_is_rejected(self):
        fin, _ = self.send(kind="fin")
        self.send(996, kind="ack", agreement="changes_requested", reply_to=fin["id"])
        with self.assertRaises(broker.Invalid):
            self.send(996, kind="fin_ack", agreement="agreed", reply_to=fin["id"])
        self.send(text="revised proposal")
    def test_reply_validation_and_ack_loops(self):
        req, _ = self.send()
        with self.assertRaises(broker.Invalid):
            self.send(kind="ack", agreement="agreed", reply_to=req["id"])
        ack, _ = self.send(996,kind="ack",agreement="received",reply_to=req["id"])
        with self.assertRaises(broker.Invalid):
            self.send(kind="ack",agreement="received",reply_to=ack["id"])
        with self.assertRaises(broker.Invalid):
            self.send(996,kind="fin_ack",agreement="agreed",reply_to=req["id"])
        with self.assertRaises(broker.Invalid):
            self.send(996,conversation_id="other",reply_to=req["id"])
    def test_validation_limits_identity_and_durability(self):
        self.send(text="é" * 8192)
        for req in [self.msg(text="é"*8193), self.msg(sender="zeul"), self.msg(v=True),
                    self.msg(id="invalid"), self.msg(kind="fin",agreement="agreed"), self.msg(op={}),
                    {"v":1,"op":"receive","after":-1,"limit":100},
                    {"v":1,"op":"receive","after":0,"limit":True},
                    {"v":1,"op":"receive","after":0,"limit":101}]:
            with self.subTest(req=str(req)[:60]), self.assertRaises(broker.Invalid):
                broker.process(self.db,1000,req)
        with self.assertRaises(broker.Invalid):
            self.send(65534)
        with contextlib.closing(broker.connect(self.db)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 2)
    def test_frame_depth_duplicate_keys_and_quoted_brackets(self):
        def frame(data):
            left, right = socket.socketpair()
            with left, right:
                left.sendall(data)
                return broker.read_frame(right)
        with self.assertRaises(broker.Invalid):
            frame(b"[" * 10000 + b"0" + b"]" * 10000 + b"\n")
        with self.assertRaises(broker.Invalid):
            frame(b'{"v":1,"v":1}\n')
        with self.assertRaises(broker.Invalid):
            frame(b'{"v":' + b'9' * 5000 + b'}\n')
        self.assertEqual(frame(b'{"text":"[ [{ \\"quote\\" ] ] }"}\n')["text"], '[ [{ "quote" ] ] }')
    def test_participant_history_and_state_recovery(self):
        req, _ = self.send()
        self.assertEqual(self.receive(1000)["messages"], [])
        history = broker.process(self.db,1000,{"v":1,"op":"history","after":0,"limit":100,"conversation_id":None})
        self.assertEqual(history["messages"][0]["id"],req["id"])
        state = broker.process(self.db,996,{"v":1,"op":"conversation","conversation_id":"test"})
        self.assertEqual(state["conversation"]["state"],"open")
        with self.assertRaises(broker.Invalid):
            broker.process(self.db,996,{"v":1,"op":"conversation","conversation_id":"missing"})

if __name__ == "__main__":
    unittest.main(verbosity=2)
