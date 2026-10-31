import json
from pathlib import Path
import tempfile
import io
import contextlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from tools import watch_github as watch

class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        (self.root/"events").mkdir()
        self.state={"repo":"owner/repo","snapshots":{},"seen":{}}
        self.snapshot={"issue:1":{"number":1,"updatedAt":"2026-01-01T00:00:00Z"}}
        self.topic={"number":1,"id":10,"body":"topic","html_url":"https://github.com/owner/repo/issues/1","user":{"login":"owner"}}
        self.comment={"id":20,"body":"proposal","html_url":"https://github.com/owner/repo/issues/1#issuecomment-20","user":{"login":"peer"}}
        self.rate={"cost":1,"remaining":4000,"resetAt":"2026-01-01T01:00:00Z"}
    def tearDown(self):
        self.tmp.cleanup()
    def test_event_before_checkpoint_and_no_duplicate_on_unchanged_snapshot(self):
        original=watch.atomic
        def guarded(path,value):
            if path.name=="collector.json":
                self.assertEqual(len(list((self.root/"events").glob("*.json"))),2)
            original(path,value)
        with patch.object(watch,"inventory",return_value=(self.snapshot,self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",return_value=[self.comment]),patch.object(watch,"atomic",side_effect=guarded):
            events,_=watch.collect("owner/repo",self.state,self.root)
            self.assertEqual(len(events),2)
            second,_=watch.collect("owner/repo",self.state,self.root)
            self.assertEqual(second,[])
        self.assertEqual(len(list((self.root/"events").glob("*.json"))),2)
    def test_partial_failure_replays_without_losing_or_overwriting_event(self):
        with patch.object(watch,"inventory",return_value=(self.snapshot,self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",side_effect=RuntimeError("API failure")):
            with self.assertRaises(RuntimeError):
                watch.collect("owner/repo",self.state,self.root)
        self.assertFalse((self.root/"collector.json").exists())
        first=next((self.root/"events").glob("*.json"))
        original=first.read_text()
        restarted={"repo":"owner/repo","snapshots":{},"seen":{}}
        with patch.object(watch,"inventory",return_value=(self.snapshot,self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",return_value=[self.comment]):
            watch.collect("owner/repo",restarted,self.root)
        self.assertEqual(first.read_text(),original)
        self.assertEqual(len(list((self.root/"events").glob("*.json"))),2)
    def test_changed_comment_gets_distinct_durable_event(self):
        with patch.object(watch,"inventory",return_value=(self.snapshot,self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",return_value=[self.comment]):
            watch.collect("owner/repo",self.state,self.root)
        changed={"issue:1":{"number":1,"updatedAt":"2026-01-01T00:01:00Z"}}
        edited={**self.comment,"body":"changes requested"}
        with patch.object(watch,"inventory",return_value=(changed,self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",return_value=[edited]):
            events,_=watch.collect("owner/repo",self.state,self.root)
        self.assertEqual([e["kind"] for e in events],["comment"])
        self.assertEqual(len(list((self.root/"events").glob("*.json"))),3)
    def test_return_to_prior_state_is_a_new_transition_even_if_processed(self):
        identities=[]
        for status in ("PENDING","SUCCESS","PENDING"):
            node={"number":3,"url":"https://github.com/owner/repo/pull/3",
                  "headRefOid":"same-head","updatedAt":"same-time",
                  "aggregate":"PENDING","individual_check":{"id":"same-check","state":status}}
            with patch.object(watch,"inventory",return_value=({"pr:3":node},self.rate)),patch.object(watch,"gh",return_value=self.topic),patch.object(watch,"pages",return_value=[]):
                events,_=watch.collect("owner/repo",self.state,self.root)
            identities.append(next(e["event_id"] for e in events if e["kind"]=="pr_state"))
            if len(identities)==1:
                (self.root/"processed.json").write_text(json.dumps(identities))
        self.assertEqual(len(set(identities)),3)
        self.assertNotIn(identities[-1],json.loads((self.root/"processed.json").read_text()))
    def test_no_poll_starts_at_expired_deadline(self):
        clock=[0.0]
        calls=[]
        def collect(*_):
            calls.append(clock[0])
            return [],self.rate
        def sleep(seconds):
            clock[0]+=seconds
        with patch("sys.argv",["watch","--repo","owner/repo","--state-dir",str(self.root),"--seconds","1","--stream"]),patch.object(watch,"collect",side_effect=collect),patch.object(watch.time,"monotonic",side_effect=lambda:clock[0]),patch.object(watch.time,"sleep",side_effect=sleep),contextlib.redirect_stdout(io.StringIO()):
            watch.main()
        self.assertEqual(calls,[0.0])
    def test_api_timeout_uses_remaining_window(self):
        with patch.object(watch,"BUDGET_DEADLINE",10.0),patch.object(watch.time,"monotonic",return_value=9.75),patch.object(watch.subprocess,"run",return_value=SimpleNamespace(returncode=0,stdout="{}",stderr="")) as run:
            watch.gh(["graphql"])
        self.assertEqual(run.call_args.kwargs["timeout"],.25)
        with patch.object(watch,"BUDGET_DEADLINE",10.0),patch.object(watch.time,"monotonic",return_value=10.0),patch.object(watch.subprocess,"run") as run:
            with self.assertRaises(watch.WindowEnded):
                watch.gh(["graphql"])
            run.assert_not_called()

if __name__=="__main__":
    unittest.main()
