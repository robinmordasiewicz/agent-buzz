import json
from pathlib import Path
import tempfile
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

if __name__=="__main__":
    unittest.main()
