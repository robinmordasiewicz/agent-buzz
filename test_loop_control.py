import contextlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from tools import loop_control as loop

class LoopTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.thread=str(uuid.uuid4())
        self.script=self.root/"collector.py"
        self.script.write_text("import time\ntime.sleep(30)\n")
        self.state={}
    def tearDown(self):
        path=self.root/"control.json"
        if path.exists():
            loop.stop_workers(json.loads(path.read_text()))
        self.tmp.cleanup()
    def test_idempotent_start_and_exact_worker_stop(self):
        self.state=loop.start_workers(self.root,self.state,self.thread,30,{"mail":self.script,"github":self.script},grace=.05)
        first=[r["pid"] for r in self.state["workers"]]
        again=loop.start_workers(self.root,self.state,self.thread,30,{"mail":self.script,"github":self.script},grace=.05)
        self.assertEqual(first,[r["pid"] for r in again["workers"]])
        self.assertTrue(loop.report(again)["running"])
        with self.assertRaisesRegex(ValueError,"different goal"):
            loop.start_workers(self.root,again,str(uuid.uuid4()),30,{"mail":self.script,"github":self.script})
        loop.stop_workers(again)
        self.assertFalse(loop.report(again)["running"])
        self.assertTrue((self.root/"mail.log").exists())
    def test_stale_identity_never_signals_unrelated_process(self):
        with subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"],start_new_session=True) as unrelated:
            try:
                current=loop.identity(unrelated.pid)
                record={"pid":unrelated.pid,"start_ticks":current["start_ticks"],"argv":["different-collector-command"]}
                loop.stop_workers({"workers":[record]})
                self.assertIsNone(unrelated.poll())
                record["argv"]=current["argv"]
                record["start_ticks"]="invalid-start-ticks"
                loop.stop_workers({"workers":[record]})
                self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=5)
    def test_partial_startup_failure_stops_its_other_worker(self):
        bad=self.root/"bad.py"
        bad.write_text("raise SystemExit(1)\n")
        with self.assertRaisesRegex(RuntimeError,"startup failed"):
            loop.start_workers(self.root,{},self.thread,30,{"mail":self.script,"github":bad},grace=.1)
        saved=json.loads((self.root/"control.json").read_text())
        self.assertFalse(any(loop.owned(r) for r in saved["workers"]))

if __name__=="__main__":
    unittest.main()
