"""Local fixtures, not live two-account or model-wakeup certification."""
from datetime import datetime, timezone
import errno
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from tools import baseline as bench


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.eid = str(uuid.uuid4())
        self.registry = patch.object(bench, "REGISTRY", self.root / "registry")
        self.registry.start()

    def tearDown(self):
        self.registry.stop()
        self.tmp.cleanup()

    def test_payload_ids_and_explicit_sample_sets(self):
        a, b = bench.expected(self.eid, "robin"), bench.expected(self.eid, "zeul")
        self.assertEqual(len(a), 264)
        self.assertEqual(len(set(a) | set(b)), 528)
        self.assertEqual(sum(p == "measured" for p, _ in a.values()), 256)
        for mid, (phase, index) in a.items():
            self.assertEqual(bench.request(self.eid, "robin", phase, index)["id"], mid)
            self.assertEqual(len(bench.text(self.eid, "robin", phase, index).encode("utf-8")), 512)
        self.assertTrue(set(a).isdisjoint(bench.expected(str(uuid.uuid4()), "robin")))

    def test_fresh_run_registry_refuses_reuse_even_with_different_output(self):
        bench.reserve(self.root / "one", self.eid, "robin")
        with self.assertRaises(FileExistsError):
            bench.reserve(self.root / "two", self.eid, "robin")
        bench.reserve(self.root / "one", self.eid, "broker")
        with self.assertRaises(ValueError):
            bench.experiment(self.eid.upper())

    def test_manifest_requires_current_window_two_references_and_exact_source(self):
        manifest = dict(experiment_id=self.eid, start_utc="1970-01-01T00:01:50Z", core_commit=bench.PIN,
                        harness_sha256=bench.sha(bench.__file__), ack_urls={
                            "robin": "https://github.com/robinmordasiewicz/agent-buzz/issues/8#issuecomment-1",
                            "zeul": "https://github.com/robinmordasiewicz/agent-buzz/issues/8#issuecomment-2"})
        path = self.root / "window.json"
        path.write_text(json.dumps(manifest))
        self.assertEqual(bench.window(path, self.eid, now=100)[1], 110)
        for change in (dict(core_commit="wrong"), dict(harness_sha256="wrong"),
                       dict(ack_urls={"robin": "only-one"}), dict(start_utc="1970-01-01T00:01:50")):
            path.write_text(json.dumps({**manifest, **change}))
            with self.assertRaises(ValueError):
                bench.window(path, self.eid, now=100)
        path.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            bench.window(path, self.eid, now=111)

    def test_core_mismatch_rejected_before_import(self):
        with self.assertRaises(ValueError):
            (self.root / "broker.py").write_text("not baseline")
            bench.core(self.root)

    def client(self):
        return bench.core(Path(__file__).parent)

    def config(self):
        return dict(experiment=self.eid, role="robin", start=time.time() - 30,
                    deadline=time.time() + 10, socket="fixture", core_dir=str(Path(__file__).parent))

    def test_real_outbox_and_exact_retries_with_fixture_transport(self):
        client, accepted = self.client(), {}
        def transport(_, req):
            duplicate = req["id"] in accepted
            accepted.setdefault(req["id"], len(accepted) + 1)
            return dict(v=1, ok=True, id=req["id"], seq=accepted[req["id"]], duplicate=duplicate)
        with patch.object(client, "exchange", side_effect=transport):
            bench.sender(client, self.config(), self.root)
        report = json.loads((self.root / "sender-result.json").read_text())
        self.assertEqual(len(accepted), 264)
        self.assertEqual(report["send_accept_ms"]["n"], 256)
        self.assertEqual(len(report["retries"]), 8)
        self.assertTrue(all(row["duplicate"] for row in report["retries"]))

    def test_changed_retry_sequence_fails(self):
        client, seen = self.client(), set()
        def transport(_, req):
            duplicate = req["id"] in seen
            seen.add(req["id"])
            return dict(v=1, ok=True, id=req["id"], seq=99 if duplicate else 1, duplicate=duplicate)
        with patch.object(bench, "WARMUP", 1), patch.object(bench, "MEASURED", 2), patch.object(bench, "RETRIES", 1), patch.object(client, "exchange", side_effect=transport):
            with self.assertRaisesRegex(ValueError, "retry changed acceptance"):
                bench.sender(client, self.config(), self.root)

    def test_cached_before_checkpoint_replays_and_does_not_advance_other_journal(self):
        client = self.client()
        config = self.config()
        accepted = datetime.now(timezone.utc).isoformat()
        with patch.object(bench, "WARMUP", 1), patch.object(bench, "MEASURED", 66), patch.object(bench, "POLL", 0):
            rows = [dict(v=1, id=mid, seq=n, conversation_id="baseline-" + self.eid,
                         reply_to=None, kind="message", agreement=None, sender="zeul", recipient="robin",
                         text=bench.text(self.eid, "zeul", phase, index), accepted_utc=accepted)
                    for n, (mid, (phase, index)) in enumerate(bench.expected(self.eid, "zeul").items(), 1)]
            def transport(_, req):
                page = [r for r in rows if r["seq"] > req["after"]][:30]
                return dict(v=1, ok=True, messages=page, next_cursor=page[-1]["seq"] if page else req["after"])
            def fixture_checkpoint(_, journal, after):
                # Model the already-tested CLI boundary; no fake Linux UID or
                # real network operation is presented as two-account evidence.
                with sqlite3.connect(journal / "client.sqlite3") as db:
                    db.execute("INSERT OR REPLACE INTO cursors VALUES('processed',?)", (after,))
            paused = client.journal(bench.private(self.root / "paused") / "journal")
            paused.close()
            with patch.object(client, "exchange", side_effect=transport), patch.object(bench, "checkpoint", side_effect=fixture_checkpoint):
                self.assertEqual(bench.consumer(client, config, self.root, "fast"), 75)
                marker = json.loads((self.root / "fast/interrupted.json").read_text())
                self.assertEqual(marker["processed_before"], 60)
                self.assertEqual(bench.consumer(client, config, self.root, "fast"), 0)
            result = json.loads((self.root / "fast/result.json").read_text())
            self.assertEqual(set(result["ids"]), {r["id"] for r in rows})
            self.assertEqual(len(result["replayed_ids"]), 7)
            with sqlite3.connect(self.root / "paused/journal/client.sqlite3") as db:
                self.assertEqual(client.cursor(db, "processed"), 0)

    @unittest.skipUnless(sys.platform == "linux", "Real process-identity fixture requires Linux")
    def test_partial_start_cleanup_preserves_unrelated_process(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        actual_spawn, launched = bench.spawn, []
        def fixture_spawn(argv, root, name, state, handles):
            if launched:
                raise RuntimeError("fixture startup failure")
            result = actual_spawn([sys.executable, "-c", "import time; time.sleep(30)"], root, name, state, handles)
            launched.append(result[0])
            return result
        try:
            with patch.object(bench, "spawn", side_effect=fixture_spawn):
                with self.assertRaisesRegex(RuntimeError, "startup failure"):
                    bench.run_account(None, self.config(), self.root)
            self.assertIsNotNone(launched[0].poll())
            self.assertIsNone(unrelated.poll())
            self.assertFalse((self.root / "complete.json").exists())
        finally:
            unrelated.terminate()
            unrelated.wait()

    @unittest.skipUnless(sys.platform == "linux", "Kernel-handle fixture requires Linux")
    def test_identity_discovery_failure_uses_kernel_bound_cleanup(self):
        handles = []
        with patch.object(bench.control, "identity", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "Child identity unavailable"):
                bench.spawn([sys.executable, "-c", "import time; time.sleep(30)"], self.root,
                            "fixture", {"workers": []}, handles)
        self.assertIsNotNone(handles[0].poll())

    @unittest.skipUnless(hasattr(bench.os, "getpgid"), "Direct-child process-group fixture requires POSIX")
    def test_pidfd_acquisition_failure_is_bounded_and_preserves_unrelated_child(self):
        unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        actual_spawn, handles_seen = bench.spawn, []
        def fixture_spawn(argv, root, name, state, handles):
            try:
                return actual_spawn([sys.executable, "-c", "import time; time.sleep(30)"], root, name, state, handles)
            finally:
                handles_seen[:] = handles
        try:
            start = time.monotonic()
            with patch.object(bench, "spawn", side_effect=fixture_spawn), patch.object(
                    bench.os, "pidfd_open", side_effect=OSError(errno.EMFILE, "fixture descriptor exhaustion"), create=True):
                with self.assertRaises(OSError):
                    bench.run_account(None, self.config(), self.root)
            self.assertLess(time.monotonic() - start, 3)
            self.assertEqual(len(handles_seen), 1)
            self.assertIsNotNone(handles_seen[0].poll())
            self.assertIsNone(unrelated.poll())
            self.assertEqual(json.loads((self.root / "control.json").read_text())["workers"], [])
            self.assertFalse((self.root / "complete.json").exists())
        finally:
            for child in handles_seen:
                bench.stop_new_child(child)
            unrelated.terminate()
            unrelated.wait(timeout=2)

    def test_delayed_commit_is_not_labelled_as_post_commit_delivery(self):
        spec = importlib.util.spec_from_file_location("baseline_timestamp_broker", Path(__file__).parent / "broker.py")
        broker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(broker)
        path = self.root / "timestamp.sqlite3"
        broker.initialize(path)
        original, completed = broker.connect, []
        class Delayed:
            def __init__(self, db):self.db = db
            def __enter__(self):return self
            def __exit__(self, *args):
                time.sleep(.06)  # Controlled fault fixture, not a performance sample.
                result = self.db.__exit__(*args)
                completed.append(time.time())
                return result
            def __getattr__(self, key):return getattr(self.db, key)
        with patch.object(broker, "connect", side_effect=lambda p:Delayed(original(p))):
            result = broker.process(path, 1000, bench.request(self.eid, "robin", "measured", 0))
        self.assertTrue(result["ok"])
        db = sqlite3.connect(path)
        try:stamp = db.execute("SELECT accepted_utc FROM messages").fetchone()[0]
        finally:db.close()
        self.assertGreater(completed[0] - datetime.fromisoformat(stamp).timestamp(), .05)
        metrics = bench.observation_metrics([dict(accepted_utc=stamp, first_observed_utc=completed[0],
            checkpoint_utc=completed[0] + .001, cached_uncheckpointed_depth=1)],
            [dict(started_utc=completed[0], observed_utc=completed[0] + .001)])
        self.assertGreater(metrics["stored_acceptance_timestamp_to_observe_ms"]["p50"], 50)
        self.assertIsNone(metrics["post_commit_latency_ms"])
        self.assertIsNone(metrics["committed_broker_queue_depth"])
        self.assertEqual(metrics["cached_uncheckpointed_messages_max"], 1)
        self.assertNotIn("commit_to_observe_ms", metrics)

    def test_broker_start_failure_cleans_only_new_fixture_directory(self):
        shared = bench.private(self.root / "shared")
        config = {**self.config(), "socket": str(shared / ("baseline-" + self.eid) / "broker.sock")}
        sentinel = shared / "unrelated"
        sentinel.write_text("preserve")
        with patch.object(bench, "SHARED", shared), patch.object(bench, "spawn", side_effect=RuntimeError("fixture startup failure")):
            with self.assertRaisesRegex(RuntimeError, "startup failure"):
                bench.run_broker(config, self.root)
        self.assertFalse(Path(config["socket"]).parent.exists())
        self.assertEqual(sentinel.read_text(), "preserve")
        self.assertTrue((self.root / "database").is_dir())

    def test_nearest_rank_and_incomplete_summary_refusal(self):
        self.assertEqual(bench.quantiles(list(range(1, 257)))["p99"], 254)
        self.assertEqual(bench.quantiles([-1, 1])["negative_samples"], 1)
        with self.assertRaisesRegex(ValueError, "Non-finite"):
            bench.quantiles([float('nan')])
        (self.root / "config.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "incomplete"):
            bench.summarize(self.root)


if __name__ == "__main__":
    unittest.main()
