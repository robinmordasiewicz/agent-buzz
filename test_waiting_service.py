import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from tools import waiting_service as wait

class WaitingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        (self.root/"events").mkdir()
        self.state={"notified":{}}
    def tearDown(self):
        self.tmp.cleanup()
    def test_blocked_goal_keeps_waiting_but_pause_or_completion_stops(self):
        expected=wait.digest("agreed objective")
        for status in ("active","blocked","usageLimited","budgetLimited"):
            self.assertTrue(wait.should_wait({"objective":"agreed objective","status":status},expected))
        for status in ("paused","complete"):
            self.assertFalse(wait.should_wait({"objective":"agreed objective","status":status},expected))
        self.assertFalse(wait.should_wait(None,expected))
        self.assertFalse(wait.should_wait({"objective":"different task","status":"active"},expected))
    def test_manifest_is_durable_before_notification_and_not_repeated(self):
        sent=[]
        def send(payload):
            record=json.loads(payload)
            manifest=Path(record["manifest"])
            self.assertTrue(manifest.exists())
            state=json.loads((self.root/"observer.json").read_text())
            self.assertTrue(state["pending_batch"]["attempted"])
            self.assertEqual(json.loads(manifest.read_text())["records"],{"mail:1":"private-record"})
            sent.append(payload)
        self.assertTrue(wait.notify({},self.state,self.root,{"mail:1":"private-record"},send=send))
        self.assertFalse(wait.notify({},self.state,self.root,{"mail:1":"private-record"},send=send))
        self.assertEqual(len(sent),1)
    def test_uncertain_delivery_is_reconciled_without_blind_resend(self):
        sent=[]
        def lost(payload):
            sent.append(payload)
            raise TimeoutError("uncertain")
        with self.assertRaises(TimeoutError):
            wait.notify({},self.state,self.root,{"mail:1":"record"},send=lost)
        restarted=json.loads((self.root/"observer.json").read_text())
        self.assertFalse(wait.notify({},restarted,self.root,{},send=lost,reconcile=lambda _:False))
        self.assertTrue(wait.notify({},restarted,self.root,{},send=lost,reconcile=lambda _:True))
        self.assertEqual(len(sent),1)
        self.assertIn("mail:1",restarted["notified"])
    def test_peer_filter_excludes_own_github_updates_and_processed_mail(self):
        mail=self.root/"mail";github=self.root/"github"
        (mail/"inbox").mkdir(parents=True);(github/"events").mkdir(parents=True)
        (mail/"state.json").write_text(json.dumps({"processed_message_ids":["handled"]}))
        for mid in ("handled","new"):(mail/"inbox"/(mid+".json")).write_text("{}")
        (github/"processed.json").write_text(json.dumps(["old"]))
        for mid,actor in (("own","owner"),("peer","peer"),("old","peer")):
            (github/"events"/(mid+".json")).write_text(json.dumps({"actor":actor}))
        records=wait.pending_records({"mail_dir":str(mail),"github_dir":str(github),"peer_login":"peer"},self.state,self.root)
        self.assertEqual(set(records),{"mail:new","github:peer"})
    def test_direct_observer_persists_before_cursor_and_does_not_process(self):
        class Client:
            @staticmethod
            def exchange(_,request):
                self.assertEqual(request["after"],22)
                return {"ok":True,"messages":[{"id":"message-id","seq":24,"sender":"zeul"}],"next_cursor":24}
        records=wait.pending_records_direct({"direct_socket":"socket","direct_after":22},self.state,self.root,Client)
        self.assertEqual(self.state["direct_cursor"],24)
        self.assertEqual(json.loads(Path(records["direct:message-id"]).read_text())["seq"],24)
        self.assertNotIn("processed_cursor",self.state)
        class Reconnected:
            @staticmethod
            def exchange(_,request):
                return {"ok":True,"messages":[],"next_cursor":24}
        restarted=json.loads(json.dumps(self.state))
        recovered=wait.pending_records_direct({"direct_socket":"socket"},restarted,self.root,Reconnected)
        self.assertEqual(recovered,records)
    def test_rollout_reconciliation_requires_actual_user_record(self):
        path=self.root/"rollout.jsonl"
        path.write_text(json.dumps({"payload":{"role":"assistant","text":"batch"}})+"\n")
        self.assertFalse(wait.already_recorded(path,"batch"))
        with path.open("a") as handle:
            handle.write(json.dumps({"payload":{"type":"user_message","message":"batch"}})+"\n")
        self.assertTrue(wait.already_recorded(path,"batch"))
    def test_only_expired_or_terminal_collectors_are_restarted(self):
        state={"goal_thread":"thread","expires_epoch":100,"workers":[]}
        path=self.root/"control.json";path.write_text(json.dumps(state))
        config={"thread":"thread","loop_state":str(path),"loop_command":"control","collector_seconds":2}
        with patch.object(wait.control,"report",return_value={"running":True}),patch.object(wait.time,"time",return_value=99),patch.object(wait,"call") as call:
            wait.ensure_collectors(config)
            call.assert_not_called()
        with patch.object(wait.control,"report",return_value={"running":True}),patch.object(wait.time,"time",return_value=101),patch.object(wait,"call") as call:
            wait.ensure_collectors(config)
            self.assertEqual(call.call_args.args[0][-2:],["--seconds","2"])
    def test_self_test_does_not_claim_to_be_peer_input(self):
        sent=[]
        wait.notify({},self.state,self.root,{"selftest:nonce":"diagnostic"},send=sent.append)
        self.assertEqual(json.loads(sent[0])["event"],"agent_buzz_waiter_self_test")

if __name__=="__main__":
    unittest.main()
