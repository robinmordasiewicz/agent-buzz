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
        self.assertFalse(wait.notify({},restarted,self.root,{},send=lost,reconcile=lambda _:True))
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
        batch={"id":"hash","manifest":"expected-file","records":{"mail:1":"record"}}
        config={"rollout":str(path),"thread":"expected-thread"}
        meta={"type":"session_meta","payload":{"id":"expected-thread"}}
        def user(text):return json.dumps({"type":"event_msg","payload":{"type":"user_message","message":text}})+"\n"
        path.write_text(json.dumps(meta)+"\n"+user('Investigate missing batch hash; it has not reached the intended thread.'))
        self.assertFalse(wait.already_recorded(config,batch))
        wrong={**wait.notification(batch),"manifest":"wrong-file"}
        with path.open("a") as handle:handle.write(user(json.dumps(wrong)))
        self.assertFalse(wait.already_recorded(config,batch))
        with path.open("a") as handle:
            handle.write(user(json.dumps(wait.notification(batch))))
            handle.write(user('x'*(4*1024*1024+1)))
        self.assertTrue(wait.already_recorded(config,batch))
        self.assertFalse(wait.already_recorded({**config,"thread":"other-thread"},batch))
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

    def test_github_state_failure_head_and_topic_edit_ignore_original_author(self):
        github=self.root/"github";(github/"events").mkdir(parents=True)
        config={"peer_login":"peer"}
        old={"event_id":"old","kind":"pr_state","actor":None,"value":{
            "headRefOid":"a","commits":{"nodes":[{"commit":{"statusCheckRollup":{"state":"SUCCESS"}}}]}}}
        (github/"events/old.json").write_text(json.dumps(old))
        failure={**old,"event_id":"failure","previous_event_id":"old","value":{
            "headRefOid":"b","commits":{"nodes":[{"commit":{"statusCheckRollup":{"state":"FAILURE"}}}]}}}
        self.assertTrue(wait.github_relevant(failure,config,github))
        mail=self.root/"mail";(mail/"inbox").mkdir(parents=True)
        (mail/"state.json").write_text(json.dumps({"processed_message_ids":[]}))
        (github/"events/failure.json").write_text(json.dumps(failure))
        found=wait.pending_records({**config,"mail_dir":str(mail),"github_dir":str(github)},self.state,self.root)
        self.assertIn("github:failure",found)
        self.assertFalse(wait.github_relevant(failure,{**config,"github_suppressed_events":{"failure":"already handled own update"}},github))
        topic={"event_id":"topic","kind":"topic","actor":"original-owner","value":{"title":"edited by someone else","state":"open"}}
        self.assertTrue(wait.github_relevant(topic,config,github))
        old_topic={**topic,"event_id":"old-topic","value":{**topic["value"],"updated_at":"before","comments":0}}
        (github/"events/old-topic.json").write_text(json.dumps(old_topic))
        own_comment={**topic,"previous_event_id":"old-topic","value":{**topic["value"],"updated_at":"after","comments":1}}
        self.assertFalse(wait.github_relevant(own_comment,config,github))

    def test_uncertain_batch_does_not_block_later_input_or_resend(self):
        def lost(_):raise TimeoutError("uncertain")
        with self.assertRaises(TimeoutError):wait.notify({},self.state,self.root,{"mail:1":"record1"},send=lost)
        self.assertEqual(self.state["delivery_health"],"degraded_uncertain")
        sent=[]
        self.assertTrue(wait.notify({},self.state,self.root,{"mail:1":"record1","mail:2":"record2"},send=sent.append,reconcile=lambda _:False))
        self.assertEqual(self.state["delivery_health"],"degraded_uncertain")
        self.assertEqual(self.state["uncertain_batch_count"],1)
        manifest=json.loads(Path(json.loads(sent[0])["manifest"]).read_text())
        self.assertEqual(manifest["records"],{"mail:2":"record2"})
        self.assertNotIn("mail:1",self.state["notified"])
        batch=next(iter(self.state["uncertain_batches"].values()))
        (self.root/"resolutions").mkdir()
        resolution={"thread":"thread","batch_id":batch["id"],"manifest":batch["manifest"],"records":batch["records"],"decision":"handled_by_operator","reason":"inspected and handled records"}
        path=self.root/"resolutions"/(batch["id"]+".json")
        path.write_text(json.dumps({**resolution,"thread":"wrong-thread"}))
        wait.reconcile_batches({"thread":"thread"},self.state,self.root,reconcile=lambda _:False)
        self.assertEqual(self.state["uncertain_batch_count"],1)
        path.write_text(json.dumps(resolution))
        wait.reconcile_batches({"thread":"thread"},self.state,self.root,reconcile=lambda _:False)
        self.assertEqual(self.state["delivery_health"],"ready")
        self.assertIn("mail:1",self.state["notified"])

    def test_quoted_or_other_role_notifications_do_not_reconcile(self):
        path=self.root/"rollout.jsonl"
        batch={"id":"hash","manifest":"expected","records":{"mail:1":"record"}}
        text=json.dumps(wait.notification(batch))
        events=[{"type":"session_meta","payload":{"id":"thread"}},
            {"type":"event_msg","payload":{"type":"user_message","message":'> '+text}},
            {"type":"response_item","payload":{"role":"assistant","content":[{"type":"input_text","text":text}]}},
            {"type":"response_item","payload":{"role":"user","content":[{"type":"input_text","text":text},{"type":"input_text","text":"quoted"}]}}]
        path.write_text(''.join(json.dumps(e)+'\n' for e in events))
        config={"rollout":str(path),"thread":"thread"}
        self.assertFalse(wait.already_recorded(config,batch))
        with path.open('a') as handle:handle.write(json.dumps({"type":"response_item","payload":{"role":"user","content":[{"type":"input_text","text":text}]}})+'\n')
        self.assertTrue(wait.already_recorded(config,batch))

if __name__=="__main__":
    unittest.main()
