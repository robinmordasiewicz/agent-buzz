#!/usr/bin/env python3
import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import uuid
import broker
import client

class ClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.server = self.root/"server"/"messages.sqlite3"
        broker.initialize(self.server)
        self.state = self.root/"client"
        self.uid = 996 if os.getuid() == 996 else 1000
        self.peer = 1000 if self.uid == 996 else 996
        # CI exercises logic as its own filesystem UID, without claiming live
        # deployment identity coverage. Both deployed account fixtures work.
        self.users = patch.dict(client.USERS,{os.getuid(): broker.USERS[self.uid]})
        self.users.start()
    def tearDown(self):
        self.users.stop()
        self.tmp.cleanup()
    def req(self, **kw):
        return {"v":1,"op":"send","id":str(uuid.uuid4()),"conversation_id":"recovery",
                "reply_to":None,"kind":"message","agreement":None,"text":"context",**kw}
    def backend(self,path,req):
        result=broker.process(self.server,self.uid,req)
        client.validate_response(req,result)
        return result
    def cli(self,*args,state=None):
        out=io.StringIO()
        argv=["client.py","--state-dir",str(state or self.state),*args]
        with patch("sys.argv",argv),patch("client.exchange",side_effect=self.backend),contextlib.redirect_stdout(out):
            code=client.main()
        return code,json.loads(out.getvalue())
    def test_journal_precedes_wire_and_lost_acceptance_retry(self):
        req=self.req()
        with contextlib.closing(client.journal(self.state)) as db:
            def lose(path,request):
                with contextlib.closing(sqlite3.connect(self.state/"client.sqlite3")) as independent:
                    stored=independent.execute("SELECT request FROM outbox").fetchone()
                    self.assertEqual(json.loads(stored[0]),request)
                broker.process(self.server,self.uid,request)
                raise ConnectionError("lost response")
            with patch("client.exchange",side_effect=lose),self.assertRaises(ConnectionError):
                client.send(db,"unused",req)
        broker.initialize(self.server)
        code,result=self.cli("retry","--id",req["id"])
        self.assertEqual(code,0)
        self.assertTrue(result["duplicate"])
        with contextlib.closing(client.journal(self.state)) as db:
            with self.assertRaises(ValueError):
                client.send(db,"unused",{**req,"text":"altered"})
    def test_sync_cache_recovery_and_independent_checkpoints(self):
        broker.process(self.server,self.uid,self.req())
        peer=broker.process(self.server,self.peer,self.req())
        code,result=self.cli("sync")
        self.assertEqual((code,result["cached"],result["processed_cursor"]),(0,2,0))
        self.assertEqual(self.cli("checkpoint","--after",str(peer["seq"]))[0],0)
        self.assertEqual(self.cli("checkpoint","--after","0")[0],2)
        self.assertEqual(self.cli("checkpoint","--after","99")[0],2)
        code,recovered=self.cli("sync",state=self.root/"independent-consumer")
        self.assertEqual((code,recovered["cached"],recovered["processed_cursor"]),(0,2,0))
        self.assertEqual(self.cli("conversation","--conversation","recovery")[1]["conversation"]["state"],"open")
    def test_server_identity_before_transmission(self):
        conn=MagicMock()
        import struct
        conn.getsockopt.return_value=struct.pack("3i",123,996,995)
        sock=MagicMock()
        sock.__enter__.return_value=conn
        with patch("client.socket.socket",return_value=sock),self.assertRaisesRegex(ValueError,"broker UID"):
            client.exchange("unused",{"v":1,"op":"receive","after":0,"limit":100})
        conn.sendall.assert_not_called()
    def test_bad_response_rejected(self):
        req={"v":1,"op":"receive","after":0,"limit":100}
        for response in [{"v":True,"ok":True},{"v":1,"ok":True,"messages":[],"next_cursor":10}]:
            with self.assertRaises(ValueError):
                client.validate_response(req,response)

if __name__=="__main__":
    unittest.main(verbosity=2)
