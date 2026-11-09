import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from tools import waiting_launcher as launch


class LauncherTests(unittest.TestCase):
    def test_resolve_uses_stdin_and_rejects_other_thread_or_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name,data in [('pairing',{'endpoint':'paired','consumer_id':'consumer'}),
                              ('herdr-lease',{'lease':'opaque-test-secret'}),
                              ('config',{'thread':'intended'})]:
                path=root/(name+'.json');path.write_text(json.dumps(data));path.chmod(0o600)
            calls=[]
            def run(argv,**kwargs):
                calls.append((argv,kwargs))
                if 'resolve' in argv:
                    return subprocess.CompletedProcess(argv,0,json.dumps({'environment':{
                        'HERDR_ENV':'1','HERDR_PANE_ID':'pane'}}))
                return subprocess.CompletedProcess(argv,0,json.dumps({'result':{'pane':{
                    'agent_session':{'value':'intended'}}}}))
            with patch.dict(os.environ,{'HERDR_SOCKET_PATH':'stale','UNRELATED_SECRET':'private'}):
                env=launch.resolve(root,run=run)
            self.assertNotIn('UNRELATED_SECRET',env)
            self.assertNotIn('HERDR_SOCKET_PATH',env)
            self.assertEqual(calls[0][1]['input'],'opaque-test-secret')
            self.assertNotIn('opaque-test-secret',calls[0][0])
            (root/'config.json').write_text(json.dumps({'thread':'other'}))
            with self.assertRaises(RuntimeError):launch.resolve(root,run=run)
            def revoked(argv,**kwargs):return subprocess.CompletedProcess(argv,1,'','private error')
            with self.assertRaisesRegex(RuntimeError,'pairing unavailable'):
                launch.resolve(root,run=revoked)

    def test_private_lease_and_environment_allowlist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'pairing.json';path.write_text('{}');path.chmod(0o644)
            with self.assertRaises(RuntimeError):launch.private_json(path)
            path.unlink();path.symlink_to('/dev/null')
            with self.assertRaises(RuntimeError):launch.private_json(path)

    def test_endpoint_must_be_owned_live_socket_from_daemon_metadata(self):
        import socket
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'socket'
            def run(argv,**kwargs):
                return subprocess.CompletedProcess(argv,0,json.dumps({'status':'running','socketPath':str(path)}))
            path.write_text('not socket')
            with self.assertRaises(RuntimeError):launch.goal_endpoint({},run=run)
            path.unlink()
            with socket.socket(socket.AF_UNIX) as sock:
                sock.bind(str(path))
                self.assertEqual(launch.goal_endpoint({},run=run),str(path))

    def test_clean_goal_exit_is_not_restarted_and_fault_is_returned_to_manager(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'config.json';path.write_text(json.dumps({'goal_socket':'current'}));path.chmod(0o600)
            for exit_code in (0,75):
                child=unittest.mock.Mock(pid=1234)
                child.poll.side_effect=[None,exit_code,exit_code]
                with patch('sys.argv',['launcher','--state-dir',str(root),'--python','python',
                                       '--service-script','reviewed.py']), \
                     patch.object(launch,'resolve',return_value={}), \
                     patch.object(launch,'goal_endpoint',return_value='current'), \
                     patch.object(launch.subprocess,'Popen',return_value=child) as start, \
                     patch.object(launch.signal,'signal'), \
                     patch.object(launch.time,'monotonic',side_effect=range(0,1000,20)):
                    self.assertEqual(launch.main(),exit_code)
                    self.assertEqual(start.call_count,1)
                child.terminate.assert_not_called()

    def run_supervisor(self, root, resolve, popen, endpoint='current'):
        from contextlib import ExitStack
        with ExitStack() as stack:
            stack.enter_context(patch('sys.argv',['launcher','--state-dir',str(root),
                '--python','python','--service-script','reviewed.py']))
            stack.enter_context(patch.object(launch,'resolve',side_effect=resolve))
            stack.enter_context(patch.object(launch,'goal_endpoint',return_value=endpoint))
            start=stack.enter_context(patch.object(launch.subprocess,'Popen',side_effect=popen))
            stack.enter_context(patch.object(launch.signal,'signal'))
            stack.enter_context(patch.object(launch.time,'monotonic',side_effect=range(0,1000,20)))
            return launch.main(), start

    def test_same_endpoint_new_validated_context_restarts_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'config.json';path.write_text(json.dumps({'goal_socket':'current'}));path.chmod(0o600)
            first=unittest.mock.Mock(pid=1234)
            first.poll.return_value=None
            first.wait.return_value=0
            second=unittest.mock.Mock(pid=5678)
            second.poll.side_effect=[0,0]
            old={'HERDR_ENV':'1','HERDR_PANE_ID':'old'}
            new={'HERDR_ENV':'1','HERDR_PANE_ID':'new'}
            code,start=self.run_supervisor(root,[old,new],[first,second])
            self.assertEqual(code,0)
            self.assertEqual(start.call_count,2)
            self.assertEqual([c.kwargs['env'] for c in start.call_args_list],[old,new])
            first.terminate.assert_called_once()
            first.wait.assert_called_once_with(timeout=15)

    def test_clean_exit_precedes_failed_revalidation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'config.json';path.write_text(json.dumps({'goal_socket':'current'}));path.chmod(0o600)
            child=unittest.mock.Mock(pid=1234)
            child.poll.side_effect=[None,0,0]
            code,start=self.run_supervisor(root,[{},RuntimeError('pairing unavailable')],[child])
            self.assertEqual(code,0)
            self.assertEqual(start.call_count,1)
            child.terminate.assert_not_called()

    def test_exit_during_failed_revalidation_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'config.json';path.write_text(json.dumps({'goal_socket':'current'}));path.chmod(0o600)
            child=unittest.mock.Mock(pid=1234)
            child.poll.side_effect=[None,None,0,0]
            code,start=self.run_supervisor(root,[{},RuntimeError('pairing unavailable')],[child])
            self.assertEqual(code,0)
            self.assertEqual(start.call_count,1)
            child.terminate.assert_not_called()

    def test_exit_during_context_change_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            path=root/'config.json';path.write_text(json.dumps({'goal_socket':'current'}));path.chmod(0o600)
            child=unittest.mock.Mock(pid=1234)
            child.poll.side_effect=[None,None,0,0]
            code,start=self.run_supervisor(root,[{'HERDR_PANE_ID':'old'},
                                               {'HERDR_PANE_ID':'new'}],[child])
            self.assertEqual(code,0)
            self.assertEqual(start.call_count,1)
            child.terminate.assert_not_called()
