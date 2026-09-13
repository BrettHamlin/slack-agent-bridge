import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import io
from unittest.mock import patch, MagicMock
from director import dispatch_worker


class DispatchWorkerTests(unittest.TestCase):
    def test_denied_group_signal_falls_back_to_owned_child(self):
        child=MagicMock(pid=123)
        child.poll.return_value=None
        with patch('director.dispatch_worker.os.killpg',side_effect=PermissionError):
            dispatch_worker.signal_group(child,15)
        child.send_signal.assert_called_once_with(15)

    def test_denied_all_signals_is_bounded_and_reports_cleanup_failure(self):
        child=MagicMock(pid=123)
        child.stdout=io.StringIO('{"type":"turn.failed"}\n')
        child.poll.return_value=None
        child.send_signal.side_effect=PermissionError
        child.wait.side_effect=subprocess.TimeoutExpired('synthetic',1)
        with tempfile.TemporaryFile() as lock, patch('director.dispatch_worker.subprocess.Popen',return_value=child), patch('director.dispatch_worker.os.killpg',side_effect=PermissionError), patch('director.dispatch_worker.signal.signal'), patch('sys.stdout',new_callable=io.StringIO) as output:
            code=dispatch_worker.run(os.dup(lock.fileno()),['synthetic'])
            self.assertEqual(code,1)
            self.assertIn('worker.cleanup_failed',output.getvalue())
            self.assertTrue(all(call.kwargs.get('timeout')==1 for call in child.wait.call_args_list))

    def run_worker(self, script):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'thread.lock'
            with path.open('w') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX)
                script=script.replace('EXPECTED_INODE',str(os.fstat(lock.fileno()).st_ino))
                started=time.monotonic()
                proc=subprocess.Popen([sys.executable,'-m','director.dispatch_worker',str(lock.fileno()),sys.executable,'-c',script],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,pass_fds=(lock.fileno(),))
                lock.close()
                output,errors=proc.communicate(timeout=5)
                elapsed=time.monotonic()-started
            with path.open('a') as check:
                fcntl.flock(check,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return proc.returncode,output,elapsed

    def test_terminal_turn_releases_slot_without_waiting_for_long_cleanup(self):
        code,out,elapsed=self.run_worker('import json,time; print(json.dumps({"type":"turn.completed"}),flush=True); time.sleep(30)')
        self.assertEqual(code,0);self.assertIn('turn.completed',out);self.assertLess(elapsed,3)

    def test_failed_turn_is_not_reported_successful(self):
        code,out,elapsed=self.run_worker('import json,time; print(json.dumps({"type":"turn.failed"}),flush=True); time.sleep(30)')
        self.assertEqual(code,1);self.assertLess(elapsed,3)

    def test_lock_descriptor_is_not_inherited_by_codex(self):
        # Inspect descriptor identity in the child without assuming an unused
        # fd number: Python may open unrelated descriptors of its own.
        code,out,_=self.run_worker('import os,json; inherited=False\nfor fd in range(3,256):\n try:\n  inherited=inherited or os.fstat(fd).st_ino==EXPECTED_INODE\n except OSError:pass\nprint(json.dumps({"type":"turn.completed","inherited":inherited}),flush=True)')
        self.assertEqual(code,0)
        self.assertFalse(json.loads(out)['inherited'])
