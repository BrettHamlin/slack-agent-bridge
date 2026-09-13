import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from director.model_selection import Selection, _run_selection_worker, select_in_process


class SelectionWorkerTests(unittest.TestCase):
    def request(self):
        return {'prompt': 'A private task', 'backend': 'claude', 'local_only': False}

    def command(self, code):
        return [sys.executable, '-c', code]

    def test_plaintext_stdin_and_strict_selection_roundtrip(self):
        code = '''import json,sys,os
r=json.load(sys.stdin)
assert r['prompt']=='A private task'
assert 'ANTHROPIC_API_KEY' not in os.environ
print(json.dumps(dict(backend=r['backend'],grade='standard',model='claude-opus-5',effort='high',cause='llm_classifier')))
'''
        with patch.dict(os.environ, {'ANTHROPIC_API_KEY': 'synthetic-fixture'}):
            result = _run_selection_worker(self.command(code), self.request(), 3)
        self.assertEqual(result.model, 'claude-opus-5')

    def test_foreign_harness_and_extra_fields_rejected(self):
        for result in [dict(backend='codex', grade='standard',model='gpt-5.6-terra',effort='high',cause='llm_classifier'),
                       dict(backend='claude',grade='standard',model='claude-opus-5',effort='high',cause='llm_classifier',extra='bad'),
                       dict(backend='claude',grade='standard',model='claude-opus-5',effort='low',cause='llm_classifier')]:
            code = 'print('+repr(json.dumps(result))+')'
            with self.assertRaisesRegex(ValueError, 'selection_output_invalid'):
                _run_selection_worker(self.command(code), self.request(), 3)

    def test_timeout_reaps_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / 'pid'
            code = 'import os,time;open('+repr(str(pidfile))+',"w").write(str(os.getpid()));time.sleep(20)'
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, 'selection_worker_interrupted'):
                _run_selection_worker(self.command(code), self.request(), 0.3)
            self.assertLess(time.monotonic()-started, 3)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)

    def test_outer_timeout_uses_separate_local_worker_same_backend(self):
        expected = Selection('claude','standard','claude-opus-5','high','local_heuristic')
        requests = []
        def run(command, request, timeout):
            requests.append(dict(request))
            if len(requests) == 1:
                raise RuntimeError('selection_worker_interrupted')
            return expected
        with patch('director.model_selection._run_selection_worker', side_effect=run):
            self.assertEqual(select_in_process('A task', 'claude'), expected)
        self.assertEqual([r['local_only'] for r in requests], [False, True])
        self.assertEqual([r['backend'] for r in requests], ['claude', 'claude'])

    def test_real_local_worker_does_not_need_auth(self):
        decision = select_in_process('xyzzy', 'codex', local_only=True, timeout=15)
        self.assertEqual(decision.grade, 'standard')
        self.assertEqual(decision.cause, 'local_heuristic')

    def test_worker_constructor_failure_runs_local_fallback(self):
        import io
        from types import SimpleNamespace
        from director import selection_worker
        from director.model_selection import ModelSelector
        calls = []
        def factory(backend, *, local_only):
            calls.append(local_only)
            if not local_only:
                raise RuntimeError('synthetic OAuth setup failure')
            return ModelSelector(backend, local_only=True)
        output = io.StringIO()
        request = json.dumps(self.request()).encode()
        with tempfile.TemporaryDirectory() as auth_directory, \
                patch.dict(os.environ, {'CHATGPT_TOKEN_DIR': auth_directory}), \
                patch('director.model_selection.ModelSelector', side_effect=factory), \
                patch('sys.stdin', SimpleNamespace(buffer=io.BytesIO(request))), \
                patch('sys.stdout', output):
            self.assertEqual(selection_worker.main(), 0)
        self.assertEqual(calls, [False, True])
        self.assertEqual(json.loads(output.getvalue())['backend'], 'claude')

    def test_held_auth_lock_times_out_to_local_without_waiting_for_holder(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'CHATGPT_TOKEN_DIR': directory}):
            code = "from director.selection_worker import _auth_lock; import time; lock=_auth_lock(False); lock.__enter__(); print('ready',flush=True); time.sleep(20)"
            holder = subprocess.Popen(self.command(code), stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), 'ready')
                started = time.monotonic()
                decision = select_in_process('xyzzy', 'codex', timeout=3, fallback_timeout=15)
                self.assertEqual(decision.cause, 'local_heuristic')
                self.assertIsNone(holder.poll())
                self.assertLess(time.monotonic() - started, 10)
            finally:
                holder.terminate()
                holder.wait(timeout=3)
                holder.stdout.close()

    def test_local_worker_does_not_create_auth_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            auth = Path(directory) / 'not-created'
            with patch.dict(os.environ, {'CHATGPT_TOKEN_DIR': str(auth)}):
                select_in_process('xyzzy', local_only=True, timeout=15)
            self.assertFalse(auth.exists())
