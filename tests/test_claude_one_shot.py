import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

from director.claude_one_shot import ClaudeOneShot, ClaudeProcessError, parse_result, subscription_environment
from director.model_selection import Selection


SESSION = '12345678-1234-1234-1234-123456789012'
SELECTION = Selection('claude', 'standard', 'claude-opus-5', 'high', 'llm_classifier')


class ClaudeOneShotTests(unittest.TestCase):
    def test_subscription_environment_drops_keys_without_mutating_parent(self):
        parent = {'ANTHROPIC_API_KEY': 'synthetic-api-key', 'CLAUDE_CODE_OAUTH_TOKEN': 'synthetic-token',
                  'SLACK_BOT_TOKEN': 'synthetic-slack', 'CLAUDE_CODE_USE_VERTEX': '1',
                  'CLAUDE_CONFIG_DIR': '/synthetic/claude', 'PATH': '/usr/bin'}
        child = subscription_environment(parent)
        self.assertEqual(child, {'CLAUDE_CONFIG_DIR': '/synthetic/claude', 'PATH': '/usr/bin'})
        self.assertIn('ANTHROPIC_API_KEY', parent)

    def test_exact_resume_and_fixed_permissions_and_mcp(self):
        transport = ClaudeOneShot([sys.executable], '/tmp')
        servers = {'director-publish-reply': {'command': '/fixture/python', 'args': ['-m', 'director.agent_tools']}}
        new = transport.arguments(SELECTION, SESSION, resume=False, mcp_servers=servers)
        old = transport.arguments(SELECTION, SESSION, resume=True, mcp_servers=servers)
        self.assertEqual(new[-2:], ['--session-id', SESSION])
        self.assertEqual(old[-2:], ['--resume', SESSION])
        self.assertNotIn('--fork-session', old)
        self.assertNotIn('--fallback-model', old)
        self.assertEqual(old[old.index('--permission-mode') + 1], 'auto')
        self.assertEqual(json.loads(old[old.index('--mcp-config') + 1]), {'mcpServers': servers})

    def test_error_flag_overrides_success_subtype_and_session_mismatch_rejected(self):
        payload = dict(type='result', subtype='success', is_error=False, session_id=SESSION,
                       result='done', permission_denials=[])
        self.assertEqual(parse_result(json.dumps(payload), 0, SESSION).text, 'done')
        for change in ({'is_error': True}, {'session_id': str(uuid.uuid4())}, {'type': 'assistant'},
                       {'result': None}, {'permission_denials': 'invalid'}):
            with self.subTest(change=change), self.assertRaises(ClaudeProcessError):
                parse_result(json.dumps({**payload, **change}), 0, SESSION)
        with self.assertRaises(ClaudeProcessError):
            parse_result(json.dumps(payload), 1, SESSION)
        with self.assertRaises(ClaudeProcessError):
            parse_result('not json', 0, SESSION)

    def test_wrong_backend_and_missing_session_fail_before_launch(self):
        transport = ClaudeOneShot([sys.executable], '/tmp')
        with self.assertRaisesRegex(ValueError, 'claude_session_id_invalid'):
            transport.arguments(SELECTION, '', resume=True, mcp_servers={})
        with self.assertRaisesRegex(ValueError, 'claude_selection_invalid'):
            transport.arguments(Selection('codex','standard','gpt-5.6-terra','high','test'),
                                SESSION, resume=True, mcp_servers={})

    def test_real_child_receives_stdin_and_clean_environment(self):
        script = '''import json, os, sys
assert 'ANTHROPIC_API_KEY' not in os.environ
prompt=sys.stdin.read()
print(json.dumps(dict(type='result', subtype='success', is_error=False,
                     session_id=sys.argv[-1], result=prompt, permission_denials=[])))
'''
        with tempfile.TemporaryDirectory() as directory:
            transport = ClaudeOneShot([sys.executable, '-c', script], directory,
                                      environment={**os.environ, 'ANTHROPIC_API_KEY': 'synthetic'})
            result = transport.run('synthetic input', SELECTION, SESSION, resume=False, mcp_servers={})
            self.assertEqual(result.text, 'synthetic input')
            self.assertEqual(result.session_id, SESSION)

    def test_timeout_kills_real_child_and_does_not_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / 'pid'
            script = "import os,time; open('pid','w').write(str(os.getpid())); time.sleep(60)"
            transport = ClaudeOneShot([sys.executable, '-c', script], directory, timeout=.3)
            with self.assertRaisesRegex(ClaudeProcessError, 'claude_turn_timeout'):
                transport.run('synthetic input', SELECTION, SESSION, resume=True, mcp_servers={})
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)
