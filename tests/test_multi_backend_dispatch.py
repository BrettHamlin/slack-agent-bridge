"""Cross-harness isolation for Director's shared managed-job lifecycle."""
import hashlib
import json
import queue
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from director.agent_gateway import AgentEvent, AgentGateway, AgentProfile
from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer


class MultiBackendDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        (self.project / 'state').mkdir()
        self.inbox = InboxStore(self.project / 'state/inbox.sqlite3')
        self.connection = sqlite3.connect(self.project / 'state/inbox.sqlite3')
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=self.connection,
                                  send_outgoing=lambda **_: SimpleNamespace(state='sent'))
        self.d = Dispatcher(self.project, {'database_path': 'state/inbox.sqlite3',
                            'dispatcher': {'enabled': True, 'runtime': 'acp', 'timeout_seconds': 10,
                                           'acp': {'command': ['synthetic-acp']}}}, self.inbox, service)
        self.codex = self.gateway('codex-default', 'codex-acp')
        self.claude = self.gateway('claude-default', 'claude-code')
        self.d.gateway = self.codex
        self.d._gateways = {'codex-default': self.codex, 'claude-default': self.claude}
        self.d._profiles['claude-default'] = self.claude.profile

    def tearDown(self):
        self.d.close()
        self.connection.close()
        self.inbox.close()
        self.temp.cleanup()

    @staticmethod
    def gateway(profile, backend):
        return SimpleNamespace(profile=SimpleNamespace(identifier=profile, backend=backend),
                               poll=Mock(return_value=()), cancel=Mock(), close=Mock())

    def running(self, root, profile, backend):
        message = self.inbox.ingest(InboundPointer('event-' + root, 'T', 'C', root, root)).message
        self.d.enqueue(100)
        job = self.d.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
        with self.d.db:
            self.d.db.execute("UPDATE jobs SET state='running',runtime='acp',started_at=100,"
                              "agent_profile=?,agent_session_id='same-session',agent_generation=1,"
                              "agent_turn_id='1:1' WHERE key=?", (profile, job['key']))
            self.d.db.execute('INSERT INTO agent_sessions VALUES (?,?,?,?,?,?,?)',
                              (root, profile, backend, 'same-session', None, 100, 100))
        return self.d.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()

    def test_runtime_loss_does_not_block_other_profile_with_same_generation(self):
        codex = self.running('100.1', 'codex-default', 'codex-acp')
        claude = self.running('200.1', 'claude-default', 'claude-code')
        self.claude.poll.return_value = (AgentEvent('runtime_lost', '', generation=1,
                                                   profile_id='claude-default'),)
        self.d._consume_acp_events(101)
        states = dict(self.d.db.execute('SELECT key,state FROM jobs'))
        self.assertEqual(states[codex['key']], 'running')
        self.assertEqual(states[claude['key']], 'blocked')

    def test_wrong_profile_terminal_cannot_finish_matching_root_session_and_turn(self):
        job = self.running('100.1', 'codex-default', 'codex-acp')
        self.claude.poll.return_value = (AgentEvent('terminal', job['root'], 'same-session', '1:1',
                                                   generation=1, profile_id='claude-default'),)
        with patch.object(self.d, '_finish') as finish:
            self.d._consume_acp_events(101)
        finish.assert_not_called()
        self.codex.poll.return_value = (AgentEvent('terminal', job['root'], 'same-session', '1:1',
                                                  generation=1, profile_id='codex-default'),)
        self.claude.poll.return_value = ()
        with patch.object(self.d, '_finish') as finish:
            self.d._consume_acp_events(102)
        self.assertEqual(finish.call_args.args[0]['key'], job['key'])
        self.assertEqual(finish.call_count, 1)

    def test_gateway_resolves_colliding_session_id_with_its_profile(self):
        self.running('100.1', 'codex-default', 'codex-acp')
        self.running('200.1', 'claude-default', 'claude-code')
        gateway = AgentGateway(self.d.db, project=self.project, state_directory=self.project / 'state',
                               profile=AgentProfile('claude-default', 'claude-code', ('synthetic',)),
                               config_path=str(self.project / 'config.json'))
        events = queue.Queue()
        events.put(('terminal', 'same-session', {'turn_id': '1:1'}))
        gateway.driver = SimpleNamespace(observe_liveness=lambda: None, events=events, generation=1)
        result = gateway.poll()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].root, '200.1')
        self.assertEqual(result[0].profile_id, 'claude-default')

    def test_timeout_cancels_bound_claude_gateway_only(self):
        job = self.running('200.1', 'claude-default', 'claude-code')
        with patch.object(self.d, 'reconcile_job'), patch.object(self.d, '_notify_failure'):
            self.d.tick(111)
        self.claude.cancel.assert_called_once_with(job['root'])
        self.codex.cancel.assert_not_called()

    def test_claude_progress_cannot_create_native_guardian_approval(self):
        job = self.running('200.1', 'claude-default', 'claude-code')
        authority = self.d._create_publish_authority(job)
        with self.d.db:
            self.d.db.execute("UPDATE agent_reply_authorities SET state='active',session_id='same-session',"
                              "generation=1,turn_id='1:1' WHERE authority=?", (authority,))
        payload = {'text': 'reply', 'responsibility_id': None, 'execution_fence': None}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        candidate = {'sessionId': 'same-session', 'turnId': 'native-turn', 'reviewId': 'review',
                     'fingerprint': 'f' * 64, 'authority': authority, 'payloadDigest': digest,
                     'replyDisplay': {'text': 'reply', 'conversation_title': None,
                                      'conversation_emoji': None, 'conversation_preview': None}}
        event = AgentEvent('progress', job['root'], 'same-session', detail={
            'tool': {'raw_output': {'directorApproval': candidate}}}, generation=1,
            profile_id='claude-default')
        self.assertIsNotNone(self.d._guardian_candidate(event), 'fixture must be a valid native-shaped candidate')
        self.claude.poll.return_value = (event,)
        self.d._consume_acp_events(101)
        self.assertEqual(self.d.db.execute('SELECT COUNT(*) FROM guardian_reply_approvals').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
