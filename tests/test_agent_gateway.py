import asyncio
import sqlite3
import sys
import tempfile
import time
import threading
import unittest
import os
import json
import signal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from director.agent_gateway import ACPDriver, AgentEvent, AgentGateway, AgentProfile, CapabilityError, GatewayError
from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer


SDK_PEER = Path(__file__).parent / 'fixtures' / 'acp_sdk_test_agent.py'


class AgentGatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name)
        self.state = self.project / 'state'
        self.state.mkdir()
        self.db = sqlite3.connect(self.state / 'jobs.sqlite3')
        self.db.row_factory = sqlite3.Row
        self.profile = AgentProfile('codex-default', 'sdk-test-peer', (sys.executable, '-u', str(SDK_PEER)),
                                    frozenset({'load'}))
        self.gateway = AgentGateway(self.db, project=self.project, state_directory=self.state,
                                    profile=self.profile, config_path=str(self.project / 'config/test.json'))

    def tearDown(self):
        self.gateway.close(wait=True)
        self.db.close()
        self.temp.cleanup()

    def events(self, count=1):
        deadline = time.monotonic() + 2
        found = []
        while time.monotonic() < deadline:
            found.extend(self.gateway.poll())
            if len(found) >= count:
                return found
            time.sleep(.01)
        return found

    def new_gateway(self, *, mode='normal', profile=None):
        gateway = AgentGateway(
            self.db, project=self.project, state_directory=self.state,
            profile=profile or AgentProfile(
                'codex-default', 'sdk-test-peer', (sys.executable, '-u', str(SDK_PEER)),
                frozenset({'load'}), (('ACP_TEST_MODE', mode),),
            ), config_path=str(self.project / 'config/test.json'),
        )
        return gateway

    def test_sessions_are_isolated_and_terminal_events_are_rooted(self):
        one = self.gateway.prompt('100.0', 'one')
        two = self.gateway.prompt('200.0', 'two')
        self.assertNotEqual(one.session_id, two.session_id)
        # Each real prompt generates a session-update and a terminal callback.
        events = self.events(4)
        self.assertEqual({event.root for event in events if event.kind == 'terminal'}, {'100.0', '200.0'})

    def test_binding_survives_gateway_restart_and_loads_before_reuse(self):
        binding = self.gateway.prompt('100.0', 'one')
        self.events()
        self.gateway.close(wait=True)
        self.gateway = self.new_gateway()
        resumed = self.gateway.start_or_resume('100.0')
        self.assertEqual(resumed.session_id, binding.session_id)
        self.assertEqual(resumed.profile_id, 'codex-default')

    def test_profile_change_cannot_silently_take_over_existing_session(self):
        self.gateway.prompt('100.0', 'one')
        self.events()
        other = self.new_gateway(profile=AgentProfile('other', 'other-acp', self.profile.command))
        try:
            with self.assertRaisesRegex(GatewayError, 'session_profile_binding_changed'):
                other.start_or_resume('100.0')
        finally:
            other.close(wait=True)

    def test_explicit_legacy_migration_records_original_identifier(self):
        binding = self.gateway.start_or_resume('100.0', legacy_session_id='legacy-codex-thread')
        self.assertEqual(binding.session_id, 'legacy-codex-thread')
        self.assertEqual(binding.migrated_from, 'legacy-codex-thread')
        persisted = self.gateway.binding('100.0')
        self.assertEqual(persisted.migrated_from, 'legacy-codex-thread')

    def test_permission_event_is_preserved_for_dispatcher_policy(self):
        marker = self.state / 'permission-result'
        gateway = self.new_gateway(profile=AgentProfile(
            'permission', 'sdk-test-peer', (sys.executable, '-u', str(SDK_PEER)),
            frozenset({'load'}), (('ACP_TEST_PERMISSION_RESULT_PATH', str(marker)),),
        ))
        try:
            gateway.prompt('100.0', 'fixture-permission')
            deadline = time.monotonic() + 2
            events = []
            while time.monotonic() < deadline:
                events.extend(gateway.poll())
                if marker.exists() and any(event.kind == 'terminal' for event in events):
                    break
                time.sleep(.01)
            self.assertEqual([event.kind for event in events if event.kind == 'permission'], ['permission'])
            self.assertEqual(marker.read_text(), 'selected:reject')
        finally:
            gateway.close(wait=True)

    def test_native_guardian_override_uses_only_the_adapter_owned_opaque_handle(self):
        calls = []

        class Connection:
            async def ext_method(self, method, payload):
                calls.append((method, payload))
                return {'approved': True}

        driver = ACPDriver.__new__(ACPDriver)
        driver.connection = Connection()
        driver.call = lambda coroutine: asyncio.run(coroutine)
        driver.approve_guardian_denied_action('session-native', 'review-native', 'f' * 64)
        self.assertEqual(calls, [
            ('director/approve_guardian_denied_action', {
                'sessionId': 'session-native', 'reviewId': 'review-native', 'fingerprint': 'f' * 64,
            }),
        ])

    def test_process_death_from_the_real_sdk_peer_is_an_explicit_event(self):
        self.gateway.prompt('100.0', 'die')
        events = self.events(2)
        self.assertIn('runtime_lost', [event.kind for event in events])

    def test_real_sdk_request_error_is_a_correlated_terminal_not_runtime_loss(self):
        gateway = self.new_gateway(mode='remote-request-error')
        try:
            binding = gateway.prompt('request-error-root', 'normal reply')
            deadline = time.monotonic() + 2
            events = []
            while time.monotonic() < deadline:
                events.extend(gateway.poll())
                if any(event.kind == 'terminal' for event in events):
                    break
                time.sleep(.01)
            terminal = [event for event in events if event.kind == 'terminal']
            self.assertEqual([(event.root, event.session_id, event.turn_id) for event in terminal],
                             [('request-error-root', binding.session_id, '1:1')])
            self.assertEqual(terminal[0].detail['remote_request_error'], -32001)
            self.assertTrue(gateway.driver.alive)
            self.assertNotIn('runtime_lost', [event.kind for event in events])
        finally:
            gateway.close(wait=True)

    def test_dispatcher_finishes_real_sdk_request_error_without_runtime_fence(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        original = os.environ.get('ACP_TEST_MODE')
        os.environ['ACP_TEST_MODE'] = 'remote-request-error'
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-request-error', 'T', 'C', '905.1', '905.1')).message
            dispatcher.tick(now=100)
            started = time.monotonic()
            deadline = started + 3
            while time.monotonic() < deadline:
                dispatcher.tick(now=101 + (time.monotonic() - started))
                job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                if job['state'] != 'preparing' and job['state'] != 'running':
                    break
                time.sleep(.02)
            self.assertEqual((job['state'], job['error_code']),
                             ('retry', 'worker_ended_without_verified_delivery'))
            self.assertTrue(dispatcher.gateway.driver.alive)
            self.assertFalse(dispatcher.acp_locks)
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            connection.close()
            inbox.close()

    def test_cancel_is_sent_to_the_real_sdk_peer_and_terminal_is_correlated(self):
        binding = self.gateway.prompt('100.0', 'hold')
        self.gateway.cancel('100.0')
        events = self.events(1)
        terminal = [event for event in events if event.kind == 'terminal']
        self.assertEqual([(event.root, event.session_id, event.turn_id) for event in terminal],
                         [('100.0', binding.session_id, '1:1')])

    def test_cancelling_one_real_session_keeps_another_session_usable(self):
        first = self.gateway.prompt('100.0', 'hold')
        second = self.gateway.prompt('200.0', 'normal one')
        self.gateway.cancel('100.0')
        events = self.events(4)
        self.assertEqual({event.root for event in events if event.kind == 'terminal'}, {'100.0', '200.0'})
        self.assertTrue(self.gateway.driver.alive)
        self.gateway.submit(second, 'normal two')
        events = self.events(2)
        terminal = [event for event in events if event.kind == 'terminal']
        self.assertEqual([(event.root, event.session_id) for event in terminal], [('200.0', second.session_id)])
        self.assertNotIn('runtime_lost', [event.kind for event in events])

    def test_real_sdk_failure_modes_are_durable_gateway_errors(self):
        for mode, error in [('bad-init', 'runtime_unavailable'), ('no-load', 'missing_capabilities'),
                            ('unsupported-protocol', 'unsupported_protocol_version')]:
            gateway = self.new_gateway(mode=mode)
            try:
                with self.assertRaisesRegex((GatewayError, CapabilityError), error):
                    gateway.start_or_resume('bad-' + mode)
            finally:
                gateway.close(wait=True)
        unavailable = self.new_gateway(profile=AgentProfile('missing', 'sdk-test-peer', ('/not/a/command',), frozenset({'load'})))
        try:
            with self.assertRaisesRegex(GatewayError, 'runtime_unavailable'):
                unavailable.start_or_resume('unavailable')
        finally:
            unavailable.close(wait=True)

    def test_real_sdk_load_failure_does_not_create_or_replace_a_binding(self):
        gateway = self.new_gateway(mode='load-fails')
        try:
            with self.assertRaises(GatewayError):
                gateway.start_or_resume('100.0', legacy_session_id='legacy-session')
            self.assertIsNone(gateway.binding('100.0'))
        finally:
            gateway.close(wait=True)

    def test_new_session_settings_remain_in_private_diagnostic_after_a_prompt(self):
        gateway = self.new_gateway(profile=AgentProfile(
            'settings', 'sdk-test-peer', (sys.executable, '-u', str(SDK_PEER)),
            frozenset({'load'}), (),
            (('mode', 'agent'), ('model', 'gpt-6-astra'), ('reasoning_effort', 'medium')),
        ))
        try:
            binding = gateway.prompt('settings-root', 'normal reply')
            diagnostic = json.loads((self.state / 'dispatch' / 'agent-gateway.json').read_text())
            self.assertEqual(diagnostic['session_id'], binding.session_id)
            self.assertEqual(diagnostic['metadata']['modes']['current_mode_id'], 'agent')
            options = {item['id']: item['current_value'] for item in diagnostic['metadata']['config_options']}
            self.assertEqual(options, {'model': 'gpt-6-astra', 'reasoning_effort': 'medium'})
        finally:
            gateway.close(wait=True)

    def test_runtime_environment_keeps_director_config_and_removes_slack_credentials(self):
        profile = AgentProfile('codex-default', 'codex-acp', self.profile.command,
                               runtime_environment=(('CODEX_CONFIG', '{"model":"gpt-6-astra"}'),
                                                    ('INITIAL_AGENT_MODE', 'agent')))
        gateway = AgentGateway(self.db, project=self.project, state_directory=self.state, profile=profile,
                               config_path=str(self.project / 'config/test.json'))
        try:
            old_bot, old_app = os.environ.get('SLACK_BOT_TOKEN'), os.environ.get('SLACK_APP_TOKEN')
            os.environ['SLACK_BOT_TOKEN'] = 'synthetic'; os.environ['SLACK_APP_TOKEN'] = 'synthetic'
            try:
                environment = gateway._environment()
            finally:
                if old_bot is None: os.environ.pop('SLACK_BOT_TOKEN', None)
                else: os.environ['SLACK_BOT_TOKEN'] = old_bot
                if old_app is None: os.environ.pop('SLACK_APP_TOKEN', None)
                else: os.environ['SLACK_APP_TOKEN'] = old_app
            self.assertNotIn('SLACK_BOT_TOKEN', environment)
            self.assertNotIn('SLACK_APP_TOKEN', environment)
            self.assertEqual(environment['DIRECTOR_CONFIG'], str(self.project / 'config/test.json'))
            self.assertEqual(json.loads(environment['CODEX_CONFIG'])['model'], 'gpt-6-astra')
            self.assertEqual(environment['INITIAL_AGENT_MODE'], 'agent')
        finally:
            gateway.close(wait=True)

    def test_bare_runtime_command_never_adds_relative_path_entries(self):
        gateway = AgentGateway(
            self.db, project=self.project, state_directory=self.state,
            profile=AgentProfile('bare', 'sdk-test-peer', ('node', 'adapter.js')),
            config_path=str(self.project / 'config/test.json'),
        )
        try:
            entries = gateway._environment()['PATH'].split(':')
            self.assertNotIn('', entries)
            self.assertNotIn('.', entries)
        finally:
            gateway.close(wait=True)

    def test_confirmed_empty_group_is_never_signaled_after_close(self):
        self.gateway.prompt('group-close', 'normal reply')
        driver = self.gateway.driver
        group_id = driver.process_group_id
        self.events(2)

        driver.close(wait=True)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not driver.group_empty:
            time.sleep(.01)
        self.assertTrue(driver.group_empty)
        with self.assertRaises(ProcessLookupError):
            os.killpg(group_id, 0)
        # A retained driver can be closed again during receiver cleanup.  Once
        # the original group is confirmed empty, that later cleanup must not
        # signal a recycled process-group id.
        with patch('director.agent_gateway.os.killpg') as killpg:
            driver.close(wait=True)
        killpg.assert_not_called()

    def test_retired_runtime_loss_only_fences_its_generation(self):
        old_binding = self.gateway.prompt('old-root', 'hold')
        old_driver = self.gateway.driver
        os.killpg(old_driver.process_group_id, signal.SIGKILL)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and old_driver.process.returncode is None:
            time.sleep(.01)

        new_binding = self.gateway.prompt('new-root', 'normal reply')
        new_driver = self.gateway.driver
        self.assertGreater(new_driver.generation, old_driver.generation)
        events = self.events(3)
        loss = next(event for event in events if event.kind == 'runtime_lost')
        self.assertEqual(loss.generation, old_driver.generation)
        terminal = [event for event in events if event.kind == 'terminal']
        self.assertEqual([(event.root, event.session_id, event.turn_id) for event in terminal],
                         [('new-root', new_binding.session_id, '2:1')])

        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        notices = []
        service = SimpleNamespace(
            _lock=threading.RLock(), _connection=connection,
            send_outgoing=lambda *args, **kwargs: (notices.append((args, kwargs)) or SimpleNamespace(state='sent')),
        )
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            old_message = inbox.ingest(InboundPointer('event-old', 'T', 'C', '920.1', '920.1')).message
            new_message = inbox.ingest(InboundPointer('event-new', 'T', 'C', '921.1', '921.1')).message
            dispatcher.enqueue(100)
            with dispatcher.db:
                for message, binding, driver in ((old_message, old_binding, old_driver),
                                                 (new_message, new_binding, new_driver)):
                    job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                    dispatcher.db.execute(
                        "UPDATE jobs SET state='running',runtime='acp',agent_profile='codex-default',agent_session_id=?,agent_turn_id=?,agent_generation=? WHERE key=?",
                        (binding.session_id, '1:1' if driver is old_driver else '2:1', driver.generation, job['key']),
                    )
            old_job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (old_message.id,)).fetchone()
            with connection:
                connection.execute('INSERT INTO slack_outbox VALUES (?,?,?)',
                                   ('dispatch-answer:' + old_job['key'], 'sent', '920.2'))
            inbox.mark_completed_if_revision(old_message.id, old_message.revision)
            dispatcher.gateway = SimpleNamespace(poll=lambda: (loss,), close=lambda **_kwargs: None)
            dispatcher._consume_acp_events(101)
            states = dispatcher.db.execute('SELECT root,state FROM jobs ORDER BY root').fetchall()
            self.assertEqual([(row['root'], row['state']) for row in states],
                             [('920.1', 'blocked'), ('921.1', 'running')])
            self.assertEqual(notices, [])
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_completion_receipt_during_acp_prepare_supersedes_without_a_prompt(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp', 'model': 'gpt-6-astra',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default', 'migrate_legacy_sessions': False}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-1', 'T', 'C', '100.1', '100.1')).message
            dispatcher.tick(now=100)
            job = dispatcher.db.execute('SELECT * FROM jobs').fetchone()
            self.assertEqual((job['state'], job['runtime'], job['agent_profile']), ('preparing', 'acp', 'codex-default'))
            self.assertIsNone(job['agent_turn_id'])
            self.assertFalse(dispatcher.acp_locks)
            runtime_config = dict(dispatcher.gateway.profile.runtime_environment)['CODEX_CONFIG']
            self.assertFalse(json.loads(runtime_config)['mcp_servers.1password.enabled'])
            inbox.mark_completed_if_revision(message.id, message.revision)
            with connection:
                connection.execute('INSERT INTO slack_outbox VALUES (?,?,?)', ('dispatch-answer:' + job['key'], 'sent', '101.0'))
            deadline = time.monotonic() + 2
            clock = 101.0
            while time.monotonic() < deadline:
                dispatcher.tick(now=clock)
                job = dispatcher.db.execute('SELECT * FROM jobs').fetchone()
                if job['state'] != 'preparing':
                    break
                time.sleep(.01)
                clock += .6
            # A verified receipt arriving while the SDK prepares suppresses the
            # pending prompt entirely; it is not a model-delivery completion.
            self.assertEqual((job['state'], job['agent_turn_id']), ('superseded', None))
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_dispatcher_requires_explicit_legacy_migration_and_fences_restart(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            inbox.ingest(InboundPointer('event-1', 'T', 'C', '100.1', '100.1'))
            dispatcher.enqueue(100)
            with dispatcher.db:
                dispatcher.db.execute('INSERT INTO sessions VALUES (?,?)', ('100.1', 'legacy-session'))
            self.assertFalse(dispatcher._launch(dispatcher.db.execute('SELECT * FROM jobs').fetchone(), 101))
            job = dispatcher.db.execute('SELECT * FROM jobs').fetchone()
            self.assertEqual(job['error_code'], 'legacy_session_migration_required')
            with dispatcher.db:
                dispatcher.db.execute("UPDATE jobs SET state='running',runtime='acp'")
            dispatcher.close()
            dispatcher = Dispatcher(self.project, config, inbox, service)
            job = dispatcher.db.execute('SELECT * FROM jobs').fetchone()
            self.assertEqual((job['state'], job['error_code']), ('blocked', 'acp_runtime_recovery_required'))
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_dispatcher_legacy_migration_loads_empty_metadata_as_unverified(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default', 'migrate_legacy_sessions': True}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-migrate', 'T', 'C', '910.1', '910.1')).message
            dispatcher.enqueue(100)
            with dispatcher.db:
                dispatcher.db.execute('INSERT INTO sessions VALUES (?,?)', (message.thread_ts or message.source_ts, 'legacy-session'))
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            self.assertTrue(dispatcher._launch(job, 101))
            deadline = time.monotonic() + 2
            clock = 102.0
            while time.monotonic() < deadline:
                dispatcher.tick(now=clock)
                migrated = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                if migrated['state'] == 'running' and migrated['agent_turn_id']:
                    break
                time.sleep(.02)
                clock += .6
            self.assertEqual((migrated['state'], migrated['agent_session_id']), ('running', 'legacy-session'))
            binding = dispatcher.db.execute('SELECT * FROM agent_sessions WHERE root=?', (migrated['root'],)).fetchone()
            self.assertEqual(binding['migrated_from'], 'legacy-session')
            diagnostic = json.loads((self.state / 'dispatch' / 'agent-gateway.json').read_text())
            self.assertEqual(diagnostic['metadata']['settings_verification'], 'unverified')
            self.assertEqual(set(diagnostic['metadata']['missing_settings']), {'mode', 'model', 'reasoning_effort'})
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_blocked_acp_reconciliation_requires_group_gone_before_settle_or_retry(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            delivered = inbox.ingest(InboundPointer('event-reconcile-done', 'T', 'C', '911.1', '911.1')).message
            retryable = inbox.ingest(InboundPointer('event-reconcile-retry', 'T', 'C', '912.1', '912.1')).message
            dispatcher.enqueue(100)
            delivered_job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (delivered.id,)).fetchone()
            retry_job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (retryable.id,)).fetchone()
            with dispatcher.db, connection:
                for job in (delivered_job, retry_job):
                    dispatcher.db.execute("UPDATE jobs SET state='blocked',runtime='acp',agent_session_id=?,agent_group_id=? WHERE key=?",
                                          ('session-' + job['key'], 1234, job['key']))
                    dispatcher.db.execute("INSERT INTO agent_sessions(root,profile_id,backend,session_id,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                                          (job['root'], 'codex-default', 'codex-acp', 'session-' + job['key'], 1, 1))
                inbox.mark_completed_if_revision(delivered.id, delivered.revision)
                connection.execute('INSERT INTO slack_outbox VALUES (?,?,?)', ('dispatch-answer:' + delivered_job['key'], 'sent', '100.2'))
            with patch.object(Dispatcher, '_runtime_group_gone', return_value=False):
                self.assertEqual(dispatcher.reconcile_job(delivered_job['key'])['outcome'], 'delivered_but_runtime_active')
                self.assertEqual(dispatcher.reconcile_job(retry_job['key'], retry_if_stopped=True)['outcome'], 'unresolved')
            with patch.object(Dispatcher, '_runtime_group_gone', return_value=True):
                self.assertEqual(dispatcher.reconcile_job(delivered_job['key'])['outcome'], 'done')
                self.assertEqual(dispatcher.reconcile_job(retry_job['key'], retry_if_stopped=True)['outcome'], 'retry_ready')
            states = dispatcher.db.execute('SELECT key,state,error_code FROM jobs ORDER BY key').fetchall()
            self.assertEqual([(row['key'], row['state'], row['error_code']) for row in states],
                             [(delivered_job['key'], 'done', None), (retry_job['key'], 'retry', 'acp_reconcile_retry_ready')])
            self.assertEqual(dispatcher.db.execute('SELECT count(*) FROM agent_sessions').fetchone()[0], 2)
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_manual_acp_retry_resets_notice_and_reports_second_attempt(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        notices = []
        service = SimpleNamespace(
            _lock=threading.RLock(), _connection=connection,
            send_outgoing=lambda *args, **kwargs: (notices.append((args, kwargs)) or SimpleNamespace(state='sent')),
        )
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-repeat-failure', 'T', 'C', '913.1', '913.1')).message
            dispatcher.enqueue(100)
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            with dispatcher.db:
                dispatcher.db.execute(
                    "UPDATE jobs SET state='blocked',runtime='acp',attempts=1,error_code='acp_timeout_uncertain', "
                    "agent_turn_ended_at=? WHERE key=?", (100, job['key']),
                )
            blocked = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            dispatcher._notify_failure(blocked, 101)
            self.assertEqual(notices[0][1]['idempotency_key'], 'dispatch-failure:' + job['key'] + ':a1')

            self.assertEqual(dispatcher.reconcile_job(job['key'], retry_if_stopped=True)['outcome'], 'retry_ready')
            retried = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertEqual((retried['state'], retried['notice_at'], retried['notice_retry_at']), ('retry', None, 0))
            with dispatcher.db:
                dispatcher.db.execute("UPDATE jobs SET state='running',attempts=2 WHERE key=?", (job['key'],))
            dispatcher._finish(dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone(), 102)
            failed = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertEqual((failed['state'], failed['error_code']), ('failed', 'worker_ended_without_verified_delivery'))
            self.assertEqual([notice[1]['idempotency_key'] for notice in notices], [
                'dispatch-failure:' + job['key'] + ':a1',
                'dispatch-failure:' + job['key'] + ':a2',
            ])
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_legacy_configuration_fences_persisted_acp_work_and_never_reuses_an_acp_binding(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        legacy = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'legacy', 'codex_path': 'missing-codex'}}
        dispatcher = Dispatcher(self.project, legacy, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-legacy', 'T', 'C', '300.1', '300.1')).message
            dispatcher.enqueue(100)
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            with dispatcher.db:
                dispatcher.db.execute("UPDATE jobs SET state='running',runtime='acp',agent_session_id='old-session' WHERE key=?", (job['key'],))
            dispatcher.close()
            dispatcher = Dispatcher(self.project, legacy, inbox, service)
            fenced = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertEqual((fenced['state'], fenced['error_code']), ('blocked', 'acp_runtime_recovery_required'))

            # A rollback to the legacy CLI may not submit into an ACP-bound root.
            second = inbox.ingest(InboundPointer('event-legacy-2', 'T', 'C', '400.1', '400.1')).message
            dispatcher.enqueue(102)
            candidate = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (second.id,)).fetchone()
            with dispatcher.db:
                dispatcher.db.execute(
                    "INSERT INTO agent_sessions(root,profile_id,backend,session_id,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                    (candidate['root'], 'codex-default', 'codex-acp', 'acp-session', 1, 1),
                )
            self.assertFalse(dispatcher._launch(candidate, 103))
            candidate = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (candidate['key'],)).fetchone()
            self.assertEqual(candidate['error_code'], 'acp_session_rollback_required')

            # The legacy launcher fenced this row before it started a process
            # or submitted a prompt. Restoring ACP can therefore retry the
            # same durable job/binding without requiring a dead process group.
            stable_key = candidate['key']
            dispatcher.close()
            acp = {**legacy, 'dispatcher': {
                'enabled': True, 'runtime': 'acp',
                'acp': {'command': [sys.executable, '-u', str(SDK_PEER)], 'profile': 'codex-default'},
            }}
            dispatcher = Dispatcher(self.project, acp, inbox, service)
            dispatcher.tick(now=104)
            recovery_started = time.monotonic()
            deadline = recovery_started + 3
            while time.monotonic() < deadline:
                dispatcher.tick(now=104 + (time.monotonic() - recovery_started))
                restored = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (stable_key,)).fetchone()
                if restored['state'] == 'running' and restored['agent_turn_id']:
                    break
                time.sleep(.02)
            self.assertEqual((restored['key'], restored['state'], restored['agent_session_id']),
                             (stable_key, 'running', 'acp-session'))
            self.assertEqual(restored['error_code'], None)
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_stalled_sdk_prepare_does_not_block_dispatch_or_take_a_running_root_lock(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp', 'max_workers': 1,
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default'}}}
        original = os.environ.get('ACP_TEST_MODE')
        os.environ['ACP_TEST_MODE'] = 'stall-init'
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            first = inbox.ingest(InboundPointer('event-stall', 'T', 'C', '500.1', '500.1')).message
            started = time.monotonic()
            dispatcher.tick(now=100)
            self.assertLess(time.monotonic() - started, .25)
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (first.id,)).fetchone()
            self.assertEqual(job['state'], 'preparing')
            self.assertIsNone(job['agent_turn_id'])
            self.assertFalse(dispatcher.acp_locks)

            # A different root remains serviceable while the first peer is still
            # initializing; it must at least enter the durable queue promptly.
            second = inbox.ingest(InboundPointer('event-stall-2', 'T', 'C', '600.1', '600.1')).message
            started = time.monotonic()
            dispatcher.tick(now=101)
            self.assertLess(time.monotonic() - started, .25)
            queued = dispatcher.db.execute('SELECT state FROM jobs WHERE message_id=?', (second.id,)).fetchone()
            self.assertIn(queued['state'], ('pending', 'preparing'))
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            connection.close()
            inbox.close()

    def test_source_revision_change_during_sdk_prepare_supersedes_before_any_prompt(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default'}}}
        original = os.environ.get('ACP_TEST_MODE')
        os.environ['ACP_TEST_MODE'] = 'stall-init'
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-race-1', 'T', 'C', '700.1', '700.1')).message
            dispatcher.tick(now=100)
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            self.assertEqual(job['state'], 'preparing')

            # A newer event for the same source advances the revision while the
            # SDK future remains unresolved.  The old source must never prompt.
            updated = inbox.ingest(InboundPointer('event-race-2', 'T', 'C', '700.1', '700.2')).message
            self.assertEqual(updated.revision, message.revision + 1)
            os.environ['ACP_TEST_MODE'] = 'normal'
            deadline = time.monotonic() + 4
            clock = 101.0
            while time.monotonic() < deadline:
                dispatcher.tick(now=clock)
                old = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                if old['state'] != 'preparing':
                    break
                time.sleep(.05)
                clock += .6
            self.assertEqual((old['state'], old['agent_turn_id']), ('superseded', None))
            # The successor revision is eligible to prepare or run immediately.
            # Its root lock is valid; only the superseded revision must never
            # acquire a turn lock or submit a prompt.
            successor = dispatcher.db.execute(
                'SELECT * FROM jobs WHERE message_id=? AND revision=?',
                (message.id, updated.revision),
            ).fetchone()
            self.assertIsNotNone(successor)
            self.assertNotEqual(successor['key'], job['key'])
            self.assertNotIn(job['key'], dispatcher.acp_locks)
            if successor['state'] == 'running':
                self.assertIn(successor['key'], dispatcher.acp_locks)
            else:
                self.assertNotIn(successor['key'], dispatcher.acp_locks)
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            connection.close()
            inbox.close()

    def test_never_answering_sdk_prepare_is_bounded_retriable_and_cleans_up(self):
        """Initialize, new, and load cannot occupy a receiver worker forever."""
        original = os.environ.get('ACP_TEST_MODE')
        try:
            for number, (mode, legacy) in enumerate((('never-init', False), ('never-new', False), ('never-load', True)), start=1):
                with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                    project = Path(directory)
                    state = project / 'state'
                    state.mkdir()
                    inbox = InboxStore(state / 'inbox.sqlite3')
                    connection = sqlite3.connect(state / 'inbox.sqlite3')
                    connection.row_factory = sqlite3.Row
                    connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
                    service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                              send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
                    config = {
                        'database_path': 'state/inbox.sqlite3',
                        '_config_path': str(project / 'config/test.json'),
                        'dispatcher': {
                            'enabled': True, 'runtime': 'acp', 'max_workers': 1,
                            'acp': {
                                'command': [sys.executable, '-u', str(SDK_PEER)],
                                'profile': 'codex-default', 'migrate_legacy_sessions': legacy,
                                'startup_timeout_seconds': .05, 'request_timeout_seconds': .05,
                                'prepare_timeout_seconds': .1,
                            },
                        },
                    }
                    os.environ['ACP_TEST_MODE'] = mode
                    dispatcher = Dispatcher(project, config, inbox, service)
                    try:
                        source_ts = f'80{number}.1'
                        message = inbox.ingest(InboundPointer('event-' + mode, 'T', 'C', source_ts, source_ts)).message
                        dispatcher.enqueue(100)
                        if legacy:
                            with dispatcher.db:
                                dispatcher.db.execute('INSERT INTO sessions VALUES (?,?)', (message.thread_ts or message.source_ts, 'legacy-session'))
                        started = time.monotonic()
                        dispatcher.tick(now=100)
                        self.assertLess(time.monotonic() - started, .25)
                        job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                        self.assertEqual((job['state'], job['attempts'], job['agent_turn_id']), ('preparing', 1, None))
                        driver = dispatcher.gateway.driver
                        deadline = time.monotonic() + 1
                        while driver.process is None and time.monotonic() < deadline:
                            time.sleep(.01)
                        self.assertIsNotNone(driver.process)
                        pid = driver.process.pid

                        started = time.monotonic()
                        dispatcher.tick(now=101)
                        self.assertLess(time.monotonic() - started, .25)
                        job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                        self.assertEqual((job['state'], job['attempts'], job['error_code'], job['agent_turn_id']),
                                         ('retry', 1, 'acp_prepare_timeout', None))
                        self.assertFalse(dispatcher.preparations)
                        self.assertFalse(dispatcher.acp_locks)

                        deadline = time.monotonic() + 3
                        while driver.thread.is_alive() and time.monotonic() < deadline:
                            time.sleep(.02)
                        self.assertFalse(driver.thread.is_alive(), 'timed-out SDK loop leaked')
                        with self.assertRaises(ProcessLookupError):
                            os.killpg(pid, 0)
                    finally:
                        dispatcher.close()
                        connection.close()
                        inbox.close()
        finally:
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original

    def test_failed_prepare_future_recovers_on_a_fresh_healthy_runtime(self):
        """A startup-timeout failure cannot poison later work for the profile."""
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {
            'database_path': 'state/inbox.sqlite3',
            '_config_path': str(self.project / 'config/test.json'),
            'dispatcher': {
                'enabled': True, 'runtime': 'acp', 'max_workers': 1,
                'acp': {
                    'command': [sys.executable, '-u', str(SDK_PEER)], 'profile': 'codex-default',
                    'startup_timeout_seconds': 1, 'request_timeout_seconds': 1,
                    # This is intentionally much longer than startup so
                    # `_advance_preparations` handles the failed future first.
                    'prepare_timeout_seconds': 10,
                },
            },
        }
        original = os.environ.get('ACP_TEST_MODE')
        os.environ['ACP_TEST_MODE'] = 'never-init'
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-recover', 'T', 'C', '901.1', '901.1')).message
            dispatcher.tick(now=100)
            old_driver = dispatcher.gateway.driver
            deadline = time.monotonic() + 1
            while old_driver.process is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertIsNotNone(old_driver.process)
            old_pid = old_driver.process.pid
            preparation = next(iter(dispatcher.preparations.values()))
            deadline = time.monotonic() + 3
            while not preparation.future.done() and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(preparation.future.done(), 'never-init prepare did not settle')
            dispatcher.tick(now=101)
            failed = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            self.assertEqual((failed['state'], failed['attempts']), ('retry', 1))
            self.assertEqual(failed['error_code'], 'acp_prepare_failed:GatewayError')
            self.assertFalse(dispatcher.preparations)

            os.environ['ACP_TEST_MODE'] = 'normal'
            dispatcher.tick(now=132)
            recovery_started = time.monotonic()
            deadline = recovery_started + 3
            while time.monotonic() < deadline:
                # Keep durable time in step with wall time. Advancing it by
                # .6 every 20ms falsely expires this healthy prepare before
                # the real SDK fixture can finish its startup handshake.
                dispatcher.tick(now=132 + (time.monotonic() - recovery_started))
                recovered = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                if recovered['state'] == 'running' and recovered['agent_turn_id']:
                    break
                time.sleep(.02)
            self.assertEqual((recovered['state'], recovered['runtime'], recovered['attempts']),
                             ('running', 'acp', 2),
                             f"recovery did not complete: state={recovered['state']} error={recovered['error_code']}")
            self.assertIsNotNone(recovered['agent_session_id'])
            self.assertIsNotNone(recovered['agent_turn_id'])
            self.assertNotEqual(dispatcher.gateway.driver.process.pid, old_pid)
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            connection.close()
            inbox.close()

    def test_actual_terminal_without_delivery_remains_retryable_and_late_duplicate_is_fenced(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default'}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-terminal', 'T', 'C', '902.1', '902.1')).message
            dispatcher.tick(now=100)
            deadline = time.monotonic() + 2
            clock = 101.0
            while time.monotonic() < deadline:
                dispatcher.tick(now=clock)
                job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
                if job['state'] == 'running' and job['agent_turn_id']:
                    break
                time.sleep(.02)
                clock += .6
            self.assertEqual(job['state'], 'running')
            session_id, turn_id = job['agent_session_id'], job['agent_turn_id']
            # The terminal callback came from the real SDK peer, but neither
            # required delivery proof exists, so completion is retryable.
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                clock += .6
                dispatcher.tick(now=clock)
                retry = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                if retry['state'] != 'running':
                    break
                time.sleep(.02)
            self.assertEqual((retry['state'], retry['error_code']), ('retry', 'worker_ended_without_verified_delivery'))
            # Model a duplicated late terminal from the transport; its durable
            # attempt no longer matches a running job and must do nothing.
            dispatcher.gateway.driver.events.put(('terminal', session_id, {'turn_id': turn_id}))
            dispatcher.tick(now=clock + .6)
            fenced = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertEqual((fenced['state'], fenced['attempts'], fenced['error_code']),
                             ('retry', 1, 'worker_ended_without_verified_delivery'))
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_process_death_blocks_every_running_real_sdk_session(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp', 'max_workers': 2,
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)],
                                         'profile': 'codex-default'}}}
        original = os.environ.get('ACP_TEST_MODE')
        os.environ['ACP_TEST_MODE'] = 'hold-prompts'
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            first = inbox.ingest(InboundPointer('event-death-1', 'T', 'C', '903.1', '903.1')).message
            second = inbox.ingest(InboundPointer('event-death-2', 'T', 'C', '904.1', '904.1')).message
            dispatcher.tick(now=100)
            deadline = time.monotonic() + 2
            clock = 101.0
            while time.monotonic() < deadline:
                dispatcher.tick(now=clock)
                jobs = dispatcher.db.execute('SELECT * FROM jobs ORDER BY message_id').fetchall()
                if len(jobs) == 2 and all(job['state'] == 'running' and job['agent_turn_id'] for job in jobs):
                    break
                time.sleep(.02)
                clock += .6
            self.assertEqual(len(jobs), 2)
            self.assertTrue(all(job['state'] == 'running' for job in jobs))
            self.assertNotEqual(jobs[0]['agent_session_id'], jobs[1]['agent_session_id'])
            os.killpg(dispatcher.gateway.driver.process.pid, signal.SIGKILL)
            deadline = time.monotonic() + 1
            while dispatcher.gateway.driver.process.returncode is None and time.monotonic() < deadline:
                time.sleep(.02)
            dispatcher.tick(now=clock + .6)
            blocked = dispatcher.db.execute('SELECT state,error_code FROM jobs ORDER BY message_id').fetchall()
            # Neither source has a completion receipt/outbox proof.  A shared
            # runtime loss must fence both unresolved turns; it may not turn
            # either one into a replayable retry merely because a stale SDK
            # terminal/error notification arrived first.
            self.assertEqual([row['state'] for row in blocked], ['blocked', 'blocked'])
            self.assertTrue(all(row['error_code'].startswith('acp_') for row in blocked))
            self.assertFalse(dispatcher.acp_locks)
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            connection.close()
            inbox.close()

    def test_timeout_requires_real_terminal_before_reconcile_while_peer_stays_healthy(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp', 'max_workers': 2, 'timeout_seconds': 20,
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        original = os.environ.get('ACP_TEST_MODE')
        received = self.state / 'cancel-received'
        release = self.state / 'cancel-release'
        original_markers = {
            name: os.environ.get(name)
            for name in ('ACP_TEST_CANCEL_RECEIVED_PATH', 'ACP_TEST_CANCEL_RELEASE_PATH')
        }
        os.environ['ACP_TEST_MODE'] = 'delay-cancel'
        os.environ['ACP_TEST_CANCEL_RECEIVED_PATH'] = str(received)
        os.environ['ACP_TEST_CANCEL_RELEASE_PATH'] = str(release)
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            first = inbox.ingest(InboundPointer('event-timeout-1', 'T', 'C', '906.1', '906.1')).message
            second = inbox.ingest(InboundPointer('event-timeout-2', 'T', 'C', '907.1', '907.1')).message
            dispatcher.tick(now=100)
            started = time.monotonic()
            deadline = started + 3
            while time.monotonic() < deadline:
                dispatcher.tick(now=101 + (time.monotonic() - started))
                jobs = dispatcher.db.execute('SELECT * FROM jobs ORDER BY message_id').fetchall()
                if len(jobs) == 2 and all(job['state'] == 'running' and job['agent_turn_id'] for job in jobs):
                    break
                time.sleep(.02)
            self.assertEqual([job['state'] for job in jobs], ['running', 'running'])
            first_job, second_job = jobs
            with dispatcher.db:
                dispatcher.db.execute('UPDATE jobs SET started_at=? WHERE key=?', (100, first_job['key']))
                dispatcher.db.execute('UPDATE jobs SET started_at=? WHERE key=?', (200, second_job['key']))

            dispatcher.tick(now=200)
            timed_out = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (first_job['key'],)).fetchone()
            healthy = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (second_job['key'],)).fetchone()
            self.assertEqual((timed_out['state'], timed_out['error_code'], timed_out['agent_turn_ended_at']),
                             ('blocked', 'acp_timeout_uncertain', None))
            self.assertEqual(healthy['state'], 'running')
            self.assertTrue(dispatcher.gateway.driver.alive)

            # The peer has received the cancel notification, but deliberately
            # keeps the prompt open. A local send/remote receipt is not proof
            # that the agent turn actually ended.
            deadline = time.monotonic() + 2
            while not received.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(received.exists(), 'fixture did not receive cancel notification')
            dispatcher.tick(now=200.6)
            timed_out = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (first_job['key'],)).fetchone()
            self.assertIsNone(timed_out['agent_turn_ended_at'])
            self.assertEqual(dispatcher.reconcile_job(timed_out['key'], retry_if_stopped=True)['outcome'], 'unresolved')

            release.write_text('release')
            wait_started = time.monotonic()
            deadline = wait_started + 2
            while time.monotonic() < deadline:
                dispatcher.tick(now=201 + (time.monotonic() - wait_started))
                timed_out = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (first_job['key'],)).fetchone()
                if timed_out['agent_turn_ended_at'] is not None:
                    break
                time.sleep(.02)
            self.assertIsNotNone(timed_out['agent_turn_ended_at'])
            self.assertEqual(dispatcher.reconcile_job(timed_out['key'], retry_if_stopped=True), {
                'key': timed_out['key'], 'outcome': 'retry_ready',
                'runtime_group_gone': False, 'turn_ended': True,
            })
            healthy = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (second_job['key'],)).fetchone()
            self.assertEqual(healthy['state'], 'running')
            self.assertTrue(dispatcher.gateway.driver.alive)
        finally:
            dispatcher.close()
            if original is None:
                os.environ.pop('ACP_TEST_MODE', None)
            else:
                os.environ['ACP_TEST_MODE'] = original
            for name, value in original_markers.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            connection.close()
            inbox.close()

    def test_timeout_terminal_evidence_requires_exact_attempt_and_resets_on_retry(self):
        inbox = InboxStore(self.state / 'inbox.sqlite3')
        connection = sqlite3.connect(self.state / 'inbox.sqlite3')
        connection.row_factory = sqlite3.Row
        connection.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        service = SimpleNamespace(_lock=threading.RLock(), _connection=connection,
                                  send_outgoing=lambda *args, **kwargs: SimpleNamespace(state='sent'))
        config = {'database_path': 'state/inbox.sqlite3', '_config_path': str(self.project / 'config/test.json'),
                  'dispatcher': {'enabled': True, 'runtime': 'acp',
                                 'acp': {'command': [sys.executable, '-u', str(SDK_PEER)]}}}
        dispatcher = Dispatcher(self.project, config, inbox, service)
        try:
            message = inbox.ingest(InboundPointer('event-terminal-fence', 'T', 'C', '908.1', '908.1')).message
            dispatcher.enqueue(100)
            job = dispatcher.db.execute('SELECT * FROM jobs WHERE message_id=?', (message.id,)).fetchone()
            with dispatcher.db:
                dispatcher.db.execute(
                    "UPDATE jobs SET state='blocked',runtime='acp',error_code='acp_timeout_uncertain', "
                    "agent_session_id='session-a',agent_turn_id='7:3',agent_generation=7,agent_turn_ended_at=NULL WHERE key=?",
                    (job['key'],),
                )

            invalid_events = (
                AgentEvent('terminal', job['root'], 'session-a', '7:2', generation=7),
                AgentEvent('terminal', job['root'], 'session-a', '7:3', generation=8),
                AgentEvent('terminal', job['root'], 'session-b', '7:3', generation=7),
            )
            for event in invalid_events:
                with self.subTest(event=event):
                    dispatcher.gateway = SimpleNamespace(poll=lambda event=event: (event,), close=lambda **_kwargs: None)
                    dispatcher._consume_acp_events(101)
                    blocked = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                    self.assertIsNone(blocked['agent_turn_ended_at'])
                    self.assertEqual(dispatcher.reconcile_job(job['key'], retry_if_stopped=True)['outcome'], 'unresolved')

            with dispatcher.db:
                dispatcher.db.execute("UPDATE jobs SET error_code='acp_runtime_lost_uncertain' WHERE key=?", (job['key'],))
            exact = AgentEvent('terminal', job['root'], 'session-a', '7:3', generation=7)
            dispatcher.gateway = SimpleNamespace(poll=lambda: (exact,), close=lambda **_kwargs: None)
            dispatcher._consume_acp_events(102)
            blocked = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertIsNone(blocked['agent_turn_ended_at'])

            # A real retry starts a fresh ACP attempt and must erase any old
            # terminal proof before the new prompt is submitted.
            with dispatcher.db:
                dispatcher.db.execute(
                    "UPDATE jobs SET state='retry',error_code='acp_reconcile_retry_ready',agent_turn_ended_at=? WHERE key=?",
                    (99, job['key']),
                )
            dispatcher.gateway = None
            retry = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            self.assertTrue(dispatcher._launch(retry, 200))
            started = time.monotonic()
            deadline = started + 3
            while time.monotonic() < deadline:
                dispatcher.tick(now=201 + (time.monotonic() - started))
                retried = dispatcher.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                if retried['state'] == 'running' and retried['agent_turn_id']:
                    break
                time.sleep(.02)
            self.assertEqual(retried['state'], 'running')
            self.assertIsNone(retried['agent_turn_ended_at'])
            self.assertNotEqual(retried['agent_turn_id'], '7:3')
        finally:
            dispatcher.close()
            connection.close()
            inbox.close()

    def test_close_kills_an_owned_descendant_after_its_adapter_parent_exits(self):
        marker = self.state / 'descendant.pid'
        gateway = self.new_gateway(profile=AgentProfile(
            'orphan', 'sdk-test-peer', (sys.executable, '-u', str(SDK_PEER)),
            frozenset({'load'}), (('ACP_TEST_DESCENDANT_PID_PATH', str(marker)),),
        ))
        try:
            gateway.prompt('orphan-root', 'orphan-descendant')
            deadline = time.monotonic() + 2
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(marker.exists(), 'fixture did not create a synthetic descendant')
            child_pid = int(marker.read_text())
            os.kill(child_pid, 0)
            gateway.close(wait=True)
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(.02)
            else:
                self.fail('owned descendant survived bounded process-group cleanup')
        finally:
            gateway.close(wait=True)


if __name__ == '__main__':
    unittest.main()
