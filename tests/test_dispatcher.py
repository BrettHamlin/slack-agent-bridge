import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.project=Path(self.temp.name)
        (self.project/'state').mkdir()
        self.inbox=InboxStore(self.project/'state/inbox.sqlite3')
        conn=sqlite3.connect(self.project/'state/inbox.sqlite3')
        conn.row_factory=sqlite3.Row
        conn.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        self.notices=[]
        self.service=SimpleNamespace(_lock=threading.RLock(),_connection=conn,send_outgoing=lambda *a,**k:(self.notices.append(k) or SimpleNamespace(state='sent')))
        self.config={'database_path':'state/inbox.sqlite3','dispatcher':{'enabled':True,'codex_path':'codex','model':'gpt-6-astra'}}
        self.d=Dispatcher(self.project,self.config,self.inbox,self.service)

    def tearDown(self):
        self.d.children={}
        self.d.close();self.service._connection.close();self.inbox.close();self.temp.cleanup()

    def source(self,ts='100.1',root=None):
        return self.inbox.ingest(InboundPointer('event-'+ts,'T','C',ts,ts,root)).message

    def job(self):
        return self.d.db.execute('SELECT * FROM jobs ORDER BY created_at LIMIT 1').fetchone()

    def test_idle_never_starts_model(self):
        with patch('director.dispatcher.subprocess.Popen') as popen:
            self.d.tick(now=100);self.d.tick(now=101)
        popen.assert_not_called()

    def test_cleanup_failure_blocks_same_root_across_restart(self):
        self.source();self.d.enqueue(100)
        output=self.project/'worker.jsonl'
        output.write_text('{"type": "worker.cleanup_failed"}\n')
        with self.d.db:self.d.db.execute("UPDATE jobs SET attempts=1,state='running',stdout_path=?",(str(output),))
        self.d._finish(self.job(),101)
        self.assertEqual(self.job()['state'],'blocked')
        self.d.close();self.d=Dispatcher(self.project,self.config,self.inbox,self.service)
        self.source('100.2','100.1');self.d.enqueue(102)
        job=self.d.db.execute("SELECT * FROM jobs WHERE state='pending'").fetchone()
        with patch('director.dispatcher.subprocess.Popen') as popen:self.assertFalse(self.d._launch(job,103))
        popen.assert_not_called()

    def test_failure_notice_retries_after_send_error_and_restart(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute("UPDATE jobs SET attempts=2,state='running'")
        with patch.object(self.service,'send_outgoing',side_effect=RuntimeError):self.d._finish(self.job(),101)
        self.assertIsNone(self.job()['notice_at'])
        self.d.close();self.d=Dispatcher(self.project,self.config,self.inbox,self.service)
        self.d.tick(now=132)
        self.assertEqual(self.job()['notice_at'],132)
        self.d.tick(now=200)
        self.assertEqual(len(self.notices),1)
        self.assertEqual(self.notices[0]['idempotency_key'],'dispatch-failure:'+self.job()['key']+':a2')

    def test_uncertain_failure_notice_keeps_key_until_confirmed(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute("UPDATE jobs SET attempts=2,state='running'")
        with patch.object(self.service,'send_outgoing',return_value=SimpleNamespace(state='uncertain')):
            self.d._finish(self.job(),101)
        self.assertIsNone(self.job()['notice_at'])
        self.d.tick(now=132)
        self.assertEqual(self.notices[0]['idempotency_key'],'dispatch-failure:'+self.job()['key']+':a2')

    def test_prompt_pipe_failure_retains_running_child(self):
        self.source();self.d.enqueue(100)
        class BrokenInput:
            def write(self,text):raise BrokenPipeError()
            def close(self):pass
        child=SimpleNamespace(stdin=BrokenInput(),poll=lambda:None)
        with patch('director.dispatcher.subprocess.Popen',return_value=child):self.assertTrue(self.d._launch(self.job(),101))
        self.assertEqual(self.job()['state'],'running')
        self.assertEqual(self.job()['attempts'],1)
        self.assertIs(self.d.children[self.job()['key']],child)

    def test_timeout_signal_denial_fences_root_and_notifies(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute("UPDATE jobs SET state='running',attempts=1,started_at=100")
        self.d.children[self.job()['key']]=SimpleNamespace(pid=123,poll=lambda:None)
        sent=[]
        self.service.send_outgoing=lambda *args,**kwargs:(sent.append((args,kwargs)) or SimpleNamespace(state='sent'))
        with patch('director.dispatcher.os.killpg',side_effect=PermissionError):self.d.tick(now=1001)
        self.assertEqual((self.job()['state'],self.job()['error_code']),('blocked','worker_signal_denied'))
        self.assertEqual(len(sent),1)
        self.assertIn('worker recovery is required',sent[0][0][0])

    def test_missing_process_on_timeout_does_not_break_tick(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute("UPDATE jobs SET state='running',attempts=1,started_at=100")
        self.d.children[self.job()['key']]=SimpleNamespace(pid=123,poll=lambda:None)
        with patch('director.dispatcher.os.killpg',side_effect=ProcessLookupError):self.d.tick(now=1001)
        self.assertIsNotNone(self.inbox.get_checkpoint('dispatcher.loop'))

    def test_source_starts_immediately_with_astra_and_approval_review(self):
        m=self.source();child=SimpleNamespace(stdin=io.StringIO())
        with patch.dict(os.environ,{'SLACK_BOT_TOKEN':'synthetic','SLACK_APP_TOKEN':'synthetic'}),patch('director.dispatcher.subprocess.Popen',return_value=child) as popen:
            self.d.tick(now=100)
        args=popen.call_args.args[0];kwargs=popen.call_args.kwargs
        self.assertIn('gpt-6-astra',args);self.assertIn('--approve-for-me',args)
        self.assertNotIn('--dangerously-bypass-approvals-and-sandbox',args)
        self.assertNotIn('SLACK_BOT_TOKEN',kwargs['env'])
        self.assertEqual(self.job()['message_id'],m.id)
        self.assertEqual(self.job()['state'],'running')

    def test_same_thread_serializes_but_other_thread_can_start(self):
        self.source('100.1','100.0');self.source('100.2','100.0');self.source('200.1','200.0')
        with patch('director.dispatcher.subprocess.Popen',side_effect=lambda *a,**k:SimpleNamespace(stdin=io.StringIO())) as popen:
            self.d.tick(now=100)
        self.assertEqual(popen.call_count,2)
        rows=self.d.db.execute("SELECT root FROM jobs WHERE state='running'").fetchall()
        self.assertEqual({r[0] for r in rows},{'100.0','200.0'})

    def test_completion_receipt_alone_is_not_delivery(self):
        m=self.source();self.d.enqueue(100);self.inbox.mark_completed_if_revision(m.id,1)
        self.assertFalse(self.d._delivered(self.job()))
        with self.service._connection:
            self.service._connection.execute('INSERT INTO slack_outbox VALUES (?,?,?)',('dispatch-answer:'+self.job()['key'],'sent','300.0'))
        self.assertTrue(self.d._delivered(self.job()))

    def test_finished_turn_without_reply_retries_once_then_fails_visibly(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute("UPDATE jobs SET attempts=1,state='running'")
        self.d._finish(self.job(),101)
        self.assertEqual(self.job()['state'],'retry')
        with self.d.db:self.d.db.execute("UPDATE jobs SET attempts=2,state='running'")
        self.d._finish(self.job(),102)
        self.assertEqual(self.job()['state'],'failed');self.assertEqual(len(self.notices),1)

    def test_completed_source_does_not_launch_again(self):
        m=self.source();self.d.enqueue(100);self.inbox.mark_completed_if_revision(m.id,1)
        with patch('director.dispatcher.subprocess.Popen') as popen:self.d.tick(now=101)
        popen.assert_not_called()

    def test_process_lock_prevents_duplicate_after_supervisor_restart(self):
        self.source();self.d.enqueue(100)
        handle=self.d._lock(self.job()['root'])
        try:
            with patch('director.dispatcher.subprocess.Popen') as popen:
                self.assertFalse(self.d._launch(self.job(),101))
            popen.assert_not_called()
        finally:handle.close()

    def test_session_is_restored_only_for_its_slack_root(self):
        self.source();self.d.enqueue(100)
        with self.d.db:self.d.db.execute('INSERT INTO sessions VALUES (?,?)',('100.1','session-test'))
        with patch('director.dispatcher.subprocess.Popen',return_value=SimpleNamespace(stdin=io.StringIO())) as popen:self.d._launch(self.job(),101)
        self.assertIn('resume',popen.call_args.args[0]);self.assertIn('session-test',popen.call_args.args[0])

    def test_launch_failure_is_bounded_and_backs_off(self):
        self.source();self.d.enqueue(100)
        with self.d.db:
            self.d.db.execute(
                """UPDATE jobs SET runtime='acp',agent_profile='old-profile',
                   agent_session_id='old-session',agent_turn_id='1:1',
                   agent_generation=1,agent_group_id=123,agent_turn_ended_at=99"""
            )
        with patch('director.dispatcher.subprocess.Popen',side_effect=FileNotFoundError):
            with self.assertRaises(FileNotFoundError):self.d._launch(self.job(),101)
        failed = self.job()
        self.assertEqual((failed['state'], failed['runtime'], failed['retry_at']), ('retry', 'legacy', 131))
        self.assertTrue(all(failed[name] is None for name in (
            'agent_profile', 'agent_session_id', 'agent_turn_id',
            'agent_generation', 'agent_group_id', 'agent_turn_ended_at',
        )))

    def test_legacy_relaunch_clears_unbound_acp_prepare_metadata(self):
        """A failed pre-prompt ACP attempt must not poison a later CLI job."""
        self.d.close()
        acp_config = {
            'database_path': 'state/inbox.sqlite3',
            'dispatcher': {'enabled': True, 'runtime': 'acp', 'acp': {}},
        }
        self.d = Dispatcher(self.project, acp_config, self.inbox, self.service)
        self.source();self.d.enqueue(100)
        self.assertFalse(self.d._launch(self.job(), 101))
        failed_prepare = self.job()
        self.assertEqual((failed_prepare['state'], failed_prepare['runtime']), ('retry', 'acp'))
        self.assertIsNone(failed_prepare['agent_session_id'])

        self.d.close()
        self.d = Dispatcher(self.project, self.config, self.inbox, self.service)
        child = SimpleNamespace(stdin=io.StringIO(), poll=lambda: 0)
        with patch('director.dispatcher.subprocess.Popen', return_value=child):
            self.assertTrue(self.d._launch(self.job(), 102))
        launched = self.job()
        self.assertEqual((launched['state'], launched['runtime']), ('running', 'legacy'))
        self.assertTrue(all(launched[name] is None for name in (
            'agent_profile', 'agent_session_id', 'agent_turn_id',
            'agent_generation', 'agent_group_id', 'agent_turn_ended_at',
        )))

        self.d.close()
        self.d = Dispatcher(self.project, self.config, self.inbox, self.service)
        self.assertEqual((self.job()['state'], self.job()['runtime']), ('running', 'legacy'))
        self.d.tick(now=103)
        finished = self.job()
        self.assertEqual((finished['state'], finished['runtime'], finished['error_code']),
                         ('failed', 'legacy', 'worker_ended_without_verified_delivery'))
