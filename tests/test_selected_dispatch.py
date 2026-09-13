"""Isolated Dispatcher integration: fake CLI and classifier, no Slack/model calls."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from director.dispatcher import Dispatcher
from director.inbox import InboxStore, InboundPointer
from director.model_selection import Selection

FAKE = '''import json,os,pathlib,sqlite3,sys,time
args=sys.argv[1:]
sid=args[-1]
prompt=sys.stdin.read()
db=sqlite3.connect('state/dispatch/jobs.sqlite3');db.row_factory=sqlite3.Row
row=db.execute("SELECT * FROM jobs WHERE agent_session_id=? AND state='running'",(sid,)).fetchone()
auth=db.execute("SELECT * FROM agent_reply_authorities WHERE job_key=? AND state='active'",(row['key'],)).fetchone()
record={'args':args,'group':os.getpgrp(),'job':dict(row),'authority':dict(auth)}
target=pathlib.Path('received-'+str(row['message_id'])+'.json')
temporary=target.with_suffix('.tmp')
temporary.write_text(json.dumps(record))
temporary.replace(target)
while not pathlib.Path('release-'+str(row['message_id'])).exists(): time.sleep(.01)
print(json.dumps({'type':'result','subtype':'success','is_error':False,'session_id':sid,'result':'fixture'}))
'''

class SelectedDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.project=Path(self.temp.name)
        (self.project/'state').mkdir()
        script=self.project/'fake_claude.py';script.write_text(FAKE)
        self.inbox=InboxStore(self.project/'state/inbox.sqlite3')
        self.conn=sqlite3.connect(self.project/'state/inbox.sqlite3')
        self.conn.row_factory=sqlite3.Row
        self.conn.execute('CREATE TABLE slack_outbox (idempotency_key TEXT PRIMARY KEY,state TEXT,slack_ts TEXT)')
        self.service=SimpleNamespace(_lock=threading.RLock(),_connection=self.conn,
                                     send_outgoing=lambda *a,**k:SimpleNamespace(state='sent'))
        self.config={'database_path':'state/inbox.sqlite3','dispatcher':{
            'enabled':True,'runtime':'acp','max_workers':2,
            'acp':{'command':[sys.executable,'/never-start-codex']},
            'claude':{'command':[sys.executable,str(script)],'timeout_seconds':15},
            'model_selection':{'enabled':True,'default_backend':'claude'}}}
        self.d=Dispatcher(self.project,self.config,self.inbox,self.service)
        self.now=100
        self.selector=patch('director.model_selection.select_in_process',
                            side_effect=lambda text,backend,timeout:Selection(backend,'standard',
                                'claude-opus-5' if backend=='claude' else 'gpt-5.6-terra','high','test')).start()

    def tearDown(self):
        self.d.close();patch.stopall();self.conn.close();self.inbox.close();self.temp.cleanup()

    def tick(self):
        self.now+=1
        self.d.tick(now=self.now)

    def until(self,predicate):
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            self.tick()
            result=predicate()
            if result:return result
            time.sleep(.015)
        self.fail('Dispatcher condition not reached: '+str([dict(r) for r in self.d.db.execute('SELECT * FROM jobs')]))

    def source(self,ts,root=None):
        return self.inbox.ingest(InboundPointer('event-'+ts,'T','C',ts,ts,root)).message

    def received(self,message):
        path=self.project/('received-'+str(message.id)+'.json')
        if path.exists():return json.loads(path.read_text())

    def complete(self,message):
        job=self.d.db.execute('SELECT * FROM jobs WHERE message_id=?',(message.id,)).fetchone()
        with self.conn:
            self.conn.execute('INSERT INTO slack_outbox VALUES (?,?,?)',
                              ('dispatch-answer:'+job['key'],'sent','fixture-reply'))
        self.inbox.mark_completed_if_revision(message.id,message.revision)
        (self.project/('release-'+str(message.id))).touch()

    def test_idle_never_classifies(self):
        self.tick();self.tick()
        self.selector.assert_not_called()
        self.assertFalse(self.d._gateways)

    def test_spawn_failure_retries_reserved_uuid_after_dispatcher_restart(self):
        message=self.source('200.1')
        with patch('director.claude_driver.subprocess.Popen',side_effect=FileNotFoundError('fixture executable absent')) as spawn:
            failed=self.until(lambda:self.d.db.execute("SELECT * FROM jobs WHERE state='retry' AND error_code='claude_spawn_failed'").fetchone())
        self.assertEqual(spawn.call_count,1)
        session_id=failed['agent_session_id']
        selection=failed['agent_selection']
        self.assertIsNotNone(session_id)
        self.assertIsNone(failed['agent_group_id'])
        self.assertIsNone(failed['agent_turn_id'])
        self.assertFalse(self.d.acp_locks)
        self.assertEqual(self.d.db.execute('SELECT started FROM agent_session_launches WHERE session_id=?',(session_id,)).fetchone()[0],0)
        self.assertEqual(self.d.db.execute("SELECT count(*) FROM agent_reply_authorities WHERE state='active'").fetchone()[0],0)
        self.assertEqual(self.selector.call_count,1)
        self.d.close()
        self.d=Dispatcher(self.project,self.config,self.inbox,self.service)
        self.now=failed['retry_at']
        received=self.until(lambda:self.received(message))
        self.assertEqual(received['job']['agent_session_id'],session_id)
        self.assertEqual(received['job']['agent_selection'],selection)
        self.assertIn('--session-id',received['args'])
        self.assertNotIn('--resume',received['args'])
        self.assertEqual(self.d.db.execute('SELECT count(*) FROM agent_sessions').fetchone()[0],1)
        self.assertEqual(self.d.db.execute('SELECT started FROM agent_session_launches WHERE session_id=?',(session_id,)).fetchone()[0],1)
        self.assertEqual(self.selector.call_count,1)
        self.complete(message)
        self.until(lambda:self.d.db.execute("SELECT count(*) FROM jobs WHERE state='done'").fetchone()[0]==1)

    def test_registration_affinity_and_serialized_same_thread(self):
        first=self.source('100.1')
        received=self.until(lambda:self.received(first))
        self.assertEqual(json.loads(received['job']['agent_selection']),
                         {'backend':'claude','grade':'standard','model':'claude-opus-5','effort':'high','cause':'test'})
        self.assertEqual(received['group'],received['job']['agent_group_id'])
        self.assertEqual(received['job']['agent_turn_id'],received['authority']['turn_id'])
        self.assertEqual(received['job']['agent_session_id'],received['authority']['session_id'])
        self.assertEqual(received['authority']['state'],'active')
        self.assertIn(received['job']['key'],self.d.acp_locks)
        self.assertIn('--session-id',received['args'])
        self.config['dispatcher']['model_selection']['default_backend']='codex'
        second=self.source('100.2','100.1')
        for _ in range(5):self.tick();time.sleep(.01)
        self.assertIsNone(self.received(second))
        self.assertEqual(self.d.db.execute("SELECT count(*) FROM jobs WHERE state='running'").fetchone()[0],1)
        self.assertEqual(self.selector.call_count,1)
        self.complete(first)
        resumed=self.until(lambda:self.received(second))
        self.assertEqual(resumed['job']['agent_session_id'],received['job']['agent_session_id'])
        self.assertEqual(resumed['job']['agent_profile'],'claude-default')
        self.assertEqual(json.loads(resumed['job']['agent_selection'])['backend'],'claude')
        self.assertIn('--resume',resumed['args'])
        self.assertEqual(self.selector.call_args.kwargs['backend'],'claude')
        self.assertEqual(self.d.db.execute('SELECT count(*) FROM agent_sessions').fetchone()[0],1)
        self.complete(second)
        self.until(lambda:self.d.db.execute("SELECT count(*) FROM jobs WHERE state='done'").fetchone()[0]==2)
        self.assertFalse(self.d.acp_locks)

if __name__=='__main__':unittest.main()
