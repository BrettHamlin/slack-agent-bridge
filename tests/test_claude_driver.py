"""Subprocess contract tests use a fake CLI, not Claude authentication or inference."""
import json
from pathlib import Path
import sys
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from director.agent_gateway import AgentProfile, AgentGateway
from director.claude_driver import ClaudeDriver
from director.model_selection import Selection

FAKE = '''import json,sys,time,pathlib
args=sys.argv[1:]
sid=args[args.index('--resume')+1] if '--resume' in args else args[args.index('--session-id')+1]
text=sys.stdin.read()
pathlib.Path(sid+'.json').write_text(json.dumps({'args':args,'text':text}))
if text=='wait': time.sleep(30)
if text=='invalid':
 print('{}'); sys.exit(0)
print(json.dumps({'type':'result','subtype':'success','is_error':False,'session_id':sid,'result':text}))
'''

class ClaudeDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cwd = Path(self.temp.name)
        script = self.cwd/'fake.py'
        script.write_text(FAKE)
        profile = AgentProfile('claude','claude', (sys.executable,str(script)), request_timeout_seconds=3)
        self.driver = ClaudeDriver(profile,self.cwd,{},self.cwd/'stderr',4)
        self.selection = Selection('claude','standard','claude-opus-5','high','test')

    def tearDown(self):
        self.driver.close(wait=True)
        self.temp.cleanup()

    def event(self):
        return self.driver.events.get(timeout=6)

    def test_registration_precedes_prompt_and_exact_session_resume(self):
        sid, response = self.driver.prepare(str(self.cwd)).result()
        self.assertEqual(AgentGateway._session_metadata(response)['modes'], {'current_mode_id': 'auto'})
        def register(turn):
            self.assertGreater(self.driver.group_for_turn(turn),0)
            self.assertFalse((self.cwd/(sid+'.json')).exists())
        self.driver.prompt(sid,'first',selection=self.selection,on_turn=register)
        self.assertEqual(self.event()[0],'terminal')
        first = json.loads((self.cwd/(sid+'.json')).read_text())
        self.assertIn('--session-id',first['args'])
        self.driver.prompt(sid,'second',selection=self.selection,on_turn=lambda turn: None)
        self.assertEqual(self.event()[0],'terminal')
        second = json.loads((self.cwd/(sid+'.json')).read_text())
        self.assertIn('--resume',second['args'])
        self.assertEqual(second['args'][-1],sid)
        with self.assertRaisesRegex(Exception,'claude_session_already_started'):
            self.driver.prepare(str(self.cwd),sid,unstarted=True).result()
        self.assertTrue(self.driver.group_empty)

    def test_load_after_restart_uses_resume_even_without_transcript(self):
        sid, _ = self.driver.prepare(str(self.cwd)).result()
        self.driver.close(wait=True)
        self.driver = ClaudeDriver(self.driver.profile,self.cwd,{},self.cwd/'stderr',5)
        self.driver.prepare(str(self.cwd),sid).result()
        self.driver.prompt(sid,'resume',selection=self.selection,on_turn=lambda turn: None)
        self.assertEqual(self.event()[0],'terminal')
        args = json.loads((self.cwd/(sid+'.json')).read_text())['args']
        self.assertIn('--resume',args)
        self.assertNotIn('--session-id',args)

    def test_callback_failure_never_releases_prompt(self):
        sid, _ = self.driver.prepare(str(self.cwd)).result()
        def reject(turn):
            raise RuntimeError('database unavailable')
        with self.assertRaises(RuntimeError):
            self.driver.prompt(sid,'forbidden',selection=self.selection,on_turn=reject)
        self.assertFalse((self.cwd/(sid+'.json')).exists())
        self.assertTrue(self.driver.group_empty)

    def test_cancel_is_per_session_and_groups_are_distinct(self):
        a, _ = self.driver.prepare(str(self.cwd)).result()
        b, _ = self.driver.prepare(str(self.cwd)).result()
        ta = self.driver.prompt(a,'wait',selection=self.selection,on_turn=lambda turn: None)
        tb = self.driver.prompt(b,'other',selection=self.selection,on_turn=lambda turn: None)
        self.assertNotEqual(self.driver.group_for_turn(ta), self.driver.group_for_turn(tb))
        self.driver.cancel(a)
        events = [self.event(), self.event()]
        self.assertEqual({e[1]: e[0] for e in events},{a:'error',b:'terminal'})
        self.assertTrue(self.driver.group_empty)

    def test_timeout_and_malformed_result_are_uncertain_errors(self):
        for prompt in ('wait', 'invalid'):
            with self.subTest(prompt=prompt):
                self.driver.transport.timeout = .15
                sid, _ = self.driver.prepare(str(self.cwd)).result()
                self.driver.prompt(sid,prompt,selection=self.selection,on_turn=lambda turn: None)
                event = self.event()
                self.assertEqual(event[0], 'error')
                self.assertTrue(self.driver.group_empty)

    def test_overlapping_same_session_is_rejected(self):
        sid, _ = self.driver.prepare(str(self.cwd)).result()
        self.driver.prompt(sid,'wait',selection=self.selection,on_turn=lambda turn: None)
        with self.assertRaisesRegex(Exception, 'claude_session_busy'):
            self.driver.prompt(sid,'duplicate',selection=self.selection,on_turn=lambda turn: None)
        with self.assertRaisesRegex(Exception, 'claude_session_busy'):
            self.driver.prepare(str(self.cwd), sid).result()
        self.driver.cancel(sid)
        self.assertEqual(self.event()[0], 'error')

    def test_gateway_durable_binding_events_and_restart_resume(self):
        database = sqlite3.connect(':memory:')
        database.row_factory = sqlite3.Row
        profile = AgentProfile('claude-test', 'claude-code', self.driver.profile.command,
                               expected_settings=(('mode','auto'), ('model','claude-opus-5'),
                                                  ('reasoning_effort','high')),
                               dynamic_selection=True, request_timeout_seconds=3)
        def make_gateway():
            return AgentGateway(database,project=self.cwd,state_directory=self.cwd/'state',
                                profile=profile,config_path=str(self.cwd/'config.json'))
        def await_event(gateway):
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                events = gateway.poll()
                if events:
                    return events[0]
                time.sleep(.01)
            self.fail('No gateway event')
        gateway = make_gateway()
        try:
            binding = gateway.start_or_resume('slack-root', mcp_servers=[gateway.publish_reply_server()])
            registered = []
            def register(turn):
                registered.append((turn, gateway.driver.group_for_turn(turn)))
                self.assertFalse((self.cwd/(binding.session_id+'.json')).exists())
            gateway.submit(binding, 'initial', selection=self.selection, on_turn=register)
            event = await_event(gateway)
            self.assertEqual((event.kind,event.root,event.session_id),
                             ('terminal','slack-root',binding.session_id))
            self.assertEqual(event.turn_id,registered[0][0])
            self.assertGreater(registered[0][1],0)
            self.assertEqual(binding.backend,'claude-code')
            args=json.loads((self.cwd/(binding.session_id+'.json')).read_text())['args']
            config=json.loads(args[args.index('--mcp-config')+1])
            self.assertEqual(config, {'mcpServers': {'director-publish-reply': {
                'command': str(self.cwd/'.venv/bin/python'),
                'args':['-m','director.agent_tools','--config',str(self.cwd/'config.json')], 'env':{}}}})
            gateway.close()
            gateway = make_gateway()
            resumed = gateway.start_or_resume('slack-root',mcp_servers=[gateway.publish_reply_server()])
            self.assertEqual(resumed,binding)
            gateway.submit(resumed,'continued',selection=self.selection,on_turn=lambda turn: None)
            self.assertEqual(await_event(gateway).kind,'terminal')
            args=json.loads((self.cwd/(binding.session_id+'.json')).read_text())['args']
            self.assertIn('--resume',args)
            self.assertNotIn('--session-id',args)
            self.assertEqual(args[-1],binding.session_id)
            self.assertEqual(database.execute('SELECT count(*) FROM agent_sessions').fetchone()[0],1)
        finally:
            gateway.close()
            database.close()

    def test_spawn_failure_retries_reserved_uuid_as_new(self):
        sid, _ = self.driver.prepare(str(self.cwd)).result()
        with patch('director.claude_driver.subprocess.Popen',side_effect=FileNotFoundError('missing')):
            with self.assertRaisesRegex(Exception,'claude_spawn_failed'):
                self.driver.prompt(sid,'never sent',selection=self.selection,on_turn=lambda turn: None)
        self.assertFalse(self.driver._sessions[sid]['resume'])
        self.assertFalse(self.driver._turns)
        self.driver.prompt(sid,'retry',selection=self.selection,on_turn=lambda turn: None)
        self.assertEqual(self.event()[0],'terminal')
        args=json.loads((self.cwd/(sid+'.json')).read_text())['args']
        self.assertIn('--session-id',args)
        self.assertEqual(args[-1],sid)

    def test_explicit_durable_unstarted_load_keeps_uuid_as_new(self):
        sid, _ = self.driver.prepare(str(self.cwd)).result()
        self.driver.close(wait=True)
        self.driver = ClaudeDriver(self.driver.profile,self.cwd,{},self.cwd/'stderr',5)
        self.driver.prepare(str(self.cwd),sid,unstarted=True).result()
        self.driver.prompt(sid,'initial after restart',selection=self.selection,on_turn=lambda turn: None)
        self.assertEqual(self.event()[0],'terminal')
        args=json.loads((self.cwd/(sid+'.json')).read_text())['args']
        self.assertIn('--session-id',args)
        self.assertEqual(args[-1],sid)

    def test_mcp_is_supplied_as_strict_stdio_configuration(self):
        server=SimpleNamespace(name='publish_reply',command='/python',args=['-m','director.agent_tools'],
                               env=[SimpleNamespace(name='SAFE',value='yes')])
        sid, _ = self.driver.prepare(str(self.cwd),mcp_servers=[server]).result()
        self.driver.prompt(sid,'hello',selection=self.selection,on_turn=lambda turn: None)
        self.assertEqual(self.event()[0],'terminal')
        args=json.loads((self.cwd/(sid+'.json')).read_text())['args']
        self.assertIn('--strict-mcp-config',args)
        config=json.loads(args[args.index('--mcp-config')+1])
        self.assertEqual(config['mcpServers']['publish_reply']['env'],{'SAFE':'yes'})
        self.assertEqual(args[args.index('--permission-mode')+1],'auto')

if __name__ == '__main__': unittest.main()
