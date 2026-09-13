from argparse import Namespace
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
from director.receiver import channel_configs, routed_listener, listen_channels, ChannelLoop
from director.slack_transport import SlackAllowlist, make_socket_mode_listener, SocketModeAck
from director.__main__ import load_config
from director.dispatcher import Dispatcher
from director.inbox import InboxStore


class ChannelIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.project = Path(self.temp.name).resolve()
        self.primary = dict(team_id='T', channel_id='C-real', owner_user_id='U', bot_user_id='B',
                            slack_app_id='A', workspace_domain='example.slack.com', enabled=True,
                            database_path='state/inbox.sqlite3', additional_configs=['config/test.json'])
        self.test = {**self.primary, 'channel_id': 'C-test', 'environment': 'test',
                     'database_path': 'state/testing/inbox.sqlite3', 'additional_configs': []}

    def tearDown(self):
        self.temp.cleanup()

    def test_separate_channel_and_state_required(self):
        self.assertEqual(len(channel_configs(self.primary,self.project,lambda _: self.test)),2)
        for change in ({'channel_id':'C-real'}, {'database_path':'state/inbox.sqlite3'},
                       {'database_path':'state/elsewhere/inbox.sqlite3'}, {'team_id':'foreign'},
                       {'environment':'production'}, {'additional_configs':['config/third.json']}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                channel_configs(self.primary,self.project,lambda _: {**self.test,**change})

    def test_no_second_socket_for_test_config(self):
        with patch('director.receiver.credentials') as creds, self.assertRaises(ValueError):
            listen_channels(self.test,self.project,lambda _: self.test,None)
        creds.assert_not_called()

    def test_dispatch_reconcile_uses_resident_dispatcher_or_fails_closed(self):
        loop = ChannelLoop.__new__(ChannelLoop)
        loop.dispatcher = MagicMock()
        loop.execute = MagicMock()
        args = Namespace(command='dispatch-reconcile', key='source-1-r1', retry_if_stopped='retry')

        loop.command(args)

        loop.dispatcher.reconcile_job.assert_called_once_with(
            'source-1-r1', retry_if_stopped=True, operator_requested=True
        )
        loop.execute.assert_not_called()
        loop.dispatcher = None
        with self.assertRaisesRegex(RuntimeError, 'Dispatcher unavailable'):
            loop.command(Namespace(command='dispatch-reconcile', key='source-1-r1', retry_if_stopped='verify'))
        loop.execute.assert_not_called()

    def test_agent_publish_uses_resident_dispatcher_or_fails_closed(self):
        loop = ChannelLoop.__new__(ChannelLoop)
        loop.dispatcher = MagicMock()
        loop.execute = MagicMock()
        args = Namespace(command='agent-publish', authority='opaque-turn-authority', payload_text='prepared reply',
                         responsibility_id='responsibility-1', fence='execution-fence')

        loop.command(args)

        loop.dispatcher.publish_agent_reply.assert_called_once_with(
            'opaque-turn-authority', 'prepared reply', 'responsibility-1', 'execution-fence', None, None, None
        )
        loop.execute.assert_not_called()
        loop.dispatcher = None
        with self.assertRaisesRegex(RuntimeError, 'Dispatcher unavailable'):
            loop.command(args)
        loop.execute.assert_not_called()

    def test_config_path_is_canonical(self):
        path=self.project/'config/test.json';path.parent.mkdir()
        path.write_text(json.dumps(self.test))
        self.assertEqual(load_config(path)['_config_path'],str(path.resolve()))

    def test_both_channel_loops_share_one_socket_and_close_on_stop(self):
        stop=MagicMock();stop.is_set.side_effect=[False, True]
        client=MagicMock();client.socket_mode_request_listeners=[]
        loops=[MagicMock(),MagicMock()]
        with patch('director.receiver.threading.Event',return_value=stop), \
             patch('director.receiver.signal.signal'), \
             patch('director.receiver.credentials',return_value={'SLACK_APP_TOKEN':'synthetic','SLACK_BOT_TOKEN':'synthetic'}), \
             patch('director.receiver.verify_identity') as verify, \
             patch('director.receiver.create_socket_mode_client',return_value=client) as create, \
             patch('director.receiver.ReceiptAckWorker') as receipt_worker, \
             patch('director.receiver.ChannelLoop',side_effect=loops):
            listen_channels(self.primary,self.project,lambda _:self.test,None)
        self.assertEqual(create.call_count,1)
        self.assertEqual(verify.call_count,2)
        self.assertEqual(len(client.socket_mode_request_listeners),1)
        client.connect.assert_called_once();client.close.assert_called_once()
        self.assertEqual(receipt_worker.call_count, 2)
        self.assertEqual(receipt_worker.return_value.start.call_count, 2)
        for loop in loops:
            loop.tick.assert_called_once();loop.close.assert_called_once()

    def test_socket_routes_one_channel_before_ack_and_never_acks_failed_write(self):
        stores=[SimpleNamespace(ingest=lambda p: calls.append(('real',p))),
                SimpleNamespace(ingest=lambda p: calls.append(('test',p)))]
        calls=[];acks=[]
        routes={( 'T',ch):make_socket_mode_listener(store,SlackAllowlist('T',ch,'U'),
                  response_factory=SocketModeAck) for ch,store in zip(['C-real','C-test'],stores)}
        route=routed_listener(routes,response_factory=SocketModeAck)
        client=SimpleNamespace(send_socket_mode_response=acks.append)
        def request(ch):
            return SimpleNamespace(type='events_api',envelope_id='env',payload={
                'team_id':'T','event_id':'E','event':{'type':'message','channel':ch,'user':'U','ts':'100.1'}})
        route(client,request('C-test'))
        self.assertEqual([x[0] for x in calls],['test']);self.assertEqual(len(acks),1)
        calls.clear();acks.clear()
        route(client,request('C-real'))
        self.assertEqual([x[0] for x in calls],['real']);self.assertEqual(len(acks),1)
        calls.clear();acks.clear()
        stores[1].ingest=lambda _: (_ for _ in ()).throw(RuntimeError('synthetic'))
        route(client,request('C-test'))
        self.assertEqual(calls,[]);self.assertEqual(acks,[])
        route(client,request('C-other'))
        self.assertEqual(calls,[]);self.assertEqual(len(acks),1)

    def test_unavailable_test_channel_does_not_stop_real_channel(self):
        stop=MagicMock();stop.is_set.side_effect=[False,True]
        client=MagicMock();client.socket_mode_request_listeners=[]
        loop=MagicMock()
        with patch('director.receiver.threading.Event',return_value=stop), \
             patch('director.receiver.signal.signal'), \
             patch('director.receiver.credentials',return_value={'SLACK_APP_TOKEN':'synthetic','SLACK_BOT_TOKEN':'synthetic'}), \
             patch('director.receiver.verify_identity',side_effect=[None,RuntimeError('synthetic membership loss')]), \
             patch('director.receiver.create_socket_mode_client',return_value=client), \
             patch('director.receiver.ReceiptAckWorker'), \
             patch('director.receiver.ChannelLoop',return_value=loop) as construct:
            listen_channels(self.primary,self.project,lambda _:self.test,None)
        self.assertEqual(construct.call_count,1);client.connect.assert_called_once()
        loop.tick.assert_called_once()
        with InboxStore(self.project/'state/inbox.sqlite3') as store:
            self.assertEqual(store.get_checkpoint('receiver.test_environment_error').value,'RuntimeError')

    def test_interaction_goes_only_to_its_channel(self):
        calls=[];acks=[]
        route=routed_listener({('T','C-real'):lambda *_:calls.append('real'),
                               ('T','C-test'):lambda *_:calls.append('test')},SocketModeAck)
        route(SimpleNamespace(send_socket_mode_response=acks.append),SimpleNamespace(type='interactive',
              envelope_id='E',payload={'team':{'id':'T'},'channel':{'id':'C-test'}}))
        self.assertEqual(calls,['test']);self.assertEqual(acks,[])

    def test_worker_storage_context_and_cli_binding_do_not_use_production(self):
        (self.project/'state/testing').mkdir(parents=True)
        (self.project/'state/manager-context.md').write_text('REAL PRIVATE CONTEXT')
        cfg={**self.test,'_config_path':str(self.project/'config/test.json'),
             'dispatcher':{'codex_path':'codex'}}
        with InboxStore(self.project/cfg['database_path']) as inbox:
            d=Dispatcher(self.project,cfg,inbox,None)
            try:
                self.assertEqual(d.directory,self.project/'state/testing/dispatch')
                job={'key':'source-1-r1','kind':'source','message_id':1,'revision':1,
                     'root':'100.1','attempts':0}
                prompt=d.prompt(job)
                self.assertNotIn('REAL PRIVATE CONTEXT',prompt)
                self.assertIn(str(self.project/'config/test.json'),prompt)
                d.db.execute("INSERT INTO jobs(key,root,kind,state,created_at) VALUES ('source-1-r1','100.1','source','pending',1)")
                d.db.commit()
                child=SimpleNamespace(stdin=io.StringIO())
                with patch('director.dispatcher.subprocess.Popen',return_value=child) as popen:
                    self.assertTrue(d._launch(job,2))
                self.assertEqual(popen.call_args.kwargs['env']['DIRECTOR_CONFIG'],cfg['_config_path'])
                self.assertNotIn('SLACK_BOT_TOKEN',popen.call_args.kwargs['env'])
            finally:
                d.children={};d.close()
