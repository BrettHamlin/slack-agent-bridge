import json
import tempfile
import threading
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from director.inbox import InboxStore
from director.service_queue import (ServiceQueue, submit_command, validate,
    ReceiverUnavailable, CommandUncertain, ServiceCommandFailed, IDENTITY)


class ServiceQueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'inbox.sqlite3'
        self.config = dict(zip(IDENTITY, ('T-test','C-test','U-owner','U-bot','test.slack.com')))
        with InboxStore(self.path) as store:
            store.set_checkpoint('receiver.commands', 'ready')

    def tearDown(self):
        self.temp.cleanup()

    def envelope(self, **args):
        return {'identity': self.config, 'args': {'command': 'source', 'message_id': 1, **args}}

    def put(self, queue, payload=None, state='pending', deadline=None):
        with queue.db:
            queue.db.execute('INSERT INTO commands VALUES (?,?,?,?,?,NULL,NULL)',
                ('request', json.dumps(payload or self.envelope()), state, time.time(), deadline or time.time()+30))

    def test_rejects_foreign_identity_and_arbitrary_operations(self):
        p=self.envelope()
        with self.assertRaises(ValueError): validate(p, {**self.config, 'channel_id':'other'})
        for args in ({'command':'auth_test'}, {'file':'/private/file'}, {'config': '/other'}, {'payload_text': {}}):
            with self.subTest(args=args), self.assertRaises(ValueError): validate(self.envelope(**args), self.config)

    def test_dispatch_reconcile_is_bounded_to_verify_or_explicit_retry(self):
        for retry in ('verify', 'retry'):
            args = validate(self.envelope(command='dispatch-reconcile', key='source-1-r1', retry_if_stopped=retry), self.config)
            self.assertEqual((args.command, args.key, args.retry_if_stopped), ('dispatch-reconcile', 'source-1-r1', retry))
        for args in (
            {'command': 'dispatch-reconcile', 'key': '', 'retry_if_stopped': 'verify'},
            {'command': 'dispatch-reconcile', 'key': 'x' * 257, 'retry_if_stopped': 'verify'},
            {'command': 'dispatch-reconcile', 'key': 'source-1-r1', 'retry_if_stopped': 'always'},
            {'command': 'source', 'message_id': 1, 'retry_if_stopped': 'retry'},
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                validate(self.envelope(**args), self.config)

    def test_guardian_commands_expose_only_local_job_keys(self):
        approved = validate(self.envelope(command='guardian-approve', key='source-1-r1'), self.config)
        listed = validate({'identity': self.config, 'args': {'command': 'guardian-pending'}}, self.config)
        self.assertEqual((approved.command, approved.key), ('guardian-approve', 'source-1-r1'))
        self.assertEqual(listed.command, 'guardian-pending')
        for args in (
            {'command': 'guardian-approve', 'key': ''},
            {'command': 'guardian-approve', 'key': 'x' * 257},
            {'command': 'guardian-pending', 'key': 'source-1-r1'},
            {'command': 'guardian-pending', 'authority': 'opaque-handle'},
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                envelope = {'identity': self.config, 'args': args} if args['command'] == 'guardian-pending' else self.envelope(**args)
                validate(envelope, self.config)

    def test_roundtrip_uses_resident_service_without_reading_credentials(self):
        errors=[]
        stop=threading.Event()
        def serve():
            queue=ServiceQueue(self.path)
            try:
                while not stop.is_set():
                    if queue.process_one(lambda a: {'source': a.message_id}, self.config): return
                    time.sleep(.01)
            except Exception as e: errors.append(e)
            finally: queue.close()
        with patch('director.runtime.credentials', side_effect=AssertionError('must not read mount')):
            worker=threading.Thread(target=serve); worker.start()
            try: result=submit_command(self.path, Namespace(command='source',message_id=17), self.config, timeout=2)
            finally: stop.set(); worker.join()
        self.assertEqual(errors, [])
        self.assertEqual(result, {'source':17})
        queue=ServiceQueue(self.path)
        self.assertEqual(queue.db.execute('select count(*) from commands').fetchone()[0],0)
        self.assertEqual(queue.path.stat().st_mode & 0o777, 0o600)
        queue.close()

    def test_stopped_or_stale_receiver_fails_without_mount_access(self):
        with InboxStore(self.path) as store: store.set_checkpoint('receiver.stopped','stopped')
        with patch('director.runtime.credentials', side_effect=AssertionError('must not read mount')):
            with self.assertRaises(ReceiverUnavailable): submit_command(self.path, Namespace(command='source'), self.config)

    def test_timeout_cancels_pending_operation_so_it_cannot_send_later(self):
        with self.assertRaises(CommandUncertain):
            submit_command(self.path, Namespace(command='send',key='stable',payload_text='test'),self.config,timeout=.02)
        queue=ServiceQueue(self.path)
        calls=[]
        self.assertFalse(queue.process_one(calls.append,self.config))
        self.assertEqual(calls,[])
        self.assertIsNone(queue.db.execute('select payload from commands').fetchone()[0])
        queue.close()

    def test_restart_does_not_replay_inflight_mutation(self):
        queue=ServiceQueue(self.path)
        self.put(queue, self.envelope(command='send',key='stable',payload_text='test'),state='running')
        queue.close()
        queue=ServiceQueue(self.path); queue.recover_interrupted()
        self.assertFalse(queue.process_one(lambda a:self.fail('must not replay'),self.config))
        row=queue.db.execute('select state,payload from commands').fetchone()
        self.assertEqual(tuple(row),('uncertain',None))
        queue.close()

    def test_service_exception_does_not_expose_exception_text(self):
        queue=ServiceQueue(self.path); self.put(queue)
        def fail(args): raise RuntimeError('private content')
        queue.process_one(fail,self.config)
        result=queue.db.execute('select result,payload from commands').fetchone()
        self.assertNotIn('private content',result[0]); self.assertIsNone(result[1])
        self.assertEqual(json.loads(result[0]),{'ok':False,'error_type':'RuntimeError'})
        queue.close()

    def test_expired_request_is_not_executed(self):
        queue=ServiceQueue(self.path); self.put(queue,deadline=time.time()-1)
        self.assertFalse(queue.process_one(lambda a:self.fail('expired'),self.config))
        queue.close()
