import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from director.inbox import InboxStore, InboundPointer
from director.intake_health import notify_unread_sources


class IntakeHealthTests(unittest.TestCase):
    def test_delay_is_reported_once_without_claiming_read_or_completion(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'inbox.sqlite3'
            calls=[]
            service=SimpleNamespace(send_outgoing=lambda *a,**k: (calls.append(k) or SimpleNamespace(state='sent')))
            with InboxStore(path) as store:
                m=store.ingest(InboundPointer('event','T','C','100.1','100.1','100.0')).message
                notify_unread_sources(store,service,now=m.updated_at+119)
                self.assertEqual(calls,[])
                notify_unread_sources(store,service,now=m.updated_at+120)
                self.assertEqual(calls,[{'idempotency_key':f'intake-delay:{m.id}:1','thread_ts':'100.0'}])
                self.assertEqual(store.get_receipts(m.id).read_revisions,())
                self.assertEqual(store.get_receipts(m.id).completed_revisions,())
            with InboxStore(path) as store:
                notify_unread_sources(store,service,now=m.updated_at+500)
                self.assertEqual(len(calls),1)

    def test_read_but_unanswered_source_gets_one_processing_notice(self):
        with tempfile.TemporaryDirectory() as temp, InboxStore(Path(temp)/'db') as store:
            m=store.ingest(InboundPointer('event','T','C','100.1','100.1')).message
            store.mark_read_if_revision(m.id,1)
            calls=[]
            service=SimpleNamespace(send_outgoing=lambda *a,**k:(calls.append((a,k)) or SimpleNamespace(state='sent')))
            notify_unread_sources(store,service,now=m.updated_at+500)
            notify_unread_sources(store,service,now=m.updated_at+600)
            self.assertEqual(len(calls),1)
            self.assertIn("I've read",calls[0][0][0])
            self.assertEqual(calls[0][1]['idempotency_key'],f'processing-delay:{m.id}:1')
            self.assertEqual(store.get_receipts(m.id).completed_revisions,())

    def test_completed_source_gets_no_notice(self):
        with tempfile.TemporaryDirectory() as temp, InboxStore(Path(temp)/'db') as store:
            m=store.ingest(InboundPointer('event','T','C','100.1','100.1')).message
            store.mark_completed_if_revision(m.id,1)
            notify_unread_sources(store,SimpleNamespace(send_outgoing=lambda *a,**k:self.fail('completed')),now=m.updated_at+500)
