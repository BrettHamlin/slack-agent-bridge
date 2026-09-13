"""Deterministic visibility for saved, unanswered owner messages; no model calls."""
import time

UNREAD_NOTICE_SECONDS = 120


def notify_unread_sources(store, service, *, now=None):
    timestamp = time.time() if now is None else now
    attempted = 0
    for message in store.list_open_messages():
        if message.event_type == 'message_deleted':
            continue
        read = message.revision in store.get_receipts(message.id).read_revisions
        if timestamp - message.updated_at < UNREAD_NOTICE_SECONDS:
            continue
        key = f'{"processing-delay" if read else "intake-delay"}:{message.id}:{message.revision}'
        if store.get_checkpoint(key):
            continue
        # The outbox preserves the stable key across restart and uncertain sends.
        result = service.send_outgoing(
            ("I've read your message, but processing is taking longer than expected. It is still saved; you don't need to resend it."
             if read else "Your message is saved, but the manager hasn't read it yet. It remains queued; you don't need to resend it."),
            idempotency_key=key, thread_ts=message.thread_ts or message.source_ts)
        if result.state == 'sent':
            store.set_checkpoint(key, 'notified', now=timestamp)
        attempted += 1
        if attempted >= 5:
            break
