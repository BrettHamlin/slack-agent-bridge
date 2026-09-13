"""Owner-local command queue for the already authenticated Slack receiver.

Credentials stay in the receiver process. Requests contain only bounded service
operations, never code, credentials, arbitrary Slack methods, or file paths.
A timeout is not permission to retry a mutation under a different outgoing key.
"""
from argparse import Namespace
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

COMMANDS = frozenset(('source', 'read', 'send', 'reconcile', 'recover',
    'deliver-reminders', 'canvas-sync', 'inbox-card', 'responsibility-post-card',
    'responsibility-publish-result', 'repair-inbox-card-link', 'test-defer-inbox-card',
    'dispatch-reconcile', 'guardian-pending', 'guardian-approve', 'agent-publish', 'conversation-feed-import', 'conversation-feed-hide'))
FIELDS = frozenset(('command', 'message_id', 'revision', 'key', 'thread_ts', 'title',
    'conversation_url', 'responsibility_id', 'fence', 'due_at', 'payload_text',
    'retry_if_stopped', 'authority', 'conversation_title', 'conversation_emoji', 'conversation_preview', 'feed_root'))
IDENTITY = ('team_id', 'channel_id', 'owner_user_id', 'bot_user_id', 'workspace_domain')
MAX_PAYLOAD = 1024 * 1024
MAX_DISPATCH_KEY_LENGTH = 256


class ReceiverUnavailable(RuntimeError):
    pass


class CommandUncertain(RuntimeError):
    pass


class ServiceCommandFailed(RuntimeError):
    pass


def validate(payload, config):
    if not isinstance(payload, dict) or set(payload) != {'identity', 'args'}:
        raise ValueError('Invalid command envelope')
    if payload['identity'] != {key: config[key] for key in IDENTITY}:
        raise ValueError('Director identity mismatch')
    args = payload['args']
    if not isinstance(args, dict) or set(args) - FIELDS or args.get('command') not in COMMANDS:
        raise ValueError('Unsupported service command')
    if any(not isinstance(v, (str, int, float, type(None))) or isinstance(v, bool) for v in args.values()):
        raise ValueError('Invalid argument type')
    if args.get('command') == 'dispatch-reconcile':
        key = args.get('key')
        if not isinstance(key, str) or not key or len(key) > MAX_DISPATCH_KEY_LENGTH:
            raise ValueError('dispatch-reconcile requires a bounded job key')
        if args.get('retry_if_stopped') not in ('verify', 'retry'):
            raise ValueError('dispatch-reconcile retry intent is invalid')
    elif args.get('retry_if_stopped') is not None:
        raise ValueError('retry intent is only valid for dispatch-reconcile')
    if args.get('command') == 'guardian-approve':
        key = args.get('key')
        if not isinstance(key, str) or not key or len(key) > MAX_DISPATCH_KEY_LENGTH:
            raise ValueError('guardian-approve requires a bounded job key')
    if args.get('command') == 'guardian-pending' and any(
            args.get(name) is not None for name in FIELDS if name != 'command'):
        raise ValueError('guardian-pending accepts no arguments')
    if args.get('command') == 'agent-publish':
        authority = args.get('authority')
        text = args.get('payload_text')
        if (not isinstance(authority, str) or not authority or len(authority) > 256
                or not isinstance(text, str) or not text or len(text.encode()) > MAX_PAYLOAD):
            raise ValueError('agent-publish requires bounded authority and text')
        if (args.get('responsibility_id') is None) != (args.get('fence') is None):
            raise ValueError('agent-publish responsibility binding is incomplete')
    elif any(args.get(name) is not None for name in ('conversation_title', 'conversation_emoji', 'conversation_preview')):
        raise ValueError('conversation metadata is only valid for agent-publish')
    elif args.get('authority') is not None:
        raise ValueError('authority is only valid for agent-publish')
    values = dict.fromkeys(FIELDS)
    values.update(args)
    # Existing validation checks whether a caller supplied a body. It never
    # opens this marker: file contents are materialized by the CLI caller.
    values['file'] = bool(values['payload_text'] is not None)
    return Namespace(**values)


class ServiceQueue:
    def __init__(self, inbox_path):
        self.path = Path(inbox_path).parent / 'service-commands.sqlite3'
        os.umask(0o077)
        self.db = sqlite3.connect(self.path, timeout=5)
        os.chmod(self.path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS commands (
            id TEXT PRIMARY KEY, payload TEXT, state TEXT NOT NULL,
            created_at REAL NOT NULL, deadline REAL NOT NULL,
            result TEXT, finished_at REAL)''')
        self.db.commit()
        self._next_cleanup = 0

    def close(self):
        self.db.close()

    def recover_interrupted(self):
        # No automatic replay: Slack may have committed a send before a crash.
        with self.db:
            self.db.execute("UPDATE commands SET state='uncertain', payload=NULL, finished_at=? WHERE state='running'", (time.time(),))

    def process_one(self, execute, config):
        now = time.time()
        if now >= self._next_cleanup:
            with self.db:
                self.db.execute("DELETE FROM commands WHERE finished_at < ?", (now - 300,))
                self.db.execute("UPDATE commands SET state='expired',payload=NULL,finished_at=? WHERE state='pending' AND deadline<=?", (now, now))
            self._next_cleanup = now + 60
        # Idle checks are reads; do not take a database write lock four times
        # per second when there is no command to execute.
        row = self.db.execute("SELECT * FROM commands WHERE state='pending' AND deadline>? ORDER BY created_at LIMIT 1", (now,)).fetchone()
        if row is None:
            return False
        with self.db:
            claimed = self.db.execute("UPDATE commands SET state='running' WHERE id=? AND state='pending' AND deadline>?", (row['id'], time.time()))
            if claimed.rowcount != 1:
                return False
        try:
            args = validate(json.loads(row['payload']), config)
            result = {'ok': True, 'result': execute(args)}
        except Exception as error:
            # Exception text can contain tokens, source prose, or SDK responses.
            result = {'ok': False, 'error_type': type(error).__name__}
        with self.db:
            self.db.execute("UPDATE commands SET state='finished',payload=NULL,result=?,finished_at=? WHERE id=?", (json.dumps(result), time.time(), row['id']))
        return True


def submit_command(path, args, config, *, timeout=120):
    from .inbox import InboxStore
    with InboxStore(path) as inbox:
        ready = inbox.get_checkpoint('receiver.commands')
        stopped = inbox.get_checkpoint('receiver.stopped')
    if ready is None or time.time() - ready.updated_at > 120 or (stopped and stopped.updated_at > ready.updated_at):
        raise ReceiverUnavailable('Authenticated receiver command service unavailable')
    payload = {'identity': {key: config[key] for key in IDENTITY},
               'args': {key: getattr(args, key, None) for key in FIELDS}}
    validate(payload, config)
    encoded = json.dumps(payload)
    if len(encoded.encode()) > MAX_PAYLOAD:
        raise ValueError('Command too large')
    queue = ServiceQueue(path)
    request_id = str(uuid.uuid4())
    deadline = time.time() + timeout
    try:
        with queue.db:
            queue.db.execute('INSERT INTO commands VALUES (?,?,\'pending\',?,?,NULL,NULL)', (request_id, encoded, time.time(), deadline))
        while time.time() < deadline:
            row = queue.db.execute('SELECT state,result FROM commands WHERE id=?', (request_id,)).fetchone()
            if row['state'] == 'finished':
                result = json.loads(row['result'])
                with queue.db:
                    queue.db.execute('DELETE FROM commands WHERE id=?', (request_id,))
                if not result['ok']:
                    raise ServiceCommandFailed(result['error_type'])
                return result['result']
            if row['state'] in ('uncertain', 'expired'):
                raise CommandUncertain('Reconcile with the original stable key')
            time.sleep(0.1)
        with queue.db:
            queue.db.execute("UPDATE commands SET state='expired',payload=NULL,finished_at=? WHERE id=? AND state='pending'", (time.time(), request_id))
        raise CommandUncertain('Receiver command timed out; reconcile before retry')
    finally:
        queue.close()
