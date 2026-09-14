"""Deterministic, durable dispatch of real Slack work to an agent runtime.

No model is invoked for idle checks. Each Slack thread has its own persisted
Codex session and process lock. Only verified delivery plus a completion receipt
counts as an answered source; a model turn ending does not.
"""
import fcntl
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
import uuid

from .responsibilities import ResponsibilityStore
from .agent_gateway import AgentGateway, AgentProfile, CapabilityError, GatewayError


@dataclass
class DispatchPreparation:
    """ACP setup plus independently fetched, verified prompt context."""

    gateway: object
    context: Future
    authority: str
    owner: object = None

    @property
    def future(self):
        return self.gateway.future

    @property
    def driver(self):
        return self.gateway.driver


def _validate_owner_action(value):
    """Validate optional owner-review metadata without turning it into work."""
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {'title', 'detail'}:
        return None
    title, detail = value['title'], value['detail']
    if (not isinstance(title, str) or not title.strip() or len(title) > 75
            or not isinstance(detail, str) or len(detail) > 300
            or '\n' in title or '\n' in detail):
        return None
    return title, detail


class Dispatcher:
    def __init__(self, project, config, inbox, service, *, feed=None, feed_wake=None, needs_you=None, needs_you_wake=None):
        self.project = Path(project)
        self.config = config
        self.options = config.get('dispatcher', {})
        self.inbox = inbox
        self.service = service
        self.feed = feed
        self.feed_wake = feed_wake
        self.feed_error = ''
        self.needs_you = needs_you
        self.needs_you_wake = needs_you_wake
        self.needs_you_error = ''
        self.state_directory = (self.project / config['database_path']).resolve().parent
        self.directory = self.state_directory / 'dispatch'
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.directory / 'jobs.sqlite3', timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS jobs (
                key TEXT PRIMARY KEY, message_id INTEGER, revision INTEGER,
                root TEXT NOT NULL, kind TEXT NOT NULL, responsibility_id TEXT,
                state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL, started_at REAL, finished_at REAL,
                retry_at REAL NOT NULL DEFAULT 0, stdout_path TEXT,
                error_code TEXT, reply_ts TEXT);
            CREATE TABLE IF NOT EXISTS sessions (root TEXT PRIMARY KEY, session_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_sessions (
                root TEXT PRIMARY KEY, profile_id TEXT NOT NULL, backend TEXT NOT NULL,
                session_id TEXT NOT NULL, migrated_from TEXT, created_at REAL NOT NULL,
                updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_reply_authorities (
                authority TEXT PRIMARY KEY, job_key TEXT NOT NULL,
                kind TEXT NOT NULL, root TEXT NOT NULL,
                message_id INTEGER, revision INTEGER,
                responsibility_id TEXT, execution_fence TEXT,
                session_id TEXT, generation INTEGER, turn_id TEXT,
                text_digest TEXT, conversation_title TEXT, conversation_emoji TEXT, conversation_preview TEXT,
                owner_action_title TEXT, owner_action_detail TEXT,
                state TEXT NOT NULL, created_at REAL NOT NULL, published_at REAL);
            CREATE TABLE IF NOT EXISTS guardian_reply_approvals (
                approval_id TEXT PRIMARY KEY, job_key TEXT NOT NULL UNIQUE,
                authority TEXT NOT NULL UNIQUE, root TEXT NOT NULL,
                session_id TEXT NOT NULL, generation INTEGER NOT NULL, turn_id TEXT NOT NULL,
                native_turn_id TEXT NOT NULL,
                review_id TEXT NOT NULL, fingerprint TEXT NOT NULL, payload_digest TEXT NOT NULL,
                proposal_text TEXT, conversation_title TEXT, conversation_emoji TEXT, conversation_preview TEXT,
                state TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL,
                resolved_at REAL);
        ''')
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(jobs)')}
        self.db.execute('CREATE INDEX IF NOT EXISTS agent_reply_authorities_job ON agent_reply_authorities(job_key)')
        self.db.execute('CREATE INDEX IF NOT EXISTS guardian_reply_approvals_state ON guardian_reply_approvals(state,expires_at)')
        for name in ('notice_at', 'notice_retry_at', 'runtime', 'agent_profile', 'agent_session_id',
                     'agent_turn_id', 'agent_generation', 'agent_group_id',
                     'responsibility_fence', 'agent_selection'):
            if name not in columns:
                definition = ('TEXT' if name in ('runtime', 'agent_profile', 'agent_session_id', 'agent_turn_id')
                              else 'INTEGER' if name in ('agent_generation', 'agent_group_id') else 'REAL')
                if name in ('responsibility_fence', 'agent_selection'):
                    definition = 'TEXT'
                self.db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {definition}')
        if 'agent_turn_ended_at' not in columns:
            self.db.execute('ALTER TABLE jobs ADD COLUMN agent_turn_ended_at REAL')
        authority_columns = {row[1] for row in self.db.execute('PRAGMA table_info(agent_reply_authorities)')}
        if 'text_digest' not in authority_columns:
            self.db.execute('ALTER TABLE agent_reply_authorities ADD COLUMN text_digest TEXT')
        for name in ('conversation_title', 'conversation_emoji', 'conversation_preview', 'owner_action_title', 'owner_action_detail'):
            if name not in authority_columns:
                self.db.execute(f'ALTER TABLE agent_reply_authorities ADD COLUMN {name} TEXT')
        approval_columns = {row[1] for row in self.db.execute('PRAGMA table_info(guardian_reply_approvals)')}
        if 'native_turn_id' not in approval_columns:
            self.db.execute('ALTER TABLE guardian_reply_approvals ADD COLUMN native_turn_id TEXT')
        for name in ('proposal_text', 'conversation_title', 'conversation_emoji', 'conversation_preview'):
            if name not in approval_columns:
                self.db.execute(f'ALTER TABLE guardian_reply_approvals ADD COLUMN {name} TEXT')
        self.db.commit()
        self.children = {}
        self.acp_locks = {}
        self.preparations = {}
        self._context_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='director-context')
        self.gateway = None
        self._gateways = {}
        self._profiles = {}
        self._acp_profile = None
        if self.options.get('runtime', 'legacy') == 'acp':
            acp = self.options.get('acp', {})
            command = acp.get('command')
            if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
                # Keep construction side-effect free. A missing installation is
                # reported as a durable failed dispatch, never a CLI fallback.
                self._acp_profile = GatewayError('runtime_unavailable:missing_acp_command')
            else:
                mode = str(acp.get('initial_agent_mode', 'agent'))
                if mode != 'agent':
                    self._acp_profile = GatewayError('invalid_acp_agent_mode')
                else:
                    runtime_config = {
                        'model': acp.get('model', self.options.get('model', 'gpt-6-astra')),
                        'model_reasoning_effort': acp.get('reasoning_effort', 'medium'),
                        'approval_policy': 'on-request',
                        'sandbox_mode': 'workspace-write',
                        'mcp_servers.1password.enabled': False,
                    }
                    self._acp_profile = AgentProfile(
                        str(acp.get('profile', 'codex-default')),
                        str(acp.get('backend', 'codex-acp')),
                        tuple(command),
                        frozenset(acp.get('required_capabilities', ('load',))),
                        (('INITIAL_AGENT_MODE', mode), ('CODEX_CONFIG', json.dumps(runtime_config, separators=(',', ':')))),
                        (('mode', mode), ('model', str(runtime_config['model'])),
                         ('reasoning_effort', str(runtime_config['model_reasoning_effort']))),
                        float(acp.get('startup_timeout_seconds', 30)),
                        float(acp.get('request_timeout_seconds', 30)),
                        dynamic_selection=bool(self.options.get('model_selection', {}).get('enabled', False)),
                    )
            # A newly started receiver cannot prove what a previously resident
            # ACP process was doing. Preserve the original outgoing key and
            # require reconciliation rather than replaying it.
        if isinstance(self._acp_profile, AgentProfile):
            self._profiles[self._acp_profile.identifier] = self._acp_profile
        selection_options = self.options.get('model_selection', {})
        if selection_options.get('enabled'):
            claude = self.options.get('claude', {})
            command = claude.get('command')
            if isinstance(command, list) and command and all(isinstance(item, str) and item for item in command):
                profile = AgentProfile(
                    str(claude.get('profile', 'claude-default')), 'claude-code', tuple(command),
                    expected_settings=(('mode', 'auto'), ('model', 'claude-opus-5'), ('reasoning_effort', 'high')),
                    request_timeout_seconds=float(claude.get('timeout_seconds', 300)),
                    dynamic_selection=True)
                if profile.identifier in self._profiles:
                    raise GatewayError('duplicate_agent_profile')
                self._profiles[profile.identifier] = profile
        restart_inactive_keys = [str(row['job_key']) for row in self.db.execute(
            "SELECT job_key FROM guardian_reply_approvals WHERE state IN ('pending','submitting')"
        ).fetchall()]
        with self.db:
            self.db.execute("UPDATE jobs SET state='blocked',error_code='acp_runtime_recovery_required' WHERE state='running' AND runtime='acp'")
            self.db.execute("UPDATE jobs SET state='retry',retry_at=0,error_code='acp_prepare_interrupted' WHERE state='preparing' AND runtime='acp'")
            # The adapter owns the opaque native denial event in memory. A
            # receiver restart cannot safely recreate it or approve anything.
            self.db.execute(
                "UPDATE guardian_reply_approvals SET state='stale',resolved_at=? WHERE state IN ('pending','submitting')",
                (time.time(),),
            )
            self.db.execute(
                """UPDATE agent_reply_authorities SET state='closed'
                   WHERE authority IN (SELECT authority FROM guardian_reply_approvals WHERE state='stale')
                     AND state='approval_pending'"""
            )
            self.db.execute(
                """UPDATE jobs SET error_code='guardian_approval_adapter_restart'
                   WHERE state='blocked' AND error_code='guardian_approval_pending'
                     AND key IN (SELECT job_key FROM guardian_reply_approvals WHERE state='stale')"""
            )
        for key in restart_inactive_keys:
            self._project_guardian_terminal(key, 'restart_inactive')
        self.last_tick = 0
        self.last_health = 0

    def enqueue(self, now):
        with self.db:
            for message in self.inbox.list_open_messages():
                key = f'source-{message.id}-r{message.revision}'
                self.db.execute('INSERT OR IGNORE INTO jobs(key,message_id,revision,root,kind,state,created_at) VALUES (?,?,?,?,?,?,?)',
                    (key, message.id, message.revision, message.thread_ts or message.source_ts, 'source', 'pending', now))
        with ResponsibilityStore(self.project / self.config['database_path']) as store:
            for work in store.list(runnable_only=True):
                # A source worker owns initial responsibility creation/execution.
                # Only dispatch independent work when its source is accounted for.
                if work.source_message_id:
                    receipts = self.inbox.get_receipts(work.source_message_id)
                    if work.source_revision not in receipts.completed_revisions:
                        continue
                key = f'work-{work.id}-v{work.version}'
                with self.db:
                    self.db.execute('INSERT OR IGNORE INTO jobs(key,root,kind,responsibility_id,state,created_at) VALUES (?,?,?,?,?,?)',
                        (key, work.current_thread or 'responsibilities', 'work', work.id, 'pending', now))

    def _lock(self, root):
        name = hashlib.sha256(root.encode()).hexdigest()[:24]
        handle = open(self.directory / (name + '.lock'), 'a')
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return None
        return handle

    def _delivered(self, job):
        if job['kind'] != 'source':
            with ResponsibilityStore(self.project / self.config['database_path']) as store:
                work = store.get(job['responsibility_id'])
                return work.state in ('completed', 'waiting_input', 'deferred', 'cancelled')
        with self.service._lock:
            row = self.service._connection.execute('SELECT state,slack_ts FROM slack_outbox WHERE idempotency_key=?', ('dispatch-answer:' + job['key'],)).fetchone()
        if not row or row['state'] != 'sent' or not row['slack_ts']:
            return False
        receipts = self.inbox.get_receipts(job['message_id'])
        if job['revision'] in receipts.completed_revisions:
            return True
        binding = self.db.execute(
            """SELECT authority,responsibility_id,execution_fence FROM agent_reply_authorities
               WHERE job_key=? AND kind='source' AND responsibility_id IS NOT NULL
                 AND execution_fence IS NOT NULL
               ORDER BY created_at DESC LIMIT 1""",
            (job['key'],),
        ).fetchone()
        if binding is not None:
            # A source-turn continuation publishes one shared outbox result for
            # both source and responsibility. Recover its durable work result
            # under the original execution fence before completing the source.
            try:
                with ResponsibilityStore(self.project / self.config['database_path']) as store:
                    work = store.get(binding['responsibility_id'])
                    if work.state != 'completed':
                        if not store.execution_gate(binding['responsibility_id'], binding['execution_fence']):
                            return False
                        store.complete(binding['responsibility_id'], binding['execution_fence'])
            except Exception:
                return False
            with self.db:
                self.db.execute(
                    "UPDATE agent_reply_authorities SET state='published',published_at=? WHERE authority=?",
                    (time.time(), binding['authority']),
                )
        # Slack has already confirmed the stable job outbox key. A receiver
        # crash between that acknowledgement and the local receipt must recover
        # completion without another agent turn or Slack send.
        return bool(self.inbox.mark_completed_if_revision(job['message_id'], job['revision']))

    @staticmethod
    def _runtime_group_gone(group_id):
        """Return true only when the recorded owned process group is absent.

        This method deliberately sends no signal. A surviving or inaccessible
        group remains uncertain, so reconciliation cannot release its root.
        """
        if not group_id:
            return False
        try:
            os.killpg(int(group_id), 0)
        except ProcessLookupError:
            return True
        except (PermissionError, ValueError, TypeError):
            return False
        return False

    def _source_superseded(self, job):
        if job['kind'] != 'source':
            return False
        current = self.inbox.get_message(job['message_id'])
        return current.revision != job['revision'] or current.event_type == 'message_deleted'

    def _guardian_approval(self, job_key, *, states=('pending',)):
        placeholders = ','.join('?' for _ in states)
        return self.db.execute(
            f"SELECT * FROM guardian_reply_approvals WHERE job_key=? AND state IN ({placeholders})",
            (job_key, *states),
        ).fetchone()

    @staticmethod
    def _guardian_candidate(event):
        """Validate the adapter's exact current-denial and safe display fields."""
        tool = ((event.detail or {}).get('tool') or {}) if isinstance(event.detail, dict) else {}
        raw = tool.get('raw_output') if isinstance(tool, dict) else None
        candidate = raw.get('directorApproval') if isinstance(raw, dict) else None
        expected = {'sessionId', 'turnId', 'reviewId', 'fingerprint', 'authority', 'payloadDigest', 'replyDisplay'}
        opaque = {'sessionId', 'turnId', 'reviewId', 'fingerprint', 'authority', 'payloadDigest'}
        if not isinstance(candidate, dict) or set(candidate) != expected:
            return None
        if not all(isinstance(candidate[name], str) and candidate[name] for name in opaque):
            return None
        digest = candidate['payloadDigest']
        if len(digest) != 64 or any(char not in '0123456789abcdef' for char in digest):
            return None
        display = candidate['replyDisplay']
        expected_display = {'text', 'conversation_title', 'conversation_emoji', 'conversation_preview'}
        if not isinstance(display, dict) or set(display) != expected_display or not isinstance(display['text'], str) or not display['text']:
            return None
        if any(display[name] is not None and not isinstance(display[name], str)
               for name in expected_display - {'text'}):
            return None
        payload = {'text': display['text'], 'responsibility_id': None, 'execution_fence': None}
        if any(display[name] is not None for name in expected_display - {'text'}):
            payload.update(
                conversation_title=display['conversation_title'], conversation_emoji=display['conversation_emoji'],
                conversation_preview=display['conversation_preview'],
            )
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        if hashlib.sha256(encoded.encode()).hexdigest() != digest:
            return None
        return candidate

    def _capture_guardian_reply_denial(self, job, event, now):
        """Durably stage one current source reply denial, never an arbitrary action.

        The adapter validates the native event and holds it in its own process.
        Director receives only an opaque one-shot handle and binds it to the
        currently active source authority before the terminal event closes it.
        """
        profile = self._profiles.get(job['agent_profile'])
        if profile is None or profile.backend != 'codex-acp':
            return False
        candidate = self._guardian_candidate(event)
        if candidate is None or job['kind'] != 'source' or job['responsibility_id'] is not None:
            return False
        # Native App Server turns are UUIDs while Director's driver turn is a
        # local generation counter. Progress updates carry no local turn ID.
        # Authority is therefore the cross-layer correlation fence; replayed
        # native events carry an old authority and cannot match this attempt.
        if candidate['sessionId'] != job['agent_session_id'] or candidate['sessionId'] != event.session_id:
            return False
        authority = self.db.execute(
            """SELECT * FROM agent_reply_authorities WHERE job_key=? AND kind='source'
               AND state='active' ORDER BY created_at DESC LIMIT 1""",
            (job['key'],),
        ).fetchone()
        if (authority is None or authority['authority'] != candidate['authority']
                or authority['session_id'] != job['agent_session_id']
                or authority['generation'] != job['agent_generation']
                or authority['turn_id'] != job['agent_turn_id']
                or authority['text_digest'] is not None
                or authority['responsibility_id'] is not None or authority['execution_fence'] is not None):
            return False
        current = self.inbox.get_message(job['message_id'])
        if (current.revision != job['revision'] or current.event_type == 'message_deleted'
                or (current.thread_ts or current.source_ts) != job['root']):
            return False
        existing = self.db.execute(
            'SELECT * FROM guardian_reply_approvals WHERE job_key=?', (job['key'],)
        ).fetchone()
        if existing is not None:
            return bool(
                existing['state'] == 'pending' and existing['authority'] == authority['authority']
                and existing['review_id'] == candidate['reviewId']
                and existing['fingerprint'] == candidate['fingerprint']
            )
        expires_at = now + float(self.options.get('guardian_approval_ttl_seconds', 600))
        approval_id = uuid.uuid4().hex
        with self.db:
            self.db.execute(
                """INSERT INTO guardian_reply_approvals
                   (approval_id,job_key,authority,root,session_id,generation,turn_id,review_id,
                    native_turn_id,fingerprint,payload_digest,proposal_text,conversation_title,
                    conversation_emoji,conversation_preview,state,created_at,expires_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'pending', ?,?)""",
                (approval_id, job['key'], authority['authority'], job['root'], job['agent_session_id'],
                 job['agent_generation'], job['agent_turn_id'], candidate['reviewId'],
                 candidate['turnId'], candidate['fingerprint'], candidate['payloadDigest'],
                 candidate['replyDisplay']['text'], candidate['replyDisplay']['conversation_title'],
                 candidate['replyDisplay']['conversation_emoji'], candidate['replyDisplay']['conversation_preview'],
                 now, expires_at),
            )
            locked = self.db.execute(
                """UPDATE agent_reply_authorities SET text_digest=?,state='approval_pending'
                   WHERE authority=? AND state='active' AND text_digest IS NULL""",
                (candidate['payloadDigest'], authority['authority']),
            )
            if locked.rowcount != 1:
                raise GatewayError('guardian_approval_authority_race')
        return True

    def _project_guardian_pending(self, approval):
        """Best-effort Card-B projection after the job is durably blocked."""
        if self.feed is None or not getattr(self.feed, 'configured', False):
            return
        try:
            title = approval['conversation_title'] if isinstance(approval['conversation_title'], str) else 'Pending reply review'
            emoji = approval['conversation_emoji'] if isinstance(approval['conversation_emoji'], str) else '🔐'
            preview = approval['conversation_preview'] if isinstance(approval['conversation_preview'], str) else 'Exact reply awaiting review.'
            self.feed.record_guardian_pending(
                root=approval['root'], job_key=approval['job_key'], title=title, emoji=emoji, preview=preview,
                proposal_text=approval['proposal_text'], expires_at=float(approval['expires_at']),
            )
            self.feed_error = ''
            if self.feed_wake:
                self.feed_wake()
        except Exception as error:
            # Projection failure never releases or weakens the native approval.
            self.feed_error = type(error).__name__

    def _project_guardian_terminal(self, job_key, state):
        if self.feed is None or not getattr(self.feed, 'configured', False):
            return
        try:
            if self.feed.set_guardian_approval_state(job_key, state) and self.feed_wake:
                self.feed_wake()
        except Exception as error:
            self.feed_error = type(error).__name__

    def reconcile_job(self, key, retry_if_stopped=False, *, operator_requested=False):
        """Safely settle one blocked ACP job through the resident receiver.

        Verified delivery and source supersession are retained as evidence, but
        neither releases the root while the old owned runtime group might still
        act. Explicit retry is allowed only after that group is proven gone or
        a correlated ACP prompt response proves the old turn ended.
        Session bindings and the stable outbox key are never removed here.
        """
        job = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
        if not job:
            return {'key': key, 'outcome': 'not_found'}
        approval = self._guardian_approval(key, states=('pending', 'submitting', 'uncertain'))
        if approval is not None:
            return {'key': key, 'outcome': 'guardian_approval_' + approval['state']}
        # This fence was created by the legacy launcher before any process or
        # prompt existed. Once ACP is restored it is safe to resume using the
        # retained durable session binding; legacy mode deliberately keeps it
        # fenced so it cannot silently roll back into CLI execution.
        if job['state'] == 'blocked' and job['error_code'] == 'acp_session_rollback_required':
            if self.options.get('runtime', 'legacy') != 'acp':
                return {'key': key, 'outcome': 'rollback_fenced'}
            with self.db:
                self.db.execute(
                    "UPDATE jobs SET state='retry',finished_at=NULL,retry_at=0,error_code='acp_rollback_retry_ready', "
                    "notice_at=NULL,notice_retry_at=0 WHERE key=? AND state='blocked'",
                    (key,),
                )
            return {'key': key, 'outcome': 'rollback_retry_ready'}
        if job['runtime'] != 'acp' or job['state'] != 'blocked':
            return {'key': key, 'outcome': 'not_blocked', 'state': job['state']}
        group_gone = self._runtime_group_gone(job['agent_group_id'])
        turn_ended = job['agent_turn_ended_at'] is not None
        runtime_released = group_gone or turn_ended
        used = self._guardian_approval(key, states=('used',))
        if (used is not None and job['error_code'] == 'guardian_approved_retry_unpublished'):
            # A native approval is spent even when its one normal retry fails.
            # Never turn this row into a retry. Only an explicit receiver
            # command may release the root after the old runtime is known gone.
            if self._delivered(job):
                if not runtime_released:
                    return {
                        'key': key,
                        'outcome': 'delivered_but_runtime_active',
                        'runtime_group_gone': False,
                    }
                with self.db:
                    self.db.execute(
                        """UPDATE jobs SET state='done',finished_at=?,error_code=NULL WHERE key=?
                           AND state='blocked' AND error_code='guardian_approved_retry_unpublished'""",
                        (time.time(), key),
                    )
                return {
                    'key': key,
                    'outcome': 'done',
                    'runtime_group_gone': group_gone,
                    'turn_ended': turn_ended,
                }
            if not runtime_released or not operator_requested:
                return {
                    'key': key,
                    'outcome': 'guardian_approval_used',
                    'runtime_group_gone': group_gone,
                    'turn_ended': turn_ended,
                }
            with self.db:
                self.db.execute(
                    """UPDATE jobs SET state='failed',finished_at=? WHERE key=?
                       AND state='blocked' AND error_code='guardian_approved_retry_unpublished'""",
                    (time.time(), key),
                )
            return {
                'key': key,
                'outcome': 'guardian_approval_spent_settled',
                'runtime_group_gone': group_gone,
                'turn_ended': turn_ended,
            }
        delivered = self._delivered(job)
        superseded = self._source_superseded(job)
        if delivered or superseded:
            settled = 'done' if delivered else 'superseded'
            if not runtime_released:
                return {
                    'key': key,
                    'outcome': 'delivered_but_runtime_active' if delivered else 'superseded_but_runtime_active',
                    'runtime_group_gone': False,
                }
            with self.db:
                self.db.execute(
                    'UPDATE jobs SET state=?,finished_at=?,error_code=NULL WHERE key=? AND state=\'blocked\'',
                    (settled, time.time(), key),
                )
            return {'key': key, 'outcome': settled, 'runtime_group_gone': group_gone,
                    'turn_ended': turn_ended}
        if retry_if_stopped and runtime_released:
            with self.db:
                self.db.execute(
                    "UPDATE jobs SET state='retry',finished_at=NULL,retry_at=0,error_code='acp_reconcile_retry_ready', "
                    "notice_at=NULL,notice_retry_at=0 "
                    "WHERE key=? AND state='blocked'",
                    (key,),
                )
            return {'key': key, 'outcome': 'retry_ready', 'runtime_group_gone': group_gone,
                    'turn_ended': turn_ended}
        return {
            'key': key,
            'outcome': 'unresolved',
            'runtime_group_gone': group_gone,
            'turn_ended': turn_ended,
            'retry_requested': bool(retry_if_stopped),
        }

    def _recover_pending_guardian_feed(self):
        """Retry only the idempotent projection of already-fenced pending work."""
        if self.feed is None or not getattr(self.feed, 'configured', False):
            return
        rows = self.db.execute(
            """SELECT a.* FROM guardian_reply_approvals AS a JOIN jobs AS j ON j.key=a.job_key
               WHERE a.state='pending' AND j.state='blocked' AND j.error_code='guardian_approval_pending'"""
        ).fetchall()
        for approval in rows:
            self._project_guardian_pending(approval)

    def _expire_guardian_approvals(self, now):
        """Fail closed after an unapproved native event can no longer be fresh."""
        rows = self.db.execute(
            "SELECT * FROM guardian_reply_approvals WHERE state='pending' AND expires_at<=?", (now,)
        ).fetchall()
        expired_jobs = []
        for approval in rows:
            with self.db:
                self.db.execute(
                    "UPDATE guardian_reply_approvals SET state='expired',resolved_at=? "
                    "WHERE approval_id=? AND state='pending' AND expires_at<=?",
                    (now, approval['approval_id'], now),
                )
                self.db.execute(
                    "UPDATE agent_reply_authorities SET state='closed' WHERE authority=? AND state='approval_pending'",
                    (approval['authority'],),
                )
                self.db.execute(
                    "UPDATE jobs SET state='failed',finished_at=?,error_code='guardian_approval_expired' WHERE key=? AND state='blocked' "
                    "AND error_code='guardian_approval_pending'",
                    (now, approval['job_key']),
                )
            current = self.db.execute('SELECT * FROM jobs WHERE key=?', (approval['job_key'],)).fetchone()
            if current is not None and current['state'] == 'failed':
                expired_jobs.append(current)
        for job in expired_jobs:
            self._project_guardian_terminal(job['key'], 'expired')
            self._notify_failure(job, now)
        self._recover_pending_guardian_feed()

    def approve_guardian_reply(self, key):
        """Explicitly approve one current native denial and submit its one retry.

        This receiver command never accepts event data or an authority from its
        caller. The native adapter owns the event and accepts only its opaque
        review/fingerprint pair. A stale source, restart, altered payload, or
        second invocation remains blocked.
        """
        now = time.time()
        approval = self._guardian_approval(key)
        if approval is None:
            return {'key': key, 'outcome': 'guardian_approval_not_pending'}
        if approval['expires_at'] <= now:
            self._expire_guardian_approvals(now)
            return {'key': key, 'outcome': 'guardian_approval_expired'}
        job = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
        authority = self.db.execute(
            'SELECT * FROM agent_reply_authorities WHERE authority=?', (approval['authority'],)
        ).fetchone()
        if (job is None or authority is None or job['kind'] != 'source'
                or job['state'] != 'blocked' or job['error_code'] != 'guardian_approval_pending'
                or authority['state'] != 'approval_pending'
                or authority['text_digest'] != approval['payload_digest']):
            return {'key': key, 'outcome': 'guardian_approval_stale'}
        current = self.inbox.get_message(job['message_id'])
        if (current.revision != job['revision'] or current.event_type == 'message_deleted'
                or (current.thread_ts or current.source_ts) != job['root']):
            with self.db:
                self.db.execute("UPDATE guardian_reply_approvals SET state='stale',resolved_at=? WHERE approval_id=? AND state='pending'",
                                (now, approval['approval_id']))
                self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE authority=? AND state='approval_pending'",
                                (approval['authority'],))
                self.db.execute(
                    """UPDATE jobs SET state='superseded',finished_at=?,error_code=NULL WHERE key=?
                       AND state='blocked' AND error_code='guardian_approval_pending'""",
                    (now, key),
                )
            return {'key': key, 'outcome': 'guardian_approval_source_stale'}
        profile = self._profiles.get(job['agent_profile'])
        if profile is None or profile.backend != 'codex-acp':
            return {'key': key, 'outcome': 'guardian_approval_backend_invalid'}
        gateway = self._gateways.get(profile.identifier) or self.gateway
        if gateway is None or gateway.driver is None or not gateway.driver.alive:
            return {'key': key, 'outcome': 'guardian_approval_runtime_unavailable'}
        binding = gateway.binding(job['root'])
        if (binding is None or binding.session_id != approval['session_id']
                or job['agent_session_id'] != approval['session_id']
                or job['agent_generation'] != approval['generation']
                or gateway.driver.generation != approval['generation']):
            return {'key': key, 'outcome': 'guardian_approval_runtime_stale'}
        try:
            context = self._prepare_context(job)['text']
        except Exception:
            return {'key': key, 'outcome': 'guardian_approval_context_unavailable'}
        refreshed = self.inbox.get_message(job['message_id'])
        if (refreshed.revision != job['revision'] or refreshed.event_type == 'message_deleted'
                or (refreshed.thread_ts or refreshed.source_ts) != job['root']):
            with self.db:
                self.db.execute("UPDATE guardian_reply_approvals SET state='stale',resolved_at=? WHERE approval_id=? AND state='pending'",
                                (time.time(), approval['approval_id']))
                self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE authority=? AND state='approval_pending'",
                                (approval['authority'],))
                self.db.execute(
                    """UPDATE jobs SET state='superseded',finished_at=?,error_code=NULL WHERE key=?
                       AND state='blocked' AND error_code='guardian_approval_pending'""",
                    (time.time(), key),
                )
            return {'key': key, 'outcome': 'guardian_approval_source_stale'}
        handle = self._lock(job['root'])
        if handle is None:
            return {'key': key, 'outcome': 'guardian_approval_root_busy'}
        try:
            with self.db:
                claimed = self.db.execute(
                    "UPDATE guardian_reply_approvals SET state='submitting' WHERE approval_id=? AND state='pending' AND expires_at>?",
                    (approval['approval_id'], time.time()),
                )
            if claimed.rowcount != 1:
                return {'key': key, 'outcome': 'guardian_approval_not_pending'}
            try:
                gateway.approve_guardian_denied_action(binding, approval['review_id'], approval['fingerprint'])
            except GatewayError:
                with self.db:
                    self.db.execute("UPDATE guardian_reply_approvals SET state='uncertain',resolved_at=? WHERE approval_id=?",
                                    (time.time(), approval['approval_id']))
                    self.db.execute("UPDATE agent_reply_authorities SET state='approval_uncertain' WHERE authority=? AND state='approval_pending'",
                                    (approval['authority'],))
                    self.db.execute("UPDATE jobs SET error_code='guardian_approval_uncertain' WHERE key=? AND state='blocked'",
                                    (key,))
                return {'key': key, 'outcome': 'guardian_approval_uncertain'}

            def reserve_turn(turn):
                newest = self.inbox.get_message(job['message_id'])
                if (newest.revision != job['revision'] or newest.event_type == 'message_deleted'
                        or (newest.thread_ts or newest.source_ts) != job['root']):
                    raise GatewayError('guardian_approval_source_stale')
                with self.db:
                    self.db.execute(
                        """UPDATE jobs SET state='running',started_at=?,finished_at=NULL,error_code=NULL,
                           agent_turn_id=?,agent_turn_ended_at=NULL WHERE key=? AND state='blocked'""",
                        (time.time(), str(turn), key),
                    )
                    self.db.execute(
                        """UPDATE agent_reply_authorities SET state='active',turn_id=?
                           WHERE authority=? AND state='approval_pending'""",
                        (str(turn), approval['authority']),
                    )
                    self.db.execute(
                        "UPDATE guardian_reply_approvals SET state='used',resolved_at=? WHERE approval_id=? AND state='submitting'",
                        (time.time(), approval['approval_id']),
                    )

            retry_prompt = self.prompt(job, context, approval['authority']) + (
                "\nThe owner has granted one native approval for the immediately preceding denied "
                "publish_reply. Invoke publish_reply once with exactly the same arguments as that denied "
                "call, including reply text and every conversation metadata field. Do not regenerate or alter it; "
                "the receiver accepts only the approved payload."
            )
            submit_options = {'on_turn': reserve_turn}
            if job['agent_selection']:
                from .model_selection import Selection
                submit_options['selection'] = Selection(**json.loads(job['agent_selection']))
            gateway.submit(binding, retry_prompt, **submit_options)
            self.acp_locks[key] = handle
            handle = None
            return {'key': key, 'outcome': 'guardian_approval_submitted'}
        except Exception:
            # Native approval may already be committed. It is never repeated.
            current_job = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
            if current_job is not None and current_job['state'] == 'running':
                self._block_acp_job(current_job, 'guardian_approval_submit_uncertain', time.time())
            with self.db:
                self.db.execute("UPDATE guardian_reply_approvals SET state='uncertain',resolved_at=? WHERE approval_id=? AND state='submitting'",
                                (time.time(), approval['approval_id']))
                self.db.execute("UPDATE agent_reply_authorities SET state='approval_uncertain' WHERE authority=? AND state='approval_pending'",
                                (approval['authority'],))
                self.db.execute("UPDATE jobs SET error_code='guardian_approval_uncertain' WHERE key=? AND state='blocked'",
                                (key,))
            return {'key': key, 'outcome': 'guardian_approval_uncertain'}
        finally:
            if handle is not None:
                handle.close()

    def pending_guardian_approvals(self):
        """Return only local keys and expiry metadata for explicit owner action."""
        now = time.time()
        rows = self.db.execute(
            """SELECT a.job_key AS key,a.root,a.expires_at FROM guardian_reply_approvals AS a
               JOIN jobs AS j ON j.key=a.job_key
               WHERE a.state='pending' AND a.expires_at>? AND j.state='blocked'
                 AND j.error_code='guardian_approval_pending'
               ORDER BY a.created_at""",
            (now,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _session(self, job):
        if job['runtime'] == 'acp':
            return
        if not job['stdout_path']:
            return
        try:
            with open(job['stdout_path']) as stream:
                for index, line in enumerate(stream):
                    if index >= 10:
                        break
                    event = json.loads(line)
                    if event.get('type') == 'thread.started':
                        with self.db:
                            self.db.execute('INSERT OR REPLACE INTO sessions VALUES (?,?)', (job['root'], event['thread_id']))
                        return
        except (OSError, ValueError):
            return

    def _finish(self, job, now):
        self._session(job)
        current = self.inbox.get_message(job['message_id']) if job['kind'] == 'source' else None
        approval = self._guardian_approval(job['key'])
        if approval is not None:
            # The native approval is explicitly owner-triggered. Preserve the
            # one current authority/digest through this terminal turn instead
            # of turning a denied reply into a generic automatic retry.
            valid_source = bool(
                current and current.revision == job['revision']
                and current.event_type != 'message_deleted'
                and (current.thread_ts or current.source_ts) == job['root']
            )
            with self.db:
                if valid_source:
                    self.db.execute(
                        "UPDATE jobs SET state='blocked',finished_at=?,error_code='guardian_approval_pending' "
                        "WHERE key=? AND state='running'",
                        (now, job['key']),
                    )
                    self.db.execute(
                        "UPDATE agent_reply_authorities SET state='approval_pending' "
                        "WHERE authority=? AND state='active'",
                        (approval['authority'],),
                    )
                else:
                    self.db.execute(
                        "UPDATE guardian_reply_approvals SET state='stale',resolved_at=? WHERE approval_id=? AND state='pending'",
                        (now, approval['approval_id']),
                    )
                    self.db.execute(
                        "UPDATE agent_reply_authorities SET state='closed' WHERE authority=? AND state='active'",
                        (approval['authority'],),
                    )
            current_job = self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            if current_job and current_job['state'] == 'blocked':
                # The button becomes clickable only after the terminal job and
                # authority fence are durable; a live turn cannot race it.
                self._project_guardian_pending(approval)
                self._notify_failure(current_job, now)
                return
        used_approval = self._guardian_approval(job['key'], states=('used',))
        if used_approval is not None:
            # Native approval grants exactly one normal retry. A retry that
            # ends without verified delivery must stay visible; it cannot fall
            # through to the dispatcher's ordinary automatic attempt budget.
            if current and current.revision != job['revision']:
                state, error = 'superseded', None
            elif self._delivered(job):
                state, error = 'done', None
            else:
                state, error = 'blocked', 'guardian_approved_retry_unpublished'
            with self.db:
                self.db.execute('UPDATE jobs SET state=?,finished_at=?,error_code=? WHERE key=? AND state=\'running\'',
                                (state, now, error, job['key']))
                self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE authority=? AND state != 'published'",
                                (used_approval['authority'],))
            if state == 'blocked':
                self._project_guardian_terminal(job['key'], 'failed')
                self._notify_failure(self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone(), now)
            return
        cleanup_failed = False
        if job['stdout_path']:
            try:
                with open(job['stdout_path'], 'rb') as stream:
                    stream.seek(0, os.SEEK_END)
                    stream.seek(max(0, stream.tell() - 4096))
                    cleanup_failed = any(line.strip() == b'{"type": "worker.cleanup_failed"}' for line in stream)
            except OSError:
                pass
        if cleanup_failed:
            state, error = 'blocked', 'worker_cleanup_unconfirmed'
        elif current and current.revision != job['revision']:
            state, error = 'superseded', None
        elif self._delivered(job):
            state, error = 'done', None
        else:
            state = 'retry' if job['attempts'] < 2 else 'failed'
            error = 'worker_ended_without_verified_delivery'
        with self.db:
            self.db.execute('UPDATE jobs SET state=?,finished_at=?,retry_at=?,error_code=? WHERE key=?',
                (state, now, now + 30 * max(1, job['attempts']), error, job['key']))
            self.db.execute(
                "UPDATE agent_reply_authorities SET state='closed' WHERE job_key=? AND state != 'published'",
                (job['key'],),
            )
        if state in ('failed', 'blocked'):
            self._notify_failure(self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone(), now)

    def _block_acp_job(self, job, code, now):
        """Fence an uncertain ACP turn; its side effects may already exist."""
        with self.db:
            self.db.execute("UPDATE jobs SET state='blocked',finished_at=?,error_code=? WHERE key=? AND state='running'",
                            (now, code, job['key']))
        current = self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
        lock = self.acp_locks.pop(job['key'], None)
        if lock:
            lock.close()
        if current and current['state'] == 'blocked':
            self._notify_failure(current, now)

    def _prepare_failure(self, job, now, error):
        """A setup failure happened before any ACP prompt was submitted.

        It is safe to retry the original durable job and outgoing key.  This
        differs from a submitted turn, whose effects may require
        reconciliation before it can be retried.
        """
        state = 'retry' if job['attempts'] < 2 else 'failed'
        with self.db:
            self.db.execute(
                "UPDATE jobs SET state=?,finished_at=?,retry_at=?,error_code=? WHERE key=? AND state='preparing'",
                (state, now if state == 'failed' else None,
                 now + 30 * max(1, job['attempts']), error, job['key']),
            )
        current = self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
        if current and current['state'] == 'failed':
            self._notify_failure(current, now)

    @staticmethod
    def _compact_text(value, limit):
        text = value if isinstance(value, str) else ''
        if len(text) <= limit:
            return text, False
        return text[:limit] + '\n[truncated]', True

    def _prepare_context(self, job):
        """Fetch only current, bounded evidence before sending an ACP prompt.

        This runs in a small worker pool so Slack API latency cannot stall the
        receiver tick. Evidence is passed directly to the model and never
        persisted or logged by the dispatcher.
        """
        if job['kind'] != 'source':
            with ResponsibilityStore(self.project / self.config['database_path']) as store:
                work = store.get(job['responsibility_id'])
            return {
                'text': (
                    f"Verified responsibility: {work.outcome}\n"
                    f"Next action: {work.next_action}\n"
                    f"Current Slack root: {work.current_thread or 'none'}\n"
                    f"Execution fence: {job['responsibility_fence']}\n"
                    "Use publish_reply only for the result authorized by this claimed responsibility."
                ),
                'source_revision': None,
                'truncated': False,
            }
        message = self.inbox.get_message(job['message_id'])
        fetch = getattr(self.service, 'fetch_owner_source_evidence', None)
        if not callable(fetch):
            # Isolated dispatcher fixtures deliberately carry no Slack prose.
            return {
                'text': '[isolated fixture: verified source evidence supplied by test harness]',
                'source_revision': message.revision,
                'truncated': False,
            }
        fetched = fetch(message)
        current = fetched.message
        exact, exact_truncated = self._compact_text(fetched.evidence.message.get('text'), 6000)
        items = []
        truncated = exact_truncated or len(fetched.evidence.thread) > 12
        for item in fetched.evidence.thread[-12:]:
            text, item_truncated = self._compact_text(item.get('text'), 1600)
            truncated = truncated or item_truncated
            if text:
                speaker = item.get('user') if isinstance(item.get('user'), str) else 'unknown speaker'
                timestamp = item.get('ts') if isinstance(item.get('ts'), str) else 'unknown time'
                items.append(f'[{speaker} at {timestamp}]\n{text}')
        suffix = '\nThread context was bounded; fetch task-specific evidence only if needed.' if truncated else ''
        source_ts = getattr(current, 'source_ts', None) or job['root']
        permalink = (
            f"https://{self.config.get('workspace_domain', 'slack.com')}/archives/"
            f"{self.config.get('channel_id')}/p{str(source_ts).replace('.', '')}?thread_ts={job['root']}&cid={self.config.get('channel_id')}"
        )
        # Verified source evidence was prepared directly for the pending ACP
        # turn. Keep the read receipt local: the receiver owns checkmarks, and
        # do not refetch Slack evidence or make preparation depend on a reaction.
        read = self.inbox.mark_read_if_revision(job['message_id'], job['revision'])
        return {
            'text': 'Verified current source permalink: ' + permalink + '\n\nVerified current source:\n' + exact + '\n\nRelevant thread context:\n'
                    + ('\n---\n'.join(items) or '[no additional thread text]') + suffix,
            'source_revision': current.revision,
            'truncated': truncated,
            'read': read,
        }

    def _claim_responsibility(self, job):
        if job['kind'] != 'work':
            return None
        with ResponsibilityStore(self.project / self.config['database_path']) as store:
            if job['responsibility_fence']:
                if not store.execution_gate(job['responsibility_id'], job['responsibility_fence']):
                    raise GatewayError('responsibility_execution_fence_lost')
                return job['responsibility_fence']
            claim = store.claim(job['responsibility_id'], 'dispatcher:' + job['key'])
        with self.db:
            self.db.execute('UPDATE jobs SET responsibility_fence=? WHERE key=?',
                            (claim.execution_fence, job['key']))
        return claim.execution_fence

    def _create_publish_authority(self, job, fence=None):
        authority = uuid.uuid4().hex
        with self.db:
            self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE job_key=? AND state != 'published'", (job['key'],))
            self.db.execute(
                """INSERT INTO agent_reply_authorities
                   (authority,job_key,kind,root,message_id,revision,responsibility_id,
                    execution_fence,session_id,generation,turn_id,state,created_at)
                   VALUES (?,?,?,?,?,?,?,?,NULL,NULL,NULL,'preparing',?)""",
                (authority, job['key'], job['kind'], job['root'], job['message_id'],
                 job['revision'], job['responsibility_id'], fence, time.time()),
            )
        return authority

    def _discard_publish_authority(self, key):
        with self.db:
            self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE job_key=? AND state='preparing'", (key,))

    def publish_agent_reply(self, authority, text, responsibility_id=None, execution_fence=None,
                            conversation_title=None, conversation_emoji=None, conversation_preview=None,
                            needs_owner_action=None):
        """Receiver-side sole publishing gate for one ACP tool invocation."""
        if not isinstance(authority, str) or not isinstance(text, str) or not text:
            return {'published': False, 'state': 'invalid_request'}
        row = self.db.execute('SELECT * FROM agent_reply_authorities WHERE authority=?', (authority,)).fetchone()
        if not row:
            return {'published': False, 'state': 'authority_rejected'}
        if row['kind'] == 'source':
            if (responsibility_id is None) != (execution_fence is None):
                return {'published': False, 'state': 'responsibility_binding_invalid'}
            if (row['responsibility_id'] is not None or row['execution_fence'] is not None) and (
                responsibility_id != row['responsibility_id'] or execution_fence != row['execution_fence']
            ):
                return {'published': False, 'state': 'responsibility_binding_invalid'}
        owner_action = _validate_owner_action(needs_owner_action)
        if needs_owner_action is not None and owner_action is None:
            return {'published': False, 'state': 'needs_owner_action_invalid'}
        owner_action_title, owner_action_detail = owner_action if owner_action is not None else (None, None)
        payload_data = {
            'text': text,
            'responsibility_id': responsibility_id,
            'execution_fence': execution_fence,
        }
        # Existing durable authorities predate conversation metadata. Preserve
        # their exact digest when the optional fields are omitted.
        if any(value is not None for value in (conversation_title, conversation_emoji, conversation_preview)):
            payload_data.update(
                conversation_title=conversation_title,
                conversation_emoji=conversation_emoji,
                conversation_preview=conversation_preview,
            )
        if owner_action is not None:
            payload_data.update(
                owner_action_title=owner_action_title,
                owner_action_detail=owner_action_detail,
            )
        payload = json.dumps(
            payload_data,
            sort_keys=True,
            separators=(',', ':'),
        )
        digest = hashlib.sha256(payload.encode()).hexdigest()
        if row['text_digest'] is not None and row['text_digest'] != digest:
            return {'published': False, 'state': 'payload_mismatch'}
        if row['state'] == 'published':
            delivery = self.service.reconcile_outgoing('dispatch-answer:' + job['key']) if (job := self.db.execute('SELECT * FROM jobs WHERE key=?', (row['job_key'],)).fetchone()) else None
            if delivery is not None and delivery.state == 'sent':
                self._queue_conversation_feed(
                    row, delivery, 'dispatch-answer:' + job['key'],
                    row['conversation_title'], row['conversation_emoji'], row['conversation_preview'], fallback_text=text,
                )
                self._queue_needs_you(row, delivery, 'dispatch-answer:' + job['key'],
                                      row['owner_action_title'], row['owner_action_detail'])
            return {'published': True, 'state': 'sent'}
        if row['state'] not in ('active', 'uncertain'):
            return {'published': False, 'state': 'authority_rejected'}
        job = self.db.execute('SELECT * FROM jobs WHERE key=?', (row['job_key'],)).fetchone()
        if (not job or job['state'] != 'running' or job['runtime'] != 'acp'
                or job['root'] != row['root'] or job['agent_session_id'] != row['session_id']
                or job['agent_generation'] != row['generation'] or job['agent_turn_id'] != row['turn_id']):
            return {'published': False, 'state': 'authority_stale'}
        if row['text_digest'] is None:
            with self.db:
                self.db.execute(
                    '''UPDATE agent_reply_authorities
                       SET text_digest=?, conversation_title=?, conversation_emoji=?, conversation_preview=?,
                           owner_action_title=?, owner_action_detail=?
                       WHERE authority=? AND text_digest IS NULL''',
                    (digest, conversation_title, conversation_emoji, conversation_preview,
                     owner_action_title, owner_action_detail, authority),
                )
            row = self.db.execute('SELECT * FROM agent_reply_authorities WHERE authority=?', (authority,)).fetchone()
        key = 'dispatch-answer:' + job['key']
        if row['kind'] == 'source':
            def source_authorized():
                latest = self.db.execute('SELECT * FROM agent_reply_authorities WHERE authority=?', (authority,)).fetchone()
                active = self.db.execute('SELECT * FROM jobs WHERE key=?', (row['job_key'],)).fetchone()
                source = self.inbox.get_message(row['message_id'])
                return bool(
                    latest and latest['state'] == 'active' and active
                    and active['state'] == 'running' and active['runtime'] == 'acp'
                    and active['agent_session_id'] == latest['session_id']
                    and active['agent_generation'] == latest['generation']
                    and active['agent_turn_id'] == latest['turn_id']
                    and source.revision == latest['revision']
                    and source.event_type != 'message_deleted'
                    and (source.thread_ts or source.source_ts) == latest['root']
                )

            if responsibility_id is not None:
                if row['responsibility_id'] not in (None, responsibility_id) or row['execution_fence'] not in (None, execution_fence):
                    return {'published': False, 'state': 'responsibility_binding_invalid'}
                current_source = self.inbox.get_message(row['message_id'])
                if (current_source.revision != row['revision'] or current_source.event_type == 'message_deleted'
                        or (current_source.thread_ts or current_source.source_ts) != row['root']):
                    return {'published': False, 'state': 'source_stale'}
                with ResponsibilityStore(self.project / self.config['database_path']) as store:
                    work = store.get(responsibility_id)
                    if (work.current_thread != row['root'] or work.source_message_id != row['message_id']
                            or work.source_revision != row['revision']
                            or not store.execution_gate(responsibility_id, execution_fence)):
                        return {'published': False, 'state': 'responsibility_fence_lost'}
                with self.db:
                    self.db.execute(
                        "UPDATE agent_reply_authorities SET responsibility_id=?,execution_fence=? WHERE authority=?",
                        (responsibility_id, execution_fence, authority),
                    )
                from .slack_service import OutgoingAuthorizationError
                try:
                    delivery = self.service.publish_responsibility_result(
                        responsibility_id,
                        execution_fence,
                        text,
                        idempotency_key=key,
                        authorize=source_authorized,
                    )
                except OutgoingAuthorizationError:
                    return {'published': False, 'state': 'source_stale'}
                if delivery is None:
                    return {'published': False, 'state': 'responsibility_fence_lost'}
                if delivery.state != 'sent':
                    delivery = self.service.reconcile_outgoing(key)
                if delivery.state != 'sent':
                    with self.db:
                        self.db.execute("UPDATE agent_reply_authorities SET state='uncertain' WHERE authority=?", (authority,))
                    return {'published': False, 'state': 'delivery_uncertain'}
                with ResponsibilityStore(self.project / self.config['database_path']) as store:
                    store.complete(responsibility_id, execution_fence)
                completed = self.inbox.mark_completed_if_revision(row['message_id'], row['revision'])
                with self.db:
                    self.db.execute("UPDATE agent_reply_authorities SET state='published',published_at=? WHERE authority=?",
                                    (time.time(), authority))
                self._queue_conversation_feed(row, delivery, key, conversation_title, conversation_emoji, conversation_preview, fallback_text=text)
                self._queue_needs_you(row, delivery, key, owner_action_title, owner_action_detail)
                return {'published': bool(completed), 'state': 'sent' if completed else 'source_changed_after_send'}
            if row['state'] == 'uncertain':
                delivery = self.service.reconcile_outgoing(key)
                if delivery.state != 'sent':
                    return {'published': False, 'state': 'delivery_uncertain'}
                completed = self.inbox.mark_completed_if_revision(row['message_id'], row['revision'])
                with self.db:
                    self.db.execute("UPDATE agent_reply_authorities SET state='published',published_at=? WHERE authority=?",
                                    (time.time(), authority))
                self._queue_conversation_feed(row, delivery, key, conversation_title, conversation_emoji, conversation_preview, fallback_text=text)
                self._queue_needs_you(row, delivery, key, owner_action_title, owner_action_detail)
                return {'published': bool(completed), 'state': 'sent' if completed else 'source_changed_after_send'}
            if not source_authorized():
                return {'published': False, 'state': 'source_stale'}
            from .slack_service import OutgoingAuthorizationError
            try:
                delivery = self.service.send_outgoing(
                    text, idempotency_key=key, thread_ts=row['root'], authorize=source_authorized
                )
            except OutgoingAuthorizationError:
                return {'published': False, 'state': 'source_stale'}
            if delivery.state != 'sent':
                delivery = self.service.reconcile_outgoing(key)
            if delivery.state != 'sent':
                with self.db:
                    self.db.execute("UPDATE agent_reply_authorities SET state='uncertain' WHERE authority=?", (authority,))
                return {'published': False, 'state': 'delivery_uncertain'}
            completed = self.inbox.mark_completed_if_revision(row['message_id'], row['revision'])
            with self.db:
                self.db.execute("UPDATE agent_reply_authorities SET state='published',published_at=? WHERE authority=?",
                                (time.time(), authority))
            self._queue_conversation_feed(row, delivery, key, conversation_title, conversation_emoji, conversation_preview, fallback_text=text)
            self._queue_needs_you(row, delivery, key, owner_action_title, owner_action_detail)
            return {'published': bool(completed), 'state': 'sent' if completed else 'source_changed_after_send'}
        if row['kind'] != 'work' or not row['responsibility_id'] or not row['execution_fence']:
            return {'published': False, 'state': 'authority_rejected'}
        delivery = self.service.publish_responsibility_result(
            row['responsibility_id'], row['execution_fence'], text, idempotency_key=key
        )
        if delivery is None:
            return {'published': False, 'state': 'responsibility_fence_lost'}
        if delivery.state != 'sent':
            delivery = self.service.reconcile_outgoing(key)
        if delivery.state != 'sent':
            with self.db:
                self.db.execute("UPDATE agent_reply_authorities SET state='uncertain' WHERE authority=?", (authority,))
            return {'published': False, 'state': 'delivery_uncertain'}
        with ResponsibilityStore(self.project / self.config['database_path']) as store:
            store.complete(row['responsibility_id'], row['execution_fence'])
        with self.db:
            self.db.execute("UPDATE agent_reply_authorities SET state='published',published_at=? WHERE authority=?",
                            (time.time(), authority))
        self._queue_conversation_feed(row, delivery, key, conversation_title, conversation_emoji, conversation_preview, fallback_text=text)
        self._queue_needs_you(row, delivery, key, owner_action_title, owner_action_detail)
        return {'published': True, 'state': 'sent'}

    def _queue_conversation_feed(self, authority, delivery, outgoing_key, title, emoji, preview, *, fallback_text=None):
        """Queue a projection after the original answer is confirmed, never before."""
        if self.feed is None or not getattr(self.feed, 'configured', False):
            return
        # A denied first reply can legitimately omit optional feed metadata.
        # Its established pending identity remains authoritative; use a bounded
        # exact-reply preview only after the original delivery is confirmed.
        approval = self.db.execute(
            "SELECT * FROM guardian_reply_approvals WHERE job_key=?", (authority['job_key'],)
        ).fetchone()
        if approval is not None and (not isinstance(title, str) or not isinstance(emoji, str) or not isinstance(preview, str)):
            title = title if isinstance(title, str) else 'Pending reply review'
            emoji = emoji if isinstance(emoji, str) else '🔐'
            if not isinstance(preview, str):
                text = fallback_text if isinstance(fallback_text, str) else approval['proposal_text']
                preview = text.strip()[:600] if isinstance(text, str) and text.strip() else 'Approved reply delivered.'
        root = authority['root']
        if root == 'responsibilities' or not delivery.slack_ts:
            return
        try:
            self.feed.record_reply(
                root=root, title=title, emoji=emoji, preview=preview,
                outgoing_key=outgoing_key, outgoing_ts=delivery.slack_ts,
            )
            self.feed_error = ''
            if self.feed_wake is not None:
                self.feed_wake()
        except Exception as error:
            # The Slack answer and its delivery receipt are already settled.
            # A feed problem is visible to maintenance but cannot delay either.
            self.feed_error = type(error).__name__

    def _queue_needs_you(self, authority, delivery, outgoing_key, title, detail):
        """Record an explicit post-delivery owner review without touching work state."""
        if self.needs_you is None or not getattr(self.needs_you, 'configured', False):
            return
        if not isinstance(title, str) or not isinstance(detail, str):
            return
        root = authority['root']
        if root == 'responsibilities' or not delivery.slack_ts:
            return
        try:
            root_text = str(root)
            conversation_url = (
                f"https://{self.config['workspace_domain']}/archives/{self.config['channel_id']}"
                f"/p{root_text.replace('.', '')}?thread_ts={root_text}&cid={self.config['channel_id']}"
            )
            self.needs_you.store.record_completed_result(
                idempotency_key=outgoing_key, root=root_text, conversation_url=conversation_url,
                title=title, detail=detail,
            )
            self.needs_you_error = ''
            if self.needs_you_wake is not None:
                self.needs_you.request_publish()
                self.needs_you_wake()
        except Exception as error:
            # Original delivery and its completion receipt are already settled.
            # Recovery retries the local projection from published authorities.
            self.needs_you_error = type(error).__name__

    def recover_needs_you(self):
        """Recreate a missing owner-review projection after a post-send crash."""
        if self.needs_you is None or not getattr(self.needs_you, 'configured', False):
            return 0
        rows = self.db.execute(
            """SELECT a.*,j.root,j.key FROM agent_reply_authorities AS a
               JOIN jobs AS j ON j.key=a.job_key
               WHERE a.state='published' AND a.owner_action_title IS NOT NULL
                 AND a.owner_action_detail IS NOT NULL"""
        ).fetchall()
        queued = 0
        for row in rows:
            if row['root'] == 'responsibilities':
                continue
            key = 'dispatch-answer:' + row['key']
            delivery = self.service.reconcile_outgoing(key)
            if delivery.state == 'sent' and delivery.slack_ts:
                self._queue_needs_you(row, delivery, key, row['owner_action_title'], row['owner_action_detail'])
                queued += 1
        return queued

    def recover_conversation_feed(self):
        """Requeue already-confirmed replies after a crash between delivery and feed staging."""
        if self.feed is None or not getattr(self.feed, 'configured', False):
            return 0
        rows = self.db.execute(
            """SELECT a.*, j.root, j.key FROM agent_reply_authorities AS a
               JOIN jobs AS j ON j.key = a.job_key
               WHERE a.state = 'published' AND (a.conversation_preview IS NOT NULL OR EXISTS (
                   SELECT 1 FROM guardian_reply_approvals AS g WHERE g.job_key=a.job_key))"""
        ).fetchall()
        queued = 0
        for row in rows:
            if row['root'] == 'responsibilities':
                continue
            key = 'dispatch-answer:' + row['key']
            delivery = self.service.reconcile_outgoing(key)
            if delivery.state == 'sent' and delivery.slack_ts:
                self._queue_conversation_feed(
                    row, delivery, key, row['conversation_title'], row['conversation_emoji'], row['conversation_preview']
                )
                queued += 1
        approval_rows = self.db.execute(
            """SELECT a.* FROM guardian_reply_approvals AS a JOIN jobs AS j ON j.key=a.job_key
               WHERE a.state='pending' AND j.state='blocked' AND j.error_code='guardian_approval_pending'"""
        ).fetchall()
        for approval in approval_rows:
            self._project_guardian_pending(approval)
            queued += 1
        return queued

    def _advance_preparations(self, now):
        """Promote completed ACP setup work without blocking the receiver tick."""
        for key, preparation in tuple(self.preparations.items()):
            job = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
            if not job or job['state'] != 'preparing':
                self.preparations.pop(key, None)
                self._discard_publish_authority(key)
                continue
            if not preparation.future.done() or not preparation.context.done():
                continue
            # A setup request has not submitted any agent work.  Re-check the
            # source fence before taking a root lock or submitting a turn.
            if job['kind'] == 'source':
                current_source = self.inbox.get_message(job['message_id'])
                receipts = self.inbox.get_receipts(job['message_id'])
                if (current_source.revision != job['revision']
                        or current_source.event_type == 'message_deleted'):
                    with self.db:
                        self.db.execute(
                            "UPDATE jobs SET state='superseded',finished_at=? WHERE key=? AND state='preparing'",
                            (now, key),
                        )
                    self.preparations.pop(key, None)
                    self._discard_publish_authority(key)
                    continue
                if job['revision'] in receipts.completed_revisions:
                    # A prior delivery may have settled while setup was in
                    # flight. This job never submitted a model turn, so it is
                    # superseded even when the stable outbox key is verified.
                    with self.db:
                        self.db.execute(
                            "UPDATE jobs SET state='superseded',finished_at=? WHERE key=? AND state='preparing'",
                            (now, key),
                        )
                    self.preparations.pop(key, None)
                    self._discard_publish_authority(key)
                    continue
            try:
                context = preparation.context.result()
            except Exception as error:
                self.preparations.pop(key, None)
                self._discard_publish_authority(key)
                self._prepare_failure(job, now, 'source_context_failed:' + type(error).__name__)
                continue
            if job['kind'] == 'source' and context['source_revision'] != job['revision']:
                with self.db:
                    self.db.execute("UPDATE jobs SET state='superseded',finished_at=? WHERE key=? AND state='preparing'",
                                    (now, key))
                self.preparations.pop(key, None)
                self._discard_publish_authority(key)
                continue
            if job['kind'] == 'source':
                latest_source = self.inbox.get_message(job['message_id'])
                if (latest_source.revision != job['revision']
                        or latest_source.event_type == 'message_deleted'):
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='superseded',finished_at=? WHERE key=? AND state='preparing'",
                                        (now, key))
                    self.preparations.pop(key, None)
                    self._discard_publish_authority(key)
                    continue
            handle = self._lock(job['root'])
            if handle is None:
                continue
            try:
                binding = (preparation.owner or self._gateway_for_acp(job['root'])).complete_prepare(preparation.gateway)
            except Exception as error:
                handle.close()
                self.preparations.pop(key, None)
                active = self.db.execute(
                    "SELECT 1 FROM jobs WHERE state='running' AND runtime='acp' AND agent_profile=?",
                    ((preparation.owner or self._gateway_for_acp(job['root'])).profile.identifier,),
                ).fetchone()
                try:
                    (preparation.owner or self._gateway_for_acp(job['root'])).recover_preparation_failure(
                        preparation.gateway, idle=not bool(active)
                    )
                except GatewayError:
                    pass
                self._prepare_failure(job, now, 'acp_prepare_failed:' + type(error).__name__)
                self._discard_publish_authority(key)
                continue
            with self.db:
                self.db.execute(
                    """UPDATE jobs SET state='running',runtime='acp',agent_profile=?,
                       agent_session_id=?,agent_turn_id=NULL,agent_generation=?,agent_group_id=?,agent_selection=?,agent_turn_ended_at=NULL,error_code=NULL WHERE key=?
                       AND state='preparing'""",
                    (binding.profile_id, binding.session_id, preparation.driver.generation,
                     preparation.driver.process_group_id,
                     json.dumps(context['selection'].__dict__, sort_keys=True) if context.get('selection') else None, key),
                )
                self.db.execute(
                    "UPDATE agent_reply_authorities SET session_id=?,generation=?,state='active' WHERE authority=? AND state='preparing'",
                    (binding.session_id, preparation.driver.generation, preparation.authority),
                )
            current = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
            if not current or current['state'] != 'running':
                handle.close()
                self.preparations.pop(key, None)
                continue
            # Persisted running state and owned root lock precede prompt
            # submission.  A process crash now stays blocked rather than
            # replaying a potentially committed agent action.
            self.acp_locks[key] = handle
            self.preparations.pop(key, None)
            try:
                def reserve_turn(turn):
                    group_for_turn = getattr(preparation.driver, 'group_for_turn', None)
                    group_id = (group_for_turn(turn) if callable(group_for_turn)
                                else preparation.driver.process_group_id)
                    with self.db:
                        self.db.execute(
                            "UPDATE jobs SET agent_turn_id=?,agent_group_id=? WHERE key=? AND state='running'",
                            (str(turn), group_id, key),
                        )
                        self.db.execute(
                            "UPDATE agent_reply_authorities SET turn_id=? WHERE authority=? AND state='active'",
                            (str(turn), preparation.authority),
                        )
                owner = preparation.owner or self._gateway_for_acp(job['root'])
                submit_options = {'on_turn': reserve_turn}
                if context.get('selection') is not None:
                    submit_options['selection'] = context['selection']
                owner.submit(binding, self.prompt(current, context['text'], preparation.authority),
                             **submit_options)
            except Exception as error:
                latest = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
                if latest and isinstance(error, GatewayError) and str(error) == 'claude_spawn_failed':
                    # Popen raised before a process or stdin release existed.
                    # The durable reservation remains unstarted for exact-ID retry.
                    lock = self.acp_locks.pop(key, None)
                    if lock:
                        lock.close()
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='preparing' WHERE key=? AND state='running' AND agent_turn_id IS NULL", (key,))
                        self.db.execute("UPDATE agent_reply_authorities SET state='closed' WHERE job_key=? AND state='active' AND turn_id IS NULL", (key,))
                    self._discard_publish_authority(key)
                    self._prepare_failure(self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone(), now, 'claude_spawn_failed')
                elif latest:
                    self._block_acp_job(latest, 'acp_submit_uncertain', now)

    def _expire_preparations(self, now):
        if not self.preparations:
            return
        acp_options = self.options.get('acp', {})
        default = 61
        if isinstance(self._acp_profile, AgentProfile):
            default = self._acp_profile.startup_timeout_seconds + self._acp_profile.request_timeout_seconds + 1
        timeout = float(acp_options.get('prepare_timeout_seconds', default))
        for key, preparation in tuple(self.preparations.items()):
            job = self.db.execute('SELECT * FROM jobs WHERE key=?', (key,)).fetchone()
            if not job or job['state'] != 'preparing' or now - job['started_at'] <= timeout:
                continue
            self.preparations.pop(key, None)
            preparation.context.cancel()
            self._discard_publish_authority(key)
            # An idle setup process can be stopped. If another conversation is
            # already running, leave the shared runtime alive and cancel only
            # this correlated SDK request.
            owner = preparation.owner or self._gateway_for_acp(job['root'])
            if not self.db.execute("SELECT 1 FROM jobs WHERE state='running' AND runtime='acp' AND agent_profile=?",
                                   (owner.profile.identifier,)).fetchone():
                try:
                    (preparation.owner or self._gateway_for_acp(job['root'])).abort_preparation(preparation.gateway)
                except GatewayError:
                    pass
            else:
                preparation.future.cancel()
            self._prepare_failure(job, now, 'acp_prepare_timeout')

    def _gateway_for_acp(self, root=None):
        binding = self.db.execute('SELECT * FROM agent_sessions WHERE root=?', (root,)).fetchone() if root else None
        if binding:
            profile = self._profiles.get(binding['profile_id'])
            if profile is None or profile.backend != binding['backend']:
                raise GatewayError('session_profile_binding_changed')
        else:
            options = self.options.get('model_selection', {})
            backend = options.get('default_backend', 'codex') if options.get('enabled') else 'codex'
            if backend == 'claude':
                profile = next((item for item in self._profiles.values() if item.backend == 'claude-code'), None)
                if profile is None:
                    raise GatewayError('runtime_unavailable:missing_claude_command')
            elif backend == 'codex':
                profile = self._acp_profile
            else:
                raise GatewayError('selection_backend_invalid')
        if isinstance(profile, GatewayError):
            raise profile
        if profile is None:
            raise GatewayError('runtime_unavailable')
        if profile is self._acp_profile and self.gateway is not None:
            return self.gateway
        gateway = self._gateways.get(profile.identifier)
        if gateway is None:
            gateway = AgentGateway(self.db, project=self.project, state_directory=self.state_directory,
                                   profile=profile, config_path=self.config.get('_config_path', str(self.project / 'config/director.json')))
            self._gateways[profile.identifier] = gateway
            if profile is self._acp_profile:
                self.gateway = gateway
        return gateway

    def _prepare_execution_context(self, job, backend):
        context = self._prepare_context(job)
        options = self.options.get('model_selection', {})
        if options.get('enabled'):
            from .model_selection import Selection, select_in_process
            saved = job['agent_selection']
            if saved:
                selection = Selection(**json.loads(saved))
                if selection.backend != backend:
                    raise GatewayError('selection_backend_binding_changed')
            else:
                selection = select_in_process(context['text'], backend=backend,
                                              timeout=float(options.get('timeout_seconds', 40)))
            context['selection'] = selection
        return context

    def _consume_acp_events(self, now):
        gateways = list(self._gateways.values())
        if self.gateway is not None and self.gateway not in gateways:
            gateways.append(self.gateway)
        for event in (event for gateway in gateways for event in gateway.poll()):
            if event.kind == 'runtime_lost':
                # A shared profile process can host several sessions, but a
                # retired generation must never fence turns already moved to a
                # fresh process. Durable generation ids make late loss events
                # safe to retain and drain.
                for job in self.db.execute(
                    "SELECT * FROM jobs WHERE state='running' AND runtime='acp' AND agent_generation=? AND (? IS NULL OR agent_profile=?)",
                    (event.generation, event.profile_id, event.profile_id),
                ).fetchall():
                    self._block_acp_job(job, 'acp_runtime_lost_uncertain', now)
                continue
            if not event.root:
                continue
            states = "('running','blocked')" if event.kind == 'terminal' else "('running')"
            if event.kind in ('permission', 'progress'):
                job = self.db.execute(
                    "SELECT * FROM jobs WHERE root=? AND agent_session_id=? AND agent_generation=? AND state IN " + states + " AND runtime='acp' AND (? IS NULL OR agent_profile=?)",
                    (event.root, event.session_id, event.generation, event.profile_id, event.profile_id),
                ).fetchone()
            else:
                job = self.db.execute(
                    "SELECT * FROM jobs WHERE root=? AND agent_session_id=? AND agent_turn_id=? AND agent_generation=? AND state IN " + states + " AND runtime='acp' AND (? IS NULL OR agent_profile=?)",
                    (event.root, event.session_id, event.turn_id, event.generation, event.profile_id, event.profile_id),
                ).fetchone()
            if not job:
                continue  # Duplicate or late event after durable terminal state.
            if event.kind == 'terminal':
                if job['state'] == 'running':
                    self._finish(job, now)
                    lock = self.acp_locks.pop(job['key'], None)
                    if lock:
                        lock.close()
                elif job['error_code'] == 'acp_timeout_uncertain':
                    with self.db:
                        self.db.execute(
                            "UPDATE jobs SET agent_turn_ended_at=? WHERE key=? AND state='blocked'",
                            (now, job['key']),
                        )
            elif event.kind == 'error':
                self._block_acp_job(job, 'acp_turn_error_uncertain', now)
            elif event.kind == 'permission':
                # The typed SDK callback declined this one request.  The agent
                # can still report that denial through the normal Slack path.
                with self.db:
                    self.db.execute("UPDATE jobs SET error_code='acp_permission_denied' WHERE key=? AND state='running'",
                                    (job['key'],))
            elif event.kind == 'progress':
                # Only the adapter's bounded Guardian extension can create a
                # candidate. Replay from a loaded session cannot pass the
                # current authority/session/turn checks in this method.
                self._capture_guardian_reply_denial(job, event, now)
            elif event.kind == 'input':
                try:
                    self._gateway_for_acp(job['root']).cancel(job['root'])
                except GatewayError:
                    pass
                self._block_acp_job(job, 'acp_input_required', now)

    def _notify_failure(self, job, now):
        if job['notice_at'] is not None or (job['notice_retry_at'] or 0) > now:
            return
        # A post-submit ACP loss remains fenced until its old process group is
        # gone, but verified delivery is already a successful user outcome.
        # Do not send a misleading recovery notice in that interval.
        if (job['state'] == 'blocked' and job['runtime'] == 'acp'
                and self._delivered(job)):
            return
        if job['kind'] == 'source':
            current = self.inbox.get_message(job['message_id'])
            if current.revision != job['revision'] or current.event_type == 'message_deleted':
                return
            if job['state'] != 'blocked' and job['revision'] in self.inbox.get_receipts(job['message_id']).completed_revisions:
                return
        with self.db:
            self.db.execute('UPDATE jobs SET notice_retry_at=? WHERE key=?', (now + 30, job['key']))
        message = 'I could not finish processing this message. It is still saved. Please reply here to retry or clarify.'
        if job['error_code'] == 'guardian_approval_pending':
            message = ('Automatic review blocked this reply. It is saved and requires one-time local approval; '
                       'replying here will not retry it.')
        elif job['error_code'] == 'guardian_approval_expired':
            message = ('The one-time approval window expired. The message is saved; send a new request if you '
                       'still want to continue.')
        elif job['error_code'] == 'guardian_approved_retry_unpublished':
            message = ('The approved retry did not finish. It is saved and will not be retried automatically.')
        elif job['state'] == 'blocked':
            if job['error_code'] == 'worker_cleanup_unconfirmed':
                message = 'Processing is blocked because worker cleanup could not be confirmed. Your message is saved; this conversation needs worker recovery before I can safely continue.'
            elif str(job['error_code'] or '').startswith('acp_'):
                message = 'This conversation needs agent-runtime recovery before I can safely continue. Your message is saved.'
            else:
                message = 'Processing is blocked because worker recovery is required before I can safely continue. Your message is saved.'
        try:
            result = self.service.send_outgoing(message, idempotency_key='dispatch-failure:' + job['key'] + ':a' + str(job['attempts']),
                thread_ts=job['root'] if job['root'] != 'responsibilities' else None)
            if result.state == 'sent':
                with self.db:
                    self.db.execute('UPDATE jobs SET notice_at=? WHERE key=?', (now, job['key']))
        except Exception:
            # The durable job and stable outgoing key are retried on later ticks.
            self.inbox.set_checkpoint('dispatcher.notice_error', 'delivery_pending', now=now)

    def prompt(self, job, prepared_context='', authority=''):
        key = 'dispatch-answer:' + job['key']
        context_file = self.state_directory / 'manager-context.md'
        manager_name = self.config.get('manager_name', 'Director')
        if not isinstance(manager_name, str) or not manager_name.strip():
            manager_name = 'Director'
        if job['kind'] == 'source':
            target = f"Source message ID {job['message_id']}, expected revision {job['revision']}, Slack root {job['root']}."
        else:
            target = f"Continue runnable responsibility {job['responsibility_id']} from its durable record."
        if not authority:
            shared_context = context_file.read_text() if context_file.exists() else ''
            return f'''You are {manager_name.strip()}'s active manager, invoked because real work arrived. Use the configured model and reasoning effort for the request. Work in {self.project}.
{target}
Environment binding: channel {self.config.get('channel_id', 'configured channel')}; config {self.config.get('_config_path', 'config/director.json')}; state directory {self.state_directory}. DIRECTOR_CONFIG is already set for every CLI subprocess: preserve it, never use another config or production state. Save answer files and shared preferences only under this state directory. For test-channel work, do not read real-channel context or create real-world assignments.
Shared preference context, with original source authority and personal/work boundaries preserved:
{shared_context}
Read docs/dispatcher-manager.md now; it is the runtime contract. The owner has authorized answering their messages through the Director Slack service. Do not stop after an acknowledgment. Fetch the original source with .venv/bin/python -m director source, and mark its verified revision read promptly. Answer all questions or carry the authorized assignment forward. Use available connected capabilities, verify account identity, and be candid if a requested tool is unavailable in this runtime. Never claim a tool is absent just because it was not initially listed: use available tool discovery. Never inspect credential files or private desktop control pipes.
For a source reply, the required stable outgoing key is {key}. Use the existing send command for conversational answers, or the responsibility-gated publication command for responsibility results. Use the source's Slack root for the reply. Do not substitute a Codex final answer for Slack delivery. Record source completion only after confirmed delivery. The dispatcher verifies both the outbox key and source completion; a final model message alone is failure.
Keep simple answers simple: do not load unrelated history, run broad research, or delegate trivial questions. Existing context is in this Slack thread and the durable responsibility store. Follow-ups must preserve that context. For substantive assignments follow the operating agreement and persist the next action. Never fabricate permissions, receipts, evidence, or completion. If a prior attempt sent the answer, reconcile/reuse this same key and account for its completion instead of creating a duplicate. End with a brief internal result after Slack delivery.'''
        shared_context, shared_truncated = self._compact_text(
            context_file.read_text() if context_file.exists() else '', 6000
        )
        context_note = 'Shared preference context was bounded.' if shared_truncated else ''
        guide_file = self.project / 'docs/agent-task-tools.md'
        task_guide, guide_truncated = self._compact_text(
            guide_file.read_text() if guide_file.exists() else '', 8000
        )
        guide_note = 'Task/card guide was bounded.' if guide_truncated else ''
        feed_instruction = ''
        if self.feed is not None and getattr(self.feed, 'configured', False):
            feed_instruction = '''\nThe separate conversation feed is enabled. With this same publish_reply call, always supply a concise model-authored `conversation_preview`, a stable meaningful `conversation_title`, and a topic-related `conversation_emoji`. The receiver records title and emoji only for the first feed entry in a Slack root, so an existing session retains its original identity. These fields are a navigation aid, never status boilerplate.\n'''
        needs_you_instruction = ''
        if self.needs_you is not None and getattr(self.needs_you, 'configured', False):
            needs_you_instruction = '''\nThe private Needs you list is enabled. Supply `needs_owner_action` in the same publish_reply call only when this successfully delivered result leaves a specific meaningful action for the owner, such as reviewing a prepared document or edits. Its title is one line and at most 75 characters; detail is one line and at most 300 characters. Do not create one for a question, an ordinary answer, or an end-to-end request you have already completed, such as an authorized message you sent. This review item is not a responsibility and never authorizes further execution.\n'''
        return f'''You are {manager_name.strip()}'s active manager, invoked because real work arrived. Use the configured model and reasoning effort for the request. Work in {self.project}.
{target}
Environment binding: channel {self.config.get('channel_id', 'configured channel')}; config {self.config.get('_config_path', 'config/director.json')}; state directory {self.state_directory}. DIRECTOR_CONFIG is already set for every CLI subprocess: preserve it, never use another config or production state. Save answer files and shared preferences only under this state directory. For test-channel work, do not read real-channel context or create real-world assignments.
Shared preference context, with original source authority and personal/work boundaries preserved:
{shared_context}
{context_note}
Prepared verified work context:
{prepared_context}
The source/root and runtime guidance are already prepared. For ordinary requests, do not read MEMORY.md, docs, help, source-fetch commands, or broad history before answering. Use task-specific discovery only when the prepared evidence is insufficient. If `publish_reply` is not initially listed, use available tool discovery to locate that specific offered tool before declaring it unavailable. The owner authorized a Slack result, but a final model message is not delivery.
Prepared task/card guide:
{task_guide}
{guide_note}
The prepared execution fence is authoritative for this turn.
Call the `publish_reply` MCP tool exactly once for this turn with the reply text and this opaque authority: {authority}. The receiver validates its current source revision or claimed responsibility fence, sends through the stable outbox key {key}, reconciles uncertainty without replay, and records completion only after confirmed delivery. Never use answer files, `director send`, manual source completion, or generic Slack publishing for this result. For a source-turn responsibility continuation, provide both `responsibility_id` and `execution_fence` from the prepared guide; it cannot bypass its execution fence. Keep simple answers simple, preserve the current thread context, and never fabricate permissions, receipts, evidence, or completion.{feed_instruction}{needs_you_instruction}'''

    def _launch(self, job, now):
        if self.db.execute("SELECT 1 FROM jobs WHERE root=? AND state IN ('running','blocked','preparing')", (job['root'],)).fetchone():
            return False
        if self.options.get('runtime', 'legacy') == 'acp':
            # Persist preparation before starting the asynchronous SDK work.
            # No root flock or prompt exists until _advance_preparations has
            # completed setup and recorded a running job.
            with self.db:
                self.db.execute(
                    """UPDATE jobs SET state='preparing',attempts=attempts+1,
                       started_at=?,runtime='acp',agent_profile=?,error_code=NULL WHERE key=?""",
                    (now, self._acp_profile.identifier if isinstance(self._acp_profile, AgentProfile) else None,
                     job['key']),
                )
            prepared = self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
            authority = None
            try:
                fence = self._claim_responsibility(prepared)
                prepared = self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone()
                authority = self._create_publish_authority(prepared, fence)
                legacy = self.db.execute('SELECT session_id FROM sessions WHERE root=?', (job['root'],)).fetchone()
                migration_requested = bool(self.options.get('acp', {}).get('migrate_legacy_sessions', False))
                if legacy and not migration_requested:
                    raise GatewayError('legacy_session_migration_required')
                gateway = self._gateway_for_acp(job['root'])
                if legacy and gateway.profile.backend != 'codex-acp':
                    raise GatewayError('legacy_session_requires_codex')
                preparation = gateway.begin_prepare(
                    job['root'], legacy_session_id=legacy['session_id'] if legacy else None,
                    mcp_servers=[gateway.publish_reply_server()],
                )
                backend = 'claude' if gateway.profile.backend == 'claude-code' else 'codex'
                context = self._context_pool.submit(self._prepare_execution_context, prepared, backend)
                self.preparations[job['key']] = DispatchPreparation(preparation, context, authority, gateway)
                return True
            except (GatewayError, CapabilityError) as error:
                if authority:
                    self._discard_publish_authority(job['key'])
                self._prepare_failure(prepared, now, str(error))
                return False
        handle = self._lock(job['root'])
        if handle is None:
            return False
        if self.db.execute('SELECT 1 FROM agent_sessions WHERE root=?', (job['root'],)).fetchone():
            handle.close()
            with self.db:
                self.db.execute("UPDATE jobs SET state='blocked',finished_at=?,error_code='acp_session_rollback_required' WHERE key=?",
                                (now, job['key']))
            self._notify_failure(self.db.execute('SELECT * FROM jobs WHERE key=?', (job['key'],)).fetchone(), now)
            return False
        executable = str(self.project / self.options['codex_path'])
        output = self.directory / f"{hashlib.sha256(job['key'].encode()).hexdigest()[:24]}-a{job['attempts'] + 1}.jsonl"
        errors = output.with_suffix('.stderr.log')
        session = self.db.execute('SELECT session_id FROM sessions WHERE root=?', (job['root'],)).fetchone()
        command = [executable, 'exec', '--json', '--approve-for-me', '-m', self.options.get('model', 'gpt-6-astra'), '-c', 'model_reasoning_effort="medium"']
        # Slack transport belongs to the receiver; loading the desktop secret
        # connector here triggers macOS App Data prompts attributed to Python.
        command.extend(['-c', 'mcp_servers.1password.enabled=false'])
        if session:
            command.extend(['resume', session['session_id']])
        command.append('-')
        env = os.environ.copy()
        env['DIRECTOR_CONFIG'] = self.config.get('_config_path', str(self.project / 'config/director.json'))
        env['PATH'] = str(Path(executable).parent) + ':/opt/homebrew/bin:/usr/local/bin:' + env.get('PATH', '/usr/bin:/bin')
        env['CODEX_MANAGED_PACKAGE_ROOT'] = str(self.project / 'state/codex-runtime/node_modules/@openai/codex')
        for name in ('SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN'):
            env.pop(name, None)
        child = None
        try:
            with open(output, 'w') as out, open(errors, 'w') as err:
                wrapped = [str(self.project / '.venv/bin/python'), '-m', 'director.dispatch_worker', str(handle.fileno()), *command]
                child = subprocess.Popen(wrapped, cwd=self.project, env=env, stdin=subprocess.PIPE,
                    stdout=out, stderr=err, text=True, start_new_session=True, pass_fds=(handle.fileno(),))
            with self.db:
                self.db.execute(
                    """UPDATE jobs SET state='running',attempts=attempts+1,
                       started_at=?,stdout_path=?,runtime='legacy',agent_profile=NULL,
                       agent_session_id=NULL,agent_turn_id=NULL,agent_generation=NULL,
                       agent_group_id=NULL,agent_turn_ended_at=NULL,error_code=NULL
                       WHERE key=?""",
                    (now, str(output), job['key']),
                )
            self.children[job['key']] = child
            child.stdin.write(self.prompt(job))
            child.stdin.close()
            return True
        except Exception:
            if child is not None:
                # Popen succeeded: retain ownership instead of scheduling a
                # duplicate when prompt writing/closing fails (e.g. EPIPE).
                self.children[job['key']] = child
                with self.db:
                    self.db.execute(
                        """UPDATE jobs SET state='running',started_at=?,stdout_path=?,
                           runtime='legacy',agent_profile=NULL,agent_session_id=NULL,
                           agent_turn_id=NULL,agent_generation=NULL,agent_group_id=NULL,
                           agent_turn_ended_at=NULL,error_code='prompt_failed' WHERE key=?""",
                        (now, str(output), job['key']),
                    )
                try:
                    child.stdin.close()
                except OSError:
                    pass
                return True
            with self.db:
                self.db.execute(
                    """UPDATE jobs SET state=CASE WHEN attempts>=1 THEN 'failed' ELSE 'retry' END,
                       attempts=attempts+1,retry_at=?,runtime='legacy',agent_profile=NULL,
                       agent_session_id=NULL,agent_turn_id=NULL,agent_generation=NULL,
                       agent_group_id=NULL,agent_turn_ended_at=NULL,error_code='launch_failed'
                       WHERE key=?""",
                    (now + 30, job['key']),
                )
            raise
        finally:
            # The wrapper holds the lock; it never reaches MCP descendants.
            handle.close()

    def tick(self, now=None):
        now = time.time() if now is None else now
        if now - self.last_tick < 0.5:
            return
        self.last_tick = now
        self.enqueue(now)
        self._expire_guardian_approvals(now)
        if self.options.get('runtime', 'legacy') == 'acp':
            for job in self.db.execute(
                "SELECT key FROM jobs WHERE state='blocked' AND error_code='acp_session_rollback_required'"
            ).fetchall():
                self.reconcile_job(job['key'])
        self._consume_acp_events(now)
        self._expire_preparations(now)
        self._advance_preparations(now)
        for job in self.db.execute("SELECT * FROM jobs WHERE state='running'").fetchall():
            if job['runtime'] == 'acp':
                if now - job['started_at'] > self.options.get('timeout_seconds', 900):
                    try:
                        self._gateway_for_acp(job['root']).cancel(job['root'])
                    except GatewayError:
                        pass
                    self._block_acp_job(job, 'acp_timeout_uncertain', now)
                continue
            self._session(job)
            child = self.children.get(job['key'])
            if child and child.poll() is None:
                if now - job['started_at'] > self.options.get('timeout_seconds', 900):
                    signum = signal.SIGKILL if now - job['started_at'] > self.options.get('timeout_seconds', 900) + 5 else signal.SIGTERM
                    try:
                        os.killpg(child.pid, signum)
                    except ProcessLookupError:
                        pass
                    except PermissionError:
                        with self.db:
                            self.db.execute("UPDATE jobs SET state='blocked',error_code='worker_signal_denied' WHERE key=?", (job['key'],))
                continue
            lock = self._lock(job['root'])
            if lock is None:
                continue
            lock.close()
            self.children.pop(job['key'], None)
            self._finish(job, now)
        # A receiver restart or runtime loss fences submitted ACP turns. Settle
        # only already-verified delivery/source changes once the owned group is
        # gone; unresolved work remains blocked for the local reconcile action.
        for job in self.db.execute(
            "SELECT key FROM jobs WHERE state='blocked' AND runtime='acp'"
        ).fetchall():
            self.reconcile_job(job['key'])
        for job in self.db.execute("SELECT * FROM jobs WHERE state IN ('failed','blocked') AND notice_at IS NULL").fetchall():
            self._notify_failure(job, now)
        running = self.db.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('running','preparing')").fetchone()[0]
        for job in self.db.execute("SELECT * FROM jobs WHERE state IN ('pending','retry') AND retry_at<=? ORDER BY created_at", (now,)).fetchall():
            if running >= self.options.get('max_workers', 2):
                break
            if job['kind'] == 'source':
                current = self.inbox.get_message(job['message_id'])
                receipts = self.inbox.get_receipts(job['message_id'])
                if current.revision != job['revision'] or job['revision'] in receipts.completed_revisions:
                    with self.db:
                        self.db.execute("UPDATE jobs SET state='superseded',finished_at=? WHERE key=?", (now, job['key']))
                    continue
            if self._launch(job, now):
                running += 1
        # This never waits: an already-resolved preparation can be submitted
        # in this tick, while a slow initialize/new/load remains durable work.
        self._advance_preparations(now)
        if now - self.last_health >= 15:
            self.last_health = now
            counts = dict(self.db.execute('SELECT state,COUNT(*) FROM jobs GROUP BY state').fetchall())
            self.inbox.set_checkpoint('dispatcher.loop', json.dumps(counts), now=now)

    def close(self):
        # Stop only subprocess groups created by this instance. Interrupted
        # work resumes from durable state and the original outgoing keys.
        for preparation in self.preparations.values():
            preparation.context.cancel()
        self.preparations.clear()
        self._context_pool.shutdown(wait=False, cancel_futures=True)
        for child in self.children.values():
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except (PermissionError, ProcessLookupError):
                    pass
        gateways = list(self._gateways.values())
        if self.gateway is not None and self.gateway not in gateways:
            gateways.append(self.gateway)
        for gateway in gateways:
            close = getattr(gateway, 'close', None)
            if close:
                close(wait=True)
        for handle in self.acp_locks.values():
            handle.close()
        self.acp_locks.clear()
        self.db.close()
