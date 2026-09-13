"""Supervise independent official Claude Code print-mode turns.

The registration callback runs after spawn but before stdin is released. Callers
must durably record ``group_for_turn(turn)`` and activate publication authority in
that callback. A prepared UUID is only a reservation; load never fabricates a
replacement if Claude cannot resume it.
"""
from concurrent.futures import Future
from types import SimpleNamespace
import os
import queue
import subprocess
import threading
import uuid

from .claude_one_shot import ClaudeOneShot, parse_result
from .agent_gateway import GatewayError


class _LaunchSetting(SimpleNamespace):
    def model_dump(self, *, mode='json'):
        return vars(self).copy()


class ClaudeDriver:
    def __init__(self, profile, cwd, env, stderr_path, generation):
        self.profile, self.cwd, self.generation = profile, cwd, generation
        self.events = queue.SimpleQueue()
        self.closed = threading.Event()
        self.startup = Future()
        self.startup.set_result(None)
        self.capabilities = {'loadSession': True}
        self.protocol_version = None
        self.process = None
        self.process_group_id = None  # Each turn has its own group.
        self.turn = 0
        self._lock = threading.RLock()
        self._sessions = {}
        self._turns = {}
        self._workers = []
        self._closing = threading.Event()
        self.transport = ClaudeOneShot(profile.command, cwd, environment=env,
                                       timeout=profile.request_timeout_seconds)
        self.thread = threading.Thread(target=self._shutdown_worker, daemon=True)
        self.thread.start()

    @property
    def alive(self):
        return not self._closing.is_set()

    @property
    def group_empty(self):
        with self._lock:
            return all(item['clean'] for item in self._turns.values())

    def wait_ready(self, timeout=None):
        if not self.alive:
            raise GatewayError('runtime_unavailable')

    @staticmethod
    def _servers(servers):
        result = {}
        for server in servers or []:
            if getattr(server, 'type', None) not in (None, 'stdio'):
                raise GatewayError('claude_mcp_stdio_required')
            name = server.name
            if name in result:
                raise GatewayError('claude_mcp_duplicate')
            result[name] = {'command': server.command, 'args': list(server.args),
                            'env': {entry.name: entry.value for entry in server.env}}
        return result

    def _response(self, session_id):
        # These describe the required launch contract, not a remote handshake.
        options = [_LaunchSetting(id=k, current_value=v)
                   for k, v in self.profile.expected_settings if k != 'mode']
        return SimpleNamespace(session_id=session_id, config_options=options,
                               modes=_LaunchSetting(current_mode_id='auto'))

    def new(self, cwd, *, mcp_servers=None):
        self.wait_ready()
        if os.path.realpath(cwd) != os.path.realpath(self.cwd):
            raise GatewayError('claude_cwd_mismatch')
        session_id = str(uuid.uuid4())
        with self._lock:
            self._sessions[session_id] = {'resume': False, 'servers': self._servers(mcp_servers)}
        return self._response(session_id)

    def load(self, cwd, session_id, *, mcp_servers=None, unstarted=False):
        self.wait_ready()
        if os.path.realpath(cwd) != os.path.realpath(self.cwd):
            raise GatewayError('claude_cwd_mismatch')
        if not isinstance(unstarted, bool):
            raise GatewayError('claude_unstarted_invalid')
        if str(uuid.UUID(session_id)) != session_id:
            raise GatewayError('claude_session_id_invalid')
        with self._lock:
            if any(t['session'] == session_id and not t['done'] for t in self._turns.values()):
                raise GatewayError('claude_session_busy')
            if unstarted and self._sessions.get(session_id, {}).get('resume'):
                raise GatewayError('claude_session_already_started')
            self._sessions[session_id] = {'resume': not unstarted, 'servers': self._servers(mcp_servers)}
        return self._response(session_id)

    def prepare(self, cwd, session_id=None, *, mcp_servers=None, unstarted=False):
        future = Future()
        try:
            response = (self.load(cwd, session_id, mcp_servers=mcp_servers, unstarted=unstarted) if session_id
                        else self.new(cwd, mcp_servers=mcp_servers))
            future.set_result((response.session_id, response))
        except Exception as error:
            future.set_exception(error)
        return future

    def group_for_turn(self, turn):
        with self._lock:
            return self._turns[turn]['process'].pid

    def prompt(self, session_id, text, *, on_turn=None, selection=None):
        self.wait_ready()
        if not isinstance(text, str) or not text.strip():
            raise GatewayError('claude_prompt_empty')
        if on_turn is None:
            raise GatewayError('claude_registration_required')
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise GatewayError('claude_session_unprepared')
            if any(t['session'] == session_id and not t['done'] for t in self._turns.values()):
                raise GatewayError('claude_session_busy')
            args = self.transport.arguments(selection, session_id, resume=session['resume'],
                                            mcp_servers=session['servers'])
            self.turn += 1
            turn = f'{self.generation}:{self.turn}'
            try:
                process = subprocess.Popen(args, cwd=self.cwd, env=self.transport.environment,
                                           stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True, start_new_session=True)
            except OSError as error:
                # No process exists and no prompt was submitted. Preserve the
                # reserved UUID's never-started state for an explicit retry.
                raise GatewayError('claude_spawn_failed') from error
            record = {'process': process, 'session': session_id, 'done': False, 'clean': False,
                      'cancelled': False, 'cleanup_lock': threading.Lock()}
            self._turns[turn] = record
            session['resume'] = True  # An uncertain attempt must never recreate it.
            try:
                on_turn(turn)
            except Exception:
                try:
                    self._cleanup(record)
                finally:
                    record['done'] = True
                    for stream in (process.stdin, process.stdout, process.stderr):
                        if stream:
                            stream.close()
                raise
            worker = threading.Thread(target=self._run, args=(turn, record, text), daemon=True)
            self._workers.append(worker)
            worker.start()
            return turn

    def _cleanup(self, record):
        with record['cleanup_lock']:
            if not record['clean']:
                self.transport._stop_group(record['process'])
                record['clean'] = True

    def _run(self, turn, record, text):
        detail = {'turn_id': turn}
        kind = 'error'
        try:
            stdout, _stderr = record['process'].communicate(text, timeout=self.transport.timeout)
            result = parse_result(stdout, record['process'].returncode, record['session'])
            if record['cancelled']:
                raise GatewayError('claude_turn_cancelled')
            detail['permission_denial_count'] = len(result.permission_denials)
            kind = 'terminal'
        except Exception as error:
            detail['error'] = str(error) if isinstance(error, GatewayError) else type(error).__name__
        finally:
            try:
                self._cleanup(record)
            except Exception:
                kind, detail['error'] = 'error', 'claude_cleanup_unconfirmed'
            record['done'] = True
            for stream in (record['process'].stdin, record['process'].stdout, record['process'].stderr):
                if stream:
                    stream.close()
            self.events.put((kind, record['session'], detail))

    def cancel(self, session_id):
        with self._lock:
            for record in self._turns.values():
                if record['session'] == session_id and not record['done']:
                    record['cancelled'] = True
                    # Cleanup runs off the receiver thread.
                    threading.Thread(target=self._cancel_record, args=(record,), daemon=True).start()

    def _cancel_record(self, record):
        try:
            self._cleanup(record)
        except Exception:
            self.events.put(('error', record['session'], {'error': 'claude_cleanup_unconfirmed'}))

    def observe_liveness(self):
        pass  # Each worker emits its correlated terminal/error after group cleanup.

    def _shutdown_worker(self):
        self._closing.wait()
        with self._lock:
            records, workers = list(self._turns.values()), list(self._workers)
        for record in records:
            if not record['clean']:
                record['cancelled'] = True
                self._cancel_record(record)
        for worker in workers:
            worker.join()
        self.closed.set()

    def close(self, *, wait=False):
        self._closing.set()
        if wait:
            self.thread.join(timeout=self.transport.timeout + 5)
