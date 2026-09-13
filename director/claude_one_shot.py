"""Official Claude Code print-mode transport; Director owns session bindings.

This transport does not retry: a lost result may follow a completed tool action.
Callers must hold the conversation lock, persist the chosen session ID before
launch, and reconcile publication independently of the CLI's terminal result.
"""
from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

from .model_selection import MODELS, Selection


class ClaudeProcessError(RuntimeError):
    """An unsuccessful or uncertain turn; never authorization to replay."""


@dataclass(frozen=True)
class ClaudeResult:
    session_id: str
    text: str
    permission_denials: tuple


def subscription_environment(environment):
    env = dict(environment)
    # Stored Claude Code login owns authentication. Never silently bill an API
    # key or redirect the subscription request through an inherited provider.
    for key in ('ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'CLAUDE_CODE_OAUTH_TOKEN',
                'ANTHROPIC_BASE_URL', 'CLAUDE_CODE_USE_BEDROCK',
                'CLAUDE_CODE_USE_VERTEX', 'CLAUDE_CODE_USE_FOUNDRY',
                'SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN'):
        env.pop(key, None)
    return env


def parse_result(stdout, returncode, session_id):
    try:
        result = json.loads(stdout)
    except (ValueError, TypeError) as error:
        raise ClaudeProcessError('claude_result_invalid') from error
    if not isinstance(result, dict):
        raise ClaudeProcessError('claude_result_invalid')
    # Error payloads can say subtype=success, so all three conditions matter.
    if returncode or result.get('is_error') is not False or result.get('subtype') != 'success':
        raise ClaudeProcessError('claude_turn_failed')
    if result.get('type') != 'result' or result.get('session_id') != session_id:
        raise ClaudeProcessError('claude_session_result_mismatch')
    text = result.get('result')
    denials = result.get('permission_denials', [])
    if not isinstance(text, str) or not isinstance(denials, list):
        raise ClaudeProcessError('claude_result_invalid')
    return ClaudeResult(session_id, text, tuple(denials))


class ClaudeOneShot:
    def __init__(self, command, cwd, *, environment=None, timeout=300):
        if not command or not all(isinstance(item, str) and item for item in command):
            raise ValueError('claude_command_invalid')
        if not Path(command[0]).is_absolute():
            raise ValueError('claude_command_must_be_absolute')
        if timeout <= 0:
            raise ValueError('claude_timeout_invalid')
        self.command = tuple(command)
        self.cwd = Path(cwd)
        self.environment = subscription_environment(os.environ if environment is None else environment)
        self.timeout = timeout

    def arguments(self, selection: Selection, session_id, *, resume, mcp_servers):
        try:
            if str(uuid.UUID(session_id)) != session_id:
                raise ValueError()
        except (ValueError, TypeError, AttributeError) as error:
            raise ValueError('claude_session_id_invalid') from error
        if (selection.backend != 'claude' or selection.grade not in MODELS['claude']
                or selection.model != MODELS['claude'][selection.grade] or selection.effort != 'high'):
            raise ValueError('claude_selection_invalid')
        if not isinstance(resume, bool):
            raise ValueError('claude_resume_required')
        if not isinstance(mcp_servers, dict):
            raise ValueError('claude_mcp_invalid')
        return [*self.command, '-p', '--output-format', 'json',
                '--model', selection.model, '--effort', selection.effort,
                '--permission-mode', 'auto', '--strict-mcp-config',
                '--mcp-config', json.dumps({'mcpServers': mcp_servers}),
                '--setting-sources', '', '--settings', '{"disableAllHooks":true}',
                '--disable-slash-commands', '--no-chrome',
                '--resume' if resume else '--session-id', session_id]

    @staticmethod
    def _stop_group(process):
        # Reap the leader and terminate MCP/tool descendants as well. A denied
        # existence probe is not evidence that the process group is empty.
        for signum, grace in ((signal.SIGTERM, 1), (signal.SIGKILL, 2)):
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                process.wait(timeout=1)
                return
            except PermissionError:
                try:
                    process.send_signal(signum)
                except (ProcessLookupError, PermissionError):
                    pass
            deadline = time.monotonic() + grace
            while time.monotonic() < deadline:
                process.poll()
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    process.wait(timeout=1)
                    return
                except PermissionError:
                    pass
                time.sleep(.02)
        process.poll()
        raise ClaudeProcessError('claude_cleanup_unconfirmed')

    def run(self, prompt, selection, session_id, *, resume, mcp_servers):
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError('claude_prompt_empty')
        args = self.arguments(selection, session_id, resume=resume, mcp_servers=mcp_servers)
        process = subprocess.Popen(args, cwd=self.cwd, env=self.environment,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            try:
                stdout, _stderr = process.communicate(prompt, timeout=self.timeout)
            except subprocess.TimeoutExpired as error:
                raise ClaudeProcessError('claude_turn_timeout') from error
            return parse_result(stdout, process.returncode, session_id)
        finally:
            try:
                self._stop_group(process)
            finally:
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream:
                        stream.close()
