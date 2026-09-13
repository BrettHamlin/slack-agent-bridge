"""Durable ACP sessions; the official Python SDK owns all protocol I/O."""
from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import dataclass
import json
import os
from pathlib import Path
import queue
import signal
import sqlite3
import threading
import time
from typing import Any

import acp
from acp.schema import McpServerStdio


class GatewayError(RuntimeError):
    pass


class CapabilityError(GatewayError):
    pass


@dataclass(frozen=True)
class AgentProfile:
    identifier: str
    backend: str
    command: tuple[str, ...]
    required_capabilities: frozenset[str] = frozenset({"load"})
    runtime_environment: tuple[tuple[str, str], ...] = ()
    expected_settings: tuple[tuple[str, str], ...] = ()
    startup_timeout_seconds: float = 30
    request_timeout_seconds: float = 30
    dynamic_selection: bool = False


@dataclass(frozen=True)
class SessionBinding:
    root: str
    profile_id: str
    backend: str
    session_id: str
    migrated_from: str | None = None


@dataclass(frozen=True)
class AgentEvent:
    kind: str
    root: str
    session_id: str | None = None
    turn_id: str | None = None
    detail: dict[str, Any] | None = None
    generation: int | None = None
    profile_id: str | None = None


@dataclass(frozen=True)
class Preparation:
    """A nonblocking initialize/load/new request owned by the caller thread."""

    root: str
    existing: SessionBinding | None
    legacy_session_id: str | None
    future: Future
    driver: Any


class _Client:
    """SDK callbacks only queue immutable event data; they never access SQLite."""

    def __init__(self, events: queue.SimpleQueue):
        self.events = events

    async def session_update(self, session_id, update, **_):
        detail = {"update_type": type(update).__name__}
        # Keep lifecycle evidence without retaining agent prose, tool input,
        # or environment data in the receiver's trace. Tool result data is
        # retained privately for the same postmortem evidence legacy JSONL
        # traces provided.
        if type(update).__name__ in {"ToolCallStart", "ToolCallProgress"}:
            detail["tool"] = {
                "id": getattr(update, "tool_call_id", None),
                "title": getattr(update, "title", None),
                "kind": getattr(update, "kind", None),
                "status": getattr(update, "status", None),
                "content": [item.model_dump(mode="json") for item in (getattr(update, "content", None) or [])],
                "raw_output": getattr(update, "raw_output", None),
            }
        self.events.put(("progress", session_id, detail))

    async def request_permission(self, session_id, tool_call, options, **_):
        detail = {
            "id": getattr(tool_call, "tool_call_id", None),
            "title": getattr(tool_call, "title", None),
            "kind": getattr(tool_call, "kind", None),
            "status": getattr(tool_call, "status", None),
        }
        self.events.put(
            ("permission", session_id, {"tool_call": detail})
        )
        # There is no interactive approval bridge yet. Select the agent's
        # narrowest offered rejection option rather than using a blanket
        # cancelled outcome when a proper option is available.
        chosen = next(
            (option for option in options if getattr(option, "kind", None) == "reject_once"),
            None,
        ) or next(
            (option for option in options if getattr(option, "kind", None) == "reject_always"),
            None,
        )
        if chosen is not None:
            return acp.RequestPermissionResponse.model_validate(
                {"outcome": {"outcome": "selected", "optionId": chosen.option_id}}
            )
        return acp.RequestPermissionResponse.model_validate(
            {"outcome": {"outcome": "cancelled"}}
        )


class ACPDriver:
    """One supervised ACP process using the SDK's typed client transport."""

    def __init__(self, profile: AgentProfile, cwd: Path, env: dict[str, str], stderr_path: Path,
                 generation: int):
        self.profile = profile
        self.cwd = cwd
        self.env = env
        self.stderr_path = stderr_path
        self.generation = generation
        self.events: queue.SimpleQueue = queue.SimpleQueue()
        self.loop = asyncio.new_event_loop()
        self.closed = threading.Event()
        self.process = None
        self.process_group_id: int | None = None
        self.connection = None
        self.capabilities: dict[str, Any] = {}
        self.protocol_version: int | None = None
        self.start_error: str | None = None
        self.start_exception: GatewayError | None = None
        self.startup: Future = Future()
        self.turn = 0
        self._runtime_loss_emitted = False
        self.group_empty = False
        self._start_task = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self._start_task = self.loop.create_task(self._start())
        self._start_task.add_done_callback(self._settle_startup)
        self.loop.run_forever()
        pending = asyncio.all_tasks(self.loop)
        for task in pending:
            task.cancel()
        if pending:
            self.loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        self.loop.close()

    def _settle_startup(self, task):
        if self.startup.done():
            return
        try:
            task.result()
        except asyncio.CancelledError:
            self.startup.set_exception(GatewayError("runtime_start_cancelled"))
            return
        except Exception as error:
            self.startup.set_exception(
                GatewayError("runtime_unavailable:" + type(error).__name__)
            )
            return
        if self.connection is not None and not self.closed.is_set():
            self.startup.set_result(True)
        else:
            self.startup.set_exception(
                self.start_exception
                or GatewayError("runtime_unavailable:" + (self.start_error or "startup"))
            )

    async def _start(self):
        try:
            self.stderr_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(
                self.stderr_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600
            )
            os.chmod(self.stderr_path, 0o600)
            self.stderr = os.fdopen(descriptor, "ab", buffering=0)
            self.process = await asyncio.create_subprocess_exec(
                *self.profile.command,
                cwd=self.cwd,
                env=self.env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=self.stderr,
                start_new_session=True,
            )
            self.process_group_id = os.getpgid(self.process.pid)
            self.connection = acp.connect_to_agent(
                _Client(self.events), self.process.stdin, self.process.stdout
            )
            initialized = await self.connection.initialize(protocol_version=1)
            self.protocol_version = initialized.protocol_version
            if self.protocol_version != 1:
                raise CapabilityError("unsupported_protocol_version")
            capabilities = initialized.agent_capabilities
            self.capabilities = (
                capabilities.model_dump(mode="json", by_alias=True) if capabilities else {}
            )
        except Exception as error:
            if isinstance(error, GatewayError):
                self.start_exception = error
                self.start_error = str(error)
            else:
                self.start_error = type(error).__name__
            self.events.put(("runtime_lost", "", {"error": self.start_error}))
            self.connection = None

    @property
    def alive(self) -> bool:
        return (
            not self.closed.is_set()
            and self.connection is not None
            and self.process is not None
            and self.process.returncode is None
        )

    def wait_ready(self, timeout: float | None = None) -> None:
        timeout = self.profile.startup_timeout_seconds if timeout is None else timeout
        try:
            self.startup.result(timeout)
        except FutureTimeout as error:
            raise GatewayError("runtime_start_timeout") from error
        except GatewayError:
            raise
        except Exception as error:
            raise GatewayError("runtime_unavailable:" + type(error).__name__) from error
        if not self.alive:
            raise GatewayError("runtime_unavailable")

    def _supports(self, capability: str) -> bool:
        if capability == "load":
            return bool(
                self.capabilities.get("loadSession")
                or self.capabilities.get("load_session")
            )
        if capability == "resume":
            session_capabilities = (
                self.capabilities.get("sessionCapabilities")
                or self.capabilities.get("session_capabilities")
                or {}
            )
            return "resume" in session_capabilities
        return False

    def _check_required_capabilities(self) -> None:
        missing = sorted(
            capability
            for capability in self.profile.required_capabilities
            if not self._supports(capability)
        )
        if missing:
            raise CapabilityError("missing_capabilities:" + ",".join(missing))

    def call(self, coro, timeout: float | None = None):
        timeout = self.profile.request_timeout_seconds if timeout is None else timeout
        self.wait_ready(timeout)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        try:
            return future.result(timeout)
        except FutureTimeout as error:
            future.cancel()
            raise GatewayError("runtime_request_timeout") from error
        except GatewayError:
            raise
        except Exception as error:
            raise GatewayError("runtime_request_failed:" + type(error).__name__) from error

    def new(self, cwd: str, *, mcp_servers=None):
        self.wait_ready()
        self._check_required_capabilities()
        return self.call(self.connection.new_session(cwd=cwd, mcp_servers=mcp_servers or []))

    def load(self, cwd: str, session_id: str, *, mcp_servers=None):
        self.wait_ready()
        self._check_required_capabilities()
        return self.call(
            self.connection.load_session(cwd=cwd, session_id=session_id, mcp_servers=mcp_servers or [])
        )

    def configure_session(self, session_id: str, *, model: str, effort: str):
        """Apply a selection to an idle, loaded session without creating a new one.

        The caller owns the conversation lock. A model change may reset effort,
        so effort is set second and both returned settings must agree. On any
        failure the caller must not submit a prompt; retry reapplies both values.
        """
        if not all(isinstance(value, str) and value for value in (session_id, model, effort)):
            raise GatewayError("runtime_selection_invalid")
        self.wait_ready()
        self.call(self.connection.set_config_option(
            config_id="model", session_id=session_id, value=model,
        ))
        response = self.call(self.connection.set_config_option(
            config_id="reasoning_effort", session_id=session_id, value=effort,
        ))
        actual = {
            str(option.id): str(option.current_value)
            for option in (getattr(response, "config_options", None) or [])
        }
        expected = {"model": model, "reasoning_effort": effort}
        if any(actual.get(key) != value for key, value in expected.items()):
            raise GatewayError("runtime_selection_unverified")
        return response

    async def _configure_for_turn(self, session_id, model, effort):
        await self.connection.set_config_option(config_id="model", session_id=session_id, value=model)
        response = await self.connection.set_config_option(
            config_id="reasoning_effort", session_id=session_id, value=effort)
        actual = {str(option.id): str(option.current_value)
                  for option in (getattr(response, "config_options", None) or [])}
        if actual.get("model") != model or actual.get("reasoning_effort") != effort:
            raise GatewayError("runtime_selection_unverified")

    def prompt(self, session_id: str, text: str, *, on_turn=None, selection=None) -> str:
        self.wait_ready()
        self.turn += 1
        turn = f"{self.generation}:{self.turn}"
        if on_turn is not None:
            on_turn(turn)

        async def run():
            if selection is not None:
                try:
                    await asyncio.wait_for(self._configure_for_turn(
                        session_id, selection.model, selection.effort),
                        self.profile.request_timeout_seconds)
                except Exception as error:
                    # No prompt was sent. Keep the existing lock/recovery fence:
                    # a timed-out settings request might still be in flight.
                    self.events.put(("error", session_id, {
                        "turn_id": turn, "error": "selection_configuration_failed",
                        "error_type": type(error).__name__, "prompt_submitted": False}))
                    return
                self.events.put(("configured", session_id, {
                    "turn_id": turn, "model": selection.model, "effort": selection.effort}))
            try:
                await self.connection.prompt(
                    session_id=session_id, prompt=[acp.text_block(text)]
                )
                self.events.put(("terminal", session_id, {"turn_id": turn}))
            except acp.RequestError as error:
                # A JSON-RPC error is a correlated, terminal response from the
                # agent. It is safe to apply the normal receipt/retry gate;
                # only transport/runtime errors leave effects uncertain.
                self.events.put((
                    "terminal", session_id,
                    {"turn_id": turn, "remote_request_error": error.code},
                ))
            except Exception as error:
                self.events.put(
                    ("error", session_id, {"turn_id": turn, "error": type(error).__name__})
                )

        try:
            asyncio.run_coroutine_threadsafe(run(), self.loop)
        except Exception as error:
            raise GatewayError("runtime_submit_failed:" + type(error).__name__) from error
        return turn

    def cancel(self, session_id: str) -> None:
        if not self.alive:
            raise GatewayError("runtime_unavailable")
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.connection.cancel(session_id=session_id), self.loop
            )
        except Exception as error:
            raise GatewayError("runtime_cancel_failed:" + type(error).__name__) from error

        def record_failure(completed):
            try:
                completed.result()
            except Exception as error:
                self.events.put(("runtime_lost", "", {"error": "cancel:" + type(error).__name__}))

        future.add_done_callback(record_failure)

    def approve_guardian_denied_action(self, session_id: str, review_id: str,
                                       fingerprint: str) -> None:
        """Submit one adapter-owned, opaque Guardian override.

        The official ACP SDK owns the request framing through ``ext_method``.
        Director never receives or reconstructs the native Guardian event: the
        version-pinned adapter retains it and validates this one-shot lookup.
        """
        if not all(isinstance(value, str) and value for value in (session_id, review_id, fingerprint)):
            raise GatewayError("guardian_approval_invalid_request")

        async def run():
            response = await self.connection.ext_method(
                "director/approve_guardian_denied_action",
                {"sessionId": session_id, "reviewId": review_id, "fingerprint": fingerprint},
            )
            if not isinstance(response, dict) or response != {"approved": True}:
                raise GatewayError("guardian_approval_rejected")

        self.call(run())

    def prepare(self, cwd: str, session_id: str | None = None, *, mcp_servers=None) -> Future:
        """Return immediately; the event loop waits for initialize itself."""

        async def run():
            async def await_startup():
                # Do not wrap the concurrent future in a shielded asyncio
                # future: a preparation timeout must not cancel (or leave an
                # unobserved exception on) the shared initialization result.
                while not self.startup.done():
                    await asyncio.sleep(.01)
                return self.startup.result()

            try:
                await asyncio.wait_for(await_startup(), self.profile.startup_timeout_seconds)
            except asyncio.TimeoutError as error:
                raise GatewayError("runtime_prepare_start_timeout") from error
            if not self.alive:
                raise GatewayError("runtime_unavailable")
            self._check_required_capabilities()
            try:
                if session_id:
                    response = await asyncio.wait_for(
                        self.connection.load_session(
                            cwd=cwd, session_id=session_id, mcp_servers=mcp_servers or []
                        ), self.profile.request_timeout_seconds
                    )
                    return session_id, response
                response = await asyncio.wait_for(
                    self.connection.new_session(cwd=cwd, mcp_servers=mcp_servers or []),
                    self.profile.request_timeout_seconds,
                )
            except asyncio.TimeoutError as error:
                raise GatewayError("runtime_prepare_request_timeout") from error
            return response.session_id, response

        return asyncio.run_coroutine_threadsafe(run(), self.loop)

    def observe_liveness(self) -> None:
        if self.closed.is_set() or self.process is None or self.process.returncode is None:
            return
        if not self._runtime_loss_emitted:
            self._runtime_loss_emitted = True
            self.events.put(("runtime_lost", "", {"error": "process_exited"}))
        # A dead adapter can leave MCP descendants behind. Start bounded group
        # cleanup on the first observed exit instead of leaving its PGID for a
        # later retry or receiver shutdown.
        if not self.group_empty and not self.closed.is_set():
            self.close()

    async def _shutdown(self):
        if self.connection is not None:
            try:
                await self.connection.close()
            except Exception:
                pass
        if self.process is not None and self.process.stdin is not None:
            self.process.stdin.close()
            with contextlib.suppress(Exception):
                await self.process.stdin.wait_closed()
        if self.process is not None and not self.group_empty:
            # Signal the owned group even if its parent has already exited:
            # MCP descendants can still retain the same process group.
            try:
                os.killpg(self.process_group_id or self.process.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                if self.process.returncode is None:
                    try:
                        self.process.terminate()
                    except (ProcessLookupError, PermissionError):
                        pass
            try:
                await asyncio.wait_for(self._wait_for_group_exit(), 2)
            except (asyncio.TimeoutError, PermissionError):
                try:
                    os.killpg(self.process_group_id or self.process.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    try:
                        self.process.kill()
                    except (ProcessLookupError, PermissionError):
                        pass
                try:
                    await asyncio.wait_for(self._wait_for_group_exit(), 2)
                except (asyncio.TimeoutError, PermissionError):
                    self.events.put(("runtime_lost", "", {"error": "cleanup_unconfirmed"}))
        # ``asyncio.Process.wait`` releases its pipe transports. The adapter
        # parent may already have exited while an owned descendant kept the
        # group alive, and a previously confirmed empty group still owns these
        # transports, so always release them.
        if self.process is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.process.wait(), .5)
            transport = getattr(self.process, "_transport", None)
            if transport is not None:
                transport.close()
            # Reap can be the operation that removes the last zombie from the
            # process group. Confirm once more after it, without signaling, so
            # repeated close calls never target a recycled PGID.
            if not self.group_empty:
                try:
                    os.killpg(self.process_group_id or self.process.pid, 0)
                except ProcessLookupError:
                    self.group_empty = True
                except PermissionError:
                    pass
        else:
            # A missing executable can fail before a subprocess or process
            # group exists. Its retired loop is safe to release once stopped.
            self.group_empty = True
        if hasattr(self, "stderr"):
            self.stderr.close()

    async def _wait_for_group_exit(self):
        """Wait for the owned process group, not merely its adapter parent."""
        while True:
            try:
                os.killpg(self.process_group_id or self.process.pid, 0)
            except ProcessLookupError:
                self.group_empty = True
                return
            except PermissionError:
                # Permission denial is not evidence that descendants are gone.
                # Leave recovery fenced and let the bounded shutdown report an
                # explicit cleanup_unconfirmed event instead.
                raise
            await asyncio.sleep(.05)

    def close(self, *, wait: bool = False) -> None:
        """Schedule cleanup without stalling the receiver's shared tick."""

        if self.closed.is_set():
            # An earlier nonblocking liveness fence may already be cleaning up
            # this group. Receiver shutdown can still request a bounded wait;
            # do not self-join from the event-loop thread.
            if (wait and self.thread.is_alive()
                    and threading.current_thread() is not self.thread):
                self.thread.join(5)
            return
        self.closed.set()
        if self.loop.is_closed():
            return

        async def stop():
            await self._shutdown()
            # Let this cleanup task return before the event loop stops, then
            # drain any SDK housekeeping tasks in _run.
            self.loop.call_soon(self.loop.stop)

        def cancel_start_and_stop():
            if self._start_task is not None and not self._start_task.done():
                self._start_task.cancel()
            asyncio.create_task(stop())

        try:
            self.loop.call_soon_threadsafe(cancel_start_and_stop)
        except RuntimeError:
            return
        if wait:
            self.thread.join(5)


class AgentGateway:
    def __init__(self, db: sqlite3.Connection, *, project: Path, state_directory: Path,
                 profile: AgentProfile, config_path: str):
        self.db = db
        self.project = project
        self.state_directory = state_directory
        self.profile = profile
        self.config_path = config_path
        self.driver: ACPDriver | None = None
        self.retired_drivers: list[ACPDriver] = []
        self._next_generation = 1
        self.loaded: set[str] = set()
        self.session_metadata: dict[str, dict[str, Any]] = {}
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS agent_sessions (
                root TEXT PRIMARY KEY, profile_id TEXT NOT NULL, backend TEXT NOT NULL,
                session_id TEXT NOT NULL, migrated_from TEXT, created_at REAL NOT NULL,
                updated_at REAL NOT NULL)"""
        )
        self.db.execute("""CREATE TABLE IF NOT EXISTS agent_session_launches (
            profile_id TEXT NOT NULL, backend TEXT NOT NULL, session_id TEXT NOT NULL,
            started INTEGER NOT NULL CHECK(started IN (0,1)),
            PRIMARY KEY(profile_id,backend,session_id))""")
        self.db.commit()

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["DIRECTOR_CONFIG"] = self.config_path
        env.pop("SLACK_BOT_TOKEN", None)
        env.pop("SLACK_APP_TOKEN", None)
        env.setdefault("INITIAL_AGENT_MODE", "agent")
        env.update(dict(self.profile.runtime_environment))
        # Never inherit a working-directory PATH entry for the adapter. A bare
        # command is resolved only through fixed system executable directories.
        trusted = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"]
        executable = Path(self.profile.command[0])
        if executable.is_absolute():
            parent = str(executable.resolve().parent)
            if parent not in trusted:
                trusted.insert(0, parent)
        # Preserve useful inherited absolute locations without reintroducing
        # relative paths (where a bare `node` would resolve from the project).
        inherited = [
            entry for entry in env.get("PATH", "").split(os.pathsep)
            if entry and os.path.isabs(entry) and entry not in trusted
        ]
        env["PATH"] = os.pathsep.join([*trusted, *inherited])
        return env

    _environment = _env

    def _driver(self) -> ACPDriver:
        if self.driver is not None:
            startup = getattr(self.driver, "startup", None)
            is_starting = startup is not None and not startup.done()
            is_alive = getattr(self.driver, "alive", not self.driver.closed.is_set())
            if is_starting or is_alive:
                return self.driver
        if self.driver is not None:
            self._retire(self.driver, stop=True)
            self.driver = None
        driver_type = ACPDriver
        if self.profile.backend == "claude-code":
            from .claude_driver import ClaudeDriver
            driver_type = ClaudeDriver
        self.driver = driver_type(
            self.profile, self.project, self._env(),
            self.state_directory / "dispatch" / f"acp-{self.profile.identifier}.stderr.log",
            self._next_generation,
        )
        self._next_generation += 1
        self.loaded.clear()
        return self.driver

    def _retire(self, driver: ACPDriver, *, stop: bool = False) -> None:
        if driver not in self.retired_drivers:
            self.retired_drivers.append(driver)
        if stop:
            # Capture a process exit before `close` sets the closed fence;
            # otherwise a generation can disappear between receiver ticks
            # without ever producing its durable runtime-loss event.
            driver.observe_liveness()
            driver.close()

    def binding(self, root: str) -> SessionBinding | None:
        row = self.db.execute("SELECT * FROM agent_sessions WHERE root=?", (root,)).fetchone()
        if row is None:
            return None
        return SessionBinding(root, row["profile_id"], row["backend"], row["session_id"], row["migrated_from"])

    def _save(self, binding: SessionBinding) -> None:
        old = self.binding(binding.root)
        if old and (old.profile_id, old.backend) != (binding.profile_id, binding.backend):
            raise GatewayError("session_profile_binding_changed")
        now = time.time()
        self.db.execute(
            """INSERT INTO agent_sessions VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(root) DO UPDATE SET
              session_id=excluded.session_id,
              migrated_from=COALESCE(agent_sessions.migrated_from,excluded.migrated_from),
              updated_at=excluded.updated_at""",
            (binding.root, binding.profile_id, binding.backend, binding.session_id,
             binding.migrated_from, now, now),
        )
        if old is None and binding.backend == "claude-code":
            self.db.execute("INSERT INTO agent_session_launches VALUES (?,?,?,0)",
                            (binding.profile_id, binding.backend, binding.session_id))
        self.db.commit()

    def _never_started(self, session_id):
        row = self.db.execute("SELECT started FROM agent_session_launches WHERE profile_id=? AND backend=? AND session_id=?",
                              (self.profile.identifier, self.profile.backend, session_id)).fetchone()
        return row is not None and row[0] == 0

    @staticmethod
    def _session_metadata(response: Any) -> dict[str, Any]:
        modes = getattr(response, "modes", None)
        config_options = getattr(response, "config_options", None) or []
        return {
            "modes": modes.model_dump(mode="json") if modes else None,
            "config_options": [item.model_dump(mode="json") for item in config_options],
        }

    def _verify_new_session_settings(self, response: Any) -> None:
        """Reject a newly-created session whose applied settings drifted."""
        if not self.profile.expected_settings:
            return
        modes = getattr(response, "modes", None)
        options = getattr(response, "config_options", None)
        if modes is None or options is None:
            raise GatewayError("runtime_settings_unverified")
        actual = {option.id: str(option.current_value) for option in options}
        actual["mode"] = str(getattr(modes, "current_mode_id", actual.get("mode", "")))
        mismatched = [
            key for key, expected in self.profile.expected_settings
            if actual.get(key) != expected
        ]
        if mismatched:
            raise GatewayError("runtime_settings_mismatch:" + ",".join(mismatched))

    def _verify_loaded_session_settings(self, response: Any) -> dict[str, Any]:
        """Record load settings when provided, without treating omissions as drift.

        ACP load responses may legally be an empty acknowledgement. A durable
        existing or migrated session therefore remains usable when its optional
        settings are absent, while diagnostics make that fact explicit. Any
        setting the agent *does* report must still agree with this profile.
        """
        metadata = self._session_metadata(response)
        modes = getattr(response, "modes", None)
        options = {
            str(option.id): str(option.current_value)
            for option in (getattr(response, "config_options", None) or [])
            if getattr(option, "id", None) is not None
            and getattr(option, "current_value", None) is not None
        }
        if modes is not None and getattr(modes, "current_mode_id", None) is not None:
            options["mode"] = str(modes.current_mode_id)
        missing = []
        mismatched = []
        for key, expected in self.profile.expected_settings:
            if self.profile.dynamic_selection and key in ("model", "reasoning_effort"):
                # Every selected turn reapplies and verifies both before prompt.
                continue
            actual = options.get(key)
            if actual is None:
                missing.append(key)
            elif actual != expected:
                mismatched.append(key)
        if mismatched:
            raise GatewayError("runtime_settings_mismatch:" + ",".join(mismatched))
        metadata["settings_verification"] = "verified" if not missing else "unverified"
        metadata["missing_settings"] = missing
        return metadata

    def _diagnostic(self, session_id: str, metadata: dict[str, Any] | None = None) -> None:
        directory = self.state_directory / "dispatch"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        process = self.driver.process if self.driver else None
        if metadata is not None:
            self.session_metadata[session_id] = metadata
        payload = {
            "profile": self.profile.identifier,
            "backend": self.profile.backend,
            "generation": getattr(self.driver, "generation", None),
            "pid": getattr(process, "pid", None),
            "protocol_version": getattr(self.driver, "protocol_version", None),
            "agent_capabilities": getattr(self.driver, "capabilities", {}),
            "session_id": session_id,
            "prompt_count": getattr(self.driver, "turn", 0),
            "metadata": self.session_metadata.get(session_id, {}),
        }
        path = directory / "agent-gateway.json"
        path.write_text(json.dumps(payload, sort_keys=True))
        path.chmod(0o600)

    def _trace(self, event: AgentEvent) -> None:
        """Write a private SDK lifecycle trace, including tool result evidence."""
        directory = self.state_directory / "dispatch"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        import hashlib

        name = (
            hashlib.sha256(event.root.encode()).hexdigest()[:24] + ".acp.jsonl"
            if event.root else "acp-runtime.jsonl"
        )
        path = directory / name
        record = {
            "at": time.time(),
            "root": event.root,
            "generation": event.generation,
            "kind": event.kind,
            "session_id": event.session_id,
            "turn_id": event.turn_id,
            "detail": event.detail or {},
        }
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        with os.fdopen(descriptor, "a") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")

    def start_or_resume(self, root: str, *, legacy_session_id: str | None = None,
                        mcp_servers=None) -> SessionBinding:
        """Synchronous compatibility API for direct callers and focused tests."""
        old = self.binding(root)
        if old:
            if (old.profile_id, old.backend) != (self.profile.identifier, self.profile.backend):
                raise GatewayError("session_profile_binding_changed")
        # A mismatched durable binding must fail before starting a new adapter
        # process. This protects the profile fence and avoids an unnecessary
        # subprocess/pipe lifetime for a rejected rollback attempt.
        driver = self._driver()
        if old:
            if old.session_id not in self.loaded:
                load_options = {'mcp_servers': mcp_servers}
                if self.profile.backend == "claude-code":
                    load_options['unstarted'] = self._never_started(old.session_id)
                response = driver.load(str(self.project), old.session_id, **load_options)
                self.loaded.add(old.session_id)
                metadata = self._verify_loaded_session_settings(response)
            else:
                metadata = self.session_metadata.get(old.session_id, {"loaded": True})
            self._diagnostic(old.session_id, metadata)
            return old
        if legacy_session_id:
            response = driver.load(str(self.project), legacy_session_id, mcp_servers=mcp_servers)
            binding = SessionBinding(root, self.profile.identifier, self.profile.backend,
                                     legacy_session_id, legacy_session_id)
            metadata = self._verify_loaded_session_settings(response)
        else:
            response = driver.new(str(self.project), mcp_servers=mcp_servers)
            self._verify_new_session_settings(response)
            binding = SessionBinding(root, self.profile.identifier, self.profile.backend,
                                     response.session_id)
            metadata = self._session_metadata(response)
        self._save(binding)
        self.loaded.add(binding.session_id)
        self._diagnostic(binding.session_id, metadata)
        return binding

    def publish_reply_server(self):
        """Return the fixed, channel-scoped Director MCP server."""
        return McpServerStdio(
            name="director-publish-reply",
            command=str(self.project / ".venv/bin/python"),
            args=["-m", "director.agent_tools", "--config", self.config_path],
            env=[],
        )

    def begin_prepare(self, root: str, *, legacy_session_id: str | None = None,
                      mcp_servers=None) -> Preparation:
        old = self.binding(root)
        if old and (old.profile_id, old.backend) != (self.profile.identifier, self.profile.backend):
            raise GatewayError("session_profile_binding_changed")
        target = old.session_id if old else legacy_session_id
        driver = self._driver()
        prepare_options = {'mcp_servers': mcp_servers}
        if self.profile.backend == "claude-code" and target:
            prepare_options['unstarted'] = self._never_started(target)
        return Preparation(
            root,
            old,
            legacy_session_id,
            driver.prepare(str(self.project), target, **prepare_options),
            driver,
        )

    def complete_prepare(self, preparation: Preparation) -> SessionBinding:
        if self.driver is not preparation.driver or not preparation.driver.alive:
            raise GatewayError("runtime_prepare_generation_lost")
        session_id, response = preparation.future.result()
        if preparation.existing:
            self.loaded.add(session_id)
            self._diagnostic(session_id, self._verify_loaded_session_settings(response))
            return preparation.existing
        if preparation.legacy_session_id:
            metadata = self._verify_loaded_session_settings(response)
        else:
            self._verify_new_session_settings(response)
            metadata = self._session_metadata(response)
        binding = SessionBinding(preparation.root, self.profile.identifier, self.profile.backend,
                                 session_id, preparation.legacy_session_id)
        self._save(binding)
        self.loaded.add(session_id)
        self._diagnostic(binding.session_id, metadata)
        return binding

    def submit(self, binding: SessionBinding, text: str, *, on_turn=None, selection=None) -> str:
        if self.driver is None or not self.driver.alive:
            raise GatewayError("runtime_unavailable")
        if self.profile.dynamic_selection != (selection is not None):
            raise GatewayError("runtime_selection_required")
        if self.profile.backend == "claude-code":
            if on_turn is None:
                raise GatewayError("claude_registration_required")
            parent_register = on_turn
            def register_claude(turn):
                # Record an attempted native session before any callback can
                # release input. Missing legacy rows are conservatively started.
                with self.db:
                    self.db.execute("INSERT INTO agent_session_launches VALUES (?,?,?,1) ON CONFLICT(profile_id,backend,session_id) DO UPDATE SET started=1",
                                    (binding.profile_id, binding.backend, binding.session_id))
                parent_register(turn)
            on_turn = register_claude
        if selection is not None:
            from .model_selection import MODELS
            expected_backend = {"codex-acp": "codex", "claude-code": "claude"}.get(self.profile.backend)
            if (selection.backend != expected_backend
                    or selection.model != MODELS.get(expected_backend, {}).get(selection.grade)
                    or selection.effort != "high"):
                raise GatewayError("runtime_selection_binding_mismatch")
            turn = self.driver.prompt(binding.session_id, text, on_turn=on_turn, selection=selection)
        else:
            turn = self.driver.prompt(binding.session_id, text, on_turn=on_turn)
        self._diagnostic(binding.session_id)
        return turn

    def approve_guardian_denied_action(self, binding: SessionBinding, review_id: str,
                                       fingerprint: str) -> None:
        if self.driver is None or not self.driver.alive:
            raise GatewayError("runtime_unavailable")
        self.driver.approve_guardian_denied_action(binding.session_id, review_id, fingerprint)

    def prompt(self, root: str, text: str, *, legacy_session_id: str | None = None,
               mcp_servers=None) -> SessionBinding:
        binding = self.start_or_resume(root, legacy_session_id=legacy_session_id,
                                       mcp_servers=mcp_servers)
        self.submit(binding, text)
        return binding

    def cancel(self, root: str) -> None:
        binding = self.binding(root)
        if binding:
            if self.driver is None or not self.driver.alive:
                raise GatewayError("runtime_unavailable")
            self.driver.cancel(binding.session_id)

    def abort_preparation(self, preparation: Preparation) -> None:
        """Cancel unsubmitted setup and dispose an idle stalled process."""
        preparation.future.cancel()
        if self.driver is preparation.driver:
            self._retire(self.driver, stop=True)
            self.driver = None
            self.loaded.clear()

    def recover_preparation_failure(self, preparation: Preparation, *, idle: bool) -> None:
        """Release a failed pre-submit operation without disrupting live turns.

        A timed out initialize leaves a process alive but unusable: it has a
        connection object, yet no successful startup future.  Dispose that
        generation when it is idle so the next durable retry starts a clean
        runtime.  A healthy initialized runtime remains resident after a
        failed new/load request, especially while another root is active.
        """
        preparation.future.cancel()
        driver = preparation.driver
        startup = driver.startup
        unhealthy = (
            not startup.done()
            or startup.cancelled()
            or not driver.alive
        )
        if idle and self.driver is driver and unhealthy:
            self._retire(driver, stop=True)
            self.driver = None
            self.loaded.clear()

    def poll(self) -> tuple[AgentEvent, ...]:
        output = []
        drivers = [driver for driver in [self.driver, *self.retired_drivers] if driver]
        for driver in drivers:
            driver.observe_liveness()
            while True:
                try:
                    kind, session_id, detail = driver.events.get_nowait()
                except queue.Empty:
                    break
                row = (self.db.execute("SELECT root FROM agent_sessions WHERE session_id=? AND profile_id=? AND backend=?",
                                       (session_id, self.profile.identifier, self.profile.backend)).fetchone()
                       if session_id else None)
                event = AgentEvent(kind, row["root"] if row else "", session_id or None,
                                   detail.get("turn_id"), detail, driver.generation, self.profile.identifier)
                self._trace(event)
                output.append(event)
            # SDK callbacks can still arrive while the loop drains shutdown
            # tasks. Retain a retired generation until its thread has exited
            # and this pass has drained its event queue.
            if (driver is not self.driver and not driver.thread.is_alive()
                    and driver.events.empty()):
                self.retired_drivers.remove(driver)
        return tuple(output)

    def close(self, *, wait: bool = True) -> None:
        if self.driver:
            self.driver.close(wait=wait)
        for driver in self.retired_drivers:
            driver.close(wait=wait)
        self.retired_drivers.clear()
        self.driver = None
