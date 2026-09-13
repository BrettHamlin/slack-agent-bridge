"""A deliberately small ACP agent implemented with the official Python SDK.

It is a subprocess fixture for gateway tests.  Its modes are selected only by
the test process through ``ACP_TEST_MODE``; it never reads Director data.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys

import acp


class TestAgent:
    def __init__(self) -> None:
        self.connection = None
        self.cancelled: dict[str, asyncio.Event] = {}
        self.count = 0

    def on_connect(self, connection) -> None:
        self.connection = connection

    async def initialize(self, protocol_version, **_kwargs):
        mode = os.environ.get("ACP_TEST_MODE", "normal")
        if mode == "never-init":
            await asyncio.Event().wait()
        if mode == "stall-init":
            await asyncio.sleep(3)
        if mode == "bad-init":
            raise RuntimeError("synthetic initialization failure")
        capabilities = {} if mode == "no-load" else {"loadSession": True}
        return acp.InitializeResponse.model_validate(
            {"protocolVersion": 2 if mode == "unsupported-protocol" else protocol_version,
             "agentCapabilities": capabilities}
        )

    async def new_session(self, cwd, **_kwargs):
        if os.environ.get("ACP_TEST_MODE") == "never-new":
            await asyncio.Event().wait()
        self.count += 1
        return acp.NewSessionResponse.model_validate(
            {
                # A real ACP runtime assigns globally unique session IDs.
                # Include this synthetic peer's PID so an adapter replacement
                # cannot make two durable roots appear to share ``peer-1``.
                "sessionId": f"peer-{os.getpid()}-{self.count}",
                "modes": {
                    "currentModeId": "agent",
                    "availableModes": [{"id": "agent", "name": "Agent"}],
                },
                "configOptions": [
                    {
                        "type": "select", "id": "model", "name": "Model",
                        "currentValue": "gpt-6-astra", "category": "model",
                        "options": [{"value": "gpt-6-astra", "name": "gpt-6-astra"}],
                    },
                    {
                        "type": "select", "id": "reasoning_effort", "name": "Reasoning",
                        "currentValue": "medium", "category": "thought_level",
                        "options": [{"value": "medium", "name": "medium"}],
                    },
                ],
            }
        )

    async def load_session(self, cwd, session_id, **_kwargs):
        mode = os.environ.get("ACP_TEST_MODE")
        if mode == "never-load":
            await asyncio.Event().wait()
        if mode == "load-fails":
            raise RuntimeError("synthetic load failure")
        return acp.LoadSessionResponse()

    async def prompt(self, session_id, prompt, **_kwargs):
        text = "".join(getattr(part, "text", "") for part in prompt)
        if os.environ.get("ACP_TEST_MODE") == "remote-request-error":
            # Exercise the SDK's correlated JSON-RPC request-error path.  It
            # is a terminal response, not evidence that the adapter died.
            raise acp.RequestError(-32001, "synthetic remote request failure")
        if "die" in text:
            os._exit(17)
        if "orphan-descendant" in text:
            child = subprocess.Popen(
                [
                    sys.executable, '-c',
                    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)',
                ]
            )
            marker = os.environ.get('ACP_TEST_DESCENDANT_PID_PATH')
            if marker:
                Path(marker).write_text(str(child.pid))
            os._exit(18)
        if os.environ.get("ACP_TEST_MODE") in {"hold-prompts", "delay-cancel"} or "hold" in text:
            event = self.cancelled.setdefault(session_id, asyncio.Event())
            await event.wait()
            return acp.PromptResponse.model_validate({"stopReason": "cancelled"})
        if "fixture-permission" in text:
            response = await self.connection.request_permission(
                session_id,
                # ACP 0.12's request schema takes ``ToolCallUpdate`` rather
                # than the richer session-update subtype returned by the
                # helper, so validate through the SDK's generated model.
                acp.schema.ToolCallUpdate.model_validate(
                    acp.start_tool_call("fixture-permission", "Synthetic permission", kind="execute").model_dump()
                ),
                [
                    acp.schema.PermissionOption(optionId="reject", name="Reject", kind="reject_once"),
                    acp.schema.PermissionOption(optionId="allow", name="Allow", kind="allow_once"),
                ],
            )
            result_path = os.environ.get("ACP_TEST_PERMISSION_RESULT_PATH")
            if result_path:
                Path(result_path).write_text(
                    f"{response.outcome.outcome}:{getattr(response.outcome, 'option_id', '')}"
                )
            await self.connection.session_update(
                session_id, acp.update_agent_message_text(f"permission:{response.outcome.outcome}")
            )
            return acp.PromptResponse.model_validate({"stopReason": "end_turn"})
        await self.connection.session_update(
            session_id, acp.update_agent_message_text(f"reply:{session_id}:{text}")
        )
        return acp.PromptResponse.model_validate({"stopReason": "end_turn"})

    async def cancel(self, session_id, **_kwargs):
        if os.environ.get("ACP_TEST_MODE") == "delay-cancel":
            # Cancellation is an ACP notification.  Delay the actual prompt
            # terminal so tests can prove the local send is not treated as a
            # remote completion acknowledgement.
            received = os.environ.get("ACP_TEST_CANCEL_RECEIVED_PATH")
            release = os.environ.get("ACP_TEST_CANCEL_RELEASE_PATH")
            if received:
                Path(received).write_text("received")
            if release:
                while not Path(release).exists():
                    await asyncio.sleep(.01)
            else:
                await asyncio.sleep(.3)
        self.cancelled.setdefault(session_id, asyncio.Event()).set()


asyncio.run(acp.run_agent(TestAgent()))
