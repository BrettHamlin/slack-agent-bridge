from __future__ import annotations

import anyio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from director.agent_tools import publish_reply_result


WORKTREE = Path(__file__).resolve().parents[1]


def write_config(project: Path) -> Path:
    config_path = project / "config" / "director-tests.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "team_id": "T-test",
                "channel_id": "C-test",
                "owner_user_id": "U-owner",
                "bot_user_id": "U-bot",
                "workspace_domain": "example.test",
                "enabled": True,
                "database_path": "state/testing/inbox.sqlite3",
            }
        )
    )
    (project / "state" / "testing").mkdir(parents=True)
    return config_path


def result_text(result: object) -> str:
    contents = getattr(result, "content", ())
    return "\n".join(str(getattr(item, "text", "")) for item in contents)


class AgentToolsMcpTests(unittest.TestCase):
    def test_official_stdio_server_ignores_cwd_dotenv_fifo(self) -> None:
        """Low-level MCP startup must not open the owner-only dotenv FIFO."""
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            config_path = write_config(project)
            os.mkfifo(project / ".env", 0o600)

            async def scenario() -> None:
                parameters = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "director.agent_tools", "--config", str(config_path)],
                    cwd=project,
                    env={
                        "PATH": os.environ.get("PATH", ""),
                        "PYTHONPATH": str(WORKTREE),
                        "PYTHONUNBUFFERED": "1",
                    },
                )
                with anyio.fail_after(5):
                    async with stdio_client(parameters) as (read_stream, write_stream):
                        async with ClientSession(read_stream, write_stream) as session:
                            await session.initialize()
                            listed = await session.list_tools()
                            self.assertEqual([tool.name for tool in listed.tools], ["publish_reply"])
                            schema = listed.tools[0].inputSchema
                            self.assertEqual(schema["required"], ["text", "authority"])
                            self.assertEqual(schema["properties"]["text"]["type"], "string")
                            self.assertEqual(schema["properties"]["authority"]["type"], "string")
                            invalid_schema = await session.call_tool(
                                "publish_reply",
                                {"text": 7, "authority": "opaque-turn-authority"},
                            )
                            self.assertTrue(invalid_schema.isError)
                            result = await session.call_tool(
                                "publish_reply",
                                {"text": "   ", "authority": "opaque-turn-authority"},
                            )
                            text = result_text(result)
                            self.assertFalse(result.isError)
                            self.assertEqual(json.loads(text), {"published": False, "state": "invalid_text"})
                            self.assertNotIn("opaque-turn-authority", text)
                            unavailable = await session.call_tool(
                                "publish_reply",
                                {"text": "synthetic reply", "authority": "opaque-turn-authority"},
                            )
                            unavailable_text = result_text(unavailable)
                            self.assertFalse(unavailable.isError)
                            self.assertEqual(
                                json.loads(unavailable_text),
                                {"published": False, "state": "receiver_unavailable"},
                            )
                            self.assertNotIn("opaque-turn-authority", unavailable_text)

            anyio.run(scenario)

    def test_valid_authority_submits_only_bounded_command_and_redacts_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = write_config(Path(directory))
            config = json.loads(config_path.read_text())
            path = Path(directory).resolve() / "state" / "testing" / "inbox.sqlite3"
            with patch(
                "director.agent_tools.submit_command", return_value={"published": True, "state": "sent"}
            ) as submit:
                invalid_payload = publish_reply_result(path, config, "", "opaque-turn-authority")
                self.assertEqual(invalid_payload, {"published": False, "state": "invalid_text"})
                submit.assert_not_called()

                result_payload = publish_reply_result(
                    path, config, "safe prepared reply", "opaque-turn-authority"
                )

            self.assertEqual(result_payload, {"published": True, "state": "sent"})
            submitted_path, args, submitted_config = submit.call_args.args
            self.assertEqual(args.command, "agent-publish")
            self.assertEqual(args.authority, "opaque-turn-authority")
            self.assertEqual(args.payload_text, "safe prepared reply")
            self.assertEqual(submitted_path, path)
            self.assertEqual(submitted_config["channel_id"], "C-test")
            self.assertNotIn("opaque-turn-authority", json.dumps(result_payload))


if __name__ == "__main__":
    unittest.main()
