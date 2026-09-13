"""Tracked Odyssey selection settings stay explicit and host-scoped."""
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
NODE = "/opt/homebrew/bin/node"
ADAPTER = "/Users/example/Code/Director/node_modules/@agentclientprotocol/codex-acp/dist/index.js"
CLAUDE = "/Users/example/.local/bin/claude"


class OdysseyModelSelectionConfigTests(unittest.TestCase):
    def config(self, name):
        return json.loads((ROOT / "config" / name).read_text())

    def test_odyssey_profiles_keep_identity_state_and_rollback_settings(self):
        expected = {
            "director.json": {
                "channel_id": "C0000000003",
                "database_path": "state/inbox.sqlite3",
                "feed": "C0000000005",
                "thread_id": "00000000-0000-4000-8000-000000000001",
                "additional_configs": ["config/director-tests.json"],
            },
            "director-tests.json": {
                "channel_id": "C0000000004",
                "database_path": "state/testing/inbox.sqlite3",
                "feed": "C0000000007",
                "thread_id": None,
                "additional_configs": None,
            },
        }
        for name, values in expected.items():
            with self.subTest(config=name):
                config = self.config(name)
                self.assertEqual(config["team_id"], "T0000000009")
                self.assertEqual(config["slack_app_id"], "A0000000001")
                self.assertEqual(config["bot_user_id"], "U0000000011")
                self.assertEqual(config["channel_id"], values["channel_id"])
                self.assertEqual(config["database_path"], values["database_path"])
                self.assertEqual(config["conversation_feed"]["channel_id"], values["feed"])
                self.assertEqual(config.get("codex_thread_id"), values["thread_id"])
                self.assertEqual(config.get("additional_configs"), values["additional_configs"])
                self.assertEqual(config["dispatcher"]["runtime"], "acp")
                self.assertEqual(config["dispatcher"]["model"], "gpt-6-astra")
                self.assertEqual(config["dispatcher"]["acp"], {
                    "command": [NODE, ADAPTER],
                    "profile": "codex-default",
                    "backend": "codex-acp",
                    "model": "gpt-6-astra",
                    "initial_agent_mode": "agent",
                    "reasoning_effort": "medium",
                    "prepare_timeout_seconds": 90,
                    "required_capabilities": ["load"],
                    "migrate_legacy_sessions": True,
                })

    def test_selection_is_opted_in_with_separate_claude_profile(self):
        for name in ("director.json", "director-tests.json"):
            with self.subTest(config=name):
                dispatcher = self.config(name)["dispatcher"]
                self.assertEqual(dispatcher["model_selection"], {
                    "enabled": True,
                    "default_backend": "codex",
                    "timeout_seconds": 40,
                })
                self.assertEqual(dispatcher["claude"], {
                    "command": [CLAUDE],
                    "profile": "claude-default",
                    "timeout_seconds": 300,
                })
                self.assertNotEqual(dispatcher["claude"]["profile"], dispatcher["acp"]["profile"])


if __name__ == "__main__":
    unittest.main()
