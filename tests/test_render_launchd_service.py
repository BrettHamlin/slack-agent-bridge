from __future__ import annotations

import json
from pathlib import Path
import plistlib
import tempfile
import unittest

from scripts.render_launchd_service import render, service_definition


class RenderLaunchdServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.project = Path(self.temporary.name)
        (self.project / "config").mkdir()
        self.config_path = self.project / "config/director-local.local.json"
        self.config_path.write_text(json.dumps({
            "team_id": "T-local", "channel_id": "C-local", "owner_user_id": "U-owner",
            "workspace_domain": "local.slack.com", "enabled": True,
            "database_path": "state/local-acceptance/inbox.sqlite3",
            "receiver_service": "com.example.director.local",
            "conversation_feed": {"enabled": False},
        }))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_renders_a_config_bound_service_without_credentials(self) -> None:
        output = self.project / "LaunchAgents/com.example.director.local.plist"
        self.assertEqual(render(self.project, self.config_path, output), "created")
        data = plistlib.loads(output.read_bytes())
        self.assertEqual(data["Label"], "com.example.director.local")
        self.assertEqual(data["WorkingDirectory"], str(self.project.resolve()))
        self.assertEqual(data["ProgramArguments"][-3:], ["--config", str(self.config_path.resolve()), "listen"])
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(render(self.project, self.config_path, output), "unchanged")

    def test_refuses_conflicting_service_or_out_of_project_config(self) -> None:
        output = self.project / "service.plist"
        output.write_text("different")
        with self.assertRaisesRegex(FileExistsError, "different_contents"):
            render(self.project, self.config_path, output)
        outside = self.project / "outside/outside.json"
        outside.parent.mkdir()
        outside.write_text("{}")
        with self.assertRaisesRegex(ValueError, "config_must"):
            service_definition(self.project, outside)
