import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from scripts.acceptance import (LIVE_PROFILES, ROOT, TEST_CONFIG, TEST_TEAM, TEST_CHANNEL, TEST_FEED_CHANNEL, check_impact,
                                initialize, read_catalog, selected, sha, summarize, validated_target)


class AcceptanceContractTests(unittest.TestCase):
    def test_catalog_and_smoke_dependency_closure(self):
        rows = read_catalog()
        self.assertEqual(len(rows), 33)
        smoke = selected(rows, "smoke")
        self.assertTrue({"A01", "A02", "A03", "A05", "A09"} <= {r["id"] for r in smoke})
        self.assertTrue(all(set(r["dependencies"]) <= {s["id"] for s in smoke} for r in smoke))

    def body(self, changes="unchanged"):
        return ("Acceptance scenarios: A02, A08\nAcceptance changes: " + changes +
                "\nAcceptance rationale: Existing card expectations still apply.\n"
                "Acceptance evidence: pending live run after deployment\n")

    def test_product_change_requires_impact(self):
        with self.assertRaises(AssertionError):
            check_impact("", ["director/dispatcher.py"], read_catalog())

    def test_unknown_scenario_rejected(self):
        with self.assertRaises(AssertionError):
            check_impact(self.body().replace("A02, A08", "A99"), ["director/dispatcher.py"], read_catalog())

    def test_blank_field_cannot_consume_next_line(self):
        with self.assertRaises(AssertionError):
            check_impact(self.body().replace("Acceptance rationale: Existing card expectations still apply.", "Acceptance rationale:"), ["director/dispatcher.py"], read_catalog())

    def test_updated_claim_requires_scenario_diff(self):
        with self.assertRaises(AssertionError):
            check_impact(self.body("updated"), ["director/dispatcher.py"], read_catalog())
        check_impact(self.body("updated"), ["acceptance/scenarios/A02-waiting-card.md"], read_catalog())

    def test_unchanged_expectations_allow_explicit_rationale(self):
        check_impact(self.body(), ["director/dispatcher.py"], read_catalog())

    def fixture(self, root):
        self.write_test_config(root)
        target = validated_target(root, "smoke", TEST_CONFIG)
        (root / "spec.md").write_text("pinned scenario")
        (root / "evidence.log").write_text("test evidence")
        spec = {"id": "A01", "snapshot": "spec.md", "sha256": sha(root / "spec.md"),
                "mode": "computer", "dependencies": []}
        (root / "manifest.json").write_text(json.dumps({"run_id": "test", "suite": "smoke",
                                                       "target": target, "scenarios": [spec]}))
        data = {"run_id": "test", "runtime_sha": "a" * 40, "runner": "synthetic test",
                "target": target,
                "preflight": "synthetic test", "fixtures": [], "results": [{
                "id": "A01", "status": "PASS", "started_at": "2026-09-10T10:00:00+00:00",
                "finished_at": "2026-09-10T10:01:00+00:00", "observed": "synthetic assertion",
                "cleanup": "CLEAN", "evidence": [{"kind": "log", "path": "evidence.log"}]}]}
        return data

    def write_test_config(self, root, **overrides):
        profile = LIVE_PROFILES["odyssey"]
        config = {key: profile[key] for key in ("team_id", "channel_id", "environment", "channel_name",
                                                "owner_user_id", "slack_app_id", "bot_user_id", "workspace_domain",
                                                "database_path")}
        config.update({"enabled": True, "conversation_feed": {
            "enabled": True, "channel_id": profile["feed_channel_id"],
        }})
        config.update(overrides)
        (root / "config").mkdir(exist_ok=True)
        (root / TEST_CONFIG).write_text(json.dumps(config))
        return root / TEST_CONFIG

    def write_atlas_config(self, root, **overrides):
        profile = LIVE_PROFILES["atlas"]
        config = {key: profile[key] for key in ("team_id", "channel_id", "environment", "channel_name",
                                                "owner_user_id", "slack_app_id", "bot_user_id", "workspace_domain",
                                                "database_path")}
        config.update({"enabled": True, "conversation_feed": {
            "enabled": True, "channel_id": profile["feed_channel_id"],
        }})
        config.update(overrides)
        (root / "config").mkdir(exist_ok=True)
        path = root / profile["config"]
        path.write_text(json.dumps(config))
        return path

    def write_local_config(self, root, **overrides):
        config = {
            "team_id": "T-local", "channel_id": "C-local", "channel_name": "director-local-tests",
            "environment": "development", "owner_user_id": "U-owner", "slack_app_id": "A-local",
            "bot_user_id": "U-bot", "workspace_domain": "local.slack.com", "enabled": True,
            "database_path": "state/local-acceptance/inbox.sqlite3", "receiver_service": "com.example.director.local",
            "conversation_feed": {"enabled": True, "channel_id": "C-local-feed"},
        }
        config.update(overrides)
        (root / "config").mkdir(exist_ok=True)
        path = root / "config/director-local.local.json"
        path.write_text(json.dumps(config))
        return path

    def test_local_profile_requires_explicit_standalone_development_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_local_config(root)
            with self.assertRaisesRegex(AssertionError, "explicit --config"):
                validated_target(root, "smoke", profile="local")
            with patch.dict(os.environ, {"DIRECTOR_CONFIG": str(path)}):
                with self.assertRaisesRegex(AssertionError, "explicit --config"):
                    validated_target(root, "smoke", profile="local")
            target = validated_target(root, "smoke", str(path.resolve()), "local")
            self.assertEqual(target["profile"], "local")
            self.assertEqual(target["receiver_service"], "com.example.director.local")
            for change in (
                {"environment": "test"}, {"channel_name": "director-local"},
                {"database_path": "state/inbox.sqlite3"}, {"additional_configs": ["config/director-tests.json"]},
                {"conversation_feed": {"enabled": True, "channel_id": "C-local"}},
                {"slack_app_id": LIVE_PROFILES["odyssey"]["slack_app_id"]},
                {"receiver_service": LIVE_PROFILES["atlas"]["receiver_service"]},
            ):
                with self.subTest(change=change):
                    self.write_local_config(root, **change)
                    with self.assertRaises(AssertionError):
                        validated_target(root, "smoke", str(path.resolve()), "local")

    def test_local_profile_rejects_a_symlinked_config_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_local_config(root)
            (root / "config").rename(root / "real-config")
            (root / "config").symlink_to(root / "real-config", target_is_directory=True)
            with self.assertRaises(AssertionError):
                validated_target(root, "smoke", "config/director-local.local.json", "local")

    def test_local_profile_rejects_an_absolute_symlink_alias(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            path = self.write_local_config(root)
            alias = root / "config/director-alias.local.json"
            alias.symlink_to(path)
            with self.assertRaises(AssertionError):
                validated_target(root, "smoke", str(alias), "local")

    def test_live_target_rejects_production_destination_and_state(self):
        for change in ({"team_id": "OTHER"}, {"channel_id": "C0000000003"},
                       {"environment": "production"}, {"channel_name": "director-owner"},
                       {"database_path": "state/inbox.sqlite3"},
                       {"database_path": "../inbox.sqlite3"}, {"database_path": None}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); self.write_test_config(root, **change)
                with self.assertRaises(AssertionError):
                    validated_target(root, "full", TEST_CONFIG)

    def test_live_feed_target_is_pinned_and_production_feed_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_test_config(root, conversation_feed={"enabled": True, "channel_id": "C0000000007"})
            self.assertEqual(validated_target(root, "full", TEST_CONFIG)["feed_channel_id"], "C0000000007")
            for channel in ("C0000000005", TEST_CHANNEL, "COTHER", None):
                self.write_test_config(root, conversation_feed={"enabled": True, "channel_id": channel})
                with self.assertRaisesRegex(AssertionError, "profile feed"):
                    validated_target(root, "full", TEST_CONFIG)

    def test_live_target_rejects_config_and_state_symlinks(self):
        for location in ("config", "database", "dispatch", "parent"):
            with self.subTest(location=location), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); config = self.write_test_config(root)
                (root / "state/testing").mkdir(parents=True)
                if location == "config":
                    config.rename(root / "config/other.json")
                    config.symlink_to(root / "config/other.json")
                elif location == "parent":
                    (root / "state/testing").rmdir()
                    (root / "state/testing").symlink_to(root / "state", target_is_directory=True)
                else:
                    name = "inbox.sqlite3" if location == "database" else "dispatch"
                    (root / "state/testing" / name).symlink_to(root / "state" / name)
                with self.assertRaises(AssertionError):
                    validated_target(root, "smoke", TEST_CONFIG)

    def test_environment_selection_and_explicit_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.write_test_config(root)
            with patch.dict(os.environ, {"DIRECTOR_CONFIG": "config/director.json"}):
                with self.assertRaises(AssertionError):
                    validated_target(root, "smoke")
                target = validated_target(root, "smoke", TEST_CONFIG)
            self.assertEqual(target["channel_id"], TEST_CHANNEL)
            self.assertEqual(target["dispatcher_state_directory"], str(root.resolve() / "state/testing/dispatch"))

    def test_atlas_requires_explicit_profile_and_exact_development_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_atlas_config(root)
            with self.assertRaisesRegex(AssertionError, "selected profile config"):
                validated_target(root, "smoke", str(path))
            target = validated_target(root, "smoke", str(path.resolve()), "atlas")
            self.assertEqual(target["profile"], "atlas")
            self.assertEqual(target["slack_app_id"], "A0000000002")
            self.assertEqual(target["bot_user_id"], "U0000000012")
            self.assertEqual(target["receiver_service"], "com.example.director.atlas-dev")
            self.assertEqual(target["database_path"], str(root.resolve() / "state/development/inbox.sqlite3"))
            self.assertEqual(target["dispatcher_state_directory"], str(root.resolve() / "state/development/dispatch"))
            for change in ({"environment": "test"}, {"slack_app_id": "A0000000001"},
                           {"bot_user_id": "U0000000011"}, {"channel_id": TEST_CHANNEL},
                           {"database_path": "state/testing/inbox.sqlite3"},
                           {"conversation_feed": {"enabled": True, "channel_id": TEST_FEED_CHANNEL}},
                           {"additional_configs": ["config/director-tests.json"]}):
                with self.subTest(change=change):
                    self.write_atlas_config(root, **change)
                    with self.assertRaises(AssertionError):
                        validated_target(root, "full", str(path.resolve()), "atlas")

    def test_atlas_profile_is_not_selected_from_environment_or_hostname(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_atlas_config(root)
            with patch.dict(os.environ, {"DIRECTOR_CONFIG": str(path.resolve()), "HOSTNAME": "atlas"}):
                with self.assertRaisesRegex(AssertionError, "selected profile config"):
                    validated_target(root, "smoke")
                target = validated_target(root, "smoke", profile="AtLaS")
            self.assertEqual(target["profile"], "atlas")

    def test_atlas_init_pins_the_selected_profile_in_its_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_atlas_config(root)
            shutil.copytree(ROOT / "acceptance", root / "acceptance")
            with patch("scripts.acceptance.subprocess.check_output", side_effect=["a" * 40, ""]):
                directory = initialize(root, "smoke", "atlas", str(path.resolve()), "ATLAS")
            manifest = json.loads((directory / "manifest.json").read_text())
            self.assertEqual(manifest["target"]["profile"], "atlas")
            self.assertEqual(manifest["target"]["config_path"], str(path.resolve()))
            self.assertEqual(manifest["target"]["receiver_service"], "com.example.director.atlas-dev")

    def test_live_profile_rejects_an_absolute_symlink_alias(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = self.write_atlas_config(root)
            alias = root / "atlas-config-alias.json"
            alias.symlink_to(path.resolve())
            with self.assertRaisesRegex(AssertionError, "selected profile config"):
                validated_target(root, "smoke", str(alias), "atlas")

    def test_init_pins_target_and_rejects_unsafe_target_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.write_test_config(root)
            shutil.copytree(ROOT / "acceptance", root / "acceptance")
            with patch("scripts.acceptance.subprocess.check_output", side_effect=["a" * 40, ""]):
                directory = initialize(root, "smoke", "safe", TEST_CONFIG)
            manifest = json.loads((directory / "manifest.json").read_text())
            results = json.loads((directory / "results.json").read_text())
            self.assertEqual(manifest["target"], results["target"])
            self.assertEqual(manifest["target"], validated_target(root, "smoke", TEST_CONFIG))
            self.write_test_config(root, channel_id="C0000000003")
            with self.assertRaises(AssertionError):
                initialize(root, "full", "unsafe", TEST_CONFIG)
            self.assertFalse((root / "state/acceptance/unsafe").exists())

    def test_report_rejects_target_tampering_and_config_drift(self):
        for change in ("result", "manifest", "config", "missing", "production"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); data = self.fixture(root)
                data["results"][0]["status"] = "PENDING"
                manifest = json.loads((root / "manifest.json").read_text())
                if change == "result":
                    data["target"]["channel_id"] = "C0000000003"
                elif change == "manifest":
                    manifest["target"]["database_path"] = str(root / "state/inbox.sqlite3")
                elif change == "config":
                    self.write_test_config(root, enabled=False)
                elif change == "production":
                    self.write_test_config(root, channel_id="C0000000003")
                else:
                    del manifest["target"]
                (root / "manifest.json").write_text(json.dumps(manifest))
                self.save(root, data)
                with self.assertRaises((AssertionError, KeyError)):
                    summarize(root)

    def test_report_accepts_a_historical_odyssey_manifest_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = self.fixture(root)
            manifest = json.loads((root / "manifest.json").read_text())
            old_fields = ("kind", "config_path", "config_sha256", "team_id", "channel_id",
                          "environment", "channel_name", "feed_channel_id", "database_path",
                          "dispatcher_state_directory")
            legacy_target = {field: data["target"][field] for field in old_fields}
            manifest["target"] = legacy_target
            data["target"] = legacy_target
            data["results"][0]["status"] = "PENDING"
            (root / "manifest.json").write_text(json.dumps(manifest))
            self.save(root, data)
            self.assertEqual(summarize(root)["PENDING"], 1)

    def test_isolated_init_needs_no_live_config_and_report_rejects_live_scenario(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(ROOT / "acceptance", root / "acceptance")
            with patch("scripts.acceptance.subprocess.check_output", side_effect=["a" * 40, ""]):
                directory = initialize(root, "isolated", "fixtures")
            data = json.loads((directory / "results.json").read_text())
            self.assertEqual(data["target"], {"kind": "isolated", "fixtures_only": True})
            data.update(runtime_sha="a" * 40, runner="unit test", preflight="temporary fixtures")
            self.save(directory, data)
            self.assertEqual(summarize(directory)["PENDING"], 7)
            manifest = json.loads((directory / "manifest.json").read_text())
            manifest["scenarios"][0]["mode"] = "computer"
            (directory / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(AssertionError, "live scenarios"):
                summarize(directory)

    def save(self, root, data):
        (root / "results.json").write_text(json.dumps(data))

    def test_ui_pass_requires_screenshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root); self.save(root, data)
            with self.assertRaisesRegex(AssertionError, "screenshot"):
                summarize(root)

    def test_pending_never_counts_as_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root)
            data["results"][0]["status"] = "PENDING"; self.save(root, data)
            self.assertEqual(summarize(root)["PASS"], 0)
            self.assertEqual(summarize(root)["PENDING"], 1)

    def test_owned_pending_timer_fixture_is_incomplete_not_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root)
            data["results"][0]["status"] = "PENDING"
            data["fixtures"] = [{"id": "timer", "scenario_id": "A01", "cleanup": "PENDING",
                                 "cleanup_owner": "runner", "resume_at": "2026-09-11T10:00:00+00:00"}]
            self.save(root, data)
            self.assertEqual(summarize(root)["PENDING"], 1)
            self.assertEqual(summarize(root)["PASS"], 0)
            del data["fixtures"][0]["cleanup_owner"]
            self.save(root, data)
            with self.assertRaisesRegex(AssertionError, "fixtures"):
                summarize(root)
            data["fixtures"][0]["cleanup_owner"] = "runner"
            data["results"][0]["status"] = "BLOCKED"
            self.save(root, data)
            with self.assertRaisesRegex(AssertionError, "fixtures"):
                summarize(root)

    def test_snapshot_drift_and_unresolved_cleanup_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root)
            data["results"][0]["status"] = "BLOCKED"; self.save(root, data)
            (root / "spec.md").write_text("changed expectation")
            with self.assertRaisesRegex(AssertionError, "snapshot"):
                summarize(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root)
            data["results"][0]["status"] = "BLOCKED"
            data["fixtures"] = [{"id": "test-task", "cleanup": "NEEDS_ATTENTION"}]
            self.save(root, data)
            with self.assertRaisesRegex(AssertionError, "fixtures"):
                summarize(root)

    def test_evidence_cannot_escape_run_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); data = self.fixture(root)
            data["results"][0]["status"] = "FAIL"
            data["results"][0]["evidence"][0]["path"] = "../not-in-run.log"
            self.save(root, data)
            with self.assertRaisesRegex(AssertionError, "evidence"):
                summarize(root)
