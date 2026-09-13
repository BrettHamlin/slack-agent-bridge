#!/usr/bin/env python3
"""Prepare and validate agent-run Markdown acceptance; never execute the UI."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
if not __debug__:
    raise RuntimeError('Acceptance validation requires Python without -O')
SECTIONS = ("Setup", "Actions", "Expected", "Evidence", "Cleanup", "Failure handling")
MODES = {"computer", "controlled", "isolated"}
TEST_CONFIG = "config/director-tests.json"
TEST_TEAM = "T0000000009"
TEST_CHANNEL = "C0000000004"
TEST_FEED_CHANNEL = "C0000000007"

# Live acceptance never selects a destination from the host name, an ambient
# service, or Slack state.  A runner must select one of these fixed profiles
# and present the profile's exact local config file.
LIVE_PROFILES = {
    "odyssey": {
        "config": TEST_CONFIG,
        "team_id": TEST_TEAM,
        "channel_id": TEST_CHANNEL,
        "channel_name": "director-tests",
        "environment": "test",
        "owner_user_id": "U0000000010",
        "slack_app_id": "A0000000001",
        "bot_user_id": "U0000000011",
        "workspace_domain": "example.slack.com",
        "feed_channel_id": TEST_FEED_CHANNEL,
        "database_path": "state/testing/inbox.sqlite3",
        "receiver_service": "com.example.director.receiver",
    },
    "atlas": {
        "config": "config/director-atlas.local.json",
        "team_id": "T0000000009",
        "channel_id": "C0000000008",
        "channel_name": "director-dev",
        "environment": "development",
        "owner_user_id": "U0000000010",
        "slack_app_id": "A0000000002",
        "bot_user_id": "U0000000012",
        "workspace_domain": "example.slack.com",
        "feed_channel_id": "C0000000006",
        "database_path": "state/development/inbox.sqlite3",
        "receiver_service": "com.example.director.atlas-dev",
    },
}

LOCAL_PROFILE = "local"
LOCAL_CONFIG_PATTERN = re.compile(r"config/director-[a-z0-9][a-z0-9-]*\.local\.json")
LOCAL_DATABASE_PATH = "state/local-acceptance/inbox.sqlite3"
KNOWN_PRIVATE_VALUES = frozenset(
    value
    for profile in LIVE_PROFILES.values()
    for value in (profile["channel_id"], profile["feed_channel_id"], profile["slack_app_id"], profile["bot_user_id"])
)


def _profile_name(profile):
    assert isinstance(profile, str), "Explicit known acceptance profile required"
    name = profile.casefold()
    assert name in {*LIVE_PROFILES, LOCAL_PROFILE}, "Explicit known acceptance profile required"
    return name


def _profile(profile):
    return LIVE_PROFILES[_profile_name(profile)]


def _local_target(root, config):
    """Validate an explicitly selected, standalone local acceptance receiver.

    This profile is intentionally opt-in and cannot be selected from a host name
    or ambient configuration.  It is for a newly provisioned private test app,
    never an existing Odyssey or Atlas receiver.
    """
    assert config, "Local profile requires an explicit --config path"
    root = Path(root).resolve()
    supplied = Path(config)
    relative = supplied if not supplied.is_absolute() else supplied.relative_to(root)
    assert LOCAL_CONFIG_PATTERN.fullmatch(str(relative)), "Local profile requires config/director-<host>.local.json"
    path = _exact_local_path(root, relative, str(relative), "Local config must not be a symlink")
    settings = json.loads(path.read_text())
    for field in ("team_id", "channel_id", "channel_name", "owner_user_id", "slack_app_id", "bot_user_id", "workspace_domain", "receiver_service"):
        value = settings.get(field)
        assert isinstance(value, str) and value.strip(), f"local {field} is required"
    assert settings.get("environment") == "development", "Local acceptance requires a development environment"
    assert settings.get("channel_name", "").endswith("-tests"), "Local acceptance source channel must end in -tests"
    assert settings.get("enabled") is True, "Selected acceptance profile must be enabled"
    assert not settings.get("additional_configs"), "Local acceptance requires one standalone receiver"
    assert not ({settings["channel_id"], settings["slack_app_id"], settings["bot_user_id"]} & KNOWN_PRIVATE_VALUES), "Local profile cannot use an existing private receiver identity"
    feed = settings.get("conversation_feed") or {}
    assert isinstance(feed, dict) and feed.get("enabled") is True, "Dedicated local profile feed required"
    feed_id = feed.get("channel_id")
    assert isinstance(feed_id, str) and feed_id and feed_id != settings["channel_id"], "Local source and feed must differ"
    assert feed_id not in KNOWN_PRIVATE_VALUES, "Local profile cannot use an existing private feed"
    database = settings.get("database_path")
    assert database == LOCAL_DATABASE_PATH, "Local acceptance requires isolated state/local-acceptance state"
    service = settings["receiver_service"]
    assert re.fullmatch(r"com\.[A-Za-z0-9.-]+\.director\.[A-Za-z0-9.-]+", service), "Local receiver service label is invalid"
    assert service not in {item["receiver_service"] for item in LIVE_PROFILES.values()}, "Local profile cannot control an existing receiver"
    database_path = _exact_local_path(root, database, LOCAL_DATABASE_PATH, "Local acceptance state must not be an alias")
    dispatch = str(Path(database).parent / "dispatch")
    dispatcher_path = _exact_local_path(root, dispatch, "state/local-acceptance/dispatch", "Local dispatcher state must not be an alias")
    return {"kind": "live", "profile": LOCAL_PROFILE, "config_path": str(path), "config_sha256": sha(path),
            "team_id": settings["team_id"], "channel_id": settings["channel_id"], "environment": settings["environment"],
            "channel_name": settings["channel_name"], "slack_app_id": settings["slack_app_id"], "bot_user_id": settings["bot_user_id"],
            "owner_user_id": settings["owner_user_id"], "feed_channel_id": feed_id, "receiver_service": service,
            "database_path": str(database_path), "dispatcher_state_directory": str(dispatcher_path)}


def _exact_local_path(root, value, expected, error):
    """Reject aliases before a live run can touch state through one."""
    root = Path(root).resolve()
    supplied = Path(value)
    if supplied.is_absolute():
        path = supplied
    else:
        assert supplied == Path(expected), error
        path = root / supplied
    expected_path = root / expected
    # The manifest stores canonical absolute paths.  A caller can use the
    # documented relative path or that exact canonical path, but not a symlink
    # (including an absolute one) that happens to resolve to it.
    assert path == expected_path, error
    # ``resolve`` alone would accept a symlink pointed at the expected target.
    # Check every existing component, while still allowing a not-yet-created DB.
    current = root
    for part in Path(expected).parts:
        current /= part
        assert not current.is_symlink(), error
    return expected_path


def _legacy_odyssey_target(root, suite, config=None):
    """Read historical Odyssey manifests without relaxing new-run validation."""
    root = Path(root).resolve()
    supplied = Path(config or os.environ.get("DIRECTOR_CONFIG") or TEST_CONFIG)
    path = supplied if supplied.is_absolute() else root / supplied
    expected_config = root / TEST_CONFIG
    assert path == expected_config and not path.is_symlink(), "Legacy live run requires the dedicated test config"
    settings = json.loads(path.read_text())
    assert settings.get("team_id") == TEST_TEAM, "Test team required"
    assert settings.get("channel_id") == TEST_CHANNEL, "Test channel required; production destination rejected"
    assert settings.get("environment") == "test", "Test environment required"
    assert settings.get("channel_name") == "director-tests", "Test channel name required"
    feed = settings.get("conversation_feed") or {}
    assert isinstance(feed, dict), "Invalid test feed config"
    if feed.get("enabled"):
        assert feed.get("channel_id") == TEST_FEED_CHANNEL, "Dedicated test feed required; production feed rejected"
    database = settings.get("database_path")
    assert isinstance(database, str) and database, "Explicit test state path required"
    dispatch = str(Path(database).parent / "dispatch")
    for value, expected in ((database, "state/testing/inbox.sqlite3"),
                            (dispatch, "state/testing/dispatch")):
        assert value == expected, "Test state required; production/aliased state rejected"
        _exact_local_path(root, value, expected, "Test state required; production/aliased state rejected")
    return {"kind": "live", "config_path": str(path), "config_sha256": sha(path),
            "team_id": TEST_TEAM, "channel_id": TEST_CHANNEL,
            "environment": "test", "channel_name": "director-tests",
            "feed_channel_id": feed.get("channel_id") if feed.get("enabled") else None,
            "database_path": str(root / database),
            "dispatcher_state_directory": str(root / dispatch)}


def validated_target(root, suite, config=None, profile="odyssey"):
    """Validate metadata only; never open credentials or create runtime state."""
    assert suite in {"smoke", "full", "isolated"}, "Invalid suite"
    if suite == "isolated":
        return {"kind": "isolated", "fixtures_only": True}
    root = Path(root).resolve()
    profile = _profile_name(profile)
    if profile == LOCAL_PROFILE:
        return _local_target(root, config)
    selected = _profile(profile)
    supplied = config or os.environ.get("DIRECTOR_CONFIG") or selected["config"]
    path = _exact_local_path(root, supplied, selected["config"], "Live suites require the selected profile config (no symlinks)")
    settings = json.loads(path.read_text())
    for field in ("team_id", "channel_id", "channel_name", "environment", "owner_user_id",
                  "slack_app_id", "bot_user_id", "workspace_domain"):
        assert settings.get(field) == selected[field], f"{profile} {field} required; wrong host/profile rejected"
    assert settings.get("enabled") is True, "Selected acceptance profile must be enabled"
    assert not settings.get("additional_configs"), "Live acceptance profiles must use their registered receiver only"
    feed = settings.get("conversation_feed") or {}
    assert isinstance(feed, dict) and feed.get("enabled") is True, "Dedicated profile feed required"
    assert feed.get("channel_id") == selected["feed_channel_id"], "Dedicated profile feed required; wrong destination rejected"
    assert feed.get("channel_id") != selected["channel_id"], "Source and feed channels must differ"
    database = settings.get("database_path")
    assert database == selected["database_path"], "Selected profile state required; production/aliased state rejected"
    # Dispatcher uses the database parent, matching director.dispatcher.Dispatcher.
    dispatch = str(Path(database).parent / "dispatch")
    database_path = _exact_local_path(root, database, selected["database_path"], "Selected profile state required; production/aliased state rejected")
    dispatcher_path = _exact_local_path(
        root, dispatch, str(Path(selected["database_path"]).parent / "dispatch"),
        "Selected profile dispatcher state required; production/aliased state rejected",
    )
    return {"kind": "live", "profile": profile, "config_path": str(path), "config_sha256": sha(path),
            "team_id": selected["team_id"], "channel_id": selected["channel_id"],
            "environment": selected["environment"], "channel_name": selected["channel_name"],
            "slack_app_id": selected["slack_app_id"], "bot_user_id": selected["bot_user_id"],
            "owner_user_id": selected["owner_user_id"], "feed_channel_id": selected["feed_channel_id"],
            "receiver_service": selected["receiver_service"], "database_path": str(database_path),
            "dispatcher_state_directory": str(dispatcher_path)}


def read_catalog(root=ROOT):
    data = json.loads((root / "acceptance/catalog.json").read_text())
    assert data["version"] == 1, "Unsupported catalog version"
    rows = data["scenarios"]
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)) and ids, "Missing/duplicate scenario IDs"
    seen = set()
    for row in rows:
        assert re.fullmatch(r"A\d{2}", row["id"]), "Invalid ID"
        assert row["mode"] in MODES and row["tier"] in {"smoke", "full"}, "Invalid mode/tier"
        path = (root / "acceptance" / row["file"]).resolve()
        assert path.is_relative_to((root / "acceptance/scenarios").resolve()), "Scenario path escapes suite"
        text = path.read_text()
        assert text.startswith("# " + row["id"] + ":"), "Heading/ID mismatch"
        for section in SECTIONS:
            match = re.search(r"^## " + re.escape(section) + r"\n(.*?)(?=^## |\Z)", text, re.M | re.S)
            assert match and match[1].strip(), f"{row['id']} missing {section}"
        assert set(row["dependencies"]) <= seen, "Dependencies must precede scenario; no cycles/unknown IDs"
        if row["tier"] == "smoke":
            assert all(r["tier"] == "smoke" for r in rows if r["id"] in row["dependencies"]), "Smoke dependency outside smoke"
        seen.add(row["id"])
    return rows


def selected(rows, suite):
    return [r for r in rows if suite == "full" or (suite == "smoke" and r["tier"] == "smoke")
            or (suite == "isolated" and r["mode"] == "isolated")]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def private_write(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(content)


def initialize(root, suite, run_id, config=None, profile="odyssey"):
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", run_id), "Unsafe run ID"
    rows = selected(read_catalog(root), suite)
    run_target = validated_target(root, suite, config, profile)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root, text=True).strip())
    assert not dirty, "Commit/publish the suite before creating a pinned run"
    directory = root / "state/acceptance" / run_id
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    (directory / "spec").mkdir(mode=0o700)
    snapshots = []
    for row in rows:
        source = root / "acceptance" / row["file"]
        target = directory / "spec" / source.name
        private_write(target, source.read_text())
        snapshots.append({**row, "snapshot": str(target.relative_to(directory)), "sha256": sha(target)})
    for name in ("RUNNER.md", "MAINTENANCE.md"):
        private_write(directory / "spec" / name, (root / "acceptance" / name).read_text())
    manifest = {"version": 1, "run_id": run_id, "suite": suite, "suite_sha": revision,
                "created_at": datetime.now(timezone.utc).isoformat(), "scenarios": snapshots,
                "target": run_target}
    results = {"run_id": run_id, "runtime_sha": "", "runner": "", "preflight": "",
               "target": run_target, "fixtures": [], "results": [{"id": r["id"], "status": "PENDING", "started_at": "",
               "finished_at": "", "observed": "", "cleanup": "PENDING", "evidence": []} for r in rows]}
    private_write(directory / "manifest.json", json.dumps(manifest, indent=2) + "\n")
    private_write(directory / "results.json", json.dumps(results, indent=2) + "\n")
    return directory


def summarize(directory, config=None, profile=None):
    directory = Path(directory).resolve()
    manifest = json.loads((directory / "manifest.json").read_text())
    data = json.loads((directory / "results.json").read_text())
    assert data["run_id"] == manifest["run_id"], "Run identity mismatch"
    suite = manifest["suite"]
    target = manifest["target"]
    if suite == "isolated":
        assert all(r["mode"] == "isolated" for r in manifest["scenarios"]), "Isolated suite contains live scenarios"
        expected_target = validated_target(directory, suite)
    else:
        config_path = Path(target["config_path"])
        assert config_path.is_absolute(), "Absolute test config path required"
        target_profile = target.get("profile")
        if target_profile is None:
            if profile is not None:
                assert _profile_name(profile) == "odyssey", "Report profile differs from pinned run target"
            expected_target = _legacy_odyssey_target(config_path.parent.parent, suite, config or str(config_path))
        else:
            assert isinstance(target_profile, str), "Live run profile required"
            if profile is not None:
                assert _profile_name(profile) == target_profile, "Report profile differs from pinned run target"
            expected_target = validated_target(
                config_path.parent.parent, suite, config or str(config_path), target_profile,
            )
    assert target == expected_target, "Validated test target changed; start a new run"
    assert data["target"] == target, "Result target differs from validated test target"
    rows = {r["id"]: r for r in manifest["scenarios"]}
    outcomes = data["results"]
    assert len(outcomes) == len(rows) and {r["id"] for r in outcomes} == set(rows), "Missing/duplicate results"
    assert re.fullmatch(r"[0-9a-f]{40}", data["runtime_sha"]), "Verified runtime SHA required"
    assert data["runner"].strip() and data["preflight"].strip(), "Runner and preflight evidence required"
    by_id = {r["id"]: r for r in outcomes}
    counts = {s: 0 for s in ("PASS", "FAIL", "BLOCKED", "PENDING")}
    for row in outcomes:
        spec = rows[row["id"]]
        snapshot = (directory / spec["snapshot"]).resolve()
        assert snapshot.is_relative_to(directory) and sha(snapshot) == spec["sha256"], "Scenario snapshot changed"
        assert row["status"] in counts, "Invalid status"
        counts[row["status"]] += 1
        if row["status"] == "PENDING":
            continue
        assert row["observed"].strip(), "Observed behavior/reason required"
        assert row["cleanup"] in ("CLEAN", "NEEDS_ATTENTION"), "Cleanup assessment required"
        assert row["started_at"] and row["finished_at"], "Execution timestamps required"
        start, end = (datetime.fromisoformat(row[k]) for k in ("started_at", "finished_at"))
        assert start.tzinfo and end.tzinfo and end >= start, "Invalid timestamp order/timezone"
        kinds = set()
        for evidence in row["evidence"]:
            assert evidence["kind"] in ("screenshot", "log"), "Invalid evidence kind"
            path = (directory / evidence["path"]).resolve()
            assert path.is_relative_to(directory) and path.is_file() and path.stat().st_size, "Missing/unsafe evidence"
            if evidence["kind"] == "screenshot":
                assert path.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp"), "Screenshot image required"
            kinds.add(evidence["kind"])
        if row["status"] == "PASS":
            assert row["cleanup"] == "CLEAN" and kinds, "PASS requires cleanup and evidence"
            assert all(by_id[d]["status"] == "PASS" for d in spec["dependencies"]), "Dependency not passed"
            if spec["mode"] in ("computer", "controlled"):
                assert "screenshot" in kinds, "UI PASS requires screenshot evidence"
    for fixture in data["fixtures"]:
        if fixture.get("cleanup") == "CLEAN":
            continue
        # An active timer wait retains ownership; it cannot make a run green.
        assert (fixture.get("cleanup") == "PENDING"
                and by_id.get(fixture.get("scenario_id"), {}).get("status") == "PENDING"
                and str(fixture.get("cleanup_owner") or "").strip()
                and fixture.get("resume_at")), "Unresolved test fixtures"
        assert datetime.fromisoformat(fixture["resume_at"]).tzinfo, "Fixture resume time requires timezone"
    return counts


def check_impact(body, changed, rows):
    relevant = any(p == "AGENTS.md" or p.startswith(("director/", "tests/", "scripts/", "docs/", "acceptance/")) for p in changed)
    if not relevant:
        return
    values = {}
    for field in ("scenarios", "changes", "rationale", "evidence"):
        matches = re.findall(r"^Acceptance " + field + r":[ \t]*([^\n]+)", body, re.M | re.I)
        assert len(matches) == 1 and matches[0].strip(), f"Acceptance {field} field required exactly once"
        values[field] = matches[0].strip()
    ids = re.findall(r"\bA\d{2}\b", values["scenarios"])
    assert ids and set(ids) <= {r["id"] for r in rows}, "List existing scenario IDs"
    assert values["changes"].lower() in ("updated", "unchanged"), "Changes must be updated or unchanged"
    if values["changes"].lower() == "updated":
        assert any(p.startswith("acceptance/scenarios/") or p == "acceptance/catalog.json" for p in changed), "Updated requires scenario/catalog changes"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate")
    for command in ("list", "init"):
        item = sub.add_parser(command)
        item.add_argument("--suite", choices=("smoke", "full", "isolated"), default="smoke")
        if command == "init":
            item.add_argument("--run-id", required=True)
            item.add_argument("--config")
            item.add_argument("--profile", type=str.casefold, choices=tuple((*LIVE_PROFILES, LOCAL_PROFILE)), default="odyssey")
    item = sub.add_parser("report")
    item.add_argument("directory")
    item.add_argument("--config")
    item.add_argument("--profile", type=str.casefold, choices=tuple((*LIVE_PROFILES, LOCAL_PROFILE)))
    item = sub.add_parser("impact")
    item.add_argument("--event", required=True)
    item.add_argument("--base", required=True)
    args = parser.parse_args()
    try:
        if args.command == "report":
            counts = summarize(args.directory, args.config, args.profile)
            print(json.dumps(counts))
            return 0 if counts["PASS"] and not any(counts[s] for s in ("FAIL", "BLOCKED", "PENDING")) else 1
        rows = read_catalog()
        if args.command == "validate":
            print(f"{len(rows)} scenarios valid")
        elif args.command == "list":
            for r in selected(rows, args.suite):
                print(f"{r['id']} [{r['mode']}] {r['file']}: {r['title']}")
        elif args.command == "init":
            print(initialize(ROOT, args.suite, args.run_id, args.config, args.profile))
        elif args.command == "impact":
            event = json.loads(Path(args.event).read_text())
            changed = subprocess.check_output(["git", "diff", "--name-only", args.base, "HEAD"], cwd=ROOT, text=True).splitlines()
            check_impact(event["pull_request"].get("body") or "", changed, rows)
            print("Acceptance impact valid")
        return 0
    except (AssertionError, ValueError, KeyError, OSError) as error:
        print(f"Acceptance validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
