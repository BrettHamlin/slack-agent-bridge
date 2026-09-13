#!/usr/bin/env python3
"""Render one host-local Director LaunchAgent without reading credentials.

The renderer is intentionally separate from ``launchctl``.  It creates a
reviewable plist from the selected host config, refuses a conflicting existing
file, and can safely be re-run after an interrupted setup.
"""
from __future__ import annotations

import argparse
import os
import plistlib
import re
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from director.__main__ import load_config


LABEL_PATTERN = re.compile(r"com\.[A-Za-z0-9.-]+\.director\.[A-Za-z0-9.-]+")


def service_definition(project: Path, config_path: Path) -> tuple[str, dict]:
    """Build the credential-free LaunchAgent dictionary for this config."""
    project = project.resolve()
    config_path = config_path.resolve()
    if config_path.parent != project / "config":
        raise ValueError("config_must_be_in_project_config")
    config = load_config(config_path)
    label = config.get("receiver_service")
    if not isinstance(label, str) or not LABEL_PATTERN.fullmatch(label):
        raise ValueError("receiver_service_must_be_a_unique_launchd_label")
    database = config.get("database_path")
    if not isinstance(database, str) or not database:
        raise ValueError("database_path_required")
    state_directory = (project / database).resolve().parent
    if not state_directory.is_relative_to(project):
        raise ValueError("database_path_must_stay_in_project")
    return label, {
        "Label": label,
        "ProgramArguments": [
            str(project / ".venv/bin/python"), "-m", "director", "--config", str(config_path), "listen",
        ],
        "WorkingDirectory": str(project),
        "StandardErrorPath": str(state_directory / "receiver.stderr.log"),
        "StandardOutPath": str(state_directory / "receiver.stdout.log"),
        "KeepAlive": True,
        "RunAtLoad": True,
        "ThrottleInterval": 30,
        "Umask": 0o77,
    }


def render(project: Path, config_path: Path, output: Path) -> str:
    """Write a plist once; an identical existing plist is an idempotent no-op."""
    label, definition = service_definition(project, config_path)
    output = output.expanduser()
    payload = plistlib.dumps(definition, fmt=plistlib.FMT_XML, sort_keys=False)
    state_directory = Path(definition["StandardErrorPath"]).parent
    state_directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    if output.exists() or output.is_symlink():
        if output.is_symlink() or not output.is_file() or output.read_bytes() != payload:
            raise FileExistsError("launchagent_exists_with_different_contents")
        return "unchanged"
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
    return "created"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = args.config.resolve()
        label, _definition = service_definition(PROJECT_ROOT, config)
        output = args.output or (Path.home() / "Library/LaunchAgents" / f"{label}.plist")
        if args.dry_run:
            print({"label": label, "output": str(output.expanduser()), "credentials_read": False})
        else:
            print({"label": label, "output": str(output.expanduser()), "status": render(PROJECT_ROOT, config, output), "credentials_read": False})
        return 0
    except (OSError, ValueError, KeyError) as error:
        print({"ok": False, "error": str(error)}, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
