from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from director.canvas import (
    USER_NOTES_HEADER,
    CANVAS_READ_SCOPE,
    CANVAS_WRITE_SCOPE,
    CHANNEL_READ_SCOPE,
    LEGACY_MANAGED_MARKER,
    MANAGED_LABEL,
    SlackCanvasSync,
)


class FakeCanvasClient:
    def __init__(self) -> None:
        self.created: list[dict] = []
        self.edits: list[dict] = []
        self.lookup_sections: list[dict] = [{"id": "section-director"}]
        self.create_response: dict = {"ok": True, "canvas_id": "F-created"}
        self.info_response: dict = {"ok": True, "channel": {"properties": {"canvas": "F-existing"}}}
        self.current_marker: str | None = None
        self.create_error: Exception | None = None
        self.legacy_mode = False

    def conversations_canvases_create(self, **kwargs: object) -> dict:
        self.created.append(dict(kwargs))
        if self.create_error is not None:
            raise self.create_error
        return self.create_response

    def conversations_info(self, **kwargs: object) -> dict:
        return self.info_response

    def canvases_sections_lookup(self, **kwargs: object) -> dict:
        criteria = kwargs["criteria"]
        assert isinstance(criteria, dict)
        contains = criteria["contains_text"]
        assert isinstance(contains, str)
        if contains == LEGACY_MANAGED_MARKER:
            return {"ok": True, "sections": self.lookup_sections if self.legacy_mode else []}
        if contains == MANAGED_LABEL:
            return {"ok": True, "sections": self.lookup_sections if not self.legacy_mode else []}
        if self.current_marker is None or contains not in self.current_marker.split("\n"):
            return {"ok": True, "sections": []}
        return {"ok": True, "sections": self.lookup_sections}

    def canvases_edit(self, **kwargs: object) -> dict:
        self.edits.append(dict(kwargs))
        change = kwargs["changes"][0]
        assert isinstance(change, dict)
        document = change["document_content"]
        assert isinstance(document, dict)
        markdown = document["markdown"]
        assert isinstance(markdown, str)
        self.current_marker = "\n".join(
            line[2:] if line.startswith("> ") else "" for line in markdown.splitlines() if line != ">"
        )
        self.legacy_mode = False
        return {"ok": True}


class SlackCanvasSyncTests(unittest.TestCase):
    def test_create_attaches_channel_canvas_with_managed_and_notes_sections(self) -> None:
        client = FakeCanvasClient()
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed") as sync:
                result = sync.sync("Review open work", now=100.0)

        self.assertEqual(result.status, "created")
        self.assertEqual(result.canvas_id, "F-created")
        document = client.created[0]["document_content"]["markdown"]
        self.assertIn(MANAGED_LABEL, document)
        self.assertIn(f"## {USER_NOTES_HEADER}", document)
        self.assertEqual(client.edits, [])

    def test_create_uses_configured_notes_header(self) -> None:
        client = FakeCanvasClient()
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(
                client, Path(directory) / "director.sqlite3", "C-allowed", notes_header="Workspace notes"
            ) as sync:
                sync.sync("Review open work", now=100.0)

        document = client.created[0]["document_content"]["markdown"]
        self.assertIn("## Workspace notes", document)
        self.assertNotIn(f"## {USER_NOTES_HEADER}", document)

    def test_sdk_style_existing_canvas_error_uses_response_for_safe_discovery(self) -> None:
        class FakeSlackApiError(Exception):
            def __init__(self) -> None:
                self.response = {"ok": False, "error": "channel_canvas_already_exists"}

        client = FakeCanvasClient()
        client.create_error = FakeSlackApiError()
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed") as sync:
                result = sync.sync("Current work")

        self.assertEqual(result.status, "updated")
        self.assertEqual(result.canvas_id, "F-existing")

    def test_existing_canvas_replaces_only_exact_managed_section_and_skips_same_hash(self) -> None:
        client = FakeCanvasClient()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with SlackCanvasSync(client, path, "C-allowed", canvas_id="F-existing") as sync:
                first = sync.sync("Current work", now=100.0)
                second = sync.sync("Current work", now=101.0)

        self.assertEqual(first.status, "updated")
        self.assertEqual(second.status, "unchanged")
        self.assertEqual(len(client.edits), 1)
        change = client.edits[0]["changes"][0]
        self.assertEqual(change["operation"], "replace")
        self.assertEqual(change["section_id"], "section-director")
        self.assertTrue(change["document_content"]["markdown"].startswith("> Director updates"))
        self.assertNotIn("Owner", change["document_content"]["markdown"])
        self.assertNotIn("replace_entire", str(client.edits[0]))

    def test_remote_marker_drift_causes_repair_even_when_local_hash_matches(self) -> None:
        client = FakeCanvasClient()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "director.sqlite3"
            with SlackCanvasSync(client, path, "C-allowed", canvas_id="F-existing") as sync:
                sync.sync("Current work", now=100.0)
                client.current_marker = "Director updates\nOld work"
                repaired = sync.sync("Current work", now=101.0)

        self.assertEqual(repaired.status, "updated")
        self.assertEqual(len(client.edits), 2)

    def test_legacy_digest_block_is_migrated_once_without_full_canvas_replace(self) -> None:
        client = FakeCanvasClient()
        client.legacy_mode = True
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed", canvas_id="F-existing") as sync:
                result = sync.sync("Current work")

        self.assertEqual(result.status, "updated")
        change = client.edits[0]["changes"][0]
        self.assertEqual(change["operation"], "replace")
        self.assertEqual(change["section_id"], "section-director")
        self.assertNotIn(LEGACY_MANAGED_MARKER, change["document_content"]["markdown"])

    def test_rejects_markdown_that_cannot_be_reliably_compared_by_section_lookup(self) -> None:
        client = FakeCanvasClient()
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed", canvas_id="F-existing") as sync:
                with self.assertRaisesRegex(ValueError, "plain paragraphs"):
                    sync.sync("- unsupported list")

    def test_ambiguous_managed_section_blocks_without_writing(self) -> None:
        client = FakeCanvasClient()
        client.lookup_sections = [{"id": "one"}, {"id": "two"}]
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed", canvas_id="F-existing") as sync:
                result = sync.sync("Current work")

        self.assertEqual(result.status, "blocked")
        self.assertIn("cannot protect user notes", result.reason or "")
        self.assertEqual(client.edits, [])

    def test_missing_managed_section_is_inserted_without_replacing_canvas(self) -> None:
        client = FakeCanvasClient()
        client.lookup_sections = []
        with tempfile.TemporaryDirectory() as directory:
            with SlackCanvasSync(client, Path(directory) / "director.sqlite3", "C-allowed", canvas_id="F-existing") as sync:
                result = sync.sync("Current work")

        self.assertEqual(result.status, "inserted")
        self.assertEqual(client.edits[0]["changes"][0]["operation"], "insert_at_start")
        self.assertNotIn("section_id", client.edits[0]["changes"][0])

    def test_scope_report_covers_read_write_and_existing_canvas_discovery(self) -> None:
        report = SlackCanvasSync.scope_report()
        self.assertEqual(report[CANVAS_READ_SCOPE], ("canvases.sections.lookup",))
        self.assertIn("canvases.edit", report[CANVAS_WRITE_SCOPE])
        self.assertIn("conversations.info", report[CHANNEL_READ_SCOPE][0])
