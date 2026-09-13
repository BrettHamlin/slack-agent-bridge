"""Safe, section-scoped synchronization for a Slack channel canvas.

Slack's canvas API can replace a named section without replacing the complete
document.  This module uses that operation exclusively for existing canvases,
so the Director block can change without overwriting the configured notes or
any other user-authored canvas content.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Iterator, Mapping


MANAGED_LABEL = "Director updates"
LEGACY_MANAGED_MARKER = "DIRECTOR-MANAGED-SHA256:"
USER_NOTES_HEADER = "Owner notes"
CANVAS_READ_SCOPE = "canvases:read"
CANVAS_WRITE_SCOPE = "canvases:write"
CHANNEL_READ_SCOPE = "channels:read"


@dataclass(frozen=True)
class CanvasSyncResult:
    status: str
    canvas_id: str | None
    managed_hash: str
    reason: str | None = None

    @property
    def changed(self) -> bool:
        return self.status in {"created", "inserted", "updated"}


class CanvasSyncError(RuntimeError):
    pass


class SlackCanvasSync:
    """Synchronize only one Director-managed canvas content block.

    ``client`` is deliberately injected.  It may be a Slack SDK WebClient or a
    test double exposing the corresponding snake-case Web API methods.
    """

    def __init__(
        self,
        client: Any,
        database_path: str | Path,
        channel_id: str,
        *,
        canvas_id: str | None = None,
        title: str = "Director",
        notes_header: str = USER_NOTES_HEADER,
    ) -> None:
        if not isinstance(channel_id, str) or not channel_id:
            raise ValueError("channel_id must be a non-empty string")
        if canvas_id is not None and (not isinstance(canvas_id, str) or not canvas_id):
            raise ValueError("canvas_id must be a non-empty string when provided")
        if not isinstance(notes_header, str) or not notes_header.strip():
            raise ValueError("notes_header must be a non-empty string")
        self._client = client
        self._channel_id = channel_id
        self._configured_canvas_id = canvas_id
        self._title = title
        self._notes_header = notes_header.strip()
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(database_path), isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout = 5000")
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SlackCanvasSync":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def scope_report() -> dict[str, tuple[str, ...]]:
        """Return the Slack scopes required by each API route used here."""
        return {
            CANVAS_READ_SCOPE: ("canvases.sections.lookup",),
            CANVAS_WRITE_SCOPE: ("conversations.canvases.create", "canvases.edit"),
            CHANNEL_READ_SCOPE: ("conversations.info when discovering an existing channel canvas",),
        }

    def sync(self, managed_markdown: str, *, now: float | None = None) -> CanvasSyncResult:
        """Create or safely replace the Director-managed section.

        A missing managed section is inserted at the beginning of an existing
        canvas.  Multiple candidate headers are unsafe to disambiguate, so no
        canvas write is made and a blocked result is returned.
        """
        normalized_markdown = self._normalize_managed_markdown(managed_markdown)
        managed_hash = sha256(normalized_markdown.encode("utf-8")).hexdigest()
        section_markdown = self._render_managed_section(normalized_markdown)
        updated_at = time.time() if now is None else now

        canvas_id, was_created, blocked_reason = self._resolve_canvas(section_markdown)
        if blocked_reason is not None:
            return CanvasSyncResult("blocked", None, managed_hash, blocked_reason)
        assert canvas_id is not None
        if was_created:
            self._set_checkpoint(canvas_id, managed_hash, updated_at)
            return CanvasSyncResult("created", canvas_id, managed_hash)

        sections = self._lookup_managed_sections(canvas_id, MANAGED_LABEL)
        legacy_section = False
        if not sections:
            sections = self._lookup_managed_sections(canvas_id, LEGACY_MANAGED_MARKER)
            legacy_section = bool(sections)
        if len(sections) > 1:
            return CanvasSyncResult(
                "blocked",
                canvas_id,
                managed_hash,
                "multiple Director-managed blocks found; cannot protect user notes",
            )
        if (
            len(sections) == 1
            and not legacy_section
            and self._get_checkpoint() == (canvas_id, managed_hash)
            and self._desired_content_is_present(canvas_id, sections[0], normalized_markdown)
        ):
            # The remote line lookups are intentional. A local checkpoint alone
            # cannot notice deleted or changed managed content.
            return CanvasSyncResult("unchanged", canvas_id, managed_hash)

        if not sections:
            changes = [
                {
                    "operation": "insert_at_start",
                    "document_content": {"type": "markdown", "markdown": f"{section_markdown}\n\n"},
                }
            ]
            status = "inserted"
        else:
            section_id = sections[0]
            changes = [
                {
                    "operation": "replace",
                    "section_id": section_id,
                    "document_content": {"type": "markdown", "markdown": section_markdown},
                }
            ]
            status = "updated"
        self._require_ok(self._call("canvases_edit", canvas_id=canvas_id, changes=changes), "canvases.edit")
        self._set_checkpoint(canvas_id, managed_hash, updated_at)
        return CanvasSyncResult(status, canvas_id, managed_hash)

    def _resolve_canvas(self, section_markdown: str) -> tuple[str | None, bool, str | None]:
        if self._configured_canvas_id is not None:
            return self._configured_canvas_id, False, None
        checkpoint = self._get_checkpoint()
        if checkpoint is not None:
            return checkpoint[0], False, None

        try:
            response = self._response(self._call(
                "conversations_canvases_create",
                channel_id=self._channel_id,
                title=self._title,
                document_content={
                    "type": "markdown",
                    "markdown": f"{section_markdown}\n\n## {self._notes_header}\n\n",
                },
            ))
        except Exception as error:
            response = self._error_response(error)
            if response is None or response.get("error") != "channel_canvas_already_exists":
                raise CanvasSyncError("conversations.canvases.create request failed") from error
        if response.get("ok") is True:
            canvas_id = response.get("canvas_id")
            if isinstance(canvas_id, str) and canvas_id:
                return canvas_id, True, None
            raise CanvasSyncError("conversations.canvases.create succeeded without canvas_id")
        if response.get("error") != "channel_canvas_already_exists":
            raise CanvasSyncError(self._error_message("conversations.canvases.create", response))

        # Slack documents conversations.info as the way to discover an existing
        # channel canvas after channel_canvas_already_exists.
        info = self._response(self._call("conversations_info", channel=self._channel_id))
        self._require_ok(info, "conversations.info")
        canvas_id = self._canvas_id_from_channel_info(info)
        if canvas_id is None:
            return None, False, "existing channel canvas id is unavailable; cannot protect user notes"
        return canvas_id, False, None

    def _lookup_managed_sections(self, canvas_id: str, contains_text: str) -> list[str]:
        response = self._response(self._call(
            "canvases_sections_lookup",
            canvas_id=canvas_id,
            criteria={"section_types": ["blockquote"], "contains_text": contains_text},
        ))
        self._require_ok(response, "canvases.sections.lookup")
        raw_sections = response.get("sections")
        if not isinstance(raw_sections, list):
            raise CanvasSyncError("canvases.sections.lookup succeeded without sections")
        section_ids: list[str] = []
        for section in raw_sections:
            if not isinstance(section, Mapping) or not isinstance(section.get("id"), str) or not section["id"]:
                raise CanvasSyncError("canvases.sections.lookup returned an invalid section id")
            section_ids.append(section["id"])
        return section_ids

    def _desired_content_is_present(
        self, canvas_id: str, section_id: str, normalized_markdown: str
    ) -> bool:
        """Verify the visible label and every desired line before skipping a write.

        Slack's lookup endpoint exposes section ids rather than source markdown.
        This detects removed or changed managed lines, but cannot detect a manual
        addition that leaves every expected line intact.
        """
        for line in (MANAGED_LABEL, *self._substantive_lines(normalized_markdown)):
            if self._lookup_managed_sections(canvas_id, line) != [section_id]:
                return False
        return True

    def _get_checkpoint(self) -> tuple[str, str] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT canvas_id, managed_hash FROM canvas_sync_checkpoints WHERE channel_id = ?",
                (self._channel_id,),
            ).fetchone()
            return None if row is None else (str(row["canvas_id"]), str(row["managed_hash"]))

    def _set_checkpoint(self, canvas_id: str, managed_hash: str, updated_at: float) -> None:
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO canvas_sync_checkpoints (channel_id, canvas_id, managed_hash, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    canvas_id = excluded.canvas_id,
                    managed_hash = excluded.managed_hash,
                    updated_at = excluded.updated_at
                """,
                (self._channel_id, canvas_id, managed_hash, updated_at),
            )

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS canvas_sync_checkpoints (
                    channel_id TEXT PRIMARY KEY,
                    canvas_id TEXT NOT NULL,
                    managed_hash TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _call(self, method: str, **kwargs: Any) -> Any:
        client_method = getattr(self._client, method, None)
        if not callable(client_method):
            raise CanvasSyncError(f"injected client does not implement {method}")
        return client_method(**kwargs)

    @staticmethod
    def _response(value: Any) -> Mapping[str, Any]:
        if isinstance(value, Mapping):
            return value
        data = getattr(value, "data", None)
        if isinstance(data, Mapping):
            return data
        raise CanvasSyncError("Slack client returned a non-mapping response")

    @staticmethod
    def _error_response(error: Exception) -> Mapping[str, Any] | None:
        """Extract SlackApiError.response without importing an optional SDK."""
        response = getattr(error, "response", None)
        try:
            return SlackCanvasSync._response(response)
        except CanvasSyncError:
            return None

    @staticmethod
    def _require_ok(response: Mapping[str, Any], method: str) -> None:
        if response.get("ok") is not True:
            raise CanvasSyncError(SlackCanvasSync._error_message(method, response))

    @staticmethod
    def _error_message(method: str, response: Mapping[str, Any]) -> str:
        error = response.get("error")
        return f"{method} failed" if not isinstance(error, str) or not error else f"{method} failed: {error}"

    @staticmethod
    def _canvas_id_from_channel_info(response: Mapping[str, Any]) -> str | None:
        channel = response.get("channel")
        if not isinstance(channel, Mapping):
            return None
        properties = channel.get("properties")
        if not isinstance(properties, Mapping):
            return None
        canvas = properties.get("canvas")
        if isinstance(canvas, str) and canvas:
            return canvas
        if isinstance(canvas, Mapping):
            canvas_id = canvas.get("id")
            return canvas_id if isinstance(canvas_id, str) and canvas_id else None
        return None

    @staticmethod
    def _normalize_managed_markdown(managed_markdown: str) -> str:
        """Accept only paragraphs and whole-line bold headings for reliable lookup."""
        if not isinstance(managed_markdown, str) or not managed_markdown.strip():
            raise ValueError("managed_markdown must be non-empty")
        normalized_lines: list[str] = []
        for line in managed_markdown.strip().splitlines():
            stripped = line.strip()
            if not stripped:
                normalized_lines.append("")
                continue
            if stripped.startswith("**") and stripped.endswith("**") and len(stripped) > 4:
                heading = stripped[2:-2].strip()
                if not heading or "**" in heading:
                    raise ValueError("bold headings must use exactly one pair of ** markers")
                normalized_lines.append(f"**{heading}**")
                continue
            looks_like_ordered_list = stripped[0].isdigit() and ". " in stripped
            if (
                stripped.startswith(("- ", "+ "))
                or looks_like_ordered_list
                or any(marker in stripped for marker in ("#", "*", "_", "`", "[", "]", ">", "|"))
            ):
                raise ValueError("canvas input supports plain paragraphs and whole-line bold headings only")
            normalized_lines.append(stripped)
        return "\n".join(normalized_lines).strip()

    @staticmethod
    def _render_managed_section(content: str) -> str:
        """Render exactly one blockquote section, the unit Slack can replace."""
        quoted_lines = [f"> {MANAGED_LABEL}", ">"]
        quoted_lines.extend(">" if not line else f"> {line}" for line in content.splitlines())
        return "\n".join(quoted_lines)

    @staticmethod
    def _substantive_lines(normalized_markdown: str) -> tuple[str, ...]:
        lines: list[str] = []
        for line in normalized_markdown.splitlines():
            if not line:
                continue
            if line.startswith("**") and line.endswith("**"):
                lines.append(line[2:-2])
            else:
                lines.append(line)
        return tuple(lines)
