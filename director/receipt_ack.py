"""Asynchronous Slack reactions that confirm durable source intake.

This worker is intentionally separate from manager read receipts.  Its single
purpose is to put a checkmark on a trusted source after the receiver durably
saves that source pointer; it neither fetches source text nor advances work.
"""

from __future__ import annotations

import logging
from pathlib import Path
import secrets
import threading
from typing import Mapping

from .inbox import InboxStore, IntakeAckLease
from .slack_transport import SlackAllowlist


class ReceiptAckWorker:
    """Drain one channel's durable intake-reaction queue outside Socket Mode."""

    def __init__(
        self,
        path: str | Path,
        web_client: object,
        allowlist: SlackAllowlist,
        *,
        poll_seconds: float = 0.25,
        lease_seconds: float = 10.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if poll_seconds <= 0 or lease_seconds <= 0:
            raise ValueError("poll_seconds and lease_seconds must be positive")
        self._store = InboxStore(path)
        self._web_client = web_client
        self._allowlist = allowlist
        self._poll_seconds = poll_seconds
        self._lease_seconds = lease_seconds
        self._log = logger or logging.getLogger(__name__)
        self._holder = f"receipt-ack-{secrets.token_urlsafe(12)}"
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.Lock()

    def start(self) -> None:
        """Start recovery and prompt draining without blocking the receiver."""
        with self._lifecycle_lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._run, name="director-receipt-ack", daemon=True)
            self._thread.start()

    def wake(self) -> None:
        """Signal that an intake transaction committed; this never calls Slack."""
        self._wake.set()

    def process_once(self) -> int:
        """Process a bounded batch.  Exposed for deterministic focused tests."""
        processed = 0
        # Claim only one external mutation at a time.  A slow Slack request
        # cannot leave later rows with expired leases, and every call gets a
        # fresh source/lease fence immediately before it is sent.
        for _ in range(8):
            leases = self._store.claim_pending_intake_acks(
                self._holder,
                self._lease_seconds,
                self._allowlist.team_id,
                self._allowlist.channel_id,
                limit=1,
            )
            if not leases:
                break
            lease = leases[0]
            self._react_or_retry(lease)
            processed += 1
        return processed

    def close(self) -> None:
        """Return any in-flight work for immediate recovery before closing SQLite."""
        self._stop.set()
        self._wake.set()
        thread: threading.Thread | None
        with self._lifecycle_lock:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self._lease_seconds))
        if thread is not None and thread.is_alive():
            # A provider call is still in flight.  Keep this worker's private
            # connection alive until process exit rather than racing a close
            # against that call; its lease expires for recovery elsewhere.
            self._log.error("Slack intake reaction worker did not stop before close")
            return
        self._store.release_intake_acks(self._holder)
        self._store.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.process_once()
            except Exception as error:
                # Do not surface provider errors or request values in logs.
                self._log.error("Slack intake reaction worker failed (%s)", type(error).__name__)
            self._wake.wait(self._poll_seconds)
            self._wake.clear()

    def _react_or_retry(self, lease: IntakeAckLease) -> None:
        if not self._store.validate_intake_ack(lease):
            return
        try:
            response = _response_data(
                getattr(self._web_client, "reactions_add")(
                    channel=lease.source_channel_id,
                    timestamp=lease.source_ts,
                    name="white_check_mark",
                )
            )
            if response is not None and response.get("ok") is False and response.get("error") != "already_reacted":
                raise ReceiptAckError("Slack rejected intake reaction")
        except Exception as error:
            if _already_reacted(error):
                self._store.acknowledge_intake_ack(lease)
                return
            self._log.error("Slack intake reaction failed (%s)", type(error).__name__)
            self._store.retry_intake_ack(lease)
            return
        self._store.acknowledge_intake_ack(lease)


class ReceiptAckError(RuntimeError):
    pass


def _response_data(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    data = getattr(value, "data", None)
    return data if isinstance(data, Mapping) else None


def _already_reacted(error: Exception) -> bool:
    response = _response_data(getattr(error, "response", None))
    return response is not None and response.get("error") == "already_reacted"
