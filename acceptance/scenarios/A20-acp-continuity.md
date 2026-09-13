# A20: ACP conversations retain context and remain isolated

## Setup

Apply RUNNER.md preflight. All UI and diagnostics use private director-tests and config/director-tests.json; all state and fixtures use state/testing/. Record the published suite/runtime SHA, selected profile, adapter/Codex versions and confirmed agent session IDs. Never inspect credentials or real-channel content.

Use two new synthetic Slack roots with distinct run markers and distinct random phrases. Confirm the configured default profile uses ACP.

## Actions

In the first root ask Director to remember its synthetic phrase and acknowledge it. After the reply, ask in that thread for the phrase without repeating it. Repeat with the second root and a different phrase. Observe both conversations through Slack and record their durable gateway bindings.

## Expected

Each follow-up returns only its own phrase in its own thread. Each source has one verified reply and a completion receipt. The same root retains its profile/backend/session binding; the two roots have different agent sessions. No responsibility or attention card is created for these conversation checks. When the separate conversation feed is enabled, its navigation-only cards are expected. Adapter progress or turn completion alone cannot mark a source done.

## Evidence

Capture roots and open replies, timestamps, source revisions, outbox keys, job results and the two binding identifiers. Record whether the process was reused and prove no additional prompt was sent during idle observation.

## Cleanup

Leave labeled conversation transcripts and verify no synthetic responsibility, reminder, pending job or active turn remains. When the conversation feed is enabled, hide only these recorded synthetic session roots through the test-config CLI and verify their feed cards disappear.

## Failure handling

Use RUNNER.md: retain the failed evidence, fix through a reviewed PR and start a fresh run on the repaired published SHA. Missing control, capability, receipt or cleanup evidence is BLOCKED/PENDING, never PASS.
