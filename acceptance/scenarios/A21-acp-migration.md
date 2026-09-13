# A21: Legacy sessions and completed work survive ACP migration and restart

## Setup

Apply RUNNER.md preflight. All UI and diagnostics use private director-tests and config/director-tests.json; all state and fixtures use state/testing/. Record the published suite/runtime SHA, selected profile, adapter/Codex versions and confirmed agent session IDs. Never inspect credentials or real-channel content.

Before migration, seed a labeled synthetic Slack conversation on the old published exec runtime, record a unique phrase, correct reply and legacy session ID in a separate baseline evidence record. The candidate run starts only after the reviewed ACP candidate is published. If no baseline exists, prepare one under the documented controlled rollback procedure and start a new candidate run afterward. Confirm both channels have no active work before receiver control.

## Actions

On the ACP candidate, send a follow-up in the exact seeded Slack root asking for the phrase without restating it. Record whether the legacy session was loaded successfully and the persisted binding. Once all turns are settled, restart only the documented shared receiver, verify reconnection, and ask a second follow-up in the same root. Exercise a default-profile change only in synthetic isolated fixtures; verify it cannot move an existing root or retry. Do not silently replace an unresumable session with an empty one.

## Expected

The old phrase and conversation are preserved through migration and restart. A confirmed legacy session ID is reused, or an explicitly recorded and verified handoff is required before further execution. A load failure blocks visibly without lost history or duplicate execution. Completed sources stay completed. Existing bindings and in-flight retry routes do not follow a changed default. Test and real state remain isolated.

## Evidence

Reference the separate baseline SHA/evidence, candidate SHA, old and current session identifiers, binding snapshots, source/outbox receipts, both-channel idle preflight, receiver restart/reconnection observations and screenshots of both candidate replies.

## Cleanup

Restore the receiver and configured default profile; confirm all synthetic turns settled and no synthetic responsibilities or reminders remain. Never clear a blocked session while an original process may still act.

## Failure handling

Use RUNNER.md: retain the failed evidence, fix through a reviewed PR and start a fresh run on the repaired published SHA. Missing control, capability, receipt or cleanup evidence is BLOCKED/PENDING, never PASS.
