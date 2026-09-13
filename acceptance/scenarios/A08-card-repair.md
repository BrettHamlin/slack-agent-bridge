# A08: A missing waiting-input card is repaired

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use a new TEST: responsibility created through the documented responsibility-create CLI in waiting_input with a test-only ID and a verified test conversation URL. Intentionally omit responsibility-post-card. No real task or existing card is modified.

## Actions

Inspect the initial absence of a linked card, then wait through the documented 30-second grace period plus 60 seconds. Inspect the channel and responsibility.

## Expected

The receiver creates one linked card with Open conversation, Later, and Drop without starting a manager job for the waiting responsibility. Its ID and waiting_input state remain unchanged. Subsequent maintenance does not create duplicates.

## Evidence

Record the exact fixture command (without private data), before/after state, card screenshot, health checkpoints and job counts.

## Cleanup

Drop the repaired test card or cancel its exact responsibility via the supported CLI; verify terminal cancellation.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
