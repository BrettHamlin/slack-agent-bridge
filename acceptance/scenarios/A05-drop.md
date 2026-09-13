# A05: Drop cancels and prevents further work

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use the still-active A02 responsibility (after A04 in a full run).

## Actions

Click Drop on this fixture's card and inspect the confirmation dialog and title. Click Keep it once and verify the responsibility remains active. Reopen Drop and confirm using the dialog's Drop button. Wait for the card layout to settle, then read the resulting UI and durable state. Observe for 30 seconds without sending a continuation.

## Expected

The responsibility is cancelled; its attention controls are retired and it is not runnable. No new draft, nudge, or active replacement card appears. Cancellation is not represented as successful completion of the invitation.

## Evidence

Screenshot the stable card before Drop, the confirmation dialog, and the settled card after confirmed Drop; record responsibility state, execution fence, attention-card state, and absence of additional output over the observation window.

## Cleanup

Verify this fixture is cancelled. Do not delete unrelated or historical records.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
