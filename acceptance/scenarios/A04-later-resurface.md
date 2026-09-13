# A04: Later defers and resurfaces the same responsibility

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use the waiting A02 card. Record its current identity, state, and controls.

## Actions

Click Later and select the shortest actual option shown (currently 1 day). Verify deferred state and the due timestamp. Wait until that due time, then allow 60 seconds for maintenance. Do not alter the database or shorten the clock for this UI result. A04 may remain PENDING overnight while this run stays active with explicit cleanup ownership and a recorded resume time; this is not stopping early. Keep A05 PENDING until A04 finishes. If the run cannot retain ownership and resume, record BLOCKED with the due time and perform early-stop cleanup. An accelerated CLI test must be a separate result, not a UI pass.

## Expected

The same responsibility becomes deferred, loses runnable eligibility, then returns to waiting_input with the same logical card and one nudge. No model work starts from the timer. No duplicate responsibility or active attention card appears.

## Evidence

Capture the menu/selection, deferred card, resurfaced card, and nudge. Record due time and before/after responsibility/card IDs and states.

## Cleanup

Continue to A05 after observing the real timer. During an active overnight wait, retain the exact fixture with cleanup=PENDING, scenario_id=A04, cleanup_owner and timezone-qualified resume_at in the fixture ledger. Do not cancel it during that wait or claim the run CLEAN. If stopping early, cancel only this deferred test responsibility and verify that it cannot resurface.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
