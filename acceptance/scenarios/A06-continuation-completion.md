# A06: Answering the question completes the same task

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Create a separate fixture using the A02 prompt with suffix A06. Record the waiting responsibility/card IDs.

## Actions

Answer "Casual, please. Draft it now." in its original thread. Open the delivered result.

## Expected

The existing responsibility resumes and produces the requested two-line fictional invitation in that thread. The old attention controls retire; the responsibility reaches completed only after publication. No second responsibility is created for the same assignment.

## Evidence

Capture waiting card, continuation, final draft, and retired controls; record same responsibility ID, outgoing delivery, and completed state.

## Cleanup

If incomplete, Drop only this fixture. Preserve failed-run evidence.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
