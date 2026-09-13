# A07: Continuation in a new conversation preserves context

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Create an A02-style waiting fixture tagged {RUN}-A07. Record its ID and original thread.

## Actions

Start a new Slack root: "Continue test {RUN}-A07 from the invitation question: choose formal and draft it here."

## Expected

Director associates the explicit continuation with the same responsibility, moves the current conversation to the new thread while retaining history, and publishes the formal invitation there. Old controls do not remain actionable and no duplicate assignment is created.

## Evidence

Capture original question, new-root continuation and result; record responsibility ID, context history, old/new roots, completion, and card state.

## Cleanup

Cancel the exact fixture if it remains active; never merge real responsibilities to force a pass.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
