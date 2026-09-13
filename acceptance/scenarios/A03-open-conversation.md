# A03: Open conversation navigates without authorizing work

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use only the active {RUN}-A02 card and its recorded responsibility ID.

## Actions

Click Open conversation. Verify the opened thread. Observe the responsibility and dispatcher for 30 seconds without typing an answer.

## Expected

The button opens the exact conversation containing the tone question. It does not create another responsibility, resume work, draft the invitation, or complete the item. State remains waiting_input and the question remains available.

## Evidence

Screenshot the card before the click and the destination thread afterward. Record before/after state and job count for this responsibility.

## Cleanup

Leave A02 waiting for A04 or A05.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
