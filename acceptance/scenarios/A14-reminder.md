# A14: A scheduled reminder arrives once in the correct thread

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use a synthetic reminder due roughly two minutes ahead and a unique marker. No real calendar or external contact is involved.

## Actions

In Slack ask: "Test {RUN}-A14: Remind me in two minutes in this thread to inspect REMINDER-{RUN}." Inspect the durable reminder identity, due time and outgoing key, then wait until due plus 60 seconds.

## Expected

One due reminder is delivered in the intended thread, with durable sent evidence. It is not a responsibility Later card. Subsequent maintenance does not send it again. If the manager cannot schedule it, report FAIL with its actual response.

## Evidence

Capture request, scheduling acknowledgment and reminder; record key, due time, sent timestamp and count.

## Cleanup

Let this near-term test reminder finish and verify no due test reminder remains; do not delete the reminder DB or leave an untracked future reminder.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
