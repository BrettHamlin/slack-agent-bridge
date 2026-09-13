# A17: Waiting and idle periods remain quiet

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Create a separate A02 waiting fixture tagged {RUN}-A17; finish its source intake and record job counts.

## Actions

For 90 seconds do not answer, open a continuation, or click controls. Observe channel and durable work state. Separately observe an idle dispatcher after cleanup for 30 seconds.

## Expected

Waiting stays waiting_input and produces no repeated card, nudge, model invocation or unsolicited task work. Empty dispatcher ticks do not start model jobs. Opening Slack alone is not permission to proceed.

## Evidence

Capture initial/final card, timestamps, state and job-count snapshots.

## Cleanup

Drop this exact fixture and verify cancellation; then complete the idle observation.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
