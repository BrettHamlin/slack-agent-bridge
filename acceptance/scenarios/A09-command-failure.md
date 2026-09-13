# A09: A failed harmless command gets a visible answer

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use a new root tagged {RUN}-A09. Do not request a destructive action to provoke approval review.

## Actions

Type: "Test {RUN}-A09: Run /usr/bin/false once (expected exit 1), then report its observed exit status in this thread with FAILURE-HANDLED-{RUN}. Do not retry it or change services. This is a diagnostic check, not an ongoing task."

## Expected

The manager observes exit 1 and reports the expected failure honestly in Slack. It does not silently stop, repeatedly run the command, or claim the command succeeded. The reply is delivered once and the source/job finishes. This tests command failure, not actual approval rejection or OS signal denial.

## Evidence

Screenshot source/read receipt and open reply; inspect only this worker's command result, outgoing key, source completion, attempts and terminal state.

## Cleanup

Confirm no active test responsibility or pending source remains.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
