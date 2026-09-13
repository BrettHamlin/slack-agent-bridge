# A16: Later, Drop, and stale claims fence publication

## Setup

Use responsibility and Slack-service test fixtures with a claim captured before a deferral, cancellation or newer claim.

## Actions

Run the responsibility and Slack-service regressions. Verify the selected assertions explicitly attempt stale publication and completion after Later/Drop/new claim. Include duplicate callback handling.

## Expected

Stale fences cannot publish or complete; repeated callbacks do not cause repeated transitions; existing confirmed external delivery is not claimed to have been undone. Missing assertions are a coverage failure.

## Evidence

Record exact selected tests and output plus which transition each covers.

## Cleanup

Use isolated teardown; never replay a real Slack action payload.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
