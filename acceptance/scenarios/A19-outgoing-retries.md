# A19: Uncertain delivery and failure notices survive retries

## Setup

Use temporary Slack outbox and dispatcher fixtures. No live message should be resent just to test deduplication.

## Actions

Run dispatcher notification error/restart and uncertain-notice regressions, plus Slack-service outgoing uncertainty tests. Confirm a persisted key is reused after an interrupted or uncertain send.

## Expected

A failed notice retries after backoff/restart using the original key. Confirmed delivery is not repeated. An uncertain send is reconciled rather than blindly issued under a fresh key. Successful source completion alone is not proof of delivery.

## Evidence

Record exact test names, state transitions, keys from synthetic fixtures and command output.

## Cleanup

Use test teardown; retain logs as run evidence.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
