# A12: Concurrent conversations keep their replies separate

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use two distinct test roots and two distinct response markers.

## Actions

Send two simple requests in quick succession, asking for ALPHA-{RUN} in one and BETA-{RUN} in the other. Add a follow-up to the first thread that asks it to repeat its own marker.

## Expected

Each reply appears only in its intended thread with correct context; both requests finish. Same-thread work is serialized, no duplicate replies occur, and no marker/context leaks into the other conversation.

## Evidence

Capture both source/reply threads and the follow-up; record root/session IDs, source revisions, job attempts and completion.

## Cleanup

Leave both test sources accounted for and no active test responsibilities.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
