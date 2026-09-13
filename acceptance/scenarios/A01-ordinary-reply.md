# A01: Ordinary conversation replies without task cards

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use a new Slack root with marker {RUN}-A01. No test responsibility should exist for this marker.

## Actions

Type: "Test {RUN}-A01: Reply with ACK-{RUN}-A01. This is a conversation check, not an assignment." Observe the receiver-owned white checkmark (received and durably saved) and threaded answer; open the reply through the UI. Record the reaction delivery time separately from source preparation/read and reply delivery.

## Expected

The reply contains the marker in the correct thread. One reply, one completed source revision, and no active responsibility or attention card for this marker. The received checkmark arrives within 2 seconds of Slack source time under healthy Slack service conditions and does not assert that the model has read or completed the request. Neither the reaction nor an internal read receipt alone is completion.

## Evidence

Capture the sent message and open reply. Record source ID/revision, Slack root, elapsed received-checkmark and reply times, separate source-read/preparation evidence, and job state/attempts.

## Cleanup

Leave the labeled transcript; confirm no test work remains queued.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
