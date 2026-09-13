# A02: An explicit assignment needing input creates an attention card

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions.

Use marker {RUN}-A02 and take a baseline responsibility list. This is deliberately an assignment, even though its content is synthetic.

## Actions

Type: "Test {RUN}-A02: Please take responsibility for drafting a two-line invitation to a fictional picnic. Before drafting, ask me whether its tone should be formal or casual, and wait for my choice. Keep this test task title prefixed TEST: and any test card key prefixed test-. Do not contact anyone." Inspect the response and channel card.

## Expected

Exactly one responsibility for this marker is waiting_input, with a saved next action and current source/thread. One active linked card shows the tone question and Open conversation, Later, and Drop. No final invitation is drafted before input. Completing the source receipt must not complete the waiting responsibility.

## Evidence

Screenshot the question and all card controls. Record responsibility ID, card key/ID, source ID/revision, current thread, state, and next action. Save these fixture identifiers for A03/A04/A05.

## Cleanup

Keep this fixture only until A05 finishes. If the run stops, Drop this exact test card and verify cancellation.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
