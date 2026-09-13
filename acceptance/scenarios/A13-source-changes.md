# A13: Edited and deleted messages do not execute stale instructions

## Setup

Use isolated fixtures, because deliberately racing a real action would not give a reproducible or safe result. Review tests/test_inbox.py, tests/test_slack_service.py and tests/test_dispatcher.py for revision/fence assertions.

## Actions

Run the relevant tests with verbose output. Confirm stale source revisions cannot receive current completion/read credit or publish a stale result; deleted sources are treated as unavailable evidence. If coverage is absent, record FAIL and add the missing regression before claiming this scenario passed.

## Expected

Assertions actually exercise changed/deleted source handling and reject stale decisions. Passing unrelated tests does not count. No live message is edited to overwrite a personal instruction.

## Evidence

Required deletion anchors: `tests.test_slack_service.SlackReadTests.test_deleted_source_cannot_receive_read_reaction_or_receipt` and `tests.test_inbox.InboxStoreTests.test_deleted_revision_rejects_previous_read_and_completion`. Added after the first acceptance audit exposed missing explicit deletion coverage.

Record exact test names, command, exit status and assertion coverage. No screenshot is required for isolated coverage. A separate UI exploration may supplement, not replace, these checks.

## Cleanup

Remove only isolated temporary fixtures through their test teardown.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
