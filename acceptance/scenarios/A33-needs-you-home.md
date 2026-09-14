# A33: Needs you keeps owner review separate from completed work

## Setup

Select an explicit local test profile under RUNNER.md with its own app, source channel, state directory, and configured App Home. If inline links are enabled, use only that test host's HTTPS proxy and Sign in with Slack redirect URL. Record the published SHA and receiver config digest. Do not use Odyssey, Atlas, or a real owner conversation.

## Actions

1. In the private source channel, give the agent a harmless request that leaves a draft for review. Open the app Home.
2. Check the item. Verify it moves to Done and does not create, claim, resume, cancel, or complete a responsibility. Uncheck it and verify it returns to Active. Reopen Home and verify the state survives.
3. Snooze the active item through the authenticated action page. Verify GET/navigation alone does not change state; only the confirmed authenticated POST moves it to Snoozed with the selected return time. Bring it back through the same flow and verify it returns to Active. Replaying the POST, a stale link, a wrong owner, or a changed source item must not change state.
4. Ask for a harmless end-to-end action whose authorized send is completed. Create enough synthetic review items to exercise pagination.
5. On the owner-selected **Motorola RAZR** Slack client (record the model and USB or wireless connection), open the configured App Home and inspect Active, Snoozed, and Done at the narrow mobile width. Confirm each section wraps without clipping its task text or inline links; checkboxes and Open conversation/Snooze/Bring back links remain reachable. Capture private screenshots of each section. A Block Kit Builder or mobile-preview image may support this visual layout review, but it is not evidence of live callbacks, authentication, or complete A33 acceptance.

## Expected

The confirmed draft reply appears once under Active with a checkbox, detail, Open conversation, and Snooze. Open conversation returns to the source thread without starting work. The review lifecycle survives reopening Home, does not alter any responsibility, and only the explicit authenticated POST changes a Snooze/Bring back state. Stale, replayed, foreign-owner, or changed-source controls have no effect. The end-to-end authorized send creates no Needs you item. Every active/snoozed item is reachable through paging and Done retains only the newest 25.

## Evidence

Keep private screenshots of the App Home states, source reply receipt, action-page confirmation, and redacted local state evidence under the run directory. Include the RAZR narrow-screen screenshots for Active, Snoozed, and Done, identifying whether they came from the live configured app or Builder/mobile preview. Record the SHA, exact selected config, and each observed state transition.

## Cleanup

Preserve the synthetic source-thread history. Remove only fixture state owned by this run through the documented supported cleanup and confirm no pending review, reminder, responsibility, outbox send, or active turn remains. Record the status as BLOCKED if the isolated app, HTTPS proxy, Sign in with Slack identity, maintenance window, or mobile app access is unavailable; local tests are not a live UI pass.

## Failure handling

On unavailable App Home scope, Sign in with Slack configuration, HTTPS proxy, owner identity, or maintenance window, preserve redacted evidence and mark BLOCKED. Do not test against a real conversation, bypass authentication, or expose the loopback action server.
