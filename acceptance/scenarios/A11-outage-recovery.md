# A11: Messages sent during a receiver outage are recovered

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions. For an Atlas manifest, use the standalone Atlas A11 procedure in HOST_PROFILES.md instead of the shared-receiver commands below.

Use an authorized bounded maintenance window ONLY when BOTH channels have no active workers. Identify the single shared receiver com.example.director.receiver, which hosts both channels. Record its published SHA, configuration/health and both-channel idle evidence without credentials, verifying its test-channel binding to config/director-tests.json and both test state paths. Have the documented start command ready. Mark BLOCKED if authorization, idle evidence or service identity cannot be established.

## Actions

Briefly stop only the shared com.example.director.receiver with `launchctl bootout gui/$(id -u)/com.example.director.receiver`, then wait up to 15 seconds for `launchctl print gui/$(id -u)/com.example.director.receiver` to prove the service absent. Through Slack UI in director-tests send "Test {RUN}-A11: Reply RECOVERED-{RUN}." while it is absent. In a finally-style step, restore the same existing plist with `launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.example.director.receiver.plist"`. If bootstrap fails, preserve the first error, prove absence again, and retry that same plist once; do not retry while the service is present or its state is unknown. Verify fresh connected loops within 120 seconds. Let channel recovery run; also repeat with a follow-up in a previously tracked test thread in a separate authorized bounded outage after rechecking both channels are idle. Keep all test messages and fixture/state mutations under the test config. Do not start a second Socket Mode connection; CLI listen with the test config rejects a second socket.

## Expected

Each missed test message is recovered and answered once in the correct thread. Recovery completes within 120 seconds of reconnection and reply follows within the normal reply budget. No manual source insertion, fake receipts, or second receiver is used.

## Evidence

Screenshot sent messages and replies; record stop/start times, recovered source IDs, recovery checkpoint and per-message attempt/delivery counts.

## Cleanup

Always restore the same shared com.example.director.receiver in a finally-style step. Verify connected health, no missed test sources and no duplicates. Leave unrelated services untouched; report any restoration failure with evidence.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
