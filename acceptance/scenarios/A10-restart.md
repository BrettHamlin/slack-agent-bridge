# A10: Completed work stays completed across receiver restart

## Setup

Apply RUNNER.md preflight: all UI uses private director-tests C0000000004 in team T0000000009; all diagnostic/fixture/cleanup CLI commands select --config config/director-tests.json or DIRECTOR_CONFIG. Verify state/testing/inbox.sqlite3 and state/testing/dispatch before actions. For an Atlas manifest, use the standalone Atlas A10 procedure in HOST_PROFILES.md instead of the shared-receiver commands below.

Use an authorized bounded maintenance window. Normally verify BOTH channels have no active workers. The helper may make one narrow exception: across both dispatch stores, the only active work is one `blocked` `guardian_approved_retry_unpublished` job with exactly one `used` Guardian approval, one or more publish authorities that are all `closed`, no `pending`, `submitting`, or `uncertain` Guardian approval anywhere, and a valid owned recorded process-group ID. Identify the single shared receiver com.example.director.receiver, which hosts both channels, and record published SHA, both-channel idle evidence, completed test job attempts and reply counts. Verify its test-channel binding to config/director-tests.json, state/testing/inbox.sqlite3 and state/testing/dispatch. Record BLOCKED if authorization, idle evidence or service identity cannot be established.

That exception requires `runtime='acp'` and a non-null `finished_at`: it applies only to the completed failed retry, not a pending or running turn.

## Actions

Briefly restart that exact shared Director LaunchAgent with `python3 scripts/restart_receiver.py --config config/director.json --test-config config/director-tests.json`. The helper must prove teardown with `launchctl print` before bootstrap, preserve a first bootstrap error, and retry the same plist only once after it is again absent. For the narrow spent-Guardian exception, after bootout and confirmed service absence it must use a no-signal process-group probe; only `ProcessLookupError` proves the old group gone. A live, permission-denied, or invalid group result is `spent_guardian_runtime_unresolved`: do not bootstrap normally, restore the same shared service in a finally-style step after a fresh absence observation, and report failure. Wait up to 120 seconds for a fresh connected loop and recovery only after the applicable preflight/proof passes. Reopen the two test threads in director-tests. Keep all test data and state mutations under the test config. Do not start a second Socket Mode connection; CLI listen with the test config rejects a second socket.

## Expected

Receiver and command service recover; completed test jobs are not launched again, reply counts remain one, and no duplicate notices/cards appear. The spent-Guardian exception leaves the job, used approval, and closed authorities unchanged; it never settles the job, repeats native approval, or retries its old action. No credential values are read or requested. A first launchd registration error may be recorded as a recovered retry only when the helper proves the service absent, the same plist succeeds once, and both loops become freshly connected. A retry failure, a service still present after the first error, missing fresh health, or `spent_guardian_runtime_unresolved` is FAIL/BLOCKED, never fixed by changing credentials behind the test.

## Evidence

Record service identity, before/after health and job states/attempts; for the exception, record only the guarded state classifications and no-signal process-group result. Capture test threads after restart.

## Cleanup

Restore the same shared com.example.director.receiver service in a finally-style step and verify connected health. If restoration fails, preserve evidence and report the precise blocker. Leave unrelated services untouched.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
