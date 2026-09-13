# A15: Denied cleanup signals do not hang or duplicate workers

## Setup

Use temporary processes/fixtures only. Never force a live service or unrelated process into an unkillable state.

## Actions

Run tests.test_dispatch_worker and the dispatcher tests for cleanup_failure, timeout_signal_denial and missing_process. Exercise a real sleeping child with group signaling mocked to raise PermissionError; the owned-child fallback must stop it. For both signal paths denied, use the isolated bounded-wait fixture.

## Expected

The real child exits within three seconds. Complete denial produces worker.cleanup_failed within bounded cleanup; the root remains fenced across supervisor restart and cannot launch a duplicate. Failure notification is durable. This records controlled injection, not an actual platform approval rejection.

## Evidence

Save exact commands, elapsed times, exit codes and assertion output; record that signal denial was injected.

## Cleanup

Terminate only spawned test processes and release their locks; confirm no test process survives.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
