# A18: Runtime startup and transport respect credential boundaries

## Setup

Use synthetic environment values and temporary fixtures in tests.test_runtime and tests.test_service_queue. Never inspect live .env, .env.runtime, secret stores, or password-manager bytes.

## Actions

Run the relevant tests for runtime source validation, unsafe/missing credential rejection, service queue identity and operation allowlists, timeout/uncertain command handling. Do not lock the user's device as a test.

## Expected

Synthetic unsafe/missing sources fail closed; queue requests cannot export credentials or execute arbitrary operations. Transport uses the resident receiver and does not fall back to an interactive vault path. Assertions must verify these behaviors.

## Evidence

Record test commands/names and results only; no real secret or screenshot of a credential UI.

## Cleanup

Restore test-patched environment through teardown.

## Failure handling

Use the runner contract: preserve the failed attempt, stop dependent scenarios, repair through a PR, and rerun against the repaired published SHA. Never substitute CLI actions for required UI interactions or weaken the expected behavior to make a run pass.
