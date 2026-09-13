# A22: Gateway failures preserve ownership and verified delivery

## Setup

All data and agents in this scenario are synthetic and temporary.

Use temporary SQLite state and deterministic SDK-based ACP peers in the gateway/dispatcher test suites. No real Slack connection or credentials are needed.

## Actions

Verify that the production client uses the pinned official ACP SDK for protocol encoding, decoding and request correlation. Use a peer built with the SDK for normal contract tests; malformed-wire fixtures are explicitly isolated fault injections. Run focused tests for malformed/unsupported initialize responses; required capability failure; unavailable adapter; failed session load; simultaneous roots; duplicate/late terminal events; cancellation; process death with multiple active sessions; restart recovery; stable routing after config changes; cross-environment isolation; and disconnect after a potentially committed action. Include receipt/outbox checks and legacy rollback ownership. Verify a bare configured executable does not add the current directory to PATH; a confirmed-empty process group is never signalled later; retired runtime events retain their original generation; and rejected permissions select the offered reject option. Exercise receiver-owned dispatch reconciliation: verified delivery is settled only when ownership can safely be released, and explicit retry of unresolved work requires a matching terminal acknowledgement or independent proof that its recorded runtime group is gone. A normal remote prompt error or cancellation confirmed by the final prompt response does not require restarting unrelated sessions. Sending a cancel notification alone must not release a turn; use a peer that delays or ignores cancellation to prove the fence remains. Restore ACP after a pre-submit rollback fence and verify that conversation can resume. For an unbound root whose ACP preparation failed, roll back to legacy and verify the CLI attempt is classified as legacy, reaches normal completion, and is not fenced as an ACP turn after restart. A late terminal with the wrong session, turn or runtime generation cannot release a timeout fence; unrelated blocked errors remain fenced, and a new attempt clears prior terminal evidence. If an explicit recovery retry fails, verify a new attempt-scoped failure notice while the original answer key remains unchanged. Run successful dispatcher-level legacy migration with optional load-response metadata.

## Expected

Unsupported requirements fail before prompt execution. Each job is bound to one session/profile and one owner; failures cannot redirect it silently. No automatic replay or fallback occurs while the prior action outcome or worker termination is uncertain. An agent end-turn without verified publication/source completion is not success. Duplicate/late events cannot complete a different attempt. No existing completed source is re-run. Cancelling one session does not cancel unrelated work. A recovery command cannot release a still-active turn, change the original outgoing key or session, or operate on another environment. Missing delivery proof is never treated as success. Test teardown leaves no processes or locks.

## Evidence

Record exact test names/commands, fixture-only status, exit codes and assertion summaries; retain failure logs separately. Do not cite a live connectivity check as proof of fault handling.

## Cleanup

Terminate all owned fake peer processes and remove temporary fixtures through test teardown; restore patched environment.

## Failure handling

Use RUNNER.md: retain the failed evidence, fix through a reviewed PR and start a fresh run on the repaired published SHA. Missing control, capability, receipt or cleanup evidence is BLOCKED/PENDING, never PASS.
