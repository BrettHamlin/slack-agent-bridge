# A29: One-use native Guardian approval preserves automatic review

## Setup

Apply RUNNER.md preflight. Use only private `director-tests`, `config/director-tests.json`, and ignored `state/testing/` evidence. Record the published Director SHA, Codex version, pinned ACP adapter version, selected Codex profile and actual model/reasoning effort, and automatic-review mode. For the current runtime candidate, confirm the adapter-resolved `@openai/codex/bin/codex.js` reports the exact checked-in `@openai/codex` pin before receiver control; a shell-global Codex version or the retained legacy runtime does not satisfy this check. Start with a new harmless synthetic source in the designated controlled test root; do not reuse a historical denial or use a production conversation.

Arrange the controlled fixture so Codex automatic review denies its offered `director-publish-reply/publish_reply` call. Keep the receiver and adapter process alive through approval. Do not weaken approval settings, inject a marker, call Codex App Server directly.

## Actions

1. Observe the denied current reply transition to the durable local pending-approval record and its concise source-thread notice. Confirm the record is correlated to the active source authority, session and generation; retain only IDs and digests in evidence.
2. Run the authenticated receiver-local `guardian-pending` command and select its one returned job key. Verify its output omits reply text, authority, fingerprint, and native Guardian event data.
3. Run `guardian-approve --key JOB_KEY` once. Observe the adapter call Codex's native approval route and one normal same-session retry. The optional Slack Card-B control is covered by A30.
4. Verify the retry publishes the exact denied `publish_reply` payload, including any conversation title, emoji and preview, through the original stable outbox key. Verify one source reply, a sent outbox record, and completion receipt; if the feed is enabled, verify its single settled projection.
5. Repeat `guardian-approve` for the same key, then attempt a changed reply payload. Exercise one controlled source edit or expiry case and one receiver/adapter restart case in fresh fixtures.

## Expected

Automatic review remains enabled throughout. The approved retry retains the denied turn’s model and reasoning effort. Only a completed native denial for the current source reply can become pending. The owner command approves one adapter-held opaque record; it cannot submit an event, authority, review, text, or payload. The normal retry retains the same source authority and accepts only the locked payload digest. It delivers once, and duplicate delivery reconciles the original outbox key.

A duplicate approval, changed payload, stale source, expired record, or restart never publishes the denied reply. A used approval whose retry ends without verified delivery remains blocked without generic automatic retry. It can settle only through explicit reconciliation after independent proof that its runtime is gone, and that settlement never resends the denied action. An expired record releases the root for genuinely new work but does not resend the denied action. Slack uses an owner-bound Approve once control; A30 covers callback and display-specific acceptance.

## Evidence

Save private metadata-only records of the current authority/session/generation correlation, native review ID/fingerprint digest, command result, native-route trace, one reply/outbox/receipt correlation, and any feed projection. Capture the selected profile and automatic-review setting. Do not retain reply body, raw Guardian event, native action arguments, credential values, or unrelated thread history.

## Cleanup

Settle the synthetic source and remove only the synthetic test artifacts under `state/testing/`. Confirm no pending approval, runnable work, reminder, unresolved outbox send, or test feed card remains. Preserve failed evidence if a negative case fails.

## Failure handling

Use RUNNER.md. A missing native route, stale/restarted adapter record, denied retry, absent outbox receipt, or unexecuted controlled fixture is BLOCKED/PENDING, never PASS. Keep the original failed attempt and open a scoped regression before rerunning on a new published SHA.
