# A30: Approval feed card approves one exact denied reply

## Setup

Apply RUNNER.md using only private `director-tests` and `director-feed-tests`, `config/director-tests.json`, and ignored `state/testing/` evidence. Record the published SHA, Codex version, pinned adapter version, selected profile, and automatic-review mode. For the current runtime candidate, confirm the adapter-resolved `@openai/codex/bin/codex.js` reports the exact checked-in `@openai/codex` pin before receiver control; a shell-global Codex version or the retained legacy runtime does not satisfy this check. Create one new harmless synthetic source whose current `publish_reply` is denied by automatic review. Keep the receiver and adapter alive. Do not reuse a historical denial or use a production thread.

## Actions

1. Wait until the denied source job is terminally `guardian_approval_pending`. In `director-feed-tests`, inspect its one Card-B row: stable title/topic emoji, `Approval needed`, the complete exact proposed reply, **Approve once**, and **Open conversation**.
2. Confirm the proposed text is fully visible before clicking. Click **Open conversation** and verify that it navigates to the source thread without dispatching work.
3. Click **Approve once** exactly once. Observe its retrying state, one supported native approval call, one same-session exact-payload retry, one original source reply, a sent outbox record, completion receipt, and the usual settled latest-answer feed preview.
4. Attempt the old Approve button again and test wrong-owner, wrong-team, wrong-channel, wrong-message, stale-generation, oversized, expired, and receiver-restart fixtures through isolated tests. Do not run a second native approval or changed payload.
5. In isolated fixtures, verify that a spent approved retry without verified delivery remains blocked while runtime ownership is unresolved. For controlled recovery, after independently proving the recorded runtime is gone, the test may reuse an already recorded failed synthetic fixture from a prior run; it must not reuse that fixture's approval. Run explicit receiver reconciliation and confirm it settles as failed without a new native approval, retry, or source reply.

## Expected

Automatic review remains enabled. A card becomes clickable only after the denied source authority and blocked job are durable. The card action carries only its current opaque feed approval ID; it does not expose authority, fingerprint, review ID, native event, or reply payload. Socket Mode acknowledges the callback promptly, does not route feed traffic into intake. The listener validates owner, team, and feed channel before queueing; the ChannelLoop revalidates the current card timestamp, generation, and pending approval before calling the one-use native bridge.

The approved reply text is rendered in plain-text sections, complete and unmodified. If it exceeds Slack's complete safe display limits, the card has no Approve button and directs authenticated local review; it never approves a partial display. A used, stale, expired, failed, or restart-inactive row has only Open conversation. A spent retry with no verified delivery remains blocked until independently proved runtime release permits explicit reconciliation to failed; that reconciliation never repeats native approval, retries the old action, or publishes a reply. One normal confirmed reply restores the same title/emoji and ordinary latest-answer preview. Recovery of that historical approved reply never makes a user-hidden card visible again; a later confirmed reply may resurface it. Duplicate/stale clicks and any failure never publish the denied reply.

## Evidence

Save private metadata-only source/job/card generation/outbox/receipt correlations, callback acknowledgement timing, selected runtime settings, and screenshots of the full proposal, retrying state, settled answer, and navigation. Do not retain authority, fingerprint, review IDs, raw Guardian event, credentials, or unrelated conversation content.

## Cleanup

Settle or remove only the synthetic test fixture through supported test-config cleanup. Confirm no test feed card, pending approval, runnable job, reminder, or unresolved outbox send remains. Preserve failed evidence.

## Failure handling

Follow RUNNER.md. Any missing full-text display, unexpected callback route, stale-click acceptance, absent receipt, missing native marker, unproven runtime release, or unexecuted live click is FAIL/BLOCKED/PENDING as appropriate; never substitute a CLI approval for this scenario.
