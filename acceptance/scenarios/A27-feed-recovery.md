# A27: Feed removal and restart preserve source conversations

## Setup

Apply RUNNER.md and use A26 fixtures transferred to this scenario. Confirm no active workers in either source channel before the bounded shared receiver restart. Use only test config/state and the dedicated test feed. Record immutable title/emoji and current cards before restarting.

## Actions

Run the documented shared receiver restart helper from the published checkout. Wait for both source receiver loops and feed health to become fresh. Inspect the feed for duplicates or identity changes. Through supported test-config CLI remove session B from the feed and wait for deletion. Open B's original source thread directly and verify its full history. Then send a new explicit follow-up in B's original thread and observe its new answer/card. Inspect A remains unaffected.

## Expected

Restart produces no duplicate cards and preserves A/B titles, emojis, preview content, and source links. Removing B deletes only its tracked feed card, preserves all original messages, and starts no agent work. Recovery of an already-confirmed historical reply does not make a removed card visible again, including a card with retained Guardian approval display state. A new explicit reply in B resurfaces one B card at the newest end with the same title/emoji and latest answer. A remains intact. Shared original reply/ack behavior is healthy after restart. No real conversation or real feed card is altered.

## Evidence

Before/after feed screenshots, B source-thread screenshot after removal, restored B card screenshot, exact published/runtime SHA, both-channel idle/restart/health evidence, and durable feed state proving settled generations and delete targets.

## Cleanup

Remove only A26/A27 synthetic feed sessions using test-config CLI. Verify absent cards, preserved source history, and no synthetic responsibilities/reminders or active jobs. Mark transferred fixture cleanup CLEAN after observation.

## Failure handling

Follow RUNNER.md restart recovery. Retain original failure logs and do not restart solely because an observation timed out. Fix via a new published candidate and rerun affected tests.
