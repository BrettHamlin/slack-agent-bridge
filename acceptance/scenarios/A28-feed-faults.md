# A28: Feed uncertain mutations preserve order and ownership

## Setup

Isolated temporary synthetic fixtures only, with fake Slack clients and no live credentials or receiver. Read the conversation-feed focused tests. Record tested SHA and exact commands.

## Actions

Execute focused code tests covering repeated reply keys, A/B/A ordering, concurrent updates, stale generations, post uncertainty across restart, delete uncertainty/already-missing cards, permanent Slack rejection and rate limits, feed identity/config rejection, metadata immutability and emoji allocation, removal and revival, and exclusion of feed traffic from dispatch. Exercise original reply publication when feed queueing/publishing fails. Run the acceptance target rejection tests.

## Expected

Tests prove no blind repost after uncertain acceptance, durable replacement before deletion, deletion limited to owned feed-card IDs, latest generation winning after retries, and no original thread mutations. Titles/emojis persist, active emoji collisions are avoided, source/feed separation holds, and feed faults cannot block original reply delivery/receipts. Unavailable transport proof remains uncertain. Test config rejects real feed destinations. All selected tests pass; injected clients establish only isolated correctness, not live acceptance.

## Evidence

Private test logs with exact test names/counts, passed/failed assertions, temporary-fixture scope, and SHA. Report which fault windows were exercised; do not infer untested coverage from suite success.

## Cleanup

Close fake clients, workers, and SQLite connections and remove temporary fixtures. No live cleanup actions.

## Failure handling

Preserve failing logs and reproducer. Add a focused regression before repair and rerun against the repaired candidate. Live feed behavior remains unverified until A26/A27 pass.
