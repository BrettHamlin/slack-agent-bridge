# A26: Conversation feed keeps one useful card per session

## Setup

Apply RUNNER.md. Verify personal workspace T0000000009, private source director-tests C0000000004, and private feed director-feed-tests C0000000007 with Owner and Director membership. Test config must enable conversation_feed with that feed ID. All state is under state/testing. Record published/runtime SHA. Use unique run-tagged synthetic sessions A and B; no responsibilities, reminders, or external actions.

## Actions

In the source channel, ask A to compare fictional routes Pine and Cedar, with Cedar one transfer shorter; request a concise recommendation. Wait for the actual reply and feed card. Create a distinct B session asking which fictional picnic day is suitable when Saturday is rainy and Sunday dry. Wait for B's reply/card. Return to A's original thread and change the premise so Pine is now direct and Cedar has two transfers; ask for the revised recommendation. Wait for the reply and settled feed. Click each Open conversation button and inspect the destination. Do not send a continuation merely to test navigation. Observe for 30 seconds. Capture source receipt and feed generation metadata through supported test diagnostics.

## Expected

Each confirmed answer gains a readable feed card within 30 seconds of reply delivery. Settled feed has exactly one card per A/B session, ordered B then refreshed A at the newest end (Slack bottom). A retains its meaningful title and emoji; A/B use distinct emojis. A's preview now recommends Pine and B's recommends Sunday, without stale answer text, generic status, elapsed duration, or duplicate rows. Layout uses full-width title and answer text with one Open conversation button beneath. Button A opens A's original source thread and B opens B's; navigation starts no worker, responsibility, or reminder. Feed traffic does not enter source intake. Original replies/history remain intact. Source acknowledgment and replies still meet RUNNER.md timing budgets.

## Evidence

Screenshots of first cards, settled B/A feed, and both destination threads. Record source/answer/card timestamps, thread roots, title/emoji before and after, generation/current-card identifiers, settled feed count, and no-work observation. Never use a screenshot of the Builder as live evidence.

## Cleanup

For smoke-only runs remove the exact synthetic feed sessions using supported test-config feed removal and verify cards disappear while source history remains. If A27 will run next, record ownership transfer to A27 with exact session/card IDs. No deferred fixture may remain. Preserve source test history.

## Failure handling

Preserve failed evidence. Repair through a PR, publish, and initialize a fresh run. Do not weaken timing, uniqueness, content, or navigation expectations to match implementation.
