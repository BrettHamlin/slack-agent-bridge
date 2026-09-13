# Prepared task and card tools

This guide is supplied with the prepared turn. Use these tools only when the request needs them. Ordinary questions need only `publish_reply`; they do not create responsibilities or cards.

## Task decisions

An explicit assignment becomes a durable responsibility before execution. Preserve its ID on continuation; use list/get when the relevant ID is not already known. Thinking aloud, a capability question, and a hypothetical idea are not assignments. Prepare what is possible before requesting a specific missing input. If input is needed, persist `waiting_input` and post its linked attention card before publishing the source reply. A plain question in a thread is not a substitute for that card.

Only `runnable` work can be claimed. Claim before execution; check the returned execution fence before consequential actions and publication. A prepared work turn already carries its claim: do not claim it again. Later, Drop, expiry and a replacement claim invalidate old permission. Never continue on a rejected fence. The receiver's `publish_reply` owns result delivery and completion; a responsibility result must carry its current responsibility/fence authority. For a source-turn responsibility result, call `publish_reply(text=TEXT, authority=TURN_AUTHORITY, responsibility_id=ID, execution_fence=FENCE)`. The work must be bound to this source and current thread. A prepared work turn already binds these values. A waiting-input answer omits the optional task fields and preserves the responsibility and its card.

For a continuation in another thread, update the same responsibility's conversation URL, current root and source ID/revision. A cancelled task requires explicit owner instruction and `responsibility-resume`; an ignored card, navigation or timer does not authorize execution. Completed work resolves its card; waiting, deferred and cancelled work must remain quiet.

## Known command definitions

Run `.venv/bin/python -m director` in the supplied checkout and preserve `DIRECTOR_CONFIG`. Replace placeholders with the prepared values. Commands return JSON. Keep any necessary card/reminder body files owner-only in the supplied ignored state directory. An ordinary reply never needs an answer file.

- Inspect: `responsibility-list`; `responsibility-get --responsibility-id ID` returns the record and ordered context history.
- Create: `responsibility-create --responsibility-id ID --outcome TEXT --next-action TEXT --conversation-url URL --current-thread ROOT --source-message-id SOURCE_ID --source-revision REV --state runnable`. Use a stable ID and use `waiting_input` instead of `runnable` only for actual missing owner input.
- Continue/update: `responsibility-update --responsibility-id ID --next-action TEXT --state runnable --conversation-url URL --current-thread ROOT --source-message-id SOURCE_ID --source-revision REV`. Supply only fields that actually change; setting `waiting_input` records a needed decision.
- Claim: `responsibility-claim --responsibility-id ID --holder director-manager --lease-seconds 900`. Preserve the returned `execution_fence` and expiry.
- Check permission: `responsibility-execution-gate --responsibility-id ID --fence FENCE`. Proceed only when `allowed` is true.
- Attention card: `responsibility-post-card --responsibility-id ID --title TITLE --file CARD_BODY_PATH --key STABLE_CARD_KEY`. The existing responsibility provides the conversation URL; use a concise prepared question. Controls are Open conversation, Later and Drop. Reuse the same key for uncertain delivery.
- Owner-authorized stop/reopen: `responsibility-cancel --responsibility-id ID`; `responsibility-resume --responsibility-id ID`.
- Requested reminder: `remind --key STABLE_KEY --file REMINDER_BODY_PATH --due-at UNIX_SECONDS --thread-ts ROOT`. The receiver delivers it; do not install another model poller. `reminders` lists due reminders.
- Additional source evidence, only when the prepared context is insufficient: `source --message-id SOURCE_ID`. Inspect the returned pointer/revision before acting on changed content.

Use the supplied conversation URL when available. Otherwise construct the original-thread permalink from the supplied workspace/channel and timestamp: `https://WORKSPACE.slack.com/archives/CHANNEL/pTIMESTAMP_WITHOUT_DOT?thread_ts=ROOT&cid=CHANNEL`. Never use a different channel's root or state.

Routine investigation, preparation and drafts for assigned work are authorized. Contacting others requires explicit authorization. Preserve existing account, personal/work and credential boundaries. If a tool is rejected or unavailable, publish the specific blocker and preserve the next step without bypassing the control.
