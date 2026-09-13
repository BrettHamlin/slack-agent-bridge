# A31: Complexity selection preserves harness and conversation continuity

## Setup

Select the host explicitly under HOST_PROFILES.md and complete RUNNER.md preflight with its exact validated config. On Odyssey, select `--profile odyssey` and use only private director-tests / config/director-tests.json / state/testing; never use the Atlas app, channels, state, or service. On Atlas, select `--profile atlas` and use only its validated development target. Record the published SHA, selector version and configured grade table. Use synthetic conversations only. Enable selection for the candidate; retain the fixed Codex configuration for rollback. Claude must authenticate through its own CLI subscription login, with ANTHROPIC_API_KEY absent from the child environment. Missing authentication blocks the Claude portion.

## Actions

Start a Codex conversation, remember a synthetic marker, and issue follow-ups of differing complexity. Repeat with an explicitly selected Claude conversation. Record classifier tier, selected model/effort, harness and opaque session ID for every turn. An authorized Odyssey pilot may execute only these two new synthetic conversations: one default-Codex and one explicit-Claude check. Record every other A31 assertion as PENDING and do not call the full scenario PASS. Restart the receiver between completed turns and ask for the marker again only when that portion is authorized. In controlled fixtures, fail the classifier, return malformed classification, fail model configuration, lose the runtime after publication, and try to change a bound conversation's harness. Exercise concurrent conversations and duplicate source delivery. Verify fixed Codex rollback on a Codex-bound thread.

## Expected

The classifier uses Luna/xhigh through ChatGPT subscription authentication. Only plaintext task/context is classified; execution and credentials do not pass through the classifier. Grades and execution models follow the pinned Mipmap runbook: light Luna or Sonnet, standard Terra or Opus, heavy Sol or Fable, all high effort. Classification failure uses LiteLLM's local heuristic with an observable cause. New conversations default to Codex; explicit Claude selection uses Claude Code one-shot calls with auto permissions. Each follow-up resumes its exact backend-bound session ID, including after restart. Codex model/effort changes preserve that session. A failed configuration or mismatched returned session prevents submission/completion. No implicit harness, API-billing or model fallback occurs. Process completion alone never counts as delivery: the existing publish_reply authority, outbox and completion receipt remain decisive. Lost/uncertain execution remains fenced against duplicate actions. Concurrent roots cannot mix transcripts, session IDs or publication authorities. Idle ticks do not call the classifier. Fixed Codex rollback does not reinterpret Claude IDs.

## Evidence

Save private live Slack observations, source/receipt/job records, selection metadata, exact session IDs, restart timestamps and subprocess lifecycle evidence under the run directory. Preserve classifier and runtime failures as separate attempts. Include focused fixture tests for environment removal, explicit resume, rejected malformed/error results, timeouts and cleanup. Isolated tests do not prove subscription authentication, live tool approval, Slack delivery or transcript continuity.

## Cleanup

Remove only this run's synthetic responsibilities and reminders using RUNNER.md. Confirm no owned subprocess groups or pending synthetic jobs remain. Restore the intended candidate configuration, or the fixed Codex rollback configuration if the candidate failed, and record which is active.

## Failure handling

Mark missing live tooling, authentication, unexecuted cases and unconfirmed cleanup BLOCKED or PENDING. Preserve uncertain publication fences; do not rerun a possibly completed tool action or create a replacement session to manufacture continuity. Fix through a PR and repeat affected scenarios against the new exact SHA.
