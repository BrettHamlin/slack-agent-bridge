# Director model selection

For a fresh checkout, another machine, or an upgrade, start with [machine setup](machine-setup.md). This guide describes the repository implementation; deployment and acceptance are recorded separately per host. The selector returns a model and effort; Director executes the task through its bound harness. LiteLLM never receives an execution-model deployment to invoke the task itself.

## Configuration

Merge these fields into the explicitly selected host-local configuration, preserving that profile's Slack identity, database paths, and existing ACP settings. The example paths below assume an Atlas checkout at `/Users/example-dev/Code/Director`; confirm installed executable paths before deployment.

```json
{
  "dispatcher": {
    "runtime": "acp",
    "acp": {
      "command": ["/Users/example-dev/Code/Director/node_modules/.bin/codex-acp"],
      "profile": "codex-default",
      "model": "gpt-5.6-terra",
      "reasoning_effort": "high",
      "startup_timeout_seconds": 30,
      "request_timeout_seconds": 30,
      "prepare_timeout_seconds": 61
    },
    "model_selection": {
      "enabled": true,
      "default_backend": "codex",
      "timeout_seconds": 40
    },
    "claude": {
      "command": ["/Users/example-dev/.local/bin/claude"],
      "profile": "claude-default",
      "timeout_seconds": 300
    }
  }
}
```

`default_backend` accepts `codex` or `claude` and applies to an unbound conversation. An existing conversation retains its saved profile and backend. Profile identifiers must be distinct and remain stable after binding. An unavailable saved profile fails safely instead of redirecting the conversation into another harness. Claude configuration is optional when only Codex is used; requesting a new Claude conversation without its command is a preparation failure.

`runtime: "acp"` selects Director's managed gateway dispatch path. Codex uses the ACP adapter. The Claude gateway driver launches the official Claude Code one-shot CLI; it does not route Claude inference through Codex or LiteLLM.

## Grades and selection

The execution table follows `mipmap/mipmap-relay-services` commit `26aeb42840e186995e6821f0dc90c10b5456a057`, `ORCHESTRATION-RUNBOOK.md` and `engine/launch/execution-descriptor.mjs`.

| Grade | Task criteria | Codex model | Claude model | Execution effort |
| --- | --- | --- | --- | --- |
| light | Mechanical and enumerable; no new stateful purpose or logic branch | `gpt-5.6-luna` | `claude-sonnet-5` | `high` |
| standard | Default when neither light nor heavy is established | `gpt-5.6-terra` | `claude-opus-5` | `high` |
| heavy | Trust, authority, identity or routing invariants; migrations; cross-repository contracts; open-domain inputs; restructuring after repeated failed fixes | `gpt-5.6-sol` | `claude-fable-5-1` | `high` |

The classifier deployment is `chatgpt/responses/gpt-5.6-luna` with `xhigh` reasoning through LiteLLM. The `responses/` transport prefix selects the subscription Responses endpoint; the wire model remains `gpt-5.6-luna`. LiteLLM 1.100.1's ChatGPT Responses adapter strips server-side structured-output formatting, so the classification prompt requests JSON and the result parser validates its tier. It forces streaming and disables server-side response storage. The pinned nonstream bridge can discard text when the terminal Responses event has an empty output list, so Director collects streamed text deltas and requires a successful stop event before passing the result to the LiteLLM classifier parser. Partial or truncated output is a classifier failure. It receives plaintext task context over the worker's standard input and returns a tier. `SIMPLE` maps to light, `MEDIUM` to standard, and `COMPLEX` or `REASONING` to heavy. The classification prompt includes the source rubric and instructs uncertainty to grade upward.

The pinned release uses its LLM classifier with heuristic fallback; this is not a claim that its unreleased `hybrid` option is supported. Authentication/setup errors, invalid classifier responses, and classifier timeout fall back to LiteLLM's local heuristic. That heuristic is a library scoring mechanism, not an exact implementation of the semantic rubric. A local SIMPLE result with no semantic signal beyond short input is promoted to standard. Failure of the local worker fails preparation; it does not invent a route or switch providers.

The worker validates the selected backend, grade, model, and effort against the fixed execution table. Its output cause is `llm_classifier` or `local_heuristic`. Director records the selection in the job's `agent_selection` field, making fallback distinguishable in private dispatch evidence. Once recorded, retries of that job reuse the selection; a later job may select another model within the same harness.

## Authentication and process boundaries

OpenAI classification uses LiteLLM's own ChatGPT subscription login. It does not copy or parse Codex credentials. Codex task execution continues to use Codex's own ChatGPT login. The classifier's separate ChatGPT subscription login and live Luna/xhigh classification were verified on 2026-09-12. The daemon cannot complete an interactive sign-in.

The isolated worker prohibits LiteLLM's interactive device-login entry point while allowing its native stored-token refresh. Nonlocal selection holds a private advisory lock at `CHATGPT_TOKEN_DIR/.director-selection.lock`, defaulting to `~/.config/litellm/chatgpt/.director-selection.lock`, to serialize refresh and classification. Local-only fallback takes no authentication lock and creates no authentication directory. Provider logs are suppressed at the JSON boundary. No API-key billing fallback is configured.

Claude task execution uses the official CLI's stored subscription authentication. Director unsets `ANTHROPIC_API_KEY`, token and provider-routing overrides in the child environment; it never extracts an OAuth token. Claude runs with permission mode `auto`, JSON output, and the explicitly supplied `publish_reply` MCP configuration. User settings, hooks, slash commands, and Chrome integration are disabled for that process. Claude authentication is resolved as of 2026-09-12. Interactive Claude worked while print-mode (`-p`) calls failed OAuth refresh. Running `claude auth login --claudeai` in the user's terminal fixed the installed Claude Code `2.1.221`; Opus 5/high first-turn and same-session-ID resume proofs then passed. Preserve this finding and do not repeat login or authentication investigations unless an actual failure recurs. An OAuth-refresh failure does not establish that the subscription expired.

Director owns the durable conversation/profile/session binding. Claude gets an explicit session ID for the first turn and `--resume` with that same ID for subsequent turns. Codex changes model first and reasoning effort second on its loaded session, verifies the echoed settings, and then submits the prompt. Neither a selection failure nor an uncertain task result authorizes an automatic cross-harness retry. The existing publication authority and receipt reconciliation determine whether a reply was delivered; a CLI success payload alone does not establish delivery.

## Timeout budget

The selector's default first-process budget is 40 seconds. If that process fails or times out, the parent allows a separate local-only process up to 15 seconds. Timeout kills and reaps the selector process group. Authentication-lock waiting consumes the same first-process budget.

Preparation defaults to ACP startup timeout plus request timeout plus one second: 61 seconds with the example settings. The selector can therefore consume up to 55 seconds, leaving only six seconds of that default budget for other preparation work. Context fetching, scheduling and gateway preparation also need time. These are upper bounds on child execution, not a guarantee that the whole preparation completes within them. When increasing `model_selection.timeout_seconds`, review and adjust `acp.prepare_timeout_seconds` together, allowing adequate margin for the rest of preparation. Expiry of preparation must not be treated as proof that task inference ran.

Claude's `timeout_seconds` is its separate one-shot task deadline, not the classifier deadline. Dispatcher job deadlines and recovery rules still apply.

## Rollback

Set `dispatcher.model_selection.enabled` to `false` to restore fixed Codex selection using `dispatcher.acp.model` and `reasoning_effort`. Keep the verified ACP command and profile identifier. This is a configuration rollback, not permission to reinterpret saved Claude session IDs as Codex IDs: existing Claude bindings become unavailable when their selection-managed profile is disabled. Preserve their state and reconcile or restore the profile deliberately. Do not delete bindings to force a harness switch, or replay uncertain running jobs.

## Verification and pending evidence

Pinned dependencies are LiteLLM `1.100.1`, Python ACP SDK `0.12.1`, `@agentclientprotocol/codex-acp` `1.11.0`, and `@openai/codex` `0.154.0`. The inspected Claude Code CLI was `2.1.221`; its executable is configured externally rather than pinned by `package.json`.

Run focused code checks in the implementation checkout's environment with those dependencies installed:

```sh
python3 -m unittest tests.test_model_selection tests.test_selection_worker tests.test_session_selection
python3 scripts/acceptance.py validate
```

The selector tests include actual disposable-process timeout/reaping, strict response validation, malformed classifier fallback, constructor failure, authentication-lock contention, and local operation without authentication-directory creation. These are code-test proofs, not Slack acceptance.

Run the destination installation checks in machine-setup.md and its explicit local acceptance profile. No historical deployment evidence is included in this public snapshot.
