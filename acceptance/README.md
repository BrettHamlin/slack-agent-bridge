# Director acceptance suite

These are executable instructions for a computer-control agent, not fixed click scripts. The agent adapts to the UI; the expected results are the contract. Code tests remain the fast checks for races and fault injection.

## Start a run

From the published Director checkout, choose a named [host profile](HOST_PROFILES.md).
The default is the existing Odyssey test profile:

```sh
export DIRECTOR_CONFIG=config/director-tests.json
python3 scripts/acceptance.py validate
python3 scripts/acceptance.py list --suite smoke
python3 scripts/acceptance.py init --profile odyssey --config config/director-tests.json --suite smoke --run-id YYYYMMDD-HHMM-smoke
```

All live acceptance runs target the profile's explicit source, feed and state paths. Odyssey targets private director-tests (C0000000004) in team T0000000009, using `state/testing/inbox.sqlite3` and `state/testing/dispatch`. Atlas is a separate explicitly selected development profile; its local config, app, bot, source, feed, state, and receiver service are validated before a run begins. Do not infer a profile from host name or a running service. Production director-owner and director-conversations are never acceptance destinations.

Init records the validated target and config digest in both manifest and results; report revalidates them and rejects missing targets, production destinations/state, symlink aliases and changed config. Older manifests need a fresh run. This checks configuration metadata; the runner still verifies actual runtime and Slack identity. An isolated run records `fixtures_only` and needs no live config; every selected scenario must be isolated, with temporary fixtures and no live service/UI actions.

Give the resulting run directory and [RUNNER.md](RUNNER.md) to a computer-control-capable agent. Use a cheaper capable model for execution (for example Astra Light if offered by the runtime); do not invent an unavailable model identifier. Keep one agent controlling the desktop at a time. No recurring automation is installed by this suite.

- **smoke:** A01 ordinary reply, A02 input-needed card, A03 Open conversation, A05 Drop, A09 failed-command response, A20 ACP continuity, A26 conversation feed. Run after every deployed behavior change.
- **full:** all catalog scenarios, including controlled maintenance and isolated failure checks. Run before a release involving dispatch, cards, recovery or state transitions, and after a regression fix.
- **isolated:** A13/A15/A16/A18/A19/A22/A28. Can run without Slack/computer control. Passing this subset does not establish UI health.

A04 runs before A05 in full runs so Drop cannot destroy its fixture. Dependencies must PASS before execution; failed/blocked dependencies block dependent scenarios. During A04's active overnight wait for the shortest real Later option (currently 1 day), A04 and A05 may remain PENDING with cleanup ownership retained as specified in RUNNER.md. A06/A07/A17 own separate fixtures. Do not report the run complete while deferred work is unobserved.

## Coverage

| Area | Scenarios |
| --- | --- |
| Replies, receipts, conversation/task distinction | A01, A02, A09, A24 |
| Cards, navigation, Later, Drop, quiet waiting | A02–A05, A17 |
| Continuation, completion, cross-thread context | A06, A07 |
| Missing-card repair | A08 |
| Restart, missed intake, concurrent threads | A10–A12 |
| Changed/deleted sources and stale controls | A13, A16 |
| Reminder delivery | A14 |
| Worker cleanup, credentials, uncertain sends | A15, A18, A19, A22 |
| ACP context, prepared prompts and single-operation publication | A20–A25 |
| One-use native Guardian approval while automatic review remains enabled | A29, A30 |
| Session feed, stable identity, replacement, removal, recovery | A26–A28, A30 |
| Complexity selection, harness affinity and Claude one-shot resume | A31 |
| Portable standalone installation and controlled ownership transfer | A32 |

## Results

The run directory is ignored under `state/acceptance/<run-id>/`, mode 0700. It contains a pinned manifest, copies of scenario instructions, and `results.json`. Save screenshots/logs there with owner-only permissions; never commit private Slack screenshots, account details, credentials or conversation bodies.

Edit each finished result with PASS, FAIL or BLOCKED, observed facts, start/end timestamps, cleanup state, and evidence paths. Active waits remain PENDING with fixture ownership and resume time recorded; they are incomplete and not CLEAN. The helper validates structure and evidence presence, not screenshot truth or product behavior. The executing agent must inspect the evidence.

```sh
python3 scripts/acceptance.py report state/acceptance/<run-id> --profile odyssey --config config/director-tests.json
```

Exit 0 means all selected scenarios are PASS with required evidence and cleanup recorded. Exit 1 means failed, blocked or pending work; exit 2 means malformed/incomplete evidence. A subset pass is always named as a subset. Never call an unexecuted suite green.

## Maintenance

See [MAINTENANCE.md](MAINTENANCE.md). Every behavior-changing PR names affected scenario IDs, updates expectations or explains why they are unchanged, and states evidence or a pending live run. CI enforces the impact fields and validates the catalog, dependencies and Markdown structure. It does not pretend to run Slack UI in GitHub.

Historical regression anchors: A02/A08 cover missing attention controls; A09/A15/A19 cover the silent-worker and cleanup failures addressed in PR #18. Keep regression scenarios even after the fix.
