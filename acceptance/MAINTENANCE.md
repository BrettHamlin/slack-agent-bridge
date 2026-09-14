# Keeping the suite current

The author changing Director behavior owns updating the acceptance contract. The reviewer checks coverage; the tester executes the selected scenarios. A cheaper capable tester is appropriate; reserve implementation/review decisions for the fixing agent.

Preserve isolation in every scenario: select an explicit [host profile](HOST_PROFILES.md) before any live UI, diagnostic, fixture, or cleanup action. Odyssey remains private director-tests C0000000004 / team T0000000009 with config/director-tests.json, feed director-feed-tests C0000000007, and state/testing/. Atlas is a separate development profile with its own app, bot, source, feed, local config, and state/development/. Do not infer a target from hostname or a receiver. Review init/report safeguards when configuration or state resolution changes, including rejection of a wrong profile, production paths, cross-host destinations, and symlink aliases. Isolated fixture coverage never substitutes for a live expectation. Preserve all existing expectations, including A04's actual Later timer; stable screenshots and completed Drop confirmation are required under RUNNER.md.

## Required PR fields

Use .github/pull_request_template.md:

- Acceptance scenarios: explicit catalog IDs (for example A02, A06, A08).
- Acceptance changes: updated or unchanged.
- Acceptance rationale: what changed, or why existing expectations cover it.
- Acceptance evidence: executed run IDs/results and runtime SHA, or clearly "pending live run after deployment".

CI checks these fields for director/, tests/, scripts/, acceptance/, AGENTS.md and docs/ changes. "updated" requires a scenario/catalog diff. No-impact changes still name relevant scenarios and explain why expectations are unchanged. Evidence pending is permitted to publish a testable candidate; it does not meet release acceptance. Attach the actual run result before declaring deployment verified.

## Change routing

| Changed behavior/files | Minimum scenarios to assess |
| --- | --- |
| Inbox/transport/source receipts | A01, A11, A12, A13, A24 |
| Dispatcher/worker/runtime, including the guarded restart exception for a spent Guardian retry | A01, A09, A10, A15, A17, A18, A19, A20–A23, A29, A30 |
| Agent gateway/session bindings/ACP profiles | A12, A13, A18, A19, A20–A25 |
| One-use native Guardian approval of a denied source reply, including settlement of a spent unpublished retry | A20, A23, A25, A29, A30 |
| Responsibilities/publication gates | A02–A07, A16, A17 |
| Slack card layout/actions/repair | A02–A08, A16 |
| Conversation feed identity, publication, removal and recovery | A26–A28, A30, A01, A03, A09, A20 |
| Needs you App Home, owner-review state or action links | A33, A01, A03, A16, A25 |
| Reminders | A14, A19 |
| Manager prompt/operating agreement | A01, A02, A06, A07, A09, A14 |
| Outbox/service queue/agent publication tools | A09–A11, A13, A16, A18, A19, A25 |
| Portable installation, host-local service rendering, or local acceptance profile | A32 |

These are minimum review guidance, not a claim that path matching proves semantic coverage. New user-visible behavior needs a new stable ID or an explicit extension of an existing scenario.

| Model selection / Claude one-shot execution / dynamic session settings | A20–A25, A31 |

## New features and regressions

1. Specify the user-visible outcome, setup, actions, expected positive and negative behavior, evidence and cleanup before implementing.
2. Add a stable Axx ID/file and catalog entry; choose computer, controlled or isolated mode and prerequisites. Update the coverage map and change routing.
3. For a bug, preserve a reproducer in the scenario and note its issue/PR. Add a code test for timing/race logic where useful.
4. Never delete a regression scenario solely because the bug is fixed. Renames preserve IDs; removals require an explicit behavior-retirement explanation and dependency updates.
5. Run catalog validation and code tests. Execute smoke on the published candidate, full/affected controlled tests for state/recovery changes. Preserve failed attempts separately.
6. Review screenshots and state evidence before marking acceptance complete. Record untested/blocked cases explicitly, including unavailable control or an unobserved Later timer.
7. Update docs and scenarios together when intentional behavior changes (for example card eligibility). Do not make tests match a regression by redefining expectations after it fails.
