# A23: ACP retains model, tools and approval behavior

## Setup

Apply RUNNER.md preflight. All UI and diagnostics use private director-tests and config/director-tests.json; all state and fixtures use state/testing/. Record the published suite/runtime SHA, selected profile, adapter/Codex versions and confirmed agent session IDs. Never inspect credentials or real-channel content.

Select the configured Codex profile with automatic approval review enabled. Record each turn’s actual model and reasoning effort and verify that they match the fixed configuration or enabled selection policy. Compare against a recorded synthetic baseline from the existing exec runtime. Use only disposable synthetic content and permitted test surfaces.

## Actions

Through a test Slack conversation request a harmless local file operation under state/testing/ and verify its actual result. Request a bounded browser/computer action on a temporary public example.com tab, including reading its visible heading and closing the owned tab, and observe the action. Verify configured connected-tool availability using metadata only. Exercise permission rejection with a synthetic peer in A22 and, if a real permission/input request occurs, verify it is surfaced with correct correlation and respected. Do not weaken controls or approve a rejected request through another route.

## Expected

The configured model/reasoning/approval mode is applied and observed. Required working baseline tools remain usable, including actual computer/browser interaction where the baseline supports it. Missing capabilities are reported explicitly; an inventory response is not action proof. Approval denials and input waits remain scoped to the correct conversation; idle waiting triggers no new agent turns. Slack replies and source completion still use Director publication gates. No new credential access or globally weakened permissions is introduced.

## Evidence

Record selected settings, synthetic file result, baseline/candidate capability comparison, computer/browser action screenshots, owned-tab lifecycle, correlated permission/input outcomes and final Slack delivery. Unavailable baseline or candidate capabilities remain explicitly BLOCKED until parity is demonstrated.

## Cleanup

Remove only the synthetic file, close only the owned temporary tab, settle the test conversation and cancel any synthetic work/reminders. Preserve private evidence without credential values.

## Failure handling

Use RUNNER.md: retain the failed evidence, fix through a reviewed PR and start a fresh run on the repaired published SHA. Missing control, capability, receipt or cleanup evidence is BLOCKED/PENDING, never PASS.
