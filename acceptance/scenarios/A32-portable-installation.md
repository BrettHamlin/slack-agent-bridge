# A32: A new standalone installation remains isolated and resumable

## Setup

Use a published checkout on a new host with an explicit `local` profile and ignored `config/director-<host>.local.json`. The config must select a new private Slack app, owner, `-tests` source channel, distinct private feed channel, `state/local-acceptance/`, and unique LaunchAgent label. Do not use Odyssey or Atlas identities, state, services, credentials, or sessions.

## Actions

Have Codex follow `docs/machine-setup.md`: inspect for an existing receiver, install pinned dependencies, create the Slack app from the neutral manifest, collect only visible IDs into the local config, complete required user authentication outside chat, run `check-config`, render the local service, and verify the configured runtime identity before starting it. Interrupt after rendering, then repeat rendering before the one bounded launch attempt.

## Expected

The setup rejects an unsupported Python interpreter before dependency installation and preserves a working existing venv; it only rebuilds an incomplete setup-owned venv with a verified Python 3.10–3.14 executable. It then either reaches one connected local receiver with the selected config and no credential output, then verifies one ordinary reply, same-thread resume, and feed navigation, or reports the exact authentication, organization-policy, identity, or existing-receiver blocker. Rendering is idempotent and refuses a conflicting plist. Moving from tests to normal use first transfers ownership by stopping and proving absence of the test receiver, then starts/verifies the separately configured receiver; keeping tests live requires a separate app, credentials, checkout, and state. The agent never starts a second receiver, copies a personal config/session, selects a host by ambient state, or treats an unavailable browser/computer capability as completed work.

## Evidence

Record the public checkout SHA, local profile target from the run manifest, redacted config path, rendered service label, `check-config` result, identity-verification result, connected-health observation, reply/resume/feed evidence, and the explicit test-to-normal ownership transition if performed. Keep Slack IDs, credentials, browser captures, and all runtime artifacts private under the acceptance run.

## Cleanup

If the controlled receiver was started, stop only its recorded local service after confirming no synthetic work is active, unless it was deliberately transferred to the separately verified normal-use receiver. Preserve its local config, credentials, state, and app for a later resume unless the owner explicitly requests removal. Do not alter Odyssey, Atlas, or unrelated services.

## Failure handling

On identity mismatch, missing access, app-install approval, MFA, unavailable computer control, or conflicting existing service, stop before receiver launch and mark BLOCKED. Preserve the rendered artifact and redacted evidence; do not bypass Slack policy, inspect token values, replace another service, or retry against a different account.
