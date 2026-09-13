# Slack Agent Bridge: engineering and installation

For an installation request, follow `docs/machine-setup.md` and
`docs/model-selection.md`. Use available Codex browser/computer tools to complete
setup, asking for human action only when authentication, access, or an unresolved
choice requires it. Verify the destination Slack workspace and account before
acting. Do not expose credential values in messages, tool output, screenshots,
logs, command arguments, or Git. Use authorized provisioning and owner-only local
credential files. Preserve existing services, local configuration, and sessions;
never start a competing receiver for the same app.

For this public snapshot, use an explicitly selected `local` acceptance profile
with its exact host-local config. Odyssey/Atlas are synthetic regression fixtures,
not live destinations. Read `acceptance/HOST_PROFILES.md` and
`acceptance/RUNNER.md` before testing. Keep evidence under ignored
`state/acceptance/`; clean up only the synthetic work created by the run.

Before behavior changes, read `acceptance/MAINTENANCE.md`, update the affected
scenarios, and run `python3 scripts/acceptance.py validate` plus relevant tests.
Publish engineering changes through a branch and pull request, never by pushing
the default branch directly. Record code checks separately from live acceptance;
unexecuted or unavailable checks remain pending or blocked, never passed.
