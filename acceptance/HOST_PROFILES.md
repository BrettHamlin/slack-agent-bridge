# Acceptance host profiles

Live acceptance selects a named profile explicitly. `odyssey`, `atlas`, and
`local` are case-insensitive exact aliases. Host discovery, an ambient
receiver, a Slack window, and `DIRECTOR_CONFIG` never select a live target.
`scripts/acceptance.py init` validates the selected profile's config, Slack
identity, state and service target and records that result in the manifest.
`report` checks it again.

## Odyssey

Odyssey remains the default and the only profile used by the existing personal
receiver's shared test surface.

```sh
export DIRECTOR_CONFIG=config/director-tests.json
python3 scripts/acceptance.py init --profile odyssey \
  --config config/director-tests.json --suite smoke --run-id RUN_ID
```

The validator requires the existing personal-app test identity: private source
`C0000000004` (`director-tests`), feed `C0000000007`, test state
`state/testing/inbox.sqlite3`, and service
`com.example.director.receiver`. It rejects the personal source and feed.
The shared receiver restriction remains in force: do not start a second Socket
Mode connection for this app.

## Atlas

Atlas is a separate Director Dev Slack app, not an additional channel on the
personal receiver. On Atlas, select the profile explicitly:

```sh
export DIRECTOR_CONFIG=config/director-atlas.local.json
python3 scripts/acceptance.py init --profile atlas \
  --config config/director-atlas.local.json --suite smoke --run-id RUN_ID
```

The host-local config is intentionally not tracked. The profile only accepts
that exact path and requires all of the following metadata before it creates
an acceptance run:

| Field | Required Atlas value |
| --- | --- |
| Environment | `development` |
| Slack app / bot | `A0000000002` / `U0000000012` |
| Team / owner | `T0000000009` / `U0000000010` |
| Source / source name | `C0000000008` / `director-dev` |
| Feed / feed name | `C0000000006` / `director-dev-conversations` |
| Inbox / dispatch state | `state/development/inbox.sqlite3` / `state/development/dispatch` |
| Service | `com.example.director.atlas-dev` |

It rejects an Atlas config marked `test`, any Odyssey app, bot, source, feed,
or state path, symlink aliases, a disabled feed, and additional channel
configs. Atlas must run as its standalone `development` primary configuration;
the personal receiver's rule that an *additional* configuration must be a
`test` environment is unchanged.

For either profile, use the chosen `--profile` on `report` as well. Do not use
an Atlas config to inspect or control Odyssey, or an Odyssey test config to
inspect or control Atlas. The manifest target is the sole source for source,
feed, state, and receiver-service selection.

## Local standalone installation

Use `local` only for a newly provisioned standalone development receiver. It
must receive an explicit ignored `config/director-<host>.local.json` path; it
cannot be selected from `DIRECTOR_CONFIG`. The config must declare a distinct
app, bot, source channel, feed channel, and unique `receiver_service`, use a
private source channel ending in `-tests`, and use exactly
`state/local-acceptance/inbox.sqlite3`. It rejects Odyssey and Atlas app, bot,
source, feed, service, path, and symlink aliases.

```sh
python3 scripts/acceptance.py init --profile local \
  --config config/director-work.local.json --suite smoke --run-id RUN_ID
```

The local profile establishes a controlled private test surface. It does not
authorize a production channel, an existing shared receiver, or a second
Socket Mode connection. Use the target recorded in its manifest for every
runner command and report with `--profile local`.

## Scenario language and maintenance

The catalog's scenario prose was written against Odyssey. For an Atlas run,
the validated manifest target replaces only its route labels: source, feed,
config, inbox and dispatcher-state references mean the Atlas target recorded
in that manifest. It does not change expected product behavior, authorization,
or cleanup. Do not manually substitute channel IDs.

### Atlas controlled restart and outage checks

Atlas has a standalone, manual A10/A11 procedure. It is permitted only in an
authorized maintenance window after the Atlas manifest has been validated and
its own `state/development/dispatch/jobs.sqlite3` has no jobs in `running`,
`preparing`, `pending`, `retry`, or `blocked`. Atlas responsibilities waiting
for input are not active dispatch jobs, but do not use this procedure to settle
or alter them.

Use only `gui/$(id -u)/com.example.director.atlas-dev` and the existing
`$HOME/Library/LaunchAgents/com.example.director.atlas-dev.plist`. First
use `launchctl print` to prove the exact service is present. For A10, boot out
that service, wait at most 15 seconds until `launchctl print` proves it absent,
then bootstrap that same plist. For A11, send the synthetic Atlas UI message
only after the same absence proof and before restoration. In a finally-style
step, restore the same plist whenever bootstrap has not been confirmed.

If a bootstrap fails, retain the first safe command result, prove absence again,
and make at most one more bootstrap of that exact plist. Never retry while the
service is present or unknown. Do not save `launchctl print` output, which may
contain process metadata; record only the presence/absence classification,
return code, safe error text, timestamps, and exact service label under the
private run directory. Wait up to 120 seconds for a fresh connected loop from
the Atlas config, then verify recovery through that config. Do not run
`scripts/restart_receiver.py`, start a second receiver, use the Odyssey label,
or inspect/control any Odyssey state during this procedure.
