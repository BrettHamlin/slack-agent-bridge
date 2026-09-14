# Set up Director on a new machine

Read this before installing or enabling Director on any new host. A Git pull brings the implementation and dependency pins; it does **not** bring installed packages, host configuration, subscriptions, Slack credentials, or conversation state. Do not copy another host's local setup onto this one.

## What is already in this repository

- `director/model_selection.py`, `director/selection_worker.py`, and the gateway/drivers contain the selector and execution integration. There is no separate custom library to install or running LiteLLM proxy to deploy.
- `requirements.txt` pins LiteLLM, the Python ACP SDK, MCP, and Slack dependencies. `package.json` and `package-lock.json` pin the Codex ACP adapter and Codex runtime. `npm ci` also applies the checked-in Guardian adapter patch through `postinstall`; do not skip install scripts.
- The execution grade table and classifier policy are implemented here. Mipmap's pinned runbook is the provenance of that table, not a runtime dependency or another required checkout. See [model selection](model-selection.md) for the exact models, efforts and fallback policy.
- The checked-in legacy configs are disabled synthetic regression fixtures. Never use them for installation.
- Selection is opt-in in the effective config. Pulling the repo alone does not enable it on an existing installation. Atlas's `config/director-atlas.local.json` is host-local and is not supplied by Git.
- `config/director.example.json` is the secret-free starting point for a new standalone receiver. It intentionally contains no real workspace IDs, session IDs, channel routes, additional configurations, or credential values. `scripts/render_launchd_service.py` renders a host-specific macOS service from that selected local config; it does not read credentials or load the service.

## Codex-led installation

Run the setup from a Codex task on the destination computer. The agent may inspect the checkout, run local checks, use the available browser and computer controls to navigate Slack, create local files, and resume an interrupted setup. It must ask for action only when the current user must complete authentication, organization approval, or choose a workspace/owner/channel. It must never request, print, put in chat, or inspect Slack token values.

Before changing anything, the agent must identify whether a Director receiver already exists. Preserve an existing app, selected config, state directory, and service owner. A new standalone installation needs its own Slack app, private source channel, private conversation-feed channel, owner, local state, auth stores, and LaunchAgent label. Do not run two Socket Mode receivers for the same app.

For a new host, copy the template to an ignored file such as `config/director-work.local.json`. Fill only IDs and paths observed in the destination UI or filesystem. Keep `environment: development`, `database_path: state/local-acceptance/inbox.sqlite3`, a source channel name ending in `-tests`, and a unique `receiver_service` until controlled acceptance is complete. Do not copy `codex_thread_id`, `additional_configs`, credentials, session state, or paths from another machine.

## 1. Install the checkout's dependencies

Use the operating-system account that will run the service. The supported Python range for the pinned ACP SDK is 3.10–3.14; the verified Atlas setup used Python 3.12.12 and Node 22. Use native packages for the destination OS/architecture. The current selector uses POSIX process groups and `fcntl`; Windows is not a verified target.

Before creating a venv, discover the interpreter version. Do not assume macOS
`python3` is supported: it may still be Python 3.9. Select a Python 3.10–3.14
executable, such as `/opt/homebrew/bin/python3.14`, and verify it explicitly:

```sh
/opt/homebrew/bin/python3.14 --version
```

If `.venv` already works with a supported interpreter, preserve it. If an
interrupted setup created an incomplete `.venv` with an unsupported
interpreter, remove only that setup-owned environment and recreate it with the
selected executable. Never replace another host's active environment.

From the destination checkout root:

```sh
PYTHON=/opt/homebrew/bin/python3.14
"$PYTHON" -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
npm ci --no-audit --no-fund
.venv/bin/python -m pip check
.venv/bin/python -m unittest discover -s tests -v
npm run test:codex-acp-guardian
.venv/bin/python scripts/acceptance.py validate
```

Use `.venv/bin/python` for Director and its service, so the selector subprocess uses the same installed dependencies. Do not copy `.venv`, `node_modules`, or the legacy macOS ARM binary from another machine. Claude Code is installed separately from Anthropic's official distribution; configure the resolved executable path. Claude Code 2.1.221 was verified with `-p`, `--resume`, and permission mode `auto`. Check those flags when using a different version; do not silently replace auto permissions with a bypass mode.

## 2. Select and configure the destination

For an existing host, identify its actual service and explicit config first, and preserve its Slack identity, source/feed channels, state directories, profile IDs, and stored sessions. **Archduke/Odyssey retains its shared receiver and test-channel rules.** Do not install Atlas's standalone app/service or start a second Socket Mode receiver for the same app.

For a new independent installation, provide its own intended Slack app/bot/owner/workspace/private channels and state paths in a host-local config such as `config/director-newhost.local.json`. `config/*.local.json` is ignored by Git. Keep Slack token values out of JSON. Review the full existing config schema before using a copy: its real channel IDs and service paths must not accidentally remain selected.

Merge the `dispatcher` fields from [model-selection.md](model-selection.md) into that config. Replace every example executable path with a verified destination path. In particular:

- Set `dispatcher.runtime` to `acp` and explicitly enable `dispatcher.model_selection.enabled` to opt in. Keep `default_backend: codex` unless the owner selects another default.
- Use an absolute Node executable and this checkout's `node_modules/@agentclientprotocol/codex-acp/dist/index.js` as the two `dispatcher.acp.command` arguments. Both paths must resolve on this machine; launchd does not inherit an interactive shell's Node setup.
- Set an adequate `dispatcher.acp.prepare_timeout_seconds`; the verified Atlas value was 90 seconds with a 40-second classifier deadline and a possible additional 15-second local fallback. See the timeout budget in the model-selection guide.
- Configure `dispatcher.claude.command` with the destination's absolute Claude executable if Claude execution is wanted. Keep distinct stable `codex-default` and `claude-default` profile IDs. Bound threads retain their backend even when the default changes.
- Retain the host's intended fixed Codex model and effort for rollback. `acp.model` takes precedence over `dispatcher.model`; do not accidentally replace an existing rollback policy with the documentation example.

Use only the destination paths discovered during this installation.

Selection policy is currently code-defined, not a per-host arbitrary model table. Verify subscription access to the listed models. Unsupported models must fail explicitly; do not substitute models, API billing, or another harness automatically.

## 3. Authenticate on the destination account

These are separate login stores, even when they use the same ChatGPT subscription. Complete login interactively before enabling the daemon. Do not move token files between machines or print tokens in diagnostics.

**Codex execution:** from this checkout, use the pinned CLI:

```sh
node_modules/.bin/codex login
```

**LiteLLM classifier:** the daemon deliberately disables interactive device login. For the pinned LiteLLM version, run this one-time native authenticator bootstrap in the user's terminal, complete the displayed device-code flow, and wait for success. The return value is discarded, never printed:

```sh
.venv/bin/python - <<'PY'
import os
os.umask(0o077)
os.environ['LITELLM_LOCAL_MODEL_COST_MAP'] = 'True'
from litellm.llms.chatgpt.authenticator import Authenticator
Authenticator().get_access_token()
print('Classifier subscription login ready')
PY
```

This invokes LiteLLM's own login/refresh flow; it does not parse Codex credentials. Its default directory is `~/.config/litellm/chatgpt`. If deliberately overriding `CHATGPT_TOKEN_DIR`, use the same private directory for setup and service. The daemon must run under the same account/HOME as the login. Serialize setup with maintenance: do not race manual login against an active classifier refreshing the same store.

**Claude execution:** resolve `command -v claude`, record `claude --version`, then, if not already authenticated:

```sh
env -u ANTHROPIC_API_KEY claude auth login --claudeai
```

Check the actual one-shot path using the configured executable:

```sh
.venv/bin/python - <<'PYTHON'
import os
import subprocess
from director.claude_one_shot import subscription_environment
subprocess.run(
    ['/absolute/path/to/claude', '-p', '--model', 'claude-opus-5',
     '--effort', 'high', '--permission-mode', 'auto', 'Reply only OK'],
    env=subscription_environment(os.environ), check=True, timeout=120,
)
PYTHON
```

An interactive UI or `claude --version` alone does not verify print-mode authentication. On Atlas, `claude auth login --claudeai` fixed a print-mode OAuth refresh failure; preserve a working login and only repeat this repair if that failure recurs. Director strips API-key/token/provider overrides in its Claude child and uses the official CLI's stored subscription login. Do not extract a Claude token for LiteLLM.

**Slack:** use the destination app's approved provisioning source. In the Slack app UI, create the app from the neutral `config/slack-app-manifest.json`, enable Socket Mode, create an app-level token with `connections:write`, install the app, and invite its bot plus the configured owner to both private channels. Slack documents manifest-based app creation, Socket Mode, and the required app-level scope at [app manifests](https://docs.slack.dev/app-manifests/configuring-apps-with-app-manifests/) and [Socket Mode](https://docs.slack.dev/apis/events-api/using-socket-mode/). The agent can guide those pages, but Slack sign-in, MFA, app-install approval, and access denied by workspace policy are user or administrator actions.

For a browser-capable Codex setup, use this order and record only opaque IDs in the ignored local config:

1. Open Slack's app-creation page, select the exact requested workspace, and import the tracked neutral manifest. Do not reuse an existing personal app.
2. Confirm the manifest's bot scopes, event subscription, interactivity, and Socket Mode settings. Create the app-level token with `connections:write`, install the app, and obtain the bot identity from Slack's installed-app pages.
3. In the selected workspace, create two private channels: a `-tests` source channel and a distinct private conversation-feed channel. Invite only the configured owner and bot. Capture the workspace, app, bot, owner, source, and feed IDs from the UI/API metadata into the ignored config; never place token values there.
4. If the Codex session has an approved secret connection, use it to materialize the two tokens into the local owner-only source for `provision-runtime-env`. Otherwise pause for the user to complete that local secret transfer. Do not ask the user to paste secrets into chat, terminal output, or an agent prompt.

Director accepts its two Slack credentials from the process environment or ignored mode-0600 `.env.runtime`. Use an approved secret connection to materialize an owner-only `.env` locally, then run `.venv/bin/python -m director --config config/director-work.local.json provision-runtime-env`; that command verifies the configured Slack identity while transferring only the two runtime variables without displaying values. For an existing installation, preserve its working provisioning; do not overwrite it. See [the credentials section of the README](../README.md#credentials-and-state).

### Enable Needs you

Needs you appears in the Slack app's **Home** tab; it is not a channel and requires no third channel. Keep the private source and conversation-feed channels configured above. The tracked manifest enables Home and subscribes to `app_home_opened`; interactivity and Socket Mode must remain enabled.

Enable the feature in the selected host-local config. For checkbox completion and Open conversation only, use:

```json
"needs_you": {"enabled": true}
```

The example config includes an illustrative `action_url`. Remove it until the HTTPS and Sign in with Slack setup below is complete, or replace it with the verified destination origin. Missing or disabled `needs_you` leaves the feature off. Review state is created automatically in the existing configured SQLite database; no separate database or manual migration command is needed. Preserve that database across restarts. Existing conversation history is not backfilled: new confirmed replies create review items only when the agent explicitly reports an owner action. Checking an item moves it to Done; unchecking it returns it to Needs you.

For inline Needs you **Snooze** and **Bring back** links, set `needs_you.action_url` to a public HTTPS origin and retain the loopback `http_bind_host`/`http_port`. The existing receiver owns the action-page thread; a user-managed HTTPS reverse proxy terminates TLS and forwards only that loopback port. After the normal bot install, enable Sign in with Slack for the same app and add `https://YOUR-ORIGIN/needs-you/oauth/callback` as its redirect URL. Do not add a user `openid` scope to the bot-install manifest: Sign in with Slack requests `openid` through its separate authorize flow. Materialize `SLACK_OPENID_CLIENT_ID` and `SLACK_OPENID_CLIENT_SECRET` beside the existing runtime values through the authorized connection, mode 0600. The normal provision command deliberately transfers only the bot and Socket Mode pair, so it does not overwrite an existing runtime file to add these values. Without the proxy, redirect URL, or OpenID pair, omit `action_url`; Needs you still supports its checkbox and Open conversation link.

## 4. Verify before starting or updating the service

Choose the exact config path rather than relying on defaults. Substitute a real absolute destination path here:

```sh
.venv/bin/python -m director --config /absolute/checkout/config/director-HOST.local.json check-config
```

Verify real classifier inference separately: otherwise heuristic fallback can make a missing classifier login look operational. This uses a harmless synthetic prompt and emits selection metadata only:

```sh
.venv/bin/python - <<'PYTHON'
from dataclasses import asdict
from director.model_selection import select_in_process
selection = select_in_process(
    'Sort these fictional labels alphabetically: Cedar, Aspen, Birch.',
    backend='codex', timeout=40, fallback_timeout=15,
)
print(asdict(selection))
if selection.cause != 'llm_classifier':
    raise SystemExit('Classifier subscription inference was not verified')
PYTHON
```

Require `cause: llm_classifier` for the live classifier proof. `local_heuristic` is a supported fallback, but is not evidence that subscription classification works. This command can invoke the subscription model; it does not execute a Slack task. Validate first-turn/resume and actual publication separately through acceptance.

For an existing receiver, use its documented idle/maintenance/restart procedure, not an extra `listen` process. For a new host, first render and inspect its exact service definition:

```sh
.venv/bin/python scripts/render_launchd_service.py --config config/director-work.local.json --dry-run
.venv/bin/python scripts/render_launchd_service.py --config config/director-work.local.json
```

The renderer creates the selected label's plist only once and creates private log directories before launchd starts. If an existing plist differs, it stops rather than overwriting it. After `check-config` and credential identity verification pass, the agent may load only that rendered label, verify its connected health through the exact config, and keep the label/config/state path in its setup record. Never load the checked-in Odyssey plist unchanged on another host.

For the one bounded start, first prove the selected label is absent, then load that exact rendered plist and inspect the configured receiver:

```sh
launchctl print gui/$(id -u)/com.example.director.work >/dev/null
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.example.director.work.plist"
.venv/bin/python -m director --config config/director-work.local.json status
```

Replace the example label with `receiver_service` from the local config. If `launchctl print` reports the same recorded label/config and health is fresh, reuse that receiver. If it reports a conflicting plist, identity mismatch, or stale/absent connected health, stop and preserve the blocker; do not boot out another service or start a second receiver.

For later maintenance, first use the exact config to verify the receiver is idle and its health is current. Idle means a read-only check of its exact `state/.../dispatch/jobs.sqlite3` finds zero jobs in `running`, `preparing`, `pending`, `retry`, or `blocked`; `status` alone is not enough. A healthy service with the same recorded label/config is the receiver to reuse; do not start another one. For a planned restart, capture only bounded `launchctl` status evidence, boot out that exact label, wait until `launchctl print` reports it absent, then bootstrap the same rendered plist once and require fresh connected health. If bootstrap fails, preserve the first error and make at most one retry after proving absence again; in all failure paths, restore only the same recorded service after absence is proved. Never use this procedure for a label, plist, or config not recorded for the host.

After the selected service is running, use its explicit config with `status` and verify fresh connected health, then run smoke and affected acceptance. Read [HOST_PROFILES.md](../acceptance/HOST_PROFILES.md) and [RUNNER.md](../acceptance/RUNNER.md). A new standalone host selects the explicit `local` profile and supplies its exact local config; it never borrows Odyssey or Atlas identity. Record the published SHA and actual results.

Before calling the installation complete, run one controlled ordinary source reply, one follow-up in the same thread to prove session resume, and one feed navigation check through the configured private channels. Record the observed delivery/state evidence and clean up only the synthetic responsibility or reminder created for the run. To transfer the same app to normal-use source/feed channels, first verify the test receiver is idle, stop its exact recorded service, and prove it absent; then create a separate normal-use config/state/service label and start/verify only that receiver. Do not copy test state or session bindings. If the test receiver must stay live, use a separate app, credentials, checkout, and state instead.

## Upgrades, restart continuity, and rollback

On each host, follow its maintenance procedure, pull the reviewed release, run `pip install -r requirements.txt` in its own venv and `npm ci`, rerun relevant checks, and restart its existing service deliberately. A pull does not install changed dependencies or replace already-running Python/Node processes. Do not overwrite host-local config or working login stores during an upgrade.

### Upgrading an existing installation for Needs you

1. Identify the existing receiver's checkout, service, config and database. Follow the host's maintenance procedure and take a consistent backup of its SQLite state before upgrading. Preserve the local config, credential source and session stores.
2. Pull the reviewed release containing Needs you into that checkout. Install its dependencies with `.venv/bin/python -m pip install -r requirements.txt` and `npm ci --no-audit --no-fund`; this release adds the pinned JWT verification dependency. Run `.venv/bin/python -m pip check`, the relevant tests, and `.venv/bin/python scripts/acceptance.py validate`.
3. In the **existing** Slack app, enable the Home tab and add the `app_home_opened` bot event, matching `config/slack-app-manifest.json`. Preserve its app identity, existing event subscriptions, scopes, interactivity and Socket Mode. Complete reinstall/approval if Slack prompts for it. Do not create a replacement app or new channels for this upgrade.
4. Merge the `needs_you` settings above into the existing local config. To include inline Snooze and Bring back, complete the HTTPS proxy, redirect URL and OpenID runtime-credential setup before setting `action_url`. The receiver serves the action pages itself; the HTTPS proxy is deployment infrastructure and must also be available. A Git pull does not provision it.
5. Restart the existing service using the host's established maintenance command. Do not launch a second receiver. Open the app's Home as the configured owner and verify it loads. Run the affected controlled acceptance scenarios, including A33, under [RUNNER.md](../acceptance/RUNNER.md): create a synthetic review item, check/uncheck it, exercise Snooze/Bring back when configured, and verify restart continuity. Record the deployed SHA and distinguish native layout preview from live action acceptance.

To disable Needs you, set `needs_you.enabled` to `false` and restart the existing receiver. Retain its SQLite tables so review history is available when re-enabled; do not delete the database to disable the feature.

Same-host continuity depends on retaining Director's configured SQLite/dispatch state **and** the native Codex/Claude session stores under the service account. A fresh clone has no prior conversations. Moving ownership of an existing receiver to another machine needs a separate coordinated state/native-session migration with one active receiver; do not copy credentials or reset bindings to make old threads work.

Rollback disables `dispatcher.model_selection.enabled` while retaining the verified fixed Codex settings/profile. It cannot reinterpret Claude session IDs as Codex IDs. Preserve unavailable bindings and uncertain publication fences. See [rollback](model-selection.md#rollback) for details.
