# Slack Agent Bridge

A configurable Slack bridge for durable agent conversations and assigned work.
The Director service connects private Slack channels to Codex ACP or Claude Code,
keeps a session per conversation, and tracks replies, responsibilities, reminders,
and recovery in local SQLite state.

## Install with Codex

Clone this repository on the computer that will run the service, open the checkout
in Codex, and send:

> Follow docs/machine-setup.md and perform the complete setup on this computer.
> Use your browser and computer tools to configure the Slack app and channels,
> create my local configuration, install dependencies, configure the background
> service, and verify a Slack conversation. Ask me only for choices, login,
> access, or actions you cannot complete with the available tools. Keep all
> credentials out of chat, source control, screenshots, and logs.

The guide includes resumable setup, destination identity checks, authentication,
service installation, and acceptance. The supported installation flow targets
macOS. Codex's setup-time browser/computer access does not automatically grant
those same tools to the unattended agent runtime.

## Credentials and state

Start from [the example configuration](config/director.example.json). The setup
agent writes an ignored `config/director-<name>.local.json` for your Slack
workspace, owner, channels, state directory, service name, and executable paths.
Always select this file explicitly with `--config` or `DIRECTOR_CONFIG`.

Slack credentials come from the service environment or an ignored owner-only
`.env.runtime` file. Provision them through the authorized connection described
in the setup guide. Do not commit or copy another installation's credentials,
conversation database, or agent login/session stores.

The checked-in legacy configurations are disabled synthetic regression fixtures.
They are not installation templates or usable Slack destinations.

## How it works

The Socket Mode receiver saves incoming messages before acknowledging them.
A dispatcher runs an agent only when a conversation or responsibility needs work.
Director owns routing, durable state, delivery receipts, recovery, and Slack card
actions; the selected agent harness owns task execution. Its private App Home
also has a **Needs you** list for delivered results that still need the owner's
review. It is separate from assigned work: an item appears only when the agent
explicitly says the result needs owner action, and a completed end-to-end
request such as an authorized message already sent does not appear there.

- [Machine setup and upgrades](docs/machine-setup.md)
- [Operating agreement](docs/operating-agreement.md)
- [Agent operating instructions](docs/dispatcher-manager.md)
- [Model selection and authentication](docs/model-selection.md)
- [Gateway and session architecture](docs/agent-gateway.md)
- [Acceptance runner](acceptance/RUNNER.md)
- [Public snapshot boundaries](PUBLIC-SNAPSHOT.md)

## Development checks

Install the pinned Python and Node dependencies as described in the setup guide,
then run:

```sh
.venv/bin/python -m unittest discover -s tests
npm run test:codex-acp-guardian
.venv/bin/python scripts/acceptance.py validate
```

Passing local checks does not establish Slack connectivity or a completed
installation. Each destination runs its own isolated acceptance using an explicit
profile and retains its evidence privately.
