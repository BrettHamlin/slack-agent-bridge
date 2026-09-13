# Public source snapshot

This repository starts with a clean source snapshot. It contains no private
repository history, credentials, conversation state, or historical deployment
reports. The legacy Odyssey/Atlas acceptance fixtures use synthetic identities;
they are regression fixtures, not configured live destinations. Their checked-in
configs are disabled. Follow `docs/machine-setup.md` to create a separate local
configuration and an explicit local acceptance profile for your installation.

Local tests validate implementation behavior. A new machine must complete its
own authenticated Slack acceptance before installation is reported as verified.
