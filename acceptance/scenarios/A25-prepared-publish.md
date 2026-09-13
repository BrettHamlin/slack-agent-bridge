# A25: Prepared source and one reply operation eliminate setup round trips

## Setup

Apply RUNNER.md preflight. Use only private director-tests and config/director-tests.json. Record published SHA, ACP session and actual offered Director MCP tools. Keep the configured runtime policy and approval settings unchanged. Record each turn’s actual model and reasoning effort; fixed-mode turns must match the configured values, and selection-enabled turns must match the configured selection policy. Use A01 for the ordinary reply, A09 for task-specific tools, and A20 for same-thread context.

## Actions

Inspect metadata for the prepared source/context and actual tool trace for each synthetic source. Observe the corresponding delivered Slack answers. Run official-SDK MCP stdio handshake/tool invocation tests plus isolated prepared-source and publication safety tests. Verify the actual adapter can invoke the fixed channel-scoped tool on an immediate new session and an immediate same-session load with a changed explicit turn authority. Permit narrow tool metadata discovery, without adding a readiness sleep. Include a synthetic working directory containing an unreadable or FIFO .env file: the tool must initialize and publish through the local receiver queue without opening dotenv files or credential mounts. Verify the deployed checkout launcher as well as an isolated test checkout.

## Expected

Before inference the agent receives the verified exact source revision, relevant bounded same-thread context and compact runtime guidance. Message text remains untrusted input. Ordinary reply handling does not fetch its source, reread runtime docs, search memory, run CLI help, or create an answer file. The runtime offers a real publish_reply tool, invoked once with answer text, which owns original-thread delivery and source completion. Relevant task-specific tools remain available. The same thread retains its existing ACP session and cannot see another channel's context.

Publication preserves original source/job ownership, revision checks and stable outgoing keys. Stale/deleted sources, mismatched ownership, and revoked responsibility fences fail closed. A confirmed prior delivery is reused; uncertain delivery is reconciled without blind replay and never falsely completes the source. Only verified sent delivery permits source completion. Responsibility results retain their execution fence; a plain reply cannot bypass it. Expected input-needed task cards remain governed by A02/A03/A05.

## Evidence

Save screenshots of actual replies, metadata-only pre-inference preparation evidence, offered tool names/schema, one publish_reply invocation and correlated result, durable sent outbox and completion receipts. Record exact focused test names for preparation/source races, cross-channel rejection, gated publication, duplicate/uncertain delivery and SDK negotiation. Do not retain private agent reasoning or unrelated source bodies in reports.

## Cleanup

Use the owning A01/A09/A20 fixture ledgers. Verify no synthetic runnable task, pending job, reminder or unresolved send remains.

## Failure handling

Use RUNNER.md. A command-line wrapper described as a tool, a mock-only MCP success, or a final agent response without verified Slack delivery is not PASS.
