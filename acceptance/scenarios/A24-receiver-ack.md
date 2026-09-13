# A24: Durable intake acknowledges independently of the agent

## Setup

Apply RUNNER.md preflight and use only private director-tests with config/director-tests.json. Use A01's new synthetic source and the same published SHA. Save source, received-reaction and dispatcher timestamps. Isolated fixtures may block model preparation or fail reaction transport without touching the live service.

## Actions

Observe the actual white checkmark in Slack and the receiver's durable acknowledgement record for A01. Run focused isolated tests holding agent startup while valid intake succeeds, rejecting untrusted input, failing durable intake, retrying a reaction after restart, and handling duplicate/edit/deletion events.

## Expected

The checkmark means validated and durably saved. Healthy live delivery is within 2 seconds of source time, independent of model inference and maintenance work. Socket Mode acknowledgement remains durable-before-ack and is not held behind Slack reaction delivery. A received reaction does not mark agent-read or source completion. Failed persistence and untrusted input produce no success reaction. Failed Slack reactions remain recoverable without a model call or duplicate work. Stale/deleted sources cannot receive credit for a newer revision.

## Evidence

Capture the checkmark through computer control and record exact source/receipt/job timestamps. Save test names, assertions and logs proving independence from blocked agent preparation and recovery behavior. Report Slack transport delay separately; do not change a late result into PASS.

## Cleanup

A01 owns the labeled conversation. Verify no synthetic pending source or acknowledgement retry remains; isolated fixtures clean themselves up.

## Failure handling

Preserve evidence and fix through a reviewed PR. Do not simulate a live reaction or weaken the timing target to obtain PASS.
