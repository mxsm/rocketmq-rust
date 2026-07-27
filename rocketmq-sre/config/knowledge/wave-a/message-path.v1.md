# Message path diagnosis

Use this runbook to explain sanitized send, route, store, dispatch, deliver, and transaction-status metadata.

## Required evidence

- Pseudonymized message ID or key, never the message body.
- Route epoch, queue, offset, timestamp, and bounded stage outcome.
- Sanitized trace correlation and transaction-status metadata when available.
- Consumer lag and Broker store health for supporting or refuting evidence.

## Interpretation

A stage may be described only when its source evidence explicitly reports it. Missing trace stages remain missing; do not synthesize a complete path. Transaction status metadata is evidence of state only and is not sufficient to assert a transaction root cause.

## Read-only recommendation

Show the last verified stage, time, pseudonymous identifier, and missing stages. Do not replay, resend, query message bodies, or alter transaction state.
