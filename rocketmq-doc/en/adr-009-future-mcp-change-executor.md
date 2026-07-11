# ADR-009: Keep Change Execution Outside RocketMQ MCP

## Status

Accepted.

## Decision

`rocketmq-mcp` exposes only ephemeral, immutable, non-mutating change plans.
It does not persist plans, accept confirmation tokens, expose an Apply mode, or
call RocketMQ mutation APIs. A future Change Executor must be a separately
reviewed service with its own identity, approval workflow, persistence,
idempotency model, audit retention, and rollback authority.

## Consequences

Planning Tools remain read-only, non-destructive, and idempotent. Their
precondition and plan hashes bind the plan to a queried current-state snapshot;
an executor must revalidate those preconditions before any mutation.
