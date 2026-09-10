---
title: "Design choices and tradeoffs"
---

# Design choices and tradeoffs

This page explains how the current architecture separates responsibilities and what those choices cost. The “observed design” statements summarize present code and component contracts. The rationale is an engineering interpretation of those mechanisms, not a claim that every choice has a historical ADR or that unmeasured performance benefits have been demonstrated.

## Separate domain, protocol and transport

**Observed design.** `rocketmq-model` holds runtime-neutral domain types. `rocketmq-protocol` owns request/response structures and codecs. `rocketmq-transport` owns connection execution, framing I/O and transport budgets. Services and clients compose these layers rather than placing all behavior in one remoting crate.

**Interpretation and cost.** This separates a message/queue contract from a Tokio connection lifecycle and lets domain code be tested without opening sockets. It also creates more explicit dependencies and migration work for imports formerly reached through common/remoting paths. Moving a type between crates does not authorize changing its serialized meaning.

Use [module map](./module-map.md), [message model](./message-model.md), and [protocol/transport](./protocol-transport.md) when changing a shared type. Keep transport convenience APIs from becoming a second source of truth for domain identity.

## Make runtime ownership explicit

**Observed design.** A top-level `RuntimeOwner` creates service contexts. Components own task groups and accepted background work; the client is supplied an owned runtime context and telemetry handle. Shutdown cancels and awaits owned work, with reports and deadlines.

**Interpretation and cost.** Explicit ownership makes it possible to explain who must stop a task and release a resource. It requires more startup plumbing than a constructor that silently creates a runtime. Library code must carry the needed context, and cancellation must be handled without assuming it reverses a remote write.

The task tree and budget tree are distinct. A child context does not automatically create an independent quota. A timed-out blocking caller does not necessarily stop the underlying closure; permits must remain associated with real work. See [runtime](./runtime.md).

## Publish immutable routes after serialized mutation

**Observed design.** NameServer applies route mutations through its route owner and publishes immutable per-topic views. Session/generation checks prevent stale connection events from removing newer registrations. Ordinary NameServers do not form a replicated Raft metadata cluster.

**Interpretation and cost.** Readers can access a consistent topic snapshot while mutation ordering remains explicit. This does not give a transaction across all topics, nor does it make separately deployed NameServers instantly identical. Broker registration and client refresh remain necessary convergence mechanisms.

Distinguish durable KV configuration from transient route registration, and do not describe a second NameServer as a synchronous replica of the first. See [NameServer](./nameserver.md).

## Compose storage ports without redefining primary durability

**Observed design.** The store factory selects a coherent port bundle. Local CommitLog remains the primary record path in the documented combinations; ConsumeQueue and indexes are derived structures. A RocksDB-derived backend does not replace the primary log with an arbitrary RocksDB database. Tiered storage is a separate secondary path.

**Interpretation and cost.** A stable primary contract permits multiple derived indexing/query choices, but requires recovery to align each cursor with its engine and source epoch. An index being advanced does not establish a stronger replication or business-completion guarantee. Changing `storeType` cannot serve as a data conversion procedure.

See [storage](./storage.md), [storage backends](./storage-backends.md) and [offline tools](../operations/offline-tools.md). Record the source of each progress value before using it in recovery logic.

## Separate write authority from replica progress

**Observed design.** Controller-mode HA uses explicit Broker identity, master epoch and write-authority/lease contracts. Synchronization-set progress has its own meaning. Replication acknowledgment policies distinguish local acceptance/durability from the required remote participants. Rust Controller uses OpenRaft rather than Java Controller internal protocols.

**Interpretation and cost.** A replica being up to date is not enough to authorize it to accept writes. The separation makes split-brain prevention and failover reasoning explicit, at the cost of handling fencing, lease expiry and membership state in addition to byte progress. A single-member “all in sync” condition does not imply a remote replica acknowledged the data.

Do not combine Java and Rust Controller members or reuse consensus snapshots based on functional naming similarity. See [HA and Controller](./ha-controller.md) and [compatibility](../reference/protocol-compatibility.md).

## Share Proxy contracts across Cluster and Local backends

**Observed design.** Proxy Core defines front-door contracts. Cluster mode uses a remote RocketMQ backend; Local mode composes an embedded Broker backend with its own lifecycle. The binary's selected features constrain available runtime modes.

**Interpretation and cost.** Shared frontend behavior can support different deployment units, but Local mode changes process ownership, storage and shutdown responsibilities. Switching a mode value is not a transparent operation that migrates data, sessions or credentials. A frontend transport success must still be interpreted through the operation result.

See [Proxy architecture](./proxy.md), [deployment](../deployment/proxy.md), and [service configuration](../reference/service-configuration.md). Account for active sessions and downstream readiness when draining either mode.

## Keep observations separate from controlled execution

**Observed design.** Read-only MCP uses a read adapter and policy-bound observation pipeline. Planning tools remain non-mutating. MCP Control and SRE execution components are separate products with their own credentials, operation registration and lifecycle boundaries. Ordinary Dashboards have their own administrative access.

**Interpretation and cost.** Diagnostic access can be deployed without granting arbitrary mutation capability, while execution requires explicit typed integration. This creates separate configuration and operational work; it must not be hidden by routing a diagnostic tool through a broader Admin session. A model-generated suggestion is not authority to execute it.

Separation does not imply every authorization path is identical: the [MCP guide](../ecosystem/mcp.md) records its current omitted-cluster inventory limitation. Product boundaries are useful only when their actual enforcement and exceptions are described accurately.

## Give public errors stable identity and bounded context

**Observed design.** Canonical errors carry stable descriptors and retry hints, while protocol/HTTP/CLI boundaries map them to their own status models. Public views expose approved context and hide implementation causes. Telemetry ownership and export configuration remain explicit.

**Interpretation and cost.** Integrations can retain machine-readable identity without parsing changing human text or leaking secret-bearing causes. The cost is maintaining deliberate mappings and enough safe context for diagnosis. A single numeric status is not a substitute for retry semantics or knowledge of partial effects.

Use [error reference](../reference/errors.md) and [errors/observability](./errors-observability.md). An operator-action error should not enter the same retry loop as a refresh-route hint, and a redacted error should not be “fixed” by printing the original request.

## Apply a design change at the correct boundary

Before proposing a change, identify the contract owner, caller set, persisted/wire surfaces and lifecycle owner. State the new invariant, the behavior it replaces and the evidence needed to observe it. Use [coding standards](../contributing/coding-standards.md) and the component guide to choose focused checks.

If the proposal crosses a boundary described here, explain the tradeoff explicitly: for example, changing a local index is different from changing primary durability, and adding a cached observation is different from adding write authority. Keep documented observations and new design proposals separate so readers can tell what the current code actually does.
