---
title: "Review a production deployment"
---

Use these questions to describe the deployment you actually operate: its workload, topology, failure tolerance, and recovery procedure. Answers should identify the responsible operator and the observation supporting the decision. There is no universal replica count, storage size, or throughput figure that establishes production readiness for every workload.

## Workload and topology

| Question | What to establish | Supporting guide |
| --- | --- | --- |
| Which message semantics does the application need? | Normal/ordered/transaction/POP/delayed paths actually used; duplicate handling; completion and retry behavior | [Delivery and retry](../guides/delivery-and-retry.md), [capabilities](../overview/capability-matrix.md) |
| Is this a sharded or replicated topology? | Broker names identify groups; replica IDs belong to one group. Different Broker names alone do not provide copies of the same data | [Multi-node layout](multi-node.md) |
| What permits a successful write? | Flush policy, local durability, in-sync replica requirement, and Controller authority where enabled | [HA design](../architecture/ha-controller.md) |
| Which failure can the cluster tolerate while accepting writes? | Lost Broker, Controller voter, disk, or host; correlated failure placement; two-of-two replication can stop writes on one replica loss | [HA deployment](high-availability.md) |
| Can applications reach every advertised endpoint? | Route-returned Broker addresses, NameServer alternatives, Proxy address/TLS name, DNS and firewall behavior from the application network | [Kubernetes deployment](kubernetes.md), [Proxy deployment](proxy.md) |

Record the chosen topology, host/zone placement, service identity, ports, and data roots in the deployment's own inventory. Keep the write availability decision consistent with the application's timeout and retry behavior. A timed-out send may have been accepted; retry safety belongs to the business operation.

## Security and state

| Question | What to establish | Supporting guide |
| --- | --- | --- |
| Who can access each public, peer, health, and management endpoint? | Actual network exposure and encryption per hop, not a single global TLS assumption | [Deployment security](security.md) |
| Are application, inner-client, and operator identities separate? | Required allow/deny cases, outbound signing, receiver permissions, protected ACL/snapshot files, credential rotation | [Deployment security](security.md) |
| Where does authoritative state survive process or Pod replacement? | CommitLog paths, Broker metadata, offsets, identity, timer/transaction state, Controller storage, and PVC retention | [Backup and recovery](../operations/backup-recovery.md) |
| Can one process accidentally open another process's data? | Exclusive store ownership, stable identity-to-volume mapping, no shared writable root between replicas | [Storage design](../architecture/storage.md) |
| Is the selected backend supported by the built artifact? | Required features and native dependencies; authoritative log and derived-state boundaries; no assumed automatic backend migration | [Storage backends](../architecture/storage-backends.md) |

Document where secrets are stored without copying their values into the inventory. Keep recovery access available to the designated operators even when ordinary service authentication is impaired.

## Capacity and visibility

| Question | What to establish | Supporting guide |
| --- | --- | --- |
| What is the retained data volume per replica? | Stored bytes/message, sustained and peak rates, retention, retry/DLQ/timer backlog, indexes and operational headroom | [Capacity and performance](../operations/capacity-performance.md) |
| How quickly can consumers recover from lag? | Available drain rate above ingress, downstream processing capacity, ordering/queue limits | [Capacity and performance](../operations/capacity-performance.md) |
| Can operators distinguish the failing layer? | Application latency/errors, Broker storage and replication, consumer progress, Controller quorum, Proxy admission | [Monitoring](../operations/monitoring.md), [troubleshooting](../operations/troubleshooting.md) |
| Does telemetry actually leave the process? | Compiled exporter features, effective runtime selection, collector reachability, bounded labels, shutdown flush | [Observability configuration](../configuration/observability.md) |
| What triggers action? | Workload-specific thresholds and a linked operator response; distinguish missing telemetry from a healthy zero | [Monitoring](../operations/monitoring.md) |

Resource limits and probe thresholds should accommodate observed startup recovery and shutdown duration. CPU/memory requests in example values are starting inputs, not measured sizing recommendations.

## Maintenance and recovery

| Question | What to establish | Supporting guide |
| --- | --- | --- |
| What happens during a planned restart? | Draining, admitted work, surviving write requirements, Controller majority, Pod disruption rules, and catch-up before the next node | [Maintenance](../operations/maintenance.md) |
| What is in the backup, and what consistency does it have? | A coordinated state set or stopped-source copy, independent storage, identity and configuration needed for restore | [Backup and recovery](../operations/backup-recovery.md) |
| Has restore been tried on an isolated destination? | Recovered message bounds, offsets, permissions, routing, application reconciliation, measured elapsed time | [Backup and recovery](../operations/backup-recovery.md) |
| Can the previous version read the resulting data and metadata? | Storage/config/API compatibility, downgrade-preflight limits, and whether rollback means restoring an earlier state set | [Upgrade and rollback](../operations/upgrade-rollback.md) |
| Who responds when the expected result is absent? | Operator contacts, environment access, diagnostic commands, and decisions to stop a rollout or restore | [Admin operations](../operations/admin.md) |

State the scope of each exercise. A successful chart render, configuration parse, process health probe, or local first-message example does not demonstrate distributed recovery. Report RPO/RTO only for the failure and restore scenario actually measured; unresolved assumptions remain visible in the deployment record.
