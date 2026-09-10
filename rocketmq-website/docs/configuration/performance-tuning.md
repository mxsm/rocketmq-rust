---
title: "Performance tuning workflow"
---

Performance tuning starts with the workload and the observed limiting stage. The complete workflow is now in [Capacity and performance](../operations/capacity-performance.md).

| Need | Guide |
| --- | --- |
| Estimate retained storage, replicas, and catch-up time | [Capacity calculations](../operations/capacity-performance.md) |
| Find client, disk, replication, or downstream bottlenecks | [Monitoring](../operations/monitoring.md) |
| Select batching, compression, and bounded sends | [Producer APIs](../producer/sending-messages.md) |
| Understand consumer parallelism and completion | [Consumer overview](../consumer/overview.md) |
| Assess flush and replica changes | [Storage](../architecture/storage.md), [HA design](../architecture/ha-controller.md) |
| Change retention or restart a node | [Maintenance](../operations/maintenance.md) |

Use the actual service's configuration schema and current runtime-injected client APIs. Change one factor per comparison and preserve the intended durability contract. Example resource values are not universal tuning recommendations.
