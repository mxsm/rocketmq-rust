---
title: "Performance questions"
---

## What throughput can I expect?

There is no workload-independent throughput figure on this page. Report the actual hardware, storage, versions/features, message sizes, queue layout, client mode, flush/replication policy, duration, errors, and completed consumption. A short producer burst is not sustainable end-to-end throughput.

## How do I size or tune a deployment?

[Capacity and performance](../operations/capacity-performance.md) provides storage and catch-up calculations, bottleneck observations, and a repeatable comparison method. [Monitoring](../operations/monitoring.md) explains the signals used to locate the limiting stage.

## Does increasing message size or consumer count improve throughput?

A larger maximum message size changes a limit; it is not a batching operation. Additional consumers help only when queue assignment, ordering, and downstream capacity permit more completed work. See [sending APIs](../producer/sending-messages.md) and [consumer modes](../consumer/overview.md).

## Can I switch flush mode to reduce latency?

The change affects durability. Compare [storage semantics](../architecture/storage.md) and [HA requirements](../architecture/ha-controller.md) before treating a different acknowledgement contract as a performance result.
