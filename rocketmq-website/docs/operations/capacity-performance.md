---
title: "Plan capacity and investigate performance"
---

Size the deployment from measured stored data, ingress, consumer completion, retention, and failure recovery. Distinguish sustainable throughput from a short burst absorbed by memory or page cache. This guide gives a calculation method; its arithmetic examples are not benchmark results.

## Calculate storage per Broker group

Let:

| Variable | Meaning |
| --- | --- |
| `λ` | Sustained accepted messages per second for this Broker group |
| `s` | Measured average stored bytes per message, including encoded overhead and actual compression |
| `T` | Retention interval in seconds |
| `R` | Number of full replicas in the group |
| `Dextra` | Additional per-replica indexes, metadata, retry/DLQ/timer state, and operating logs not already counted in `λ × s` |
| `H` | Per-replica free headroom for growth, cleanup delay, recovery, and maintenance |

Approximate retained primary data per replica as `Dlog = λ × s × T`. Provision each replica for at least the estimated `Dlog + Dextra + H`, accounting separately for any split filesystems. Across the group, full-replica storage is approximately `R × (Dlog + Dextra + H)`. Distribute calculations across Broker groups according to actual traffic rather than assuming an even split.

For an illustrative `1,000 messages/s`, `1,000 stored bytes/message`, and one day, `Dlog = 86.4 GB` in decimal units per replica. Three replicas hold `259.2 GB` of that primary data before additional state and headroom. These inputs are hypothetical, not a supported throughput or recommended retention.

Measure actual segment growth over a representative interval. System Topics, retries, delayed traffic, large property sets, and compression ratios can change stored volume. Avoid counting the same system traffic twice when it is already included in measured growth. Segment allocation and cleanup can make physical disk usage differ from live payload bytes.

RocksDB-backed store metadata still accompanies a local CommitLog. Tiered storage is a separate secondary path with provider and recovery constraints; selecting it does not automatically remove local capacity requirements. See [storage backends](../architecture/storage-backends.md).

## Calculate catch-up capacity

If lag is `B` messages, current ingress is `λ`, and sustainable completed consumption is `μ`, then ideal drain time is `B / (μ − λ)` when `μ > λ`. If `μ ≤ λ`, the backlog does not drain under the same conditions.

For a hypothetical 10 million messages, `λ = 3,000/s` and `μ = 5,000/s`, ideal catch-up is 5,000 seconds, about 83 minutes. Add practical effects such as retries, rebalancing, downstream throttling, and hot queues. Aggregate capacity does not fix one serial ordered queue whose own consumption rate is below its ingress.

Check whether retention still covers the oldest required data throughout catch-up. A successful lag calculation cannot recover segments already deleted. Replica catch-up similarly competes for disk/network capacity with live traffic and may affect synchronous writes.

## Account for memory and network

Memory depends on retained work: in-flight sends, encoded buffers, consumer batches, business queues, response streams, caches, and runtime tasks. A count limit alone is insufficient when message sizes vary widely. Bound both concurrency and retained bytes where the relevant API supports it.

Compare process resident memory with operating-system page cache and container memory accounting. Memory-mapped storage does not imply all data is resident, nor does a small application heap imply low total memory demand.

Network planning includes ingress payloads, protocol overhead, replication copies, consumer egress, retries, route/control traffic, and catch-up. A Proxy adds another hop and its own admission/stream retention. TLS and compression shift CPU cost; measure their actual selected path.

## Locate the limiting stage

| Observation | Likely investigation | First controlled change |
| --- | --- | --- |
| Client waits before requests start | Producer backpressure, application concurrency, runtime/blocking budgets | Bound outstanding work and compare queue wait with remote latency |
| Broker append/flush latency rises with disk pressure | CommitLog device service time, flush policy, competing writes | Remove contention or provide adequate device capacity |
| HA ACK latency or replication lag rises | Replica disk/network, synchronized set and authority | Restore replica progress while keeping the required durability contract |
| Consumer CPU is busy | Decode/filter/business computation and hot queues | Optimize or partition the measured work where ordering permits |
| Consumer mostly waits on downstream I/O | Database/API capacity and processing concurrency | Tune bounded concurrency to the downstream limit |
| Proxy rejects or retains many streams | Admission limits, retained bytes, slow clients/downstream calls | Reduce backlog and locate the blocking hop before raising limits |

Use [monitoring](monitoring.md) to correlate these observations. Raising every thread count or queue limit often moves waiting into memory without increasing completed work.

## Run a comparable workload

1. Fix the scenario: client mode, message distribution, payload/property sizes, queue/group count, ordering, filtering, TLS, storage backend, flush and replication policy.
2. Record software/build features, machines, storage devices, filesystem, network, container limits, and background load. Keep the workload generator off the critical service resource when measuring service capacity.
3. Use isolated Topics/groups. Warm the intended path, then measure long enough to include flushing, steady resource use, consumer progress, and relevant maintenance activity.
4. Count attempted sends, accepted results by status, errors/timeouts, unique business completions, duplicates, and remaining lag. Report latency percentiles and observation duration, not only average send-call time.
5. Change one factor, repeat the same workload, and compare completed work, tail latency, durability, errors, and resource growth.

Batching can amortize per-request cost, while increasing `max_message_size` only changes a size limit. Compression trades CPU for bytes and depends on data compressibility. More consumers help only when queues/work can be assigned in parallel and the downstream system can keep up. Use current [producer](../producer/sending-messages.md) and [consumer](../consumer/overview.md) APIs rather than detached legacy constructors.

Changing `SYNC_FLUSH` to `ASYNC_FLUSH` or lowering replica requirements changes what a successful write means. Report that change as part of the workload, not as a free performance gain. Linux `sendfile`/`io_uring` and loading features have compile-time and runtime eligibility; verify the actual engine and fallback observations before attributing a result to them.

## Retention and operational headroom

Inspect actual store settings such as `fileReservedTime`, `deleteWhen`, disk pressure thresholds, and enabled cleanup behavior. Retention is not a promise that every unconsumed message will remain indefinitely; disk-pressure cleanup and segment eligibility affect the retained interval.

Reserve separate room for backup destinations, upgrades, and any temporary consolidation copies. Reducing retention can permanently remove replayable data. Establish capacity responses before disks become critical: limit ingress, restore consumption, expand supported storage, or apply a deliberate retention change after assessing business recovery needs.

## Source map

[Store configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs), [local cleanup](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/message_store/local_file_message_store/health.rs), [store performance observations](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/metrics/store.rs), [Proxy runtime limits](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/src/config.rs).
