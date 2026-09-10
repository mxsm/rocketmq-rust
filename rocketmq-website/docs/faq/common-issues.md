---
title: "Common operational questions"
---

Use the linked guides for complete Rust service commands and explanations.

| Question | Start here |
| --- | --- |
| Why can I reach NameServer but not send a message? | [Endpoint and route diagnosis](../operations/troubleshooting.md#connection-refused-or-no-route) |
| Why does a connected consumer receive nothing? | [Subscription, assignment, position, and completion](../operations/troubleshooting.md#connected-consumer-receives-nothing) |
| Why can a message be processed twice? | [Delivery, retry, and business idempotency](../guides/delivery-and-retry.md) |
| Why does startup fail after selecting an exporter or Proxy mode? | [Build and configuration diagnosis](../operations/troubleshooting.md#startup-or-build-failure) |
| How do I inspect the cluster with the Rust CLI? | [Admin operations](../operations/admin.md) |
| Which components own routes, replication, and storage? | [Architecture overview](../architecture/overview.md) |

For a first installation, use the [matched local tutorial](../getting-started/local-source.md). The current examples explicitly inject the client runtime. Do not repair a failure by copying an unrelated Java startup command, opening all firewall ports, resetting offsets, or deleting store state.
