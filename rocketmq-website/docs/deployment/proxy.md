---
title: "Deploy and exercise Proxy"
---

Choose Cluster mode to expose an existing NameServer/Broker cluster through v2 gRPC. Choose Local mode to embed a Broker backend in the Proxy process. Both modes use the same gRPC contract, but their backend ownership and provisioning differ. Read [Proxy architecture](../architecture/proxy.md) for those boundaries.

## Build the selected mode

Use the repository toolchain and provide `protoc` through `PATH` or `PROTOC`, including the standard protobuf imports used by `service.proto`. From the repository root:

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode
```

For Local mode, replace `cluster-mode` with `local-mode`. Omitting `--no-default-features` builds both modes. Add `tls` when using the built-in TLS listener; exporter features are selected separately. Selecting a mode absent from the binary fails configuration.

## Cluster mode

Start the [single-Broker cluster](../getting-started/local-source.md) first. This walkthrough uses its `DocsCluster`, `docs-broker`, and NameServer `127.0.0.1:9876`. Create a dedicated normal-message Topic and group:

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsProxyMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_proxy_probe -c DocsCluster
```

Set `NAMESRV_ADDR=127.0.0.1:9876` in the Admin terminal for `updateSubGroup`. These commands change metadata. Use an isolated tutorial cluster; reusing a live Topic/group can change its configuration.

The [cluster configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/proxy/cluster.toml) binds gRPC on loopback `8081` and disables remoting. Set `ROCKETMQ_HOME` to the repository root and `ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback`, then run:

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode -- -c rocketmq-website/examples/proxy/cluster.toml
```

Use the same command with `--printConfig` appended to inspect startup configuration without binding the service. A printed configuration does not prove that routes, auth metadata, or Broker calls work.

## Probe the gRPC path

Install [grpcurl](https://github.com/fullstorydev/grpcurl) and run it on the same host. Supplying the repository proto files avoids relying on server reflection. The following Bash commands read checked-in JSON from stdin; `-plaintext` is limited to this loopback trial.

```bash
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/QueryRoute < rocketmq-website/examples/proxy/query-route.json
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/SendMessage < rocketmq-website/examples/proxy/send.json
grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/PullMessage < rocketmq-website/examples/proxy/pull.json
```

PowerShell uses a pipeline instead of input redirection; for example:

```powershell
Get-Content -Raw rocketmq-website/examples/proxy/query-route.json | grpcurl -plaintext -import-path rocketmq-proxy-core/proto -proto service.proto -H 'x-mq-client-id: docs-proxy-probe' -d '@' 127.0.0.1:8081 apache.rocketmq.v2.MessagingService/QueryRoute
```

Apply the same pipeline form to `send.json` and `pull.json` with their method names.

The query should return an OK payload status and queues. The send writes one normal message to queue 0; inspect both aggregate status and each result entry. Its body is protobuf JSON base64 for `Hello`. The pull streams queue-0 records from logical offset 0. Inspect stream status and returned message fields; transport success alone is insufficient.

These payloads target the single `docs-broker` backend. If your route names another Broker, update `pull.json` from the actual route. A fresh Topic makes the first pulled record useful for this probe. Repeating sends reuses the example ID and does not deduplicate messages; give each independent trial a new ID and use the returned offset to locate it.

Pulling is an inspection operation here: it does not acknowledge POP receipts or commit application progress. A production consumer uses assignment/subscription, processing, and the appropriate offset or receipt completion path. For POP, receive, process, then ACK the current receipt; see [POP semantics](../consumer/pop.md).

## Local mode

Stop any Proxy already using `8081`. Create `.rocketmq-proxy-demo/store` with the platform's directory command, then run the [local configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/proxy/local.toml):

```bash
cargo run -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features local-mode -- -c rocketmq-website/examples/proxy/local.toml
```

Keep the same loopback environment profile. The embedded backend has its own `docs-local-proxy` identity and store root; `brokerListenPort=10971` is a backend setting, not the external gRPC port. Its constructor disables NameServer registration and does not reuse the standalone cluster's Topic/group metadata.

Provision local backend resources through the supported embedded service integration before applying application probes. Do not point the standalone Admin setup above at NameServer and assume it created resources in this separate embedded store. The unchanged cluster probe is documented for Cluster mode; Local mode needs matching local Topic/group/Broker metadata.

## Shared-network deployment and shutdown

Configure inbound TLS and client identity/ACLs, then separately configure the Proxy's outbound signer and Broker permissions. An inbound allow decision does not authenticate a downstream call. Use [deployment security](security.md).

Stop producers/consumers or drain this Proxy from application routing before termination. Observe sessions, receipts, prepared transactions, retained response streams, and in-flight calls. Then request normal process shutdown and inspect its results. A TCP listener closing does not prove that backend work or receipt renewal has finished.

The commands and payloads are source-backed examples, not a recorded live gRPC trial. Validate actual send/receive and shutdown behavior in the chosen mode before treating the deployment as exercised.

## Source map

[Proxy manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/Cargo.toml), [gRPC adapter](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/ingress/grpc/adapter.rs), [protocol definitions](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/proto/service.proto), [local composition](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-local/src/local.rs).
