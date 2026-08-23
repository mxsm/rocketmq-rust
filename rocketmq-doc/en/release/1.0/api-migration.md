# RocketMQ Rust 1.0 API migration

RocketMQ Rust 1.0 freezes the core-release Rust API at the first complete
`1.0.0-rc.1` candidate. The freeze is structural: every public item is recorded
by package, supported feature profile, canonical item path, kind, visibility,
signature, and feature condition. It does not use a commit, file, or artifact
digest as a compatibility decision.

This guide covers the core release only. Dashboard, MCP, SRE, OpenMessaging,
BrokerContainer, and DLedger CommitLog are not part of the 1.0 compatibility
surface.

## Compatibility policy

The 0.9-to-1.0 transition has four explicit classifications:

| Classification | Meaning |
|---|---|
| `compatible-addition` | A typed capability was added without removing a supported wire, storage, or source contract. |
| `approved-break` | A misleading or unsafe pre-1.0 entry point was removed with repository-owner approval and a documented replacement. |
| `renamed-wrapper` | The operation remains available under a name that accurately describes its behavior. |
| `removed-placeholder` | A public placeholder or production test probe was replaced by a working capability or moved behind `test-support`. |

After the freeze, additions remain compatible. Removing an item, changing its
signature or trait bounds, changing a supported feature profile, or changing a
documented default is rejected unless the baseline contains a matching,
reviewed post-freeze approval.

## Direct source migrations

### Fallible client runtime construction

`ClientRuntime::new` was removed because runtime startup can fail. Propagate the
typed error from `try_new` instead of relying on a panic-compatible constructor.

```rust,ignore
// Before 1.0
let runtime = ClientRuntime::new(config);

// 1.0: preserve the existing service context, runtime config, and telemetry.
let runtime = ClientRuntime::try_new(service_context, config, telemetry_handle)?;
```

### Tokio client cached-session reconciliation

`TransportClient::is_address_reachable` is retained as a deprecated
compatibility facade, but it has never performed a DNS, socket, or network
reachability probe. It only inspected the direct cached session and removed an
unhealthy entry.

Use `reconcile_cached_connection` when application logic needs the typed cache
result:

```rust,ignore
use rocketmq_transport::api::v1::CachedConnectionState;

match client.reconcile_cached_connection(&address) {
    CachedConnectionState::Healthy => {}
    CachedConnectionState::UnhealthyRetired => reconnect_after_cleanup(),
    CachedConnectionState::Absent => connect_if_needed(),
}
```

Request processors are fixed when a client is built. Replace the deprecated
`register_processor` compatibility call with
`TransportClient::builder(...).build()?` or
`RemotingClient::builder(...).build()?` so configuration failures remain typed.

Applications own the runtime and pass an `Arc<ClientRuntime>` to producers and
consumers. Client APIs do not create hidden Tokio runtimes.

### Mapped buffer reads

The former `MappedBuffer::read_zero_copy` name was inaccurate because the
returned bytes were owned copies. Use `read_copy`; the ownership and allocation
behavior are unchanged.

```rust,ignore
// Before 1.0
let bytes = mapped.read_zero_copy(position..position + length)?;

// 1.0
let bytes = mapped.read_copy(position..position + length)?;
```

### Narrow client capabilities

The placeholder `MQClientAPIExt` and implementation aliases are not part of the
1.0 surface. Depend on the narrow capability needed by the caller:

| Old dependency | 1.0 capability |
|---|---|
| generic producer operations | `ProducerClient` |
| pull, POP, ACK, and consumer operations | `ConsumerClient` |
| route lookup and refresh | `RouteClient` |
| administrative RPC operations | `AdminClient` |
| transaction end/check operations | `TransactionClient` |

Repository integration probes that were once exported from production roots
are available only through the explicit `test-support` feature. Application
code must not depend on those probes.

## Frozen 1.0 entry points

### MappedFileBuilder

`MappedFileBuilder` is a working, fallible builder rather than a pending public
placeholder. A path and size are required. Supported defaults create a mapped
file; invalid sizes, unsupported flush/transient/warmup combinations, and I/O
failures return `MappedFileError`.

```rust,ignore
use rocketmq_store::MappedFileBuilder;

let mapped_file = MappedFileBuilder::new("data/commitlog/00000000000000000000")
    .size_mb(1024)
    .build()?;
```

Do not unwrap builder failures in a Broker startup path. Surface the typed
configuration or I/O error before the service starts accepting traffic.

### Classic Pull compatibility facade

`DefaultMQPullConsumer` remains functional for applications that need explicit
queue and offset control. New Rust-native applications should normally use
`DefaultLitePullConsumer`; migrations from Java Classic Pull may use the stable
facade.

```rust,ignore
let consumer = DefaultMQPullConsumer::builder(client_runtime.clone())
    .consumer_group("manual-pull-group")
    .name_server_addr("127.0.0.1:9876")
    .build()?;

consumer.start().await?;
let result = consumer.pull(&queue, "TagA || TagB", offset, 32).await?;
consumer
    .update_consume_offset(&queue, result.next_begin_offset() as i64)
    .await?;
consumer.shutdown().await?;
```

The facade supports synchronous and asynchronous pull, TAG/SQL92 selectors,
block-if-not-found deadlines, queue discovery, offset persistence, queue
listeners, and scheduled pull callbacks. It uses an injected runtime and never
advertises the LitePull stream request type. Detached compatibility
constructors remain source-compatible but fail closed for operations that need
a runtime.

For overload-style calls, use `PullOptions` so timeout, count, size, offset, and
long-poll invariants are validated before a request is sent.

### Proxy gRPC Settings

Proxy settings are resolved through one immutable `ServerSettingsPolicy`
generation per telemetry request. Server-owned values override client claims;
client identity and subscription expressions remain client-owned.

The 1.0 fallback values are:

| Setting | Default |
|---|---:|
| maximum message body | 4 MiB |
| validate message type | `true` |
| consumer retry attempts | 17 |
| receive batch size | 32 |
| long-poll timeout | 20 seconds |
| FIFO | `false` |
| Lite subscription quota | 2000 |
| maximum Lite topic-name length | 64 |

Producer retry policy is built from `SettingsConfig`; consumer group policy is
resolved from the authoritative backend. If the policy cannot be resolved, the
request fails closed rather than silently reverting to unrelated constants.

Custom embedders may provide a `SettingsPolicyProvider`, but the provider must
return a complete immutable policy for the request lifetime.

## Feature-profile compatibility

The default API of every core library is recorded separately. The 24 public
feature profiles in `m09_compatibility_matrix.py` are also recorded separately,
including no-default, selected-feature, combined-feature, and all-feature
profiles for Protocol, Transport, Store, Admin, and Proxy.

Notable 1.0 defaults include:

- `rocketmq-client-rust`: `admin-full`;
- `rocketmq-transport`: `tls,socks`;
- `rocketmq-store`: `local_file_store,fast-load`;
- `rocketmq-controller`: `storage-rocksdb`;
- `rocketmq-proxy`: `cluster-mode,local-mode`.

Use `--no-default-features` only when the application also selects one of the
frozen supported profiles. An arbitrary Cargo feature power set is not an
implicit compatibility promise.

## Checking an application migration

Before adopting a release candidate:

1. replace the direct source migrations above;
2. select a frozen feature profile;
3. run the application's normal tests against `1.0.0-rc.N`;
4. exercise startup and shutdown so runtime ownership is verified;
5. exercise send, pull/POP, query, and Admin paths used by the application;
6. treat any new compiler error or default-behavior difference as a release
   candidate compatibility finding.

The repository verifies its own frozen surface with:

```powershell
python scripts/public_api_snapshot.py --scope core-release --check scripts/public-api-snapshot-baseline.json --identity structural
python scripts/stable_surface_guard.py --scope core-release --mode target
```

The snapshot command reports additions separately from breaking changes. A
post-freeze break is accepted only when its exact package, profile, item path,
and change kind match a non-empty approval record.
