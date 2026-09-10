---
title: "Client configuration reference"
---

# Client configuration reference

Client configuration is Rust application configuration. The library does not automatically load an application's TOML file or arbitrary `ROCKETMQ_*` variables. Read application settings explicitly, build the client configuration, and inject an application-owned `Arc<ClientRuntime>` into each facade. The [first-message application](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs) includes runtime ownership, startup, message exchange, and cleanup.

## Configuration layers and when they take effect

`ClientConfig` holds common discovery, connection, heartbeat, and offset-persistence settings. `ClientOptions` combines it with typed NameServer discovery. Producer, Push Consumer, and LitePull builders add operation-specific settings. Configure these before `start()`; a builder creates an object, not a running connection or a subscription on the Broker.

Producer and Push builders accept `client_config`, `client_options`, and `nameserver_discovery`. Calling `client_config` replaces the common configuration and clears previously attached typed discovery, so builder order matters. Their `name_server_addr` method changes the currently held common configuration. LitePull accepts `client_options` and applies its explicitly configured facade fields during build; its `use_tls` value is applied even when common options were supplied. Set `use_tls(true)` on LitePull as well when enabling TLS through common options.

`ClientConfig::builder().build()?` validates common settings. Producer and Push `build()` return their facade directly; startup performs further checks. LitePull `build()?` returns a result and requires a consumer group; successful construction still does not prove route, credentials, subscription, or Broker compatibility. Runtime infrastructure may already be shared between facades: do not treat mutating an old configuration value as live reconfiguration of that infrastructure. Recreate affected clients under controlled shutdown when changing startup settings.

## Common configuration

| `ClientConfig` builder method | Type / default | Unit and meaning |
| --- | --- | --- |
| `namesrv_addr` | string input / address discovery defaults below | Explicit NameServer endpoints; use semicolons between static addresses. |
| `poll_name_server_interval` | u32 / `30000` | Milliseconds between route polling; validated range `10000..=600000`. |
| `heartbeat_broker_interval` | u32 / `30000` | Milliseconds between Broker heartbeats; validated range `10000..=600000`. |
| `persist_consumer_offset_interval` | u32 / `5000` | Milliseconds between offset persistence work; validated range `1000..=60000`. This is not the Broker's disk persistence interval. |
| `mq_client_api_timeout` | u64 / `3000` | Common API timeout in milliseconds; validated range `100..=60000`. Specific APIs can have their own timeout. |
| `enable_tls` | bool / `false` | Requests TLS connections; also requires a TLS-capable transport build and compatible server configuration. |

When common configuration is constructed without an explicit address, address lookup checks process environment keys `rocketmq.namesrv.addr`, legacy `rocketmq.rocketmq-namesrv.addr`, then `NAMESRV_ADDR`. The legacy spelling produces a deprecation warning. Selection is based on presence; an empty higher-priority value is not a request to fall through. If none is present, the address field is unset, not an implicit `localhost:9876`. For dynamic discovery, use the typed `NameServerDiscoveryConfig` path rather than inventing URL syntax in a static address list.

The following fragment belongs in a function that already owns `client: Arc<ClientRuntime>` and returns a compatible error result. It constructs a producer; add lifecycle handling from the complete example.

```rust
use rocketmq_client_rust::{ClientConfig, DefaultMQProducer};

let common = ClientConfig::builder()
    .namesrv_addr("127.0.0.1:9876")
    .poll_name_server_interval(30_000)
    .heartbeat_broker_interval(30_000)
    .persist_consumer_offset_interval(5_000)
    .build()?;

let mut producer = DefaultMQProducer::builder(client.clone())
    .client_config(common)
    .producer_group("docs_reference_producer")
    .send_msg_timeout(3_000)
    .retry_times_when_send_failed(2)
    .build();
```

## Producer settings

Defaults below come from `ProducerConfig`. Values accepted by a builder are not a promise that the target Broker accepts messages of that size or supports a requested behavior.

| Producer builder method | Type / default | Meaning |
| --- | --- | --- |
| `producer_group` | string / initially empty | Set a meaningful producer group before startup; it is independent of consumer groups. |
| `send_msg_timeout` | u32 / `3000` ms | Default send deadline. An explicit per-call timeout can select another operation deadline. |
| `send_msg_max_timeout_per_request` | u32 input / unset | Optional per-request timeout cap in milliseconds; unset means no additional cap, not an unlimited overall send. |
| `retry_times_when_send_failed` | u32 / `2` | Additional synchronous send retries; attempts still consume the operation's deadline. |
| `retry_times_when_send_async_failed` | u32 / `2` | Additional asynchronous send retries; callbacks and final send status still need handling. |
| `retry_another_broker_when_not_store_ok` | bool / `false` | Whether to retry another Broker after a non-OK storage status. Retrying can duplicate an already stored message. |
| `max_message_size` | u32 / `4194304` bytes | Client-side size limit; coordinate with Broker limits and the selected API's encoding. |
| `compress_msg_body_over_howmuch` | u32 / `4096` bytes | Body-size threshold for compression; compression format must be understood by the receiver. |
| `default_topic_queue_nums` | u32 / `4` | Producer's automatic topic-creation request setting; separate from the Broker's default queue count and existing topics. |
| `auto_batch` | bool / `false` | Automatic batching selection; batch settings are not ordinary single-message guarantees. |
| `batch_max_delay_ms`, `batch_max_bytes`, `total_batch_max_bytes` | u32 / u64 / u64 inputs; unset | Optional accumulator delay and byte limits. Configure together with the batching path; unset values are not zero limits. |
| `enable_backpressure_for_async_mode` | bool / `false` | Enables the asynchronous send admission limits. |
| `back_pressure_for_async_send_num` | u32 / `1024` | Pending asynchronous send count budget. |
| `back_pressure_for_async_send_size` | u32 / `104857600` bytes | Pending asynchronous send byte budget; independent of the count budget. |

Choose one application deadline that includes discovery, connection, sends, and retries. A larger retry count cannot create extra time after that deadline. Inspect `SendResult.send_status` in addition to the outer Rust result; a timeout does not prove the Broker stored nothing. See [send variants](../producer/sending-messages.md) and [delivery and retry](../guides/delivery-and-retry.md).

## Push Consumer settings

| Push builder method | Type / default | Meaning and startup constraints |
| --- | --- | --- |
| `consumer_group` | string input | Explicitly choose a group and keep its instances' subscriptions consistent. |
| `message_model` | enum / `Clustering` | Clustering or broadcasting semantics; changes affect queue assignment and offset ownership. |
| `consume_from_where` | enum / `ConsumeFromLastOffset` | Initial position when no usable committed offset exists; it does not reset an existing group's progress. |
| `consume_thread_min` / `consume_thread_max` | u32 / `20` / `64` | Consumption concurrency configuration; each is `1..=1000` and minimum must not exceed maximum. These are not JVM thread-pool sizing rules. |
| `pull_batch_size` | u32 / `32` | Messages requested per pull; `1..=1024`. |
| `consume_message_batch_max_size` | u32 / `1` | Listener batch size; `1..=1024`, separate from the network pull batch. |
| `pull_interval` | u64 / `0` ms | Delay between pulls; `0..=65535`. Zero does not bypass flow control. |
| `pull_threshold_for_queue` | u32 / `1000` messages | Cached-message threshold per queue; `1..=65535`. |
| `pull_threshold_for_topic` | i32 / `-1` | `-1` leaves the topic-level count override disabled; otherwise `1..=6553500` messages. |
| `max_reconsume_times` | i32 / `-1` | Uses the consumption mode's retry convention; do not read `-1` as a universal fixed retry count. |
| `consume_timeout` | u64 / `15` minutes | Consumption timeout setting; it is not milliseconds and is not the pull RPC deadline. |

Attach the appropriate listener, subscribe, and start the consumer. Listener success must follow completed business effects; returning success merely because processing was scheduled can advance progress too early. Ordered, concurrent, and POP consumption have different retry and acknowledgment behavior. Use [Push Consumer](../consumer/push-consumer.md), [ordered messages](../guides/ordered-messages.md), and [POP](../consumer/pop.md) for those contracts.

## LitePull settings

| LitePull builder method | Type / default | Meaning |
| --- | --- | --- |
| `consumer_group` | string / required by build | Consumer-group identity. |
| `pull_batch_size` | i32 / `10` | Requested messages per pull. |
| `pull_thread_nums` | usize / `20` | Pull execution configuration; distinct from business processing concurrency. |
| `pull_threshold_for_queue` | i64 / `1000` messages | Per-queue cached-message threshold. |
| `pull_threshold_for_all` | i64 / `10000` messages | Aggregate cached-message threshold. |
| `poll_timeout_millis` | u64 / `5000` ms | How long a poll waits for locally available messages. |
| `broker_suspend_max_time_millis` | u64 / `20000` ms | Requested Broker suspension budget. |
| `consumer_timeout_millis_when_suspend` | u64 / `30000` ms | Client timeout for suspended pulls; leave room beyond Broker suspension. |
| `consumer_pull_timeout_millis` | u64 / `10000` ms | Pull RPC timeout setting; distinct from a local poll wait. |
| `auto_commit` | bool / `true` | Automatic offset commit behavior. For explicit business completion, choose manual commit deliberately. |
| `auto_commit_interval_millis` | u64 / `5000` ms | The setter accepts values of at least `1000`; smaller values are ignored and retain the previous value. |
| `topic_metadata_check_interval_millis` | u64 / `30000` ms | Interval for topic metadata checks. |

For example, with the same owned `client`:

```rust
use rocketmq_client_rust::DefaultLitePullConsumer;

let consumer = DefaultLitePullConsumer::builder(client.clone())
    .consumer_group("docs_reference_consumer")
    .name_server_addr("127.0.0.1:9876")
    .pull_batch_size(16)
    .poll_timeout_millis(2_000)
    .auto_commit(false)
    .build()?;
```

Subscription or explicit assignment, start, poll, business processing, and offset handling still follow construction. `commit_all()` updates local offset state; remote submission and Broker persistence are separate steps. A per-queue persistence error can be logged even when the surrounding operation returns success. Use [LitePull consumption](../consumer/pull-consumer.md) for a complete completion model, and do not enable automatic commits around unfinished asynchronous business work.

## TLS and application-owned settings

Configure peer verification and optional client identity through the common builder:

```rust
let secure_common = ClientConfig::builder()
    .namesrv_addr("namesrv.internal:9876")
    .enable_tls(true)
    .tls_test_mode_enable(false)
    .tls_client_auth_server(true)
    .tls_client_trust_cert_path("/etc/rocketmq/ca.pem")
    .tls_client_cert_path("/etc/rocketmq/client.pem")
    .tls_client_key_path("/etc/rocketmq/client.key")
    .build()?;
```

The certificate and key are for mTLS; omit both when only server authentication is required. Attach the resulting configuration to the actual facade; constructing an unused `secure_common` changes no connection. TLS support belongs to the transport dependency, not a fictional client `tls` feature. Test-mode or disabled peer verification settings change trust behavior and should not be copied from local experiments into a secured deployment.

Application configuration files and variables such as `ROCKETMQ_PRODUCER_GROUP` are application conventions unless the application reads them. The `ROCKETMQ_ENABLE_TLS_SMOKE` variables used by integration tests are test-harness inputs, not a general client configuration loader. Keep credentials out of example configuration dumps and diagnostics.

Sources: [common defaults](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/base/client_config.rs), [common validation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/base/client_config_validation.rs), [Producer defaults](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs), [Push defaults](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_push_consumer.rs), and [LitePull builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_lite_pull_consumer_builder.rs).
