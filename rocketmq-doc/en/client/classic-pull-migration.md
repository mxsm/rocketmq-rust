# Classic Pull compatibility

`DefaultMQPullConsumer` remains available for applications that require explicit queue and offset control. New applications should normally use `DefaultLitePullConsumer`; the Classic Pull facade is intended for Java client migration and compatibility-sensitive workloads.

## Runtime ownership

The Rust client does not create a hidden Tokio runtime. Build the consumer with an application-owned `ClientRuntime`:

```rust,ignore
let consumer = DefaultMQPullConsumer::builder(client_runtime.clone())
    .consumer_group("manual-pull-group")
    .name_server_addr("127.0.0.1:9876")
    .build()?;

consumer.start().await?;
// Pull messages.
consumer.shutdown().await?;
```

`DefaultMQPullConsumer::new` and `with_consumer_group` are retained for source compatibility. They are detached values and return a typed initialization error for runtime operations. They never create a runtime or background thread implicitly.

## Java-to-Rust method mapping

| Java Classic Pull method | Rust compatibility method | Notes |
|---|---|---|
| `start` / `shutdown` | `start().await` / `shutdown().await` | Uses the injected runtime; shutdown awaits the registered client and rebalance tasks. |
| `fetchSubscribeMessageQueues` | `fetch_subscribe_message_queues` | Returned topics have the configured namespace removed. |
| `pull(..., String, ...)` | `pull(...).await` | TAG expressions, including `*` and `TagA || TagB`. |
| `pull(..., MessageSelector, ...)` | `pull_with_selector(...).await` | Supports TAG and SQL92 selectors. |
| asynchronous `pull` | `pull_async` or `pull_async_with_selector` | Completion runs through the existing client callback executor. |
| `pullBlockIfNotFound` | `pull_block_if_not_found` | Sets the suspend flag and uses separate broker and client deadlines. |
| advanced pull overloads | `pull_with_options` / `pull_async_with_options` | `PullOptions` validates offset, count, size, and timeout invariants. |
| `registerMessageQueueListener` | `register_message_queue_listener` | Reports the all-queue and allocated-queue sets from the normal client rebalance cycle. |
| `updateConsumeOffset` | `update_consume_offset` | Updates the configured client offset store. |
| `fetchConsumeOffset` | `fetch_consume_offset` | Selects memory-first or store-only reads. |
| `searchOffset` / `maxOffset` / `minOffset` | `search_offset` / `max_offset` / `min_offset` | Reuses the client admin path. |
| `MQHelper.resetOffsetByTimestamp` | `MQHelper::reset_offset_by_timestamp_with_client_runtime` | Uses an injected runtime, updates every queue, and persists offsets during shutdown. The detached legacy signature fails closed instead of creating a hidden runtime. |
| `MQPullConsumerScheduleService` | `with_client_runtime`, `register_pull_task_callback`, `start`, `shutdown` | The coordinator is runtime-owned and its callback returns an async future. |

## Typed pull options

Use `PullOptions` when the Java overload would otherwise require a long positional argument list:

```rust,ignore
let options = PullOptions::new(
    queue,
    MessageSelector::by_sql("region = 'east'"),
    offset,
    32,
)?
.max_size_in_bytes(4 * 1024 * 1024)
.broker_suspend_timeout(Duration::from_secs(20))
.timeout(Duration::from_secs(30))
.block_if_not_found(true);

let result = consumer.pull_with_options(options).await?;
```

For block-if-not-found requests, the client timeout must exceed the broker suspension timeout. Classic Pull requests set the subscription flag but do not set the Lite Pull wire flag.

Classic Pull registers wildcard topic metadata for broker heartbeats and queue allocation, but remains a manual consumer. It neither advertises the LitePull stream request type nor starts LitePull background prefetch tasks. Queue-listener callbacks receive namespace-free queue names and the actual clustering allocation.

## Scheduling callbacks

`PullTaskCallback` returns a boxed future so callback implementations can call the async consumer without blocking a runtime worker:

```rust,ignore
impl PullTaskCallback for Callback {
    fn do_pull_task<'a>(
        &'a self,
        queue: &'a MessageQueue,
        context: &'a mut PullTaskContext,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async move {
            let consumer = context.get_pull_consumer()?;
            let offset = consumer.fetch_consume_offset(queue, false).await?.max(0);
            let result = consumer.pull(queue, "*", offset, 32).await?;
            consumer
                .update_consume_offset(queue, result.next_begin_offset() as i64)
                .await?;
            context.set_pull_next_delay_time_millis(100);
            Ok(())
        })
    }
}
```

Repeated `start` calls return a stable error. Repeated `shutdown` calls are idempotent. A consumer or schedule service must not be restarted after shutdown.
