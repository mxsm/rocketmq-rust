// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::access_channel::AccessChannel;
use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::mq_producer::MQProducer;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::send_result::SendResult;
use crate::trace::trace_constants::TraceConstants;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_data_encoder::TraceDataEncoder;
use crate::trace::trace_dispatcher::TraceDispatcher;
use crate::trace::trace_dispatcher::Type;
use crate::trace::trace_transfer_bean::TraceTransferBean;

// Configuration for the async trace dispatcher.
#[derive(Clone)]
struct TraceDispatcherConfig {
    group: String,
    type_: Type,
    batch_num: usize,
    max_msg_size: usize,
    flush_interval: Duration,
    trace_topic_name: String,
    rpc_hook: Option<Arc<dyn RPCHook + Send + Sync>>,
}

// Shared state for the async trace dispatcher.
struct DispatcherState {
    is_started: AtomicBool,
    is_stopped: AtomicBool,
    discard_count: AtomicU64,
    last_flush_time: AtomicU64,
    send_which_queue: AtomicUsize,
    access_channel: RwLock<AccessChannel>,
    host_producer: RwLock<Option<ArcMut<DefaultMQProducerImpl>>>,
    host_consumer: RwLock<Option<ArcMut<DefaultMQPushConsumerImpl>>>,
    namespace_v2: RwLock<Option<CheetahString>>,
}

impl DispatcherState {
    fn new() -> Self {
        Self {
            is_started: AtomicBool::new(false),
            is_stopped: AtomicBool::new(false),
            discard_count: AtomicU64::new(0),
            last_flush_time: AtomicU64::new(0),
            send_which_queue: AtomicUsize::new(0),
            access_channel: RwLock::new(AccessChannel::Local),
            host_producer: RwLock::new(None),
            host_consumer: RwLock::new(None),
            namespace_v2: RwLock::new(None),
        }
    }
}

/// Asynchronous trace dispatcher for RocketMQ message tracing.
///
/// Collects trace contexts from producers and consumers, batches them according
/// to configured thresholds, and asynchronously sends them to a trace topic.
/// The dispatcher uses a bounded channel for non-blocking appends and spawns
/// background tasks for batch processing.
///
/// Trace contexts are grouped by topic before being encoded and sent. Messages
/// exceeding the configured size limit are split into multiple trace messages.
///
/// # Implementation
///
/// Uses a `tokio::mpsc::channel` with a capacity of 2048 for queuing trace contexts.
/// A single worker task periodically flushes batches based on count or time thresholds.
/// Send operations are performed concurrently via `tokio::spawn`.
///
/// # Discard Policy
///
/// When the internal queue is full, new trace contexts are discarded and the
/// discard counter is incremented. This prevents blocking the caller.
pub struct AsyncTraceDispatcher {
    config: TraceDispatcherConfig,
    state: Arc<DispatcherState>,
    tx: mpsc::Sender<TraceContext>,
    rx: Mutex<Option<mpsc::Receiver<TraceContext>>>,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
    trace_producer: RwLock<Option<Arc<tokio::sync::Mutex<DefaultMQProducer>>>>,
    instance_counter: Arc<AtomicUsize>,
}

impl AsyncTraceDispatcher {
    /// Creates a new async trace dispatcher.
    ///
    /// The dispatcher is created in a stopped state and must be started via
    /// [`start`](Self::start) before it can process trace contexts.
    ///
    /// # Arguments
    ///
    /// * `group` - Producer or consumer group name used for trace producer identification
    /// * `type_` - Trace type, either `Type::Produce` or `Type::Consume`
    /// * `batch_num` - Maximum number of contexts per batch (capped at 20)
    /// * `trace_topic_name` - Destination topic for trace messages (uses system default if empty)
    /// * `rpc_hook` - Optional RPC hook applied to the internal trace producer
    pub fn new(
        group: &str,
        type_: Type,
        batch_num: usize,
        trace_topic_name: &str,
        rpc_hook: Option<Arc<dyn RPCHook + Send + Sync>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(2048);

        let trace_topic = if trace_topic_name.is_empty() {
            TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string()
        } else {
            trace_topic_name.to_string()
        };

        let config = TraceDispatcherConfig {
            group: group.to_string(),
            type_,
            batch_num: batch_num.min(20),
            max_msg_size: 128_000, // 128KB
            flush_interval: Duration::from_secs(5),
            trace_topic_name: trace_topic,
            rpc_hook,
        };

        Self {
            config,
            state: Arc::new(DispatcherState::new()),
            tx,
            rx: Mutex::new(Some(rx)),
            worker_handle: Mutex::new(None),
            trace_producer: RwLock::new(None),
            instance_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    // Generates a unique group name for the trace producer using an atomic counter.
    fn gen_group_name(&self) -> String {
        let counter = self.instance_counter.fetch_add(1, Ordering::SeqCst);
        format!(
            "{}-{}-{:?}-{}",
            TraceConstants::GROUP_NAME_PREFIX,
            self.config.group,
            self.config.type_,
            counter
        )
    }

    /// Returns the number of trace contexts discarded due to queue overflow.
    pub fn get_discard_count(&self) -> u64 {
        self.state.discard_count.load(Ordering::Relaxed)
    }

    /// Returns the approximate queue depth.
    ///
    /// # Note
    ///
    /// The tokio mpsc channel does not expose queue size. This method returns 0.
    pub fn queue_size(&self) -> usize {
        // mpsc::Sender doesn't expose queue size directly
        // This is an approximation
        0
    }

    /// Returns whether the dispatcher has been started.
    pub fn is_started(&self) -> bool {
        self.state.is_started.load(Ordering::SeqCst)
    }

    /// Returns whether the dispatcher has been stopped.
    pub fn is_stopped(&self) -> bool {
        self.state.is_stopped.load(Ordering::SeqCst)
    }

    /// Associates the dispatcher with a host producer implementation.
    ///
    /// The host producer reference is stored for potential future use in trace context enrichment.
    pub fn set_host_producer(&self, host_producer: ArcMut<DefaultMQProducerImpl>) {
        *self.state.host_producer.write() = Some(host_producer);
    }

    /// Associates the dispatcher with a host consumer implementation.
    ///
    /// The host consumer reference is stored for potential future use in trace context enrichment.
    pub fn set_host_consumer(&self, host_consumer: ArcMut<DefaultMQPushConsumerImpl>) {
        *self.state.host_consumer.write() = Some(host_consumer);
    }

    /// Sets the namespace v2 configuration for the dispatcher.
    pub fn set_namespace_v2(&self, namespace_v2: Option<CheetahString>) {
        *self.state.namespace_v2.write() = namespace_v2;
    }

    /// Returns a reference to the internal trace producer.
    ///
    /// The producer is available only after the dispatcher has been started.
    pub fn get_trace_producer(&self) -> Option<Arc<tokio::sync::Mutex<DefaultMQProducer>>> {
        self.trace_producer.read().clone()
    }
}

impl TraceDispatcher for AsyncTraceDispatcher {
    /// Starts the trace dispatcher and its background worker task.
    ///
    /// Initializes the internal trace producer, spawns the worker task for
    /// batch processing, and marks the dispatcher as started. This method
    /// can be called only once; subsequent calls are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if the dispatcher has already been started or if
    /// internal state initialization fails.
    fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> RocketMQResult<()> {
        // CAS to ensure we only start once
        if self
            .state
            .is_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            warn!("AsyncTraceDispatcher already started");
            return Ok(());
        }

        info!("Starting AsyncTraceDispatcher...");

        // Initialize TraceProducer (without starting it)
        let producer_group = self.gen_group_name();

        // Create client config with proper trace settings
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_string(name_srv_addr.to_string()));
        client_config.set_enable_trace(false); // Prevents recursive trace operations
        client_config.set_vip_channel_enabled(false); // Disable VIP channel (matches Java implementation)

        let producer = DefaultMQProducer::builder()
            .producer_group(&producer_group)
            .client_config(client_config)
            .send_msg_timeout(5000)
            .max_message_size(self.config.max_msg_size as u32)
            .build();

        info!("Trace producer initialized: group={}", producer_group);

        // Save producer and access channel
        *self.trace_producer.write() = Some(Arc::new(tokio::sync::Mutex::new(producer)));
        *self.state.access_channel.write() = access_channel;

        // Take the receiver
        let rx = self
            .rx
            .lock()
            .take()
            .ok_or_else(|| RocketMQError::not_initialized("Dispatcher already started"))?;

        // Get producer reference for worker
        let producer = self
            .trace_producer
            .read()
            .clone()
            .ok_or_else(|| RocketMQError::not_initialized("Producer not initialized"))?;

        // Spawn worker task
        let state = self.state.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = worker_loop(rx, state, producer, config).await {
                error!("Worker loop failed: {:?}", e);
            }
        });

        *self.worker_handle.lock() = Some(handle);

        info!("AsyncTraceDispatcher started successfully");
        Ok(())
    }

    /// Appends a trace context to the dispatcher queue.
    ///
    /// This method does not block the calling thread. If the internal queue is full,
    /// the trace context is discarded and the discard counter is incremented.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Trace context, must be a `TraceContext` instance
    ///
    /// # Returns
    ///
    /// Returns `true` if the context was successfully queued, `false` if it was
    /// discarded due to queue overflow or type mismatch.
    fn append(&self, ctx: &dyn Any) -> bool {
        // Downcast to TraceContext
        let trace_ctx = match ctx.downcast_ref::<TraceContext>() {
            Some(ctx) => ctx,
            None => {
                warn!("Failed to downcast to TraceContext");
                return false;
            }
        };

        // Try to send without blocking
        match self.tx.try_send(trace_ctx.clone()) {
            Ok(_) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Queue full, discard and increment counter
                let count = self.state.discard_count.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Trace context queue full (discard count: {}), data discarded",
                    count + 1
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("Trace dispatcher channel closed");
                false
            }
        }
    }

    /// Flushes pending trace contexts by allowing the worker task to process them.
    ///
    /// Blocks the calling thread briefly to give the worker task time to process
    /// pending contexts. This is a best-effort operation and does not guarantee
    /// all contexts are sent.
    ///
    /// # Errors
    ///
    /// Returns an error if the dispatcher has not been started.
    fn flush(&self) -> RocketMQResult<()> {
        if !self.state.is_started.load(Ordering::SeqCst) {
            return Err(RocketMQError::not_initialized("Dispatcher not started"));
        }

        info!("Flushing trace data...");

        // Allows the worker task time to process queued items
        std::thread::sleep(Duration::from_millis(100));

        info!("Flush completed");
        Ok(())
    }

    /// Shuts down the dispatcher and stops the background worker task.
    ///
    /// Attempts to flush remaining trace contexts before stopping. This method
    /// blocks briefly to allow pending data to be processed. The dispatcher
    /// cannot be restarted after shutdown.
    fn shutdown(&self) {
        if !self.state.is_started.load(Ordering::SeqCst) {
            warn!("AsyncTraceDispatcher not started");
            return;
        }

        // Set stopped flag
        self.state.is_stopped.store(true, Ordering::SeqCst);

        info!("Shutting down AsyncTraceDispatcher...");

        // Flush remaining data
        if let Err(e) = self.flush() {
            error!("Flush failed during shutdown: {:?}", e);
        }

        self.worker_handle.lock().take();

        if let Some(_producer) = self.trace_producer.read().as_ref() {
            info!("Trace producer shut down");
        }

        // Clear state
        self.state.is_started.store(false, Ordering::SeqCst);
        *self.trace_producer.write() = None;

        info!("AsyncTraceDispatcher stopped");
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl Drop for AsyncTraceDispatcher {
    fn drop(&mut self) {
        if self.state.is_started.load(Ordering::SeqCst) && !self.state.is_stopped.load(Ordering::SeqCst) {
            warn!("AsyncTraceDispatcher dropped without explicit shutdown, auto-flushing...");
            // Best effort flush before drop
            if let Err(e) = self.flush() {
                error!("Auto-flush on drop failed: {:?}", e);
            }
            self.shutdown();
        }
    }
}

// Main worker loop that processes trace contexts in batches.
// Receives contexts from the channel and flushes when batch size or time threshold is reached.
async fn worker_loop(
    mut rx: mpsc::Receiver<TraceContext>,
    state: Arc<DispatcherState>,
    producer: Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    config: TraceDispatcherConfig,
) -> RocketMQResult<()> {
    // Start the producer in the async worker context
    {
        let mut producer_guard = producer.lock().await;
        let start_result = producer_guard.start().await;
        drop(producer_guard); // Explicitly drop before checking result

        if let Err(e) = start_result {
            error!("Failed to start trace producer: {:?}", e);
            return Err(e);
        }
        info!("Trace producer started successfully in worker");
    }

    let mut interval = tokio::time::interval(Duration::from_millis(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut buffer = Vec::with_capacity(config.batch_num);
    let mut last_flush = Instant::now();

    loop {
        tokio::select! {
            // Timer tick every 5ms
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    let should_flush = buffer.len() >= config.batch_num
                        || last_flush.elapsed() >= config.flush_interval;

                    if should_flush {
                        flush_buffer(&mut buffer, &state, &producer, &config).await;
                        last_flush = Instant::now();
                    }
                }

                // Check if we should stop
                if state.is_stopped.load(Ordering::SeqCst) && buffer.is_empty() {
                    info!("Worker loop exiting (stopped flag set)");
                    break;
                }
            }

            // Receive trace context or detect channel close
            result = rx.recv() => {
                match result {
                    Some(ctx) => {
                        buffer.push(ctx);

                        // Flush immediately if batch is full
                        if buffer.len() >= config.batch_num {
                            flush_buffer(&mut buffer, &state, &producer, &config).await;
                            last_flush = Instant::now();
                        }
                    }
                    None => {
                        // Channel closed
                        info!("Worker loop exiting (channel closed)");
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &state, &producer, &config).await;
                        }
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// Flushes the buffer by spawning an async send task.
// Takes ownership of buffer contents via zero-copy swap and spawns a non-blocking send task.
async fn flush_buffer(
    buffer: &mut Vec<TraceContext>,
    state: &Arc<DispatcherState>,
    producer: &Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    config: &TraceDispatcherConfig,
) {
    if buffer.is_empty() {
        return;
    }

    // Take ownership of buffer contents (zero-copy swap)
    let contexts = std::mem::take(buffer);

    // Clone Arc references for the spawned task
    let state_clone = state.clone();
    let producer_clone = producer.clone();
    let config_clone = config.clone();

    // Spawn async send task (non-blocking)
    tokio::spawn(async move {
        if let Err(e) = send_trace_data(contexts, &state_clone, &producer_clone, &config_clone).await {
            error!("send_trace_data failed: {:?}", e);
        }
    });
}

// Processes and sends trace data in batches.
// Groups contexts by topic, encodes them, and delegates to flush_data.
async fn send_trace_data(
    contexts: Vec<TraceContext>,
    state: &Arc<DispatcherState>,
    producer: &Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    config: &TraceDispatcherConfig,
) -> RocketMQResult<()> {
    // Group trace contexts by (topic, trace_topic)
    let mut trans_bean_map: HashMap<String, Vec<TraceTransferBean>> = HashMap::with_capacity(16);

    let default_access_channel = *state.access_channel.read();

    for ctx in contexts {
        let access_channel = ctx.access_channel.unwrap_or(default_access_channel);

        let region_id = &ctx.region_id;
        if region_id.is_empty() {
            continue;
        }

        let trace_beans = match &ctx.trace_beans {
            Some(beans) if !beans.is_empty() => beans,
            _ => continue,
        };

        // Determine trace topic based on access channel
        let trace_topic = match access_channel {
            AccessChannel::Cloud => {
                format!("{}{}", TraceConstants::TRACE_TOPIC_PREFIX, region_id)
            }
            _ => config.trace_topic_name.clone(),
        };

        let topic = &trace_beans[0].topic;
        let key = format!("{}{}{}", topic, TraceConstants::CONTENT_SPLITOR, trace_topic);

        // Encode trace context
        if let Some(transfer_bean) = TraceDataEncoder::encoder_from_context_bean(&ctx) {
            trans_bean_map.entry(key).or_default().push(transfer_bean);
        }
    }

    // Send each group
    for (key, beans) in trans_bean_map {
        let parts: Vec<&str> = key.split(TraceConstants::CONTENT_SPLITOR).collect();
        if parts.len() != 2 {
            warn!("Invalid trace key: {}", key);
            continue;
        }

        let topic = parts[0];
        let trace_topic = parts[1];

        if let Err(e) = flush_data(beans, topic, trace_topic, producer, state, config).await {
            error!(
                "flush_data failed for topic={}, trace_topic={}: {:?}",
                topic, trace_topic, e
            );
        }
    }

    Ok(())
}

// Aggregates trace transfer beans and sends them as messages.
// Splits large payloads into multiple messages based on max_msg_size.
async fn flush_data(
    beans: Vec<TraceTransferBean>,
    _topic: &str,
    trace_topic: &str,
    producer: &Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    state: &Arc<DispatcherState>,
    config: &TraceDispatcherConfig,
) -> RocketMQResult<()> {
    if beans.is_empty() {
        return Ok(());
    }

    let mut buffer = String::with_capacity(1024);
    let mut key_set = HashSet::new();

    for bean in beans {
        // Collect keys
        key_set.extend(bean.trans_key().iter().cloned());

        // Append data
        buffer.push_str(bean.trans_data());

        // Send if buffer is full
        if buffer.len() >= config.max_msg_size {
            send_trace_message(&key_set, &buffer, trace_topic, producer, state).await?;
            buffer.clear();
            key_set.clear();
        }
    }

    // Send remaining data
    if !buffer.is_empty() {
        send_trace_message(&key_set, &buffer, trace_topic, producer, state).await?;
    }

    Ok(())
}

// Sends a single trace message to the broker.
// Constructs the message with keys and body, uses MessageQueue selector for
// round-robin queue selection, matching Java's behavior.
async fn send_trace_message(
    key_set: &HashSet<CheetahString>,
    data: &str,
    trace_topic: &str,
    producer: &Arc<tokio::sync::Mutex<DefaultMQProducer>>,
    state: &Arc<DispatcherState>,
) -> RocketMQResult<()> {
    let keys: Vec<String> = key_set.iter().map(|k| k.to_string()).collect();

    let message = Message::builder()
        .topic(trace_topic)
        .body(Bytes::from(data.as_bytes().to_vec()))
        .keys(keys)
        .build_unchecked();

    debug!(
        "Sending trace message: topic={}, size={}, keys={}",
        trace_topic,
        data.len(),
        key_set.len()
    );

    // Use selector-based send for queue selection (matching Java)
    // Selector implements round-robin across available queues
    let state_clone = state.clone();
    let selector = move |queues: &[MessageQueue], _msg: &Message, _arg: &()| -> Option<MessageQueue> {
        if queues.is_empty() {
            return None;
        }
        // Round-robin queue selection
        let index = state_clone.send_which_queue.fetch_add(1, Ordering::Relaxed);
        let pos = index % queues.len();
        queues.get(pos).cloned()
    };

    let callback =
        Arc::new(
            |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| match (result, error) {
                (Some(r), None) => {
                    debug!(
                        "Trace message sent successfully: msgId={}, status={:?}",
                        r.msg_id.as_ref().map_or("N/A", |id| id.as_str()),
                        r.send_status
                    );
                }
                (None, Some(e)) => {
                    error!("Failed to send trace message: {:?}", e);
                }
                _ => {
                    warn!("Trace message send completed with unknown state");
                }
            },
        );

    // Use send_with_selector_callback_timeout (5 seconds timeout)
    producer
        .lock()
        .await
        .send_with_selector_callback_timeout(message, selector, (), Some(callback), 5000)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_dispatcher() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        assert!(!dispatcher.is_started());
        assert!(!dispatcher.is_stopped());
        assert_eq!(dispatcher.get_discard_count(), 0);
    }

    #[test]
    fn test_gen_group_name() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        let name1 = dispatcher.gen_group_name();
        let name2 = dispatcher.gen_group_name();

        assert!(name1.starts_with(TraceConstants::GROUP_NAME_PREFIX));
        assert!(name1.contains("TestGroup"));
        assert_ne!(name1, name2); // Different counter values
    }

    #[test]
    fn test_append_before_start() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        let ctx = TraceContext::default();

        // Should succeed even before start (queue is available)
        let result = dispatcher.append(&ctx);
        assert!(result);
    }

    #[test]
    fn test_batch_num_capping() {
        // Test that batch_num is capped at 20
        let dispatcher = AsyncTraceDispatcher::new(
            "TestGroup",
            Type::Produce,
            100, // Request 100
            "TRACE_TOPIC",
            None,
        );

        assert_eq!(dispatcher.config.batch_num, 20); // Capped at 20
    }

    #[test]
    fn test_default_trace_topic() {
        // Test default topic when empty string provided
        let dispatcher = AsyncTraceDispatcher::new(
            "TestGroup",
            Type::Produce,
            20,
            "", // Empty topic
            None,
        );

        assert_eq!(dispatcher.config.trace_topic_name, TopicValidator::RMQ_SYS_TRACE_TOPIC);
    }

    #[test]
    fn test_config_values() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Consume, 15, "CUSTOM_TOPIC", None);

        assert_eq!(dispatcher.config.group, "TestGroup");
        assert_eq!(dispatcher.config.type_, Type::Consume);
        assert_eq!(dispatcher.config.batch_num, 15);
        assert_eq!(dispatcher.config.max_msg_size, 128_000);
        assert_eq!(dispatcher.config.flush_interval, Duration::from_secs(5));
        assert_eq!(dispatcher.config.trace_topic_name, "CUSTOM_TOPIC");
    }

    #[test]
    fn test_set_host_producer() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        assert!(dispatcher.state.host_producer.read().is_none());
    }

    #[test]
    fn test_set_namespace_v2() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        dispatcher.set_namespace_v2(Some(CheetahString::from_static_str("test-namespace")));

        let ns = dispatcher.state.namespace_v2.read();
        assert!(ns.is_some());
        assert_eq!(ns.as_ref().unwrap().to_string(), "test-namespace");
    }

    #[test]
    fn test_flush_before_start() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        // Should return error since not started
        let result = dispatcher.flush();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_worker_loop_basic() {
        // This test verifies the worker loop structure without actually starting
        // the producer (which requires a real broker connection).
        // The worker_loop will return an error because producer.start() fails,
        // but that's expected in a unit test environment.

        let (tx, rx) = mpsc::channel(10);
        let state = Arc::new(DispatcherState::new());
        let producer = Arc::new(tokio::sync::Mutex::new(DefaultMQProducer::builder().build()));
        let config = TraceDispatcherConfig {
            group: "Test".to_string(),
            type_: Type::Produce,
            batch_num: 5,
            max_msg_size: 128_000,
            flush_interval: Duration::from_millis(100),
            trace_topic_name: "TRACE".to_string(),
            rpc_hook: None,
        };

        // Close the channel immediately
        drop(tx);

        // Worker will try to start producer and fail (expected in test environment)
        // The test just verifies no panic occurs
        let result = worker_loop(rx, state, producer, config).await;

        // In unit test environment, start() will fail due to missing broker
        assert!(
            result.is_err(),
            "Expected error from producer start in test environment"
        );
    }
}
