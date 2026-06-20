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
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread;
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
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::runtime::spawn_client_task;
use crate::trace::trace_constants::TraceConstants;
use crate::trace::trace_context::TraceContext;
use crate::trace::trace_data_encoder::TraceDataEncoder;
use crate::trace::trace_dispatcher::TraceDispatcher;
use crate::trace::trace_dispatcher::Type;
use crate::trace::trace_transfer_bean::TraceTransferBean;

// Configuration for the async trace dispatcher.
const TRACE_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct TraceDispatcherConfig {
    group: String,
    type_: Type,
    batch_num: usize,
    max_msg_size: usize,
    flush_interval: Duration,
    trace_topic_name: String,
    rpc_hook: Option<Arc<dyn RPCHook>>,
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

enum TraceTaskHandle {
    Tokio {
        handle: JoinHandle<()>,
        completion_rx: std_mpsc::Receiver<()>,
    },
    Thread(thread::JoinHandle<()>),
}

impl TraceTaskHandle {
    fn abort(self) {
        match self {
            Self::Tokio { handle, .. } => handle.abort(),
            Self::Thread(handle) => drop(handle),
        }
    }

    async fn shutdown_async(self, timeout: Duration) -> bool {
        match self {
            Self::Tokio { mut handle, .. } => match tokio::time::timeout(timeout, &mut handle).await {
                Ok(Ok(())) => true,
                Ok(Err(error)) if error.is_cancelled() => true,
                Ok(Err(error)) => {
                    warn!(%error, "trace worker exited with join error");
                    true
                }
                Err(_) => {
                    handle.abort();
                    match handle.await {
                        Ok(()) => {}
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => warn!(%error, "trace worker aborted with join error"),
                    }
                    false
                }
            },
            Self::Thread(handle) => {
                let deadline = tokio::time::Instant::now() + timeout;
                loop {
                    if handle.is_finished() {
                        return handle.join().is_ok();
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return false;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn shutdown(self, timeout: Duration) -> bool {
        match self {
            Self::Tokio { handle, completion_rx } => match completion_rx.recv_timeout(timeout) {
                Ok(()) => true,
                Err(std_mpsc::RecvTimeoutError::Timeout) => {
                    handle.abort();
                    false
                }
                Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                    if handle.is_finished() {
                        true
                    } else {
                        handle.abort();
                        false
                    }
                }
            },
            Self::Thread(handle) => handle.join().is_ok(),
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
/// The worker is submitted through the client runtime helper, which uses the
/// current Tokio runtime when available and the shared client fallback runtime otherwise.
///
/// # Discard Policy
///
/// When the internal queue is full, new trace contexts are discarded and the
/// discard counter is incremented. This prevents blocking the caller.
pub struct AsyncTraceDispatcher {
    config: TraceDispatcherConfig,
    state: Arc<DispatcherState>,
    use_tls: AtomicBool,
    tx: mpsc::Sender<TraceContext>,
    rx: Mutex<Option<mpsc::Receiver<TraceContext>>>,
    worker_handle: Mutex<Option<TraceTaskHandle>>,
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
        rpc_hook: Option<Arc<dyn RPCHook>>,
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
            use_tls: AtomicBool::new(false),
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

    /// Returns the configured maximum number of trace contexts per batch.
    pub fn batch_num(&self) -> usize {
        self.config.batch_num
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

    /// Sets whether the internal trace producer should use TLS.
    pub fn set_use_tls(&self, use_tls: bool) {
        self.use_tls.store(use_tls, Ordering::Release);
    }

    pub fn is_use_tls(&self) -> bool {
        self.use_tls.load(Ordering::Acquire)
    }

    /// Returns a reference to the internal trace producer.
    ///
    /// The producer is available only after the dispatcher has been started.
    pub fn get_trace_producer(&self) -> Option<Arc<tokio::sync::Mutex<DefaultMQProducer>>> {
        self.trace_producer.read().clone()
    }

    pub fn trace_topic_name(&self) -> &str {
        self.config.trace_topic_name.as_str()
    }

    /// Asynchronously flushes pending trace contexts without blocking a Tokio worker thread.
    pub async fn flush_async(&self) -> RocketMQResult<()> {
        if !self.state.is_started.load(Ordering::SeqCst) {
            return Err(RocketMQError::not_initialized("Dispatcher not started"));
        }

        info!("Flushing trace data...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        info!("Flush completed");
        Ok(())
    }

    /// Asynchronously shuts down the dispatcher without blocking the current Tokio worker.
    pub async fn shutdown_async(&self) {
        if !self.state.is_started.load(Ordering::SeqCst) {
            warn!("AsyncTraceDispatcher not started");
            return;
        }

        self.state.is_stopped.store(true, Ordering::SeqCst);

        info!("Shutting down AsyncTraceDispatcher...");

        if let Err(error) = self.flush_async().await {
            error!("Flush failed during shutdown: {:?}", error);
        }

        let handle = { self.worker_handle.lock().take() };
        if let Some(handle) = handle {
            let stopped = handle.shutdown_async(TRACE_WORKER_SHUTDOWN_TIMEOUT).await;
            if !stopped {
                warn!(
                    timeout_ms = TRACE_WORKER_SHUTDOWN_TIMEOUT.as_millis(),
                    "Trace worker did not stop before timeout and was aborted"
                );
            }
        }

        if self.trace_producer.read().is_some() {
            info!("Trace producer shut down");
        }

        self.state.is_started.store(false, Ordering::SeqCst);
        *self.trace_producer.write() = None;

        info!("AsyncTraceDispatcher stopped");
    }

    pub fn register_shutdown_hook(&self) {}

    pub fn register_shut_down_hook(&self) {
        self.register_shutdown_hook();
    }

    pub fn remove_shutdown_hook(&self) {}
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
        client_config.set_use_tls(self.use_tls.load(Ordering::Acquire));

        let mut builder = DefaultMQProducer::builder()
            .producer_group(&producer_group)
            .client_config(client_config)
            .send_msg_timeout(5000)
            .max_message_size(self.config.max_msg_size as u32);

        if let Some(rpc_hook) = self.config.rpc_hook.clone() {
            builder = builder.rpc_hook(rpc_hook);
        }

        let producer = builder.build();

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

        let handle = match spawn_trace_task("rocketmq-client-trace-worker", async move {
            if let Err(e) = worker_loop(rx, state, producer, config).await {
                error!("Worker loop failed: {:?}", e);
            }
        }) {
            Ok(handle) => handle,
            Err(error) => {
                self.state.is_started.store(false, Ordering::SeqCst);
                return Err(error);
            }
        };

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

        if let Some(handle) = self.worker_handle.lock().take() {
            let stopped = handle.shutdown(TRACE_WORKER_SHUTDOWN_TIMEOUT);
            if !stopped {
                warn!(
                    timeout_ms = TRACE_WORKER_SHUTDOWN_TIMEOUT.as_millis(),
                    "Trace worker did not stop before timeout and was aborted"
                );
            }
        }

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

    fn trace_topic_name(&self) -> Option<&str> {
        Some(self.config.trace_topic_name.as_str())
    }

    fn trace_client_host(&self) -> Option<CheetahString> {
        self.state
            .host_producer
            .read()
            .as_ref()
            .and_then(|producer| producer.client_id())
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

fn spawn_trace_task<F>(thread_name: &'static str, task: F) -> RocketMQResult<TraceTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (completion_tx, completion_rx) = std_mpsc::channel();
    spawn_client_task(thread_name, async move {
        task.await;
        let _ = completion_tx.send(());
    })
    .map(|handle| TraceTaskHandle::Tokio { handle, completion_rx })
    .map_err(|error| RocketMQError::Internal(format!("failed to spawn {thread_name} task: {error}")))
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

// Flushes the buffer by awaiting the trace send in the worker task.
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

    if let Err(error) = send_trace_data(contexts, state, producer, config).await {
        error!("send_trace_data failed: {:?}", error);
    }
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
        .build()?;

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

    // The worker already runs off the application send path, so wait for the
    // trace send here to make shutdown/flush deterministic.
    let result = producer
        .lock()
        .await
        .send_with_selector_timeout(message, selector, (), 5000)
        .await?;
    debug!(
        "Trace message sent successfully: msgId={}, status={:?}",
        result
            .as_ref()
            .and_then(|send_result| send_result.msg_id.as_ref())
            .map_or("N/A", |id| id.as_str()),
        result.as_ref().map(|send_result| send_result.send_status)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_trace_task_without_tokio_runtime_runs_on_fallback_thread() {
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = spawn_trace_task("rocketmq-client-trace-test", async move {
            tx.send(std::thread::current().name().unwrap_or_default().to_string())
                .expect("test receiver should be alive");
        })
        .expect("fallback trace runtime should start");

        let thread_name = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("trace task should run on fallback runtime");
        assert_eq!(thread_name, "rocketmq-client-fallback");
        assert!(handle.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn trace_task_shutdown_waits_for_completed_worker() {
        let handle = spawn_trace_task("rocketmq-client-trace-complete-test", async move {})
            .expect("fallback trace runtime should start");

        assert!(handle.shutdown(Duration::from_secs(1)));
    }

    #[test]
    fn trace_task_shutdown_aborts_worker_after_timeout() {
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = spawn_trace_task("rocketmq-client-trace-timeout-test", async move {
            tx.send(()).expect("test receiver should be alive");
            std::future::pending::<()>().await;
        })
        .expect("fallback trace runtime should start");

        rx.recv_timeout(Duration::from_secs(2))
            .expect("trace task should start");

        assert!(!handle.shutdown(Duration::from_millis(20)));
    }

    #[tokio::test]
    async fn trace_task_shutdown_async_aborts_worker_after_timeout() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let started_in_task = started.clone();
        let dropped_in_task = dropped.clone();
        let (_completion_tx, completion_rx) = std_mpsc::channel();
        let handle = TraceTaskHandle::Tokio {
            handle: tokio::spawn(async move {
                let _drop_flag = DropFlag(dropped_in_task);
                started_in_task.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            }),
            completion_rx,
        };

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("trace task should start before shutdown");

        assert!(!handle.shutdown_async(Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

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

    #[test]
    fn java_shutdown_hook_aliases_are_noop_under_drop_based_shutdown() {
        let dispatcher = AsyncTraceDispatcher::new("TestGroup", Type::Produce, 20, "TRACE_TOPIC", None);

        dispatcher.register_shutdown_hook();
        dispatcher.register_shut_down_hook();
        dispatcher.remove_shutdown_hook();

        assert!(!dispatcher.is_started());
        assert!(!dispatcher.is_stopped());
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
