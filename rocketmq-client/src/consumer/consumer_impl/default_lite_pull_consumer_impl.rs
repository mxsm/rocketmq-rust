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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::assigned_message_queue::AssignedMessageQueue;
use crate::consumer::consumer_impl::lite_pull_consume_request::LitePullConsumeRequest;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::mq_client_manager::MQClientManager;

/// Subscription mode for lite pull consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionType {
    /// No subscription type set yet.
    None,
    /// Subscribe mode with automatic rebalance.
    Subscribe,
    /// Assign mode with manual queue assignment.
    Assign,
}

/// Configuration specific to lite pull consumer.
#[derive(Clone)]
pub struct LitePullConsumerConfig {
    /// Consumer group name.
    pub consumer_group: CheetahString,

    /// Message model (clustering or broadcasting).
    pub message_model: MessageModel,

    /// Where to start consuming from when no offset exists.
    pub consume_from_where: ConsumeFromWhere,

    /// Timestamp to consume from (when consume_from_where is CONSUME_FROM_TIMESTAMP).
    pub consume_timestamp: Option<CheetahString>,

    /// Strategy for allocating message queues among consumers.
    pub allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,

    /// Number of messages to pull in a single request.
    pub pull_batch_size: i32,

    /// Number of concurrent pull threads.
    pub pull_thread_nums: usize,

    /// Maximum number of messages cached per queue.
    pub pull_threshold_for_queue: i64,

    /// Maximum size in MiB of messages cached per queue.
    pub pull_threshold_size_for_queue: i32,

    /// Maximum total number of cached messages across all queues (-1 to disable).
    pub pull_threshold_for_all: i64,

    /// Maximum offset span allowed in a process queue.
    pub consume_max_span: i64,

    /// Delay in milliseconds when pull encounters an exception.
    pub pull_time_delay_millis_when_exception: u64,

    /// Delay in milliseconds when cache flow control is triggered.
    pub pull_time_delay_millis_when_cache_flow_control: u64,

    /// Delay in milliseconds when broker flow control is triggered.
    pub pull_time_delay_millis_when_broker_flow_control: u64,

    /// Default timeout for poll operations in milliseconds.
    pub poll_timeout_millis: u64,

    /// Whether to automatically commit offsets.
    pub auto_commit: bool,

    /// Interval in milliseconds between automatic offset commits.
    pub auto_commit_interval_millis: u64,

    /// Interval in milliseconds for checking topic metadata changes.
    pub topic_metadata_check_interval_millis: u64,

    /// Message request mode (pull or pop).
    pub message_request_mode: MessageRequestMode,
}

impl Default for LitePullConsumerConfig {
    fn default() -> Self {
        Self {
            consumer_group: CheetahString::from_static_str("DEFAULT_CONSUMER"),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: None,
            allocate_message_queue_strategy: Arc::new(
                crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely,
            ),
            pull_batch_size: 10,
            pull_thread_nums: 20,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_all: -1,
            consume_max_span: 2000,
            pull_time_delay_millis_when_exception: 1000,
            pull_time_delay_millis_when_cache_flow_control: 50,
            pull_time_delay_millis_when_broker_flow_control: 20,
            poll_timeout_millis: 5000,
            auto_commit: true,
            auto_commit_interval_millis: 5000,
            topic_metadata_check_interval_millis: 10000,
            message_request_mode: MessageRequestMode::Pull,
        }
    }
}

impl LitePullConsumerConfig {
    /// Converts LitePullConsumerConfig to ConsumerConfig for rebalance.
    fn to_consumer_config(&self) -> ArcMut<ConsumerConfig> {
        ArcMut::new(ConsumerConfig {
            consumer_group: self.consumer_group.clone(),
            topic: CheetahString::from_static_str(""),
            sub_expression: CheetahString::from_static_str("*"),
            message_model: self.message_model,
            consume_from_where: self.consume_from_where,
            consume_timestamp: self.consume_timestamp.clone(),
            allocate_message_queue_strategy: Some(self.allocate_message_queue_strategy.clone()),
            subscription: ArcMut::new(HashMap::new()),
            message_listener: None,
            message_queue_listener: None,
            consume_thread_min: 20,
            consume_thread_max: 20,
            adjust_thread_pool_nums_threshold: 100000,
            consume_concurrently_max_span: 2000,
            pull_threshold_for_queue: self.pull_threshold_for_queue as u32,
            pop_threshold_for_queue: 1024,
            pull_threshold_size_for_queue: self.pull_threshold_size_for_queue as u32,
            pull_threshold_for_topic: -1,
            pull_threshold_size_for_topic: -1,
            pull_interval: 0,
            consume_message_batch_max_size: 1,
            pull_batch_size: self.pull_batch_size as u32,
            pull_batch_size_in_bytes: 0,
            post_subscription_when_pull: false,
            unit_mode: false,
            max_reconsume_times: -1,
            suspend_current_queue_time_millis: 1000,
            consume_timeout: 15,
            pop_invisible_time: 60000,
            pop_batch_nums: 32,
            await_termination_millis_when_shutdown: 0,
            trace_dispatcher: None,
            client_rebalance: false,
            rpc_hook: None,
        })
    }
}

/// Service state for lifecycle management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    /// Just created, not started yet.
    CreateJust,
    /// Running.
    Running,
    /// Shutdown completed.
    ShutdownAlready,
    /// Start failed.
    StartFailed,
}

/// Core implementation of lite pull consumer.
pub struct DefaultLitePullConsumerImpl {
    // Configuration
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<LitePullConsumerConfig>,

    // Lifecycle state
    service_state: ArcMut<ServiceState>,
    subscription_type: Arc<RwLock<SubscriptionType>>,
    consumer_start_timestamp: AtomicI64,

    // Core components
    client_instance: Option<ArcMut<MQClientInstance>>,
    rebalance_impl: ArcMut<RebalanceLitePullImpl>,
    pull_api_wrapper: Option<ArcMut<PullAPIWrapper>>,
    offset_store: Option<ArcMut<OffsetStore>>,

    // Queue management
    assigned_message_queue: Arc<AssignedMessageQueue>,
    message_queue_locks: Arc<RwLock<HashMap<MessageQueue, Arc<Mutex<()>>>>>,

    // Pull task scheduling
    task_handles: Arc<RwLock<HashMap<MessageQueue, JoinHandle<()>>>>,

    // Message flow (unbounded channel for non-blocking pull)
    consume_request_tx: mpsc::UnboundedSender<LitePullConsumeRequest>,
    consume_request_rx: Arc<Mutex<mpsc::UnboundedReceiver<LitePullConsumeRequest>>>,

    // ASSIGN mode subscriptions
    topic_to_sub_expression: Arc<RwLock<HashMap<CheetahString, CheetahString>>>,

    // Flow control metrics
    consume_request_flow_control_times: Arc<AtomicU64>,
    queue_flow_control_times: Arc<AtomicU64>,
    queue_max_span_flow_control_times: Arc<AtomicU64>,

    // Auto-commit
    next_auto_commit_deadline: Arc<AtomicI64>,

    // Hooks
    consume_message_hook_list: Vec<Arc<dyn ConsumeMessageHook + Send + Sync>>,
    filter_message_hook_list: Vec<Arc<dyn FilterMessageHook + Send + Sync>>,

    // Topic change listeners
    topic_message_queue_change_listener_map:
        Arc<RwLock<HashMap<CheetahString, Arc<dyn TopicMessageQueueChangeListener + Send + Sync>>>>,

    // Shutdown coordination
    shutdown_signal: Arc<AtomicBool>,

    // Self-reference (for callbacks)
    default_lite_pull_consumer_impl: Option<ArcMut<DefaultLitePullConsumerImpl>>,
}

impl DefaultLitePullConsumerImpl {
    /// Creates a new lite pull consumer implementation.
    pub fn new(client_config: ArcMut<ClientConfig>, consumer_config: ArcMut<LitePullConsumerConfig>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            client_config,
            consumer_config: consumer_config.clone(),
            service_state: ArcMut::new(ServiceState::CreateJust),
            subscription_type: Arc::new(RwLock::new(SubscriptionType::None)),
            consumer_start_timestamp: AtomicI64::new(0),
            client_instance: None,
            rebalance_impl: ArcMut::new(RebalanceLitePullImpl::new(consumer_config.to_consumer_config())),
            pull_api_wrapper: None,
            offset_store: None,
            assigned_message_queue: Arc::new(AssignedMessageQueue::new()),
            message_queue_locks: Arc::new(RwLock::new(HashMap::new())),
            task_handles: Arc::new(RwLock::new(HashMap::new())),
            consume_request_tx: tx,
            consume_request_rx: Arc::new(Mutex::new(rx)),
            topic_to_sub_expression: Arc::new(RwLock::new(HashMap::new())),
            consume_request_flow_control_times: Arc::new(AtomicU64::new(0)),
            queue_flow_control_times: Arc::new(AtomicU64::new(0)),
            queue_max_span_flow_control_times: Arc::new(AtomicU64::new(0)),
            next_auto_commit_deadline: Arc::new(AtomicI64::new(0)),
            consume_message_hook_list: Vec::new(),
            filter_message_hook_list: Vec::new(),
            topic_message_queue_change_listener_map: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            default_lite_pull_consumer_impl: None,
        }
    }

    /// Sets the self-reference for callbacks.
    pub fn set_default_lite_pull_consumer_impl(
        &mut self,
        default_lite_pull_consumer_impl: ArcMut<DefaultLitePullConsumerImpl>,
    ) {
        self.default_lite_pull_consumer_impl = Some(default_lite_pull_consumer_impl);
    }

    /// Validates service state is Running.
    #[inline]
    fn make_sure_state_ok(&self) -> RocketMQResult<()> {
        if *self.service_state != ServiceState::Running {
            return Err(crate::mq_client_err!(format!(
                "The lite pull consumer service state not OK, {:?}, {}",
                *self.service_state,
                rocketmq_common::common::FAQUrl::suggest_todo(rocketmq_common::common::FAQUrl::CLIENT_SERVICE_NOT_OK)
            )));
        }
        Ok(())
    }

    /// Returns the current service state.
    pub fn service_state(&self) -> ServiceState {
        *self.service_state
    }

    /// Validates configuration before starting.
    fn check_config(&self) -> RocketMQResult<()> {
        if self.consumer_config.consumer_group.is_empty() {
            return Err(crate::mq_client_err!("Consumer group cannot be empty"));
        }

        if self.consumer_config.pull_batch_size < 1 || self.consumer_config.pull_batch_size > 1024 {
            return Err(crate::mq_client_err!(format!(
                "pullBatchSize must be in [1, 1024], current value: {}",
                self.consumer_config.pull_batch_size
            )));
        }

        if self.consumer_config.pull_threshold_for_queue < 1 || self.consumer_config.pull_threshold_for_queue > 65535 {
            return Err(crate::mq_client_err!(format!(
                "pullThresholdForQueue must be in [1, 65535], current value: {}",
                self.consumer_config.pull_threshold_for_queue
            )));
        }

        if self.consumer_config.poll_timeout_millis == 0 {
            return Err(crate::mq_client_err!("pollTimeoutMillis cannot be 0"));
        }

        Ok(())
    }

    /// Sets subscription type and validates no conflicts.
    async fn set_subscription_type(&self, sub_type: SubscriptionType) -> RocketMQResult<()> {
        let mut subscription_type = self.subscription_type.write().await;
        if *subscription_type == SubscriptionType::None {
            *subscription_type = sub_type;
            Ok(())
        } else if *subscription_type != sub_type {
            Err(crate::mq_client_err!("Subscribe and assign are mutually exclusive."))
        } else {
            Ok(())
        }
    }

    /// Subscribes to a topic with optional tag expression filter.
    pub async fn subscribe(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let sub_expression = sub_expression.into();

        if topic.is_empty() {
            return Err(crate::mq_client_err!("Topic cannot be empty"));
        }

        self.set_subscription_type(SubscriptionType::Subscribe).await?;

        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_expression)
            .map_err(|e| crate::mq_client_err!(format!("Failed to build subscription data: {}", e)))?;

        self.rebalance_impl.put_subscription_data(topic, subscription_data);

        if *self.service_state == ServiceState::Running {
            if let Some(ref client_instance) = self.client_instance {
                client_instance.send_heartbeat_to_all_broker_with_lock().await;
                self.update_topic_subscribe_info_when_subscription_changed().await?;
            }
        }

        Ok(())
    }

    /// Manually assigns specific message queues (ASSIGN mode).
    pub async fn assign(&mut self, message_queues: Vec<MessageQueue>) -> RocketMQResult<()> {
        if message_queues.is_empty() {
            return Err(crate::mq_client_err!("Message queues cannot be empty"));
        }

        self.set_subscription_type(SubscriptionType::Assign).await?;

        self.update_assigned_message_queue_for_assign(&message_queues).await;

        if *self.service_state == ServiceState::Running {
            self.update_pull_task_for_assign(&message_queues).await?;
        }

        Ok(())
    }

    /// Updates assigned message queues for ASSIGN mode.
    async fn update_assigned_message_queue_for_assign(&self, assigned: &[MessageQueue]) {
        let assigned_set: HashSet<MessageQueue> = assigned.iter().cloned().collect();

        let to_remove: Vec<MessageQueue> = {
            let task_handles = self.task_handles.read().await;
            task_handles
                .keys()
                .filter(|mq| !assigned_set.contains(mq))
                .cloned()
                .collect()
        };

        for mq in &to_remove {
            if let Some(pq) = self.assigned_message_queue.remove(mq).await {
                pq.set_dropped(true);
            }
        }

        if !to_remove.is_empty() {
            let mut task_handles = self.task_handles.write().await;
            for mq in &to_remove {
                task_handles.remove(mq);
            }
        }

        for mq in assigned {
            self.assigned_message_queue.put(mq.clone()).await;
        }
    }

    /// Starts pull tasks for assigned queues in ASSIGN mode.
    async fn update_pull_task_for_assign(&self, assigned: &[MessageQueue]) -> RocketMQResult<()> {
        let to_start: Vec<MessageQueue> = {
            let task_handles = self.task_handles.read().await;
            assigned
                .iter()
                .filter(|mq| !task_handles.contains_key(mq))
                .cloned()
                .collect()
        };

        for mq in to_start {
            self.start_pull_task(mq).await?;
        }
        Ok(())
    }

    /// Updates topic subscription info when subscription changes (SUBSCRIBE mode).
    async fn update_topic_subscribe_info_when_subscription_changed(&mut self) -> RocketMQResult<()> {
        let subscription_inner = self.rebalance_impl.get_subscription_inner();
        if let Some(ref mut client_instance) = self.client_instance {
            for entry in subscription_inner.iter() {
                let topic = entry.key();
                client_instance
                    .update_topic_route_info_from_name_server_topic(topic)
                    .await;
            }
        }
        Ok(())
    }

    /// Starts the lite pull consumer.
    pub async fn start(&mut self) -> RocketMQResult<()> {
        match *self.service_state {
            ServiceState::CreateJust => {
                info!(
                    "DefaultLitePullConsumerImpl [{}] starting. message_model={:?}",
                    self.consumer_config.consumer_group, self.consumer_config.message_model
                );

                *self.service_state = ServiceState::StartFailed;

                self.check_config()?;

                if self.consumer_config.message_model == MessageModel::Clustering {
                    self.client_config.change_instance_name_to_pid();
                }

                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(self.client_config.as_ref().clone(), None);
                self.client_instance = Some(client_instance.clone());

                self.rebalance_impl
                    .set_consumer_group(self.consumer_config.consumer_group.clone());
                self.rebalance_impl
                    .set_message_model(self.consumer_config.message_model);
                self.rebalance_impl
                    .set_allocate_message_queue_strategy(self.consumer_config.allocate_message_queue_strategy.clone());
                self.rebalance_impl.set_mq_client_factory(client_instance.clone());

                if self.pull_api_wrapper.is_none() {
                    let mut pull_api_wrapper = PullAPIWrapper::new(
                        client_instance.clone(),
                        self.consumer_config.consumer_group.clone(),
                        false, // unit_mode
                    );
                    pull_api_wrapper.register_filter_message_hook(self.filter_message_hook_list.clone());
                    self.pull_api_wrapper = Some(ArcMut::new(pull_api_wrapper));
                }

                // Initialize OffsetStore based on message model
                let offset_store = ArcMut::new(match self.consumer_config.message_model {
                    MessageModel::Broadcasting => OffsetStore::new_with_local(LocalFileOffsetStore::new(
                        client_instance.clone(),
                        self.consumer_config.consumer_group.clone(),
                    )),
                    MessageModel::Clustering => OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                        client_instance.clone(),
                        self.consumer_config.consumer_group.clone(),
                    )),
                });

                offset_store.load().await?;

                self.rebalance_impl.set_offset_store(offset_store.clone());

                // Store offset_store in self for later use (e.g., persist_consumer_offset)
                self.offset_store = Some(offset_store);

                // Consumer registration integrated through MQClientInstance rebalance mechanism

                let cloned = self.client_instance.as_mut().cloned().unwrap();
                self.client_instance.as_mut().unwrap().start(cloned).await?;

                self.consumer_start_timestamp
                    .store(current_millis() as i64, Ordering::Release);

                *self.service_state = ServiceState::Running;

                info!(
                    "DefaultLitePullConsumerImpl [{}] started successfully",
                    self.consumer_config.consumer_group
                );

                Ok(())
            }
            ServiceState::Running => Err(crate::mq_client_err!("The lite pull consumer is already running")),
            ServiceState::ShutdownAlready => Err(crate::mq_client_err!("The lite pull consumer has been shutdown")),
            ServiceState::StartFailed => Err(crate::mq_client_err!(format!(
                "The lite pull consumer start failed, current state: {:?}",
                *self.service_state
            ))),
        }
    }

    /// Shuts down the lite pull consumer gracefully.
    pub async fn shutdown(&mut self) -> RocketMQResult<()> {
        match *self.service_state {
            ServiceState::Running => {
                info!(
                    "DefaultLitePullConsumerImpl [{}] shutting down",
                    self.consumer_config.consumer_group
                );

                self.shutdown_signal.store(true, Ordering::Release);

                // Wait for all pull tasks to complete (5s timeout)
                let mut handles = self.task_handles.write().await;
                for (mq, handle) in handles.drain() {
                    if let Err(e) = tokio::time::timeout(Duration::from_secs(5), handle).await {
                        warn!("Pull task for {:?} did not finish in time: {}", mq, e);
                    }
                }
                drop(handles);

                self.persist_consumer_offset().await;

                if let Some(client) = self.client_instance.as_mut() {
                    client.unregister_consumer(&self.consumer_config.consumer_group).await;
                }

                if let Some(mut client) = self.client_instance.take() {
                    client.shutdown().await;
                }

                *self.service_state = ServiceState::ShutdownAlready;

                info!(
                    "DefaultLitePullConsumerImpl [{}] shutdown successfully",
                    self.consumer_config.consumer_group
                );

                Ok(())
            }
            ServiceState::CreateJust => {
                *self.service_state = ServiceState::ShutdownAlready;
                Ok(())
            }
            ServiceState::ShutdownAlready => {
                warn!("The lite pull consumer has already been shutdown");
                Ok(())
            }
            ServiceState::StartFailed => {
                *self.service_state = ServiceState::ShutdownAlready;
                Ok(())
            }
        }
    }

    /// Spawns an async pull task for a message queue.
    async fn start_pull_task(&self, mq: MessageQueue) -> RocketMQResult<()> {
        {
            let task_handles = self.task_handles.read().await;
            if task_handles.contains_key(&mq) {
                return Ok(());
            }
        }

        let default_impl = self
            .default_lite_pull_consumer_impl
            .clone()
            .ok_or_else(|| crate::mq_client_err!("Consumer self-reference not initialized"))?;
        let shutdown_signal = self.shutdown_signal.clone();
        let assigned_mq = self.assigned_message_queue.clone();
        let mq_clone = mq.clone();

        let handle = tokio::spawn(async move {
            loop {
                if shutdown_signal.load(Ordering::Acquire) {
                    break;
                }

                let pq = match assigned_mq.get_process_queue(&mq_clone).await {
                    Some(pq) if !pq.is_dropped() => pq,
                    _ => break,
                };

                if assigned_mq.is_paused(&mq_clone).await {
                    tokio::time::sleep(Duration::from_millis(
                        default_impl.consumer_config.pull_time_delay_millis_when_exception,
                    ))
                    .await;
                    continue;
                }

                match default_impl.pull_inner(&mq_clone, &pq).await {
                    Ok(delay) => {
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Pull error for {:?}: {}", mq_clone, e);
                        tokio::time::sleep(Duration::from_millis(
                            default_impl.consumer_config.pull_time_delay_millis_when_exception,
                        ))
                        .await;
                    }
                }
            }
        });

        let mut task_handles = self.task_handles.write().await;
        match task_handles.entry(mq) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(handle);
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                handle.abort();
            }
        }
        Ok(())
    }

    /// Core pull logic (returns delay in milliseconds).
    async fn pull_inner(
        &self,
        _mq: &MessageQueue,
        _process_queue: &Arc<crate::consumer::consumer_impl::process_queue::ProcessQueue>,
    ) -> RocketMQResult<u64> {
        unimplemented!("pull_inner")
    }

    /// Check all flow control conditions.
    async fn check_flow_control(
        &self,
        _mq: &MessageQueue,
        pq: &Arc<crate::consumer::consumer_impl::process_queue::ProcessQueue>,
    ) -> RocketMQResult<Option<u64>> {
        let config = &self.consumer_config;

        // Global cache flow control
        if config.pull_threshold_for_all > 0 {
            let cache_msg_count = self.get_cached_message_count().await;
            if cache_msg_count > config.pull_threshold_for_all {
                self.consume_request_flow_control_times.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(config.pull_time_delay_millis_when_cache_flow_control));
            }
        }

        // Per-queue message count
        let cached_msg_count = pq.msg_count() as i64;
        if cached_msg_count > config.pull_threshold_for_queue {
            self.queue_flow_control_times.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(config.pull_time_delay_millis_when_cache_flow_control));
        }

        // Per-queue message size
        let cached_msg_size = (pq.msg_size() / (1024 * 1024)) as i64;
        if cached_msg_size > config.pull_threshold_size_for_queue as i64 {
            self.queue_flow_control_times.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(config.pull_time_delay_millis_when_cache_flow_control));
        }

        // Offset span control
        let max_span = pq.get_max_span().await;
        if max_span as i64 > config.consume_max_span {
            self.queue_max_span_flow_control_times.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(config.pull_time_delay_millis_when_cache_flow_control));
        }

        Ok(None)
    }

    /// Get total cached message count across all queues.
    async fn get_cached_message_count(&self) -> i64 {
        self.assigned_message_queue.total_msg_count().await
    }

    /// Get total cached message size in MiB.
    async fn get_cached_message_size_in_mib(&self) -> i64 {
        self.assigned_message_queue.total_msg_size_in_mib().await
    }
}

impl MQConsumerInner for DefaultLitePullConsumerImpl {
    fn group_name(&self) -> CheetahString {
        self.consumer_config.consumer_group.clone()
    }

    fn message_model(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    fn consume_type(&self) -> ConsumeType {
        // Lite pull consumer uses active consumption model
        ConsumeType::ConsumeActively
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        let sub_inner = self.rebalance_impl.get_subscription_inner();
        sub_inner.iter().map(|entry| entry.value().clone()).collect()
    }

    async fn do_rebalance(&self) {
        if *self.subscription_type.read().await == SubscriptionType::Subscribe {
            self.rebalance_impl.mut_from_ref().do_rebalance(false).await;
        }
    }

    async fn try_rebalance(&self) -> RocketMQResult<bool> {
        self.do_rebalance().await;
        Ok(true)
    }

    async fn persist_consumer_offset(&self) {
        // Offset persistence handled by commit operations
        unimplemented!("persist_consumer_offset")
    }

    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        let subscription_inner = self.rebalance_impl.get_subscription_inner();
        if subscription_inner.contains_key(&topic) {
            self.rebalance_impl
                .rebalance_impl_inner
                .topic_subscribe_info_table
                .write()
                .await
                .insert(topic, info.clone());
        }
    }

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        let subscription_inner = self.rebalance_impl.get_subscription_inner();

        for entry in subscription_inner.iter() {
            if entry.key().as_str() == topic {
                let contains = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .topic_subscribe_info_table
                    .read()
                    .await
                    .contains_key(entry.key());
                return !contains;
            }
        }
        false
    }

    fn is_unit_mode(&self) -> bool {
        self.client_config.unit_mode
    }

    fn consumer_running_info(&self) -> rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo {
        rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::default()
    }
}
