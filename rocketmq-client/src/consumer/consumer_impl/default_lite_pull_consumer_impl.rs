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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
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
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_context::ConsumeMessageContext;
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

/// Wrapper to adapt MessageQueueListener to TopicMessageQueueChangeListener.
struct MessageQueueListenerWrapper<T: crate::consumer::message_queue_listener::MessageQueueListener> {
    listener: T,
}

impl<T: crate::consumer::message_queue_listener::MessageQueueListener> TopicMessageQueueChangeListener
    for MessageQueueListenerWrapper<T>
{
    fn on_changed(&self, topic: &str, message_queues: HashSet<MessageQueue>) {
        // Convert HashSet for both all and assigned parameters
        let all_set: HashSet<_> = message_queues.iter().cloned().collect();
        let assigned_set = message_queues;

        self.listener.message_queue_changed(topic, &all_set, &assigned_set);
    }
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

    /// Returns whether the consumer is currently running.
    pub async fn is_running(&self) -> bool {
        matches!(*self.service_state, ServiceState::Running)
    }

    /// Subscribes to a topic with a listener for queue assignment changes.
    pub async fn subscribe_with_listener<L>(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
        listener: L,
    ) -> RocketMQResult<()>
    where
        L: crate::consumer::message_queue_listener::MessageQueueListener + Send + Sync + 'static,
    {
        let topic = topic.into();
        let sub_expression = sub_expression.into();

        // First do the regular subscribe
        self.subscribe(topic.clone(), sub_expression).await?;

        // Then register the listener
        let listener_arc: Arc<dyn TopicMessageQueueChangeListener + Send + Sync> =
            Arc::new(MessageQueueListenerWrapper { listener });
        self.register_topic_message_queue_change_listener(topic, listener_arc)
            .await
    }

    /// Subscribes to a topic with a message selector.
    pub async fn subscribe_with_selector(
        &mut self,
        topic: impl Into<CheetahString>,
        selector: Option<crate::consumer::message_selector::MessageSelector>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();

        match selector {
            Some(sel) => {
                let sub_expression = sel.get_expression();
                self.subscribe(topic, sub_expression).await
            }
            None => self.subscribe(topic, "*").await,
        }
    }

    /// Checks if a message queue is paused.
    pub async fn is_paused(&self, message_queue: &MessageQueue) -> bool {
        self.assigned_message_queue.is_paused(message_queue).await
    }

    /// Updates the name server addresses.
    #[allow(unused_variables)]
    pub async fn update_name_server_address(&self, addresses: Vec<String>) {
        // Update client config using ArcMut's interior mutability
        let joined_addr = addresses.join(";");
        self.client_config.mut_from_ref().set_namesrv_addr(joined_addr.into());

        // Note: Client instance update will happen automatically on next request
    }

    /// Registers a listener for topic message queue changes.
    pub async fn register_topic_message_queue_change_listener(
        &self,
        topic: impl Into<CheetahString>,
        listener: Arc<dyn TopicMessageQueueChangeListener + Send + Sync>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let mut listeners = self.topic_message_queue_change_listener_map.write().await;
        listeners.insert(topic, listener);
        Ok(())
    }

    /// Sets the subscription expression for a topic in assign mode.
    pub async fn set_sub_expression_for_assign(
        &self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) {
        let topic = topic.into();
        let sub_expression = sub_expression.into();

        let mut map = self.topic_to_sub_expression.write().await;
        map.insert(topic, sub_expression);
    }

    /// Alias for poll with explicit timeout parameter naming.
    pub async fn poll_with_timeout(&self, timeout_millis: u64) -> RocketMQResult<Vec<ArcMut<MessageExt>>> {
        self.poll(timeout_millis).await
    }

    /// Builds subscription information for heartbeat.
    pub async fn build_subscriptions_for_heartbeat(
        &self,
        sub_expression_map: &mut HashMap<String, crate::consumer::message_selector::MessageSelector>,
    ) -> RocketMQResult<()> {
        // Get subscriptions from rebalance impl
        let subscriptions = self.rebalance_impl.get_subscription_inner();

        for item in subscriptions.iter() {
            let topic = item.key().to_string();
            let sub_data = item.value();
            let selector = crate::consumer::message_selector::MessageSelector::by_tag(&sub_data.sub_string);
            sub_expression_map.insert(topic, selector);
        }

        Ok(())
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

    /// Polls for messages with the specified timeout.
    pub async fn poll(&self, timeout_millis: u64) -> RocketMQResult<Vec<ArcMut<MessageExt>>> {
        self.make_sure_state_ok()?;

        if self.consumer_config.auto_commit {
            self.maybe_auto_commit().await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(Vec::new());
            }

            // Only hold lock during receive, release before async operations.
            let request = {
                let mut rx = self.consume_request_rx.lock().await;
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(req)) => req,
                    Ok(None) => return Ok(Vec::new()),
                    Err(_) => return Ok(Vec::new()),
                }
            };

            // Filter dropped queues without holding lock.
            if request.process_queue.is_dropped() {
                continue;
            }

            let mut messages = request.messages;
            // Execute async operations without holding the lock.
            let offset = request.process_queue.remove_message(&messages).await;
            self.assigned_message_queue
                .update_consume_offset(&request.message_queue, offset)
                .await;

            // Reset topic to remove namespace if configured
            self.reset_topic(&mut messages);

            // Execute consume message hooks if registered
            if !self.consume_message_hook_list.is_empty() {
                let mut context = ConsumeMessageContext::new(self.consumer_config.consumer_group.clone(), &messages)
                    .with_mq(request.message_queue.clone())
                    .with_namespace(self.client_config.namespace.clone().unwrap_or_default())
                    .with_access_channel(self.client_config.access_channel);

                // Execute hook before with panic protection
                for hook in &self.consume_message_hook_list {
                    if let Err(err) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hook.consume_message_before(&mut context);
                    })) {
                        tracing::error!(
                            "consumeMessageHook {} executeHookBefore panicked: {:?}",
                            hook.hook_name(),
                            err
                        );
                    }
                }

                // Update context for successful consumption
                context.status = CheetahString::from_static_str("CONSUME_SUCCESS");
                context.success = true;

                // Execute hook after with panic protection
                for hook in &self.consume_message_hook_list {
                    if let Err(err) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hook.consume_message_after(&mut context);
                    })) {
                        tracing::error!(
                            "consumeMessageHook {} executeHookAfter panicked: {:?}",
                            hook.hook_name(),
                            err
                        );
                    }
                }
            }

            return Ok(messages);
        }
    }

    /// Checks if auto-commit is needed and performs it.
    async fn maybe_auto_commit(&self) {
        let now = current_millis() as i64;
        let next_deadline = self.next_auto_commit_deadline.load(Ordering::Acquire);

        if now >= next_deadline {
            let new_deadline = now + self.consumer_config.auto_commit_interval_millis as i64;

            // Use CAS to ensure only one thread performs the commit.
            if self
                .next_auto_commit_deadline
                .compare_exchange(next_deadline, new_deadline, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if let Err(e) = self.commit_all().await {
                    tracing::warn!("Auto-commit failed: {}", e);
                    // Revert deadline on failure to allow retry
                    self.next_auto_commit_deadline.store(next_deadline, Ordering::Release);
                }
            }
        }
    }

    /// Commits offsets for all assigned message queues.
    pub async fn commit_all(&self) -> RocketMQResult<()> {
        let queues = self.assigned_message_queue.message_queues().await;

        for mq in queues {
            if let Err(e) = self.commit_sync(&mq, true).await {
                tracing::error!("Failed to commit offset for queue {:?}: {}", mq, e);
            }
        }

        Ok(())
    }

    /// Commits offset for a specific message queue.
    pub async fn commit_sync(&self, mq: &MessageQueue, persist: bool) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;

        let consume_offset = self.assigned_message_queue.get_consume_offset(mq).await;

        if consume_offset == -1 {
            tracing::warn!("Consume offset is -1 for queue {:?}, skip commit", mq);
            return Ok(());
        }

        if let Some(pq) = self.assigned_message_queue.get_process_queue(mq).await {
            if !pq.is_dropped() {
                self.update_consume_offset(mq, consume_offset).await?;

                if persist {
                    if let Some(ref offset_store) = self.offset_store {
                        let mut mqs = HashSet::new();
                        mqs.insert(mq.clone());
                        offset_store.mut_from_ref().persist_all(&mqs).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Commits offsets for multiple message queues.
    pub async fn commit(&self, offsets: HashMap<MessageQueue, i64>, persist: bool) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;

        if offsets.is_empty() {
            tracing::warn!("Offset map is empty, skip commit");
            return Ok(());
        }

        let mut mqs_to_persist = HashSet::new();

        for (mq, offset) in offsets {
            if offset == -1 {
                tracing::error!("Consume offset is -1 for queue {:?}", mq);
                continue;
            }

            if let Some(pq) = self.assigned_message_queue.get_process_queue(&mq).await {
                if !pq.is_dropped() {
                    // Continue on error to allow other queues to commit.
                    if let Err(e) = self.update_consume_offset(&mq, offset).await {
                        tracing::error!("Failed to update offset for queue {:?}: {}", mq, e);
                        continue;
                    }
                    mqs_to_persist.insert(mq);
                }
            }
        }

        if persist && !mqs_to_persist.is_empty() {
            if let Some(ref offset_store) = self.offset_store {
                offset_store.mut_from_ref().persist_all(&mqs_to_persist).await;
            }
        }

        Ok(())
    }

    /// Updates the consume offset for a message queue.
    async fn update_consume_offset(&self, mq: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        if let Some(ref offset_store) = self.offset_store {
            offset_store.update_offset(mq, offset, false).await;
        }
        Ok(())
    }

    /// Returns the set of assigned message queues.
    pub async fn assignment(&self) -> HashSet<MessageQueue> {
        self.assigned_message_queue.message_queues().await
    }

    /// Registers a consume message hook for monitoring message consumption.
    pub fn register_consume_message_hook(&mut self, hook: Arc<dyn ConsumeMessageHook + Send + Sync>) {
        self.consume_message_hook_list.push(hook);
    }

    /// Removes namespace from message topics if namespace is configured.
    fn reset_topic(&self, messages: &mut [ArcMut<MessageExt>]) {
        if messages.is_empty() {
            return;
        }

        if let Some(namespace) = &self.client_config.namespace {
            if !namespace.is_empty() {
                for msg in messages.iter_mut() {
                    let topic = msg.message.topic().to_string();
                    let topic_without_namespace =
                        NamespaceUtil::without_namespace_with_namespace(&topic, namespace.as_str());
                    msg.message
                        .set_topic(CheetahString::from_string(topic_without_namespace));
                }
            }
        }
    }

    /// Pauses consumption for the specified message queues.
    pub async fn pause(&self, message_queues: &[MessageQueue]) {
        for mq in message_queues {
            self.assigned_message_queue.set_paused(mq, true).await;
        }
    }

    /// Resumes consumption for the specified message queues.
    pub async fn resume(&self, message_queues: &[MessageQueue]) {
        for mq in message_queues {
            self.assigned_message_queue.set_paused(mq, false).await;
        }
    }

    /// Seeks to the specified offset for the given message queue.
    pub async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;
        self.seek_internal(message_queue, offset, true).await
    }

    /// Seeks to the beginning of the message queue.
    pub async fn seek_to_begin(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;
        let begin = self.min_offset(message_queue).await?;
        self.seek_internal(message_queue, begin, false).await
    }

    /// Seeks to the end of the message queue.
    pub async fn seek_to_end(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;
        let end = self.max_offset(message_queue).await?;
        self.seek_internal(message_queue, end, false).await
    }

    /// Internal seek implementation with optional offset validation.
    async fn seek_internal(
        &self,
        message_queue: &MessageQueue,
        offset: i64,
        validate_offset: bool,
    ) -> RocketMQResult<()> {
        // Validate offset range if requested (skip for seek_to_begin/end to avoid duplicate queries)
        if validate_offset {
            let min_offset = self.min_offset(message_queue).await?;
            let max_offset = self.max_offset(message_queue).await?;
            if offset < min_offset || offset > max_offset {
                return Err(crate::mq_client_err!(format!(
                    "Seek offset illegal, seek offset = {}, min offset = {}, max offset = {}",
                    offset, min_offset, max_offset
                )));
            }
        }

        // Get or create lock for this message queue
        let lock = {
            let mut locks = self.message_queue_locks.write().await;
            locks
                .entry(message_queue.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        // Execute seek with lock to prevent race conditions
        let _guard = lock.lock().await;

        // Check if message queue is still assigned (rebalance protection)
        let assigned = self.assigned_message_queue.message_queues().await;
        if !assigned.contains(message_queue) {
            let subscription_type = *self.subscription_type.read().await;
            let error_msg = if subscription_type == SubscriptionType::Subscribe {
                format!(
                    "The message queue is not in assigned list, may be rebalancing, message queue: {:?}",
                    message_queue
                )
            } else {
                format!(
                    "The message queue is not in assigned list, message queue: {:?}",
                    message_queue
                )
            };
            return Err(crate::mq_client_err!(error_msg));
        }

        // Check if ProcessQueue is dropped
        if let Some(pq) = self.assigned_message_queue.get_process_queue(message_queue).await {
            if pq.is_dropped() {
                return Err(crate::mq_client_err!(format!(
                    "ProcessQueue is dropped for message queue: {:?}",
                    message_queue
                )));
            }
        }

        self.clear_message_queue_in_cache(message_queue).await;

        // Stop old pull task
        let mut task_handles = self.task_handles.write().await;
        if let Some(handle) = task_handles.remove(message_queue) {
            handle.abort();
        }

        // Set seek offset
        self.assigned_message_queue.set_seek_offset(message_queue, offset).await;

        // Start new pull task
        if !task_handles.contains_key(message_queue) {
            drop(task_handles); // Release write lock before starting pull task
            self.start_pull_task(message_queue.clone()).await?;
        }

        Ok(())
    }

    /// Returns the committed offset for the message queue.
    pub async fn committed(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        self.make_sure_state_ok()?;

        if let Some(ref offset_store) = self.offset_store {
            let offset = offset_store
                .read_offset(message_queue, ReadOffsetType::MemoryFirstThenStore)
                .await;
            if offset == -2 {
                return Err(crate::mq_client_err!("Fetch consume offset from broker exception"));
            }
            if offset == -1 {
                tracing::warn!("No offset found for message queue: {:?}, returning -1", message_queue);
            }
            return Ok(offset);
        }

        Err(crate::mq_client_err!("Offset store is not initialized"))
    }

    /// Unsubscribes from the specified topic.
    ///
    /// Removes the topic subscription, stops and removes all pull tasks for the topic,
    /// and clears assigned message queues for the topic.
    ///
    /// This operation can be performed regardless of the consumer state.
    pub async fn unsubscribe(&mut self, topic: impl Into<CheetahString>) -> RocketMQResult<()> {
        let topic = topic.into();

        // Remove from rebalance_impl subscription
        self.rebalance_impl.get_subscription_inner().remove(&topic);

        // Stop and remove pull tasks for this topic
        let mut task_handles = self.task_handles.write().await;
        task_handles.retain(|mq, handle| {
            if mq.topic() == topic.as_str() {
                handle.abort();
                false
            } else {
                true
            }
        });
        drop(task_handles);

        // Remove from assigned_message_queue
        self.assigned_message_queue.remove_by_topic(topic.as_str()).await;

        Ok(())
    }

    /// Searches for the queue offset whose store timestamp is closest to the specified timestamp.
    ///
    /// Returns the earliest offset whose store timestamp is greater than or equal to `timestamp`.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer is not in a valid state or if the broker address
    /// cannot be resolved.
    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        client_instance.mq_admin_impl.search_offset(mq, timestamp).await
    }

    /// Fetches all message queues for the specified topic.
    ///
    /// Returns a set of message queues with namespace removed from topic names.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer is not in a valid state or if the topic route
    /// information cannot be retrieved.
    pub async fn fetch_message_queues(&self, topic: impl Into<CheetahString>) -> RocketMQResult<HashSet<MessageQueue>> {
        use crate::factory::mq_client_instance::topic_route_data2topic_subscribe_info;

        self.make_sure_state_ok()?;

        let topic = topic.into();
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        // Fetch topic route data from name server
        let topic_route_data = client_instance
            .mq_client_api_impl
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("MQClientAPIImpl not initialized"))?
            .get_topic_route_info_from_name_server_detail(topic.as_str(), 3000, true)
            .await?;

        let topic_route_data = topic_route_data
            .ok_or_else(|| crate::mq_client_err!(format!("No route data found for topic: {}", topic)))?;

        // Convert route data to subscribe info
        let mq_set = topic_route_data2topic_subscribe_info(topic.as_str(), &topic_route_data);

        if mq_set.is_empty() {
            return Err(crate::mq_client_err!(format!(
                "Can not find Message Queue for this topic: {}",
                topic
            )));
        }

        // Parse message queues to remove namespace
        self.parse_message_queues(&mq_set)
    }

    /// Parses message queues by removing namespace from their topic names.
    fn parse_message_queues(&self, queue_set: &HashSet<MessageQueue>) -> RocketMQResult<HashSet<MessageQueue>> {
        let namespace = self
            .client_config
            .get_namespace_v2()
            .map(|s| s.as_str())
            .unwrap_or_default();

        let mut result = HashSet::with_capacity(queue_set.len());
        for mq in queue_set {
            let user_topic = NamespaceUtil::without_namespace_with_namespace(mq.topic_str(), namespace);
            result.insert(MessageQueue::from_parts(user_topic, mq.broker_name(), mq.queue_id()));
        }

        Ok(result)
    }

    /// Returns the offset for the specified message queue at the given timestamp.
    ///
    /// This is an alias for `search_offset`.
    ///
    /// # Unimplemented
    ///
    /// This method is marked as low priority and currently unimplemented.
    #[allow(dead_code)]
    pub async fn offset_for_timestamp(&self, _mq: &MessageQueue, _timestamp: u64) -> RocketMQResult<i64> {
        todo!("offset_for_timestamp: Low priority method, not yet implemented")
    }

    /// Returns the earliest message store time for the specified message queue.
    ///
    /// # Unimplemented
    ///
    /// This method is marked as low priority and currently unimplemented.
    #[allow(dead_code)]
    pub async fn earliest_msg_store_time(&self, _mq: &MessageQueue) -> RocketMQResult<i64> {
        todo!("earliest_msg_store_time: Low priority method, not yet implemented")
    }

    /// Returns the maximum offset of the specified message queue.
    ///
    /// # Unimplemented
    ///
    /// This method is marked as low priority and currently unimplemented.
    /// Use the private `max_offset` method internally.
    #[allow(dead_code)]
    pub async fn max_offset_public(&self, _mq: &MessageQueue) -> RocketMQResult<i64> {
        todo!("max_offset_public: Low priority method, not yet implemented")
    }

    /// Returns the minimum offset of the specified message queue.
    ///
    /// # Unimplemented
    ///
    /// This method is marked as low priority and currently unimplemented.
    /// Use the private `min_offset` method internally.
    #[allow(dead_code)]
    pub async fn min_offset_public(&self, _mq: &MessageQueue) -> RocketMQResult<i64> {
        todo!("min_offset_public: Low priority method, not yet implemented")
    }

    /// Updates the name server address list.
    ///
    /// Returns whether auto-commit is enabled.
    ///
    /// # Unimplemented
    ///
    /// This method is marked as low priority and currently unimplemented.
    #[allow(dead_code)]
    pub fn is_auto_commit(&self) -> bool {
        todo!("is_auto_commit: Low priority method, not yet implemented")
    }

    async fn max_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        let broker_result = client_instance
            .find_broker_address_in_subscribe(message_queue.broker_name(), mix_all::MASTER_ID, true)
            .await
            .ok_or_else(|| {
                crate::mq_client_err!(format!("Broker address not found for: {}", message_queue.broker_name()))
            })?;

        client_instance
            .mq_client_api_impl
            .as_mut()
            .unwrap()
            .get_max_offset(broker_result.broker_addr.as_str(), message_queue, 3000)
            .await
    }

    async fn min_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        let broker_result = client_instance
            .find_broker_address_in_subscribe(message_queue.broker_name(), mix_all::MASTER_ID, true)
            .await
            .ok_or_else(|| {
                crate::mq_client_err!(format!("Broker address not found for: {}", message_queue.broker_name()))
            })?;

        client_instance
            .mq_client_api_impl
            .as_mut()
            .unwrap()
            .get_min_offset(broker_result.broker_addr.as_str(), message_queue, 3000)
            .await
    }

    async fn clear_message_queue_in_cache(&self, message_queue: &MessageQueue) {
        if let Some(process_queue) = self.assigned_message_queue.get_process_queue(message_queue).await {
            process_queue.clear().await;
        }
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
        // Check service state before persisting
        if let Err(e) = self.make_sure_state_ok() {
            tracing::error!("Persist consumer offset error, service state invalid: {}", e);
            return;
        }

        // Collect message queues quickly while holding locks, then release.
        let subscription_type = *self.subscription_type.read().await;

        let mqs = match subscription_type {
            SubscriptionType::Subscribe => {
                let process_queue_table = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .process_queue_table
                    .read()
                    .await;
                process_queue_table.keys().cloned().collect::<HashSet<_>>()
            }
            SubscriptionType::Assign => self.assigned_message_queue.message_queues().await,
            SubscriptionType::None => HashSet::new(),
        };

        if !mqs.is_empty() {
            if let Some(ref offset_store) = self.offset_store {
                offset_store.mut_from_ref().persist_all(&mqs).await;
            }
        }
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

    async fn reset_offsets(&self, topic: &CheetahString, offsets: HashMap<MessageQueue, i64>) {
        let Some(offset_store) = self.offset_store.as_ref() else {
            warn!(
                "lite pull reset offset ignored because offset store is not initialized. group={}, topic={}",
                self.consumer_config.consumer_group, topic
            );
            return;
        };

        for (mq, offset) in offsets {
            if mq.topic() == topic {
                offset_store.update_and_freeze_offset(&mq, offset).await;
            }
        }
    }

    async fn consumer_status(&self, topic: &CheetahString) -> HashMap<MessageQueue, i64> {
        let Some(offset_store) = self.offset_store.as_ref() else {
            warn!(
                "lite pull consumer status is empty because offset store is not initialized. group={}, topic={}",
                self.consumer_config.consumer_group, topic
            );
            return HashMap::new();
        };

        offset_store.clone_offset_table(topic).await
    }

    fn consumer_running_info(&self) -> rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo {
        rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::default()
    }
}
