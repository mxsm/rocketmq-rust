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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::assigned_message_queue::AssignedMessageQueue;
use crate::consumer::consumer_impl::lite_pull_consume_request::LitePullConsumeRequest;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_hook::FilterMessageHook;

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
            // Rebalance implementation delegates to RebalanceLitePullImpl via the Rebalance trait
            unimplemented!("do_rebalance is not yet implemented")
        }
    }

    async fn try_rebalance(&self) -> RocketMQResult<bool> {
        self.do_rebalance().await;
        Ok(true)
    }

    async fn persist_consumer_offset(&self) {
        // Offset persistence handled by commit operations
        unimplemented!("persist_consumer_offset is not yet implemented")
    }

    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        // Topic subscription updates managed by rebalance implementation
        unimplemented!("update_topic_subscribe_info is not yet implemented")
    }

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        // Subscription updates determined by metadata changes
        false
    }

    fn is_unit_mode(&self) -> bool {
        self.client_config.unit_mode
    }

    fn consumer_running_info(&self) -> rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo {
        rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::default()
    }
}
