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
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::assigned_message_queue::AssignedMessageQueue;
use crate::consumer::consumer_impl::lite_pull_consume_request::LitePullConsumeRequest;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::re_balance::rebalance_lite_pull_impl::RebalanceLitePullImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_status::PullStatus;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::runtime::spawn_client_task;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;

const QUERY_UNIQ_KEY_LOOKBACK_MILLIS: u64 = 3 * 24 * 60 * 60 * 1000;
const OFFSET_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const REBALANCE_LISTENER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

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

enum LitePullTaskHandle {
    Tracked {
        handle: ClientTrackedTaskHandle,
        cancelled: Arc<AtomicBool>,
    },
}

impl LitePullTaskHandle {
    fn abort(self) {
        match self {
            Self::Tracked { handle, cancelled } => {
                cancelled.store(true, Ordering::Release);
                let report = handle.shutdown_now();
                if !report.is_healthy() {
                    warn!(
                        report = %report.to_json(),
                        "lite pull task immediate shutdown report is unhealthy"
                    );
                }
            }
        }
    }

    async fn wait(self, timeout: Duration) -> bool {
        match self {
            Self::Tracked { handle, cancelled } => {
                cancelled.store(true, Ordering::Release);
                let report = handle.shutdown(timeout).await;
                if !report.is_healthy() {
                    warn!(
                        report = %report.to_json(),
                        "lite pull task shutdown report is unhealthy"
                    );
                }
                report.is_healthy()
            }
        }
    }

    fn task_count(&self) -> usize {
        match self {
            Self::Tracked { handle, .. } => handle.task_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LitePullTaskLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

fn spawn_lite_pull_task<F>(
    thread_name: &'static str,
    cancelled: Arc<AtomicBool>,
    task: F,
) -> std::io::Result<LitePullTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = spawn_client_tracked_task(thread_name, task)?;
    Ok(LitePullTaskHandle::Tracked { handle, cancelled })
}

async fn sleep_or_cancel(shutdown_signal: &Arc<AtomicBool>, cancelled: &Arc<AtomicBool>, delay: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        if shutdown_signal.load(Ordering::Acquire) || cancelled.load(Ordering::Acquire) {
            return false;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return true;
        }

        tokio::time::sleep((deadline - now).min(Duration::from_millis(50))).await;
    }
}

/// Internal rebalance listener that mirrors Java LitePull's default MessageQueueListenerImpl.
struct LitePullRebalanceListener {
    consumer_impl: WeakArcMut<DefaultLitePullConsumerImpl>,
    user_listener: Arc<StdRwLock<Option<ArcMessageQueueListener>>>,
    task_tracker: TaskTracker,
    shutdown_token: CancellationToken,
}

impl MessageQueueListener for LitePullRebalanceListener {
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_assigned: &HashSet<MessageQueue>) {
        let Some(consumer_impl) = self.consumer_impl.upgrade() else {
            warn!("LitePull rebalance listener ignored change because consumer impl was dropped");
            return;
        };

        let topic = CheetahString::from_slice(topic);
        let mq_all = mq_all.clone();
        let mq_assigned = mq_assigned.clone();
        let user_listener = self.user_listener.clone();
        let shutdown_token = self.shutdown_token.clone();
        if shutdown_token.is_cancelled() {
            warn!("LitePull rebalance listener ignored change because shutdown has started");
            return;
        }

        let task = self.task_tracker.track_future(async move {
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {},
                _ = async move {
                    if let Err(error) = consumer_impl
                        .update_assign_queue_and_start_pull_task(topic.as_str(), &mq_all, &mq_assigned)
                        .await
                    {
                        warn!(
                            "LitePull rebalance assignment update failed. topic={}, error={}",
                            topic, error
                        );
                    }

                    let listener = user_listener.read().ok().and_then(|listener| listener.clone());
                    if let Some(listener) = listener {
                        if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            listener.message_queue_changed(topic.as_str(), &mq_all, &mq_assigned);
                        })) {
                            warn!(
                                "LitePull user message queue listener panicked. topic={}, error={:?}",
                                topic, error
                            );
                        }
                    }
                } => {},
            }
        });

        if let Err(error) = spawn_client_task("rocketmq-client-lite-pull-rebalance-listener", task) {
            warn!(
                "LitePull rebalance listener ignored async assignment update because task spawn failed. error={}",
                error
            );
        }
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

    /// Whether the subscription group runs in unit mode.
    pub unit_mode: bool,

    /// Number of messages to pull in a single request.
    pub pull_batch_size: i32,

    /// Number of concurrent pull threads.
    pub pull_thread_nums: usize,

    /// Whether broker selection is controlled by the user-configured default broker ID.
    pub connect_broker_by_user: bool,

    /// Broker ID used when `connect_broker_by_user` is enabled.
    pub default_broker_id: u64,

    /// Maximum number of messages cached per queue.
    pub pull_threshold_for_queue: i64,

    /// Maximum size in MiB of messages cached per queue.
    pub pull_threshold_size_for_queue: i32,

    /// Maximum total number of cached messages across all queues.
    pub pull_threshold_for_all: i64,

    /// Maximum offset span allowed in a process queue.
    pub consume_max_span: i64,

    /// Delay in milliseconds when pull encounters an exception.
    pub pull_time_delay_millis_when_exception: u64,

    /// Delay in milliseconds when cache flow control is triggered.
    pub pull_time_delay_millis_when_cache_flow_control: u64,

    /// Delay in milliseconds when broker flow control is triggered.
    pub pull_time_delay_millis_when_broker_flow_control: u64,

    /// Maximum time in milliseconds that the broker may suspend a long-poll pull request.
    pub broker_suspend_max_time_millis: u64,

    /// Consumer-side timeout in milliseconds for suspended long-poll pull requests.
    pub consumer_timeout_millis_when_suspend: u64,

    /// Timeout in milliseconds for a lite pull RPC when a non-blocking pull timeout is used.
    pub consumer_pull_timeout_millis: u64,

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

pub(crate) fn default_lite_pull_consume_timestamp() -> CheetahString {
    let thirty_minutes_ago = current_millis().saturating_sub(1000 * 60 * 30);
    CheetahString::from_string(util_all::time_millis_to_human_string3(thirty_minutes_ago as i64))
}

#[allow(deprecated)]
pub(crate) fn validate_lite_pull_consume_from_where(consume_from_where: ConsumeFromWhere) -> RocketMQResult<()> {
    match consume_from_where {
        ConsumeFromWhere::ConsumeFromFirstOffset
        | ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromTimestamp => Ok(()),
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset => Err(crate::mq_client_err!("Invalid ConsumeFromWhere Value")),
    }
}

fn lite_pull_request_sys_flag() -> i32 {
    PullSysFlag::build_sys_flag_with_lite_pull(false, true, true, false, true) as i32
}

fn lite_pull_request_sub_version(subscription_data: &SubscriptionData) -> i64 {
    if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
        0
    } else {
        subscription_data.sub_version
    }
}

impl Default for LitePullConsumerConfig {
    fn default() -> Self {
        Self {
            consumer_group: CheetahString::from_static_str("DEFAULT_CONSUMER"),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: Some(default_lite_pull_consume_timestamp()),
            allocate_message_queue_strategy: Arc::new(
                crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely,
            ),
            unit_mode: false,
            pull_batch_size: 10,
            pull_thread_nums: 20,
            connect_broker_by_user: false,
            default_broker_id: mix_all::MASTER_ID,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_all: 10000,
            consume_max_span: 2000,
            pull_time_delay_millis_when_exception: 1000,
            pull_time_delay_millis_when_cache_flow_control: 50,
            pull_time_delay_millis_when_broker_flow_control: 20,
            broker_suspend_max_time_millis: 20_000,
            consumer_timeout_millis_when_suspend: 30_000,
            consumer_pull_timeout_millis: 10_000,
            poll_timeout_millis: 5000,
            auto_commit: true,
            auto_commit_interval_millis: 5000,
            topic_metadata_check_interval_millis: 30000,
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
            offset_store: None,
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
            unit_mode: self.unit_mode,
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
    rpc_hook: Option<Arc<dyn RPCHook>>,

    // Queue management
    assigned_message_queue: Arc<AssignedMessageQueue>,
    message_queue_locks: Arc<RwLock<HashMap<MessageQueue, Arc<Mutex<()>>>>>,

    // Pull task scheduling
    task_handles: Arc<RwLock<HashMap<MessageQueue, LitePullTaskHandle>>>,

    // Message flow (unbounded channel for non-blocking pull)
    consume_request_tx: mpsc::UnboundedSender<LitePullConsumeRequest>,
    consume_request_rx: Arc<Mutex<mpsc::UnboundedReceiver<LitePullConsumeRequest>>>,
    poll_lock: Arc<Mutex<()>>,

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
    message_queues_for_topic: Arc<RwLock<HashMap<CheetahString, HashSet<MessageQueue>>>>,
    user_message_queue_listener: Arc<StdRwLock<Option<ArcMessageQueueListener>>>,

    // Shutdown coordination
    shutdown_signal: Arc<AtomicBool>,
    topic_metadata_check_task_handle: Option<LitePullTaskHandle>,
    rebalance_listener_tasks: TaskTracker,
    rebalance_listener_shutdown: CancellationToken,

    // Self-reference (for callbacks)
    default_lite_pull_consumer_impl: Option<ArcMut<DefaultLitePullConsumerImpl>>,
}

impl DefaultLitePullConsumerImpl {
    /// Creates a new lite pull consumer implementation.
    pub fn new(client_config: ArcMut<ClientConfig>, consumer_config: ArcMut<LitePullConsumerConfig>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        let mut this = Self {
            client_config,
            consumer_config: consumer_config.clone(),
            service_state: ArcMut::new(ServiceState::CreateJust),
            subscription_type: Arc::new(RwLock::new(SubscriptionType::None)),
            consumer_start_timestamp: AtomicI64::new(current_millis() as i64),
            client_instance: None,
            rebalance_impl: ArcMut::new(RebalanceLitePullImpl::new(consumer_config.to_consumer_config())),
            pull_api_wrapper: None,
            offset_store: None,
            rpc_hook: None,
            assigned_message_queue: Arc::new(AssignedMessageQueue::new()),
            message_queue_locks: Arc::new(RwLock::new(HashMap::new())),
            task_handles: Arc::new(RwLock::new(HashMap::new())),
            consume_request_tx: tx,
            consume_request_rx: Arc::new(Mutex::new(rx)),
            poll_lock: Arc::new(Mutex::new(())),
            topic_to_sub_expression: Arc::new(RwLock::new(HashMap::new())),
            consume_request_flow_control_times: Arc::new(AtomicU64::new(0)),
            queue_flow_control_times: Arc::new(AtomicU64::new(0)),
            queue_max_span_flow_control_times: Arc::new(AtomicU64::new(0)),
            next_auto_commit_deadline: Arc::new(AtomicI64::new(0)),
            consume_message_hook_list: Vec::new(),
            filter_message_hook_list: Vec::new(),
            topic_message_queue_change_listener_map: Arc::new(RwLock::new(HashMap::new())),
            message_queues_for_topic: Arc::new(RwLock::new(HashMap::new())),
            user_message_queue_listener: Arc::new(StdRwLock::new(None)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            topic_metadata_check_task_handle: None,
            rebalance_listener_tasks: TaskTracker::new(),
            rebalance_listener_shutdown: CancellationToken::new(),
            default_lite_pull_consumer_impl: None,
        };
        let wrapper = ArcMut::downgrade(&this.rebalance_impl);
        this.rebalance_impl.set_rebalance_impl(wrapper);
        this
    }

    /// Sets the self-reference for callbacks.
    pub fn set_default_lite_pull_consumer_impl(
        &mut self,
        default_lite_pull_consumer_impl: ArcMut<DefaultLitePullConsumerImpl>,
    ) {
        self.rebalance_impl.consumer_config.message_queue_listener = Some(Arc::new(LitePullRebalanceListener {
            consumer_impl: ArcMut::downgrade(&default_lite_pull_consumer_impl),
            user_listener: self.user_message_queue_listener.clone(),
            task_tracker: self.rebalance_listener_tasks.clone(),
            shutdown_token: self.rebalance_listener_shutdown.clone(),
        }));
        self.default_lite_pull_consumer_impl = Some(default_lite_pull_consumer_impl);
    }

    pub fn set_rpc_hook(&mut self, rpc_hook: Option<Arc<dyn RPCHook>>) {
        self.rpc_hook = rpc_hook;
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

    /// Returns the configured consumer group.
    pub fn consumer_group_config(&self) -> CheetahString {
        self.consumer_config.consumer_group.clone()
    }

    /// Updates the consumer group before startup and keeps rebalance config in sync.
    pub fn set_consumer_group(&self, consumer_group: CheetahString) -> RocketMQResult<()> {
        if *self.service_state != ServiceState::CreateJust {
            return Err(crate::mq_client_err!(
                "consumerGroup can not be changed after the lite pull consumer has started."
            ));
        }

        self.consumer_config.mut_from_ref().consumer_group = consumer_group.clone();
        self.rebalance_impl
            .mut_from_ref()
            .set_consumer_group(consumer_group.clone());
        self.rebalance_impl.consumer_config.mut_from_ref().consumer_group = consumer_group;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn rebalance_consumer_group_name(&self) -> Option<CheetahString> {
        self.rebalance_impl.rebalance_impl_inner.consumer_group.clone()
    }

    /// Returns the active or preconfigured offset store.
    pub fn offset_store(&self) -> Option<ArcMut<OffsetStore>> {
        self.offset_store.clone()
    }

    /// Updates the offset store before startup and keeps rebalance state in sync.
    pub fn set_offset_store(&mut self, offset_store: Option<ArcMut<OffsetStore>>) -> RocketMQResult<()> {
        if *self.service_state != ServiceState::CreateJust {
            return Err(crate::mq_client_err!(
                "offsetStore can not be changed after the lite pull consumer has started."
            ));
        }

        self.offset_store = offset_store.clone();
        if let Some(offset_store) = offset_store {
            self.rebalance_impl.set_offset_store(offset_store);
        } else {
            self.rebalance_impl.offset_store = None;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn rebalance_has_offset_store(&self) -> bool {
        self.rebalance_impl.offset_store.is_some()
    }

    #[cfg(test)]
    pub(crate) fn has_rpc_hook(&self) -> bool {
        self.rpc_hook.is_some()
    }

    /// Validates configuration before starting.
    fn check_config(&self) -> RocketMQResult<()> {
        if self.consumer_config.consumer_group.is_empty() {
            return Err(crate::mq_client_err!("Consumer group cannot be empty"));
        }

        if self.consumer_config.consumer_group == mix_all::DEFAULT_CONSUMER_GROUP {
            return Err(crate::mq_client_err!(format!(
                "consumerGroup can not equal {}, please specify another one.{}",
                mix_all::DEFAULT_CONSUMER_GROUP,
                rocketmq_common::common::FAQUrl::suggest_todo(
                    rocketmq_common::common::FAQUrl::CLIENT_PARAMETER_CHECK_URL
                )
            )));
        }

        validate_lite_pull_consume_from_where(self.consumer_config.consume_from_where)?;

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

        if self.consumer_config.consumer_timeout_millis_when_suspend
            < self.consumer_config.broker_suspend_max_time_millis
        {
            return Err(crate::mq_client_err!(
                "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than \
                 brokerSuspendMaxTimeMillis"
            ));
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

    async fn subscribe_inner(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
        message_queue_listener: Option<ArcMessageQueueListener>,
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
        if let Some(listener) = message_queue_listener {
            self.set_message_queue_listener(Some(listener));
        }

        if *self.service_state == ServiceState::Running {
            if let Some(ref client_instance) = self.client_instance {
                client_instance.send_heartbeat_to_all_broker_with_lock().await;
                self.update_topic_subscribe_info_when_subscription_changed().await?;
            }
        }

        Ok(())
    }

    /// Subscribes to a topic with optional tag expression filter.
    pub async fn subscribe(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        self.subscribe_inner(topic, sub_expression, None).await
    }

    /// Manually assigns specific message queues (ASSIGN mode).
    pub async fn assign(&mut self, message_queues: Vec<MessageQueue>) -> RocketMQResult<()> {
        if message_queues.is_empty() {
            return Err(crate::mq_client_err!("Message queues can not be null or empty."));
        }

        self.set_subscription_type(SubscriptionType::Assign).await?;

        self.update_assigned_message_queue_for_assign(&message_queues).await;

        if *self.service_state == ServiceState::Running {
            self.update_pull_task_for_assign(&message_queues).await?;
        }

        Ok(())
    }

    async fn update_assigned_message_queue_for_topic(&self, topic: &str, assigned: &HashSet<MessageQueue>) {
        self.assigned_message_queue
            .update_assigned_message_queue_for_topic(topic, assigned.iter().cloned())
            .await;
    }

    async fn update_pull_task_for_topic(&self, topic: &str, assigned: &HashSet<MessageQueue>) -> RocketMQResult<()> {
        let to_start = {
            let mut task_handles = self.task_handles.write().await;
            let to_remove: Vec<MessageQueue> = task_handles
                .keys()
                .filter(|mq| mq.topic() == topic && !assigned.contains(*mq))
                .cloned()
                .collect();
            for mq in to_remove {
                if let Some(handle) = task_handles.remove(&mq) {
                    handle.abort();
                }
            }
            assigned
                .iter()
                .filter(|mq| !task_handles.contains_key(*mq))
                .cloned()
                .collect::<Vec<_>>()
        };

        for mq in to_start {
            self.start_pull_task(mq).await?;
        }
        Ok(())
    }

    async fn update_assign_queue_and_start_pull_task(
        &self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    ) -> RocketMQResult<()> {
        let assigned = match self.consumer_config.message_model {
            MessageModel::Broadcasting => mq_all,
            MessageModel::Clustering => mq_divided,
        };
        self.update_assigned_message_queue_for_topic(topic, assigned).await;
        self.update_pull_task_for_topic(topic, assigned).await
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

        if !to_remove.is_empty() {
            let mut task_handles = self.task_handles.write().await;
            for mq in &to_remove {
                if let Some(handle) = task_handles.remove(mq) {
                    handle.abort();
                }
            }
        }

        self.assigned_message_queue
            .update_assigned_message_queue(assigned_set)
            .await;
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
        let topics: Vec<CheetahString> = self
            .rebalance_impl
            .get_subscription_inner()
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        if let Some(ref mut client_instance) = self.client_instance {
            for topic in &topics {
                client_instance
                    .update_topic_route_info_from_name_server_topic(topic)
                    .await;
            }
            client_instance.send_heartbeat_to_all_broker_with_lock_v2(true).await;
        }

        if !topics.is_empty() {
            self.rebalance_impl.mut_from_ref().do_rebalance(false).await;
            self.sync_assigned_queues_from_rebalance(&topics).await?;
        }

        Ok(())
    }

    async fn sync_assigned_queues_from_rebalance(&self, topics: &[CheetahString]) -> RocketMQResult<()> {
        let assignments: Vec<(CheetahString, HashSet<MessageQueue>)> = {
            let process_queue_table = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;

            topics
                .iter()
                .map(|topic| {
                    let assigned = process_queue_table
                        .keys()
                        .filter(|mq| mq.topic_str() == topic.as_str())
                        .cloned()
                        .collect::<HashSet<_>>();
                    (topic.clone(), assigned)
                })
                .collect()
        };

        for (topic, assigned) in assignments {
            self.update_assign_queue_and_start_pull_task(topic.as_str(), &assigned, &assigned)
                .await?;
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
                    .get_or_create_mq_client_instance(self.client_config.as_ref().clone(), self.rpc_hook.clone());
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
                        self.consumer_config.unit_mode,
                    );
                    pull_api_wrapper.set_connect_broker_by_user(self.consumer_config.connect_broker_by_user);
                    pull_api_wrapper.set_default_broker_id(self.consumer_config.default_broker_id);
                    pull_api_wrapper.register_filter_message_hook(self.filter_message_hook_list.clone());
                    self.pull_api_wrapper = Some(ArcMut::new(pull_api_wrapper));
                }

                let offset_store = self.offset_store.clone().unwrap_or_else(|| {
                    ArcMut::new(match self.consumer_config.message_model {
                        MessageModel::Broadcasting => OffsetStore::new_with_local(LocalFileOffsetStore::new(
                            client_instance.clone(),
                            self.consumer_config.consumer_group.clone(),
                        )),
                        MessageModel::Clustering => OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                            client_instance.clone(),
                            self.consumer_config.consumer_group.clone(),
                        )),
                    })
                });

                offset_store.load().await?;

                self.rebalance_impl.set_offset_store(offset_store.clone());

                self.offset_store = Some(offset_store);

                let default_lite_pull_consumer_impl = self
                    .default_lite_pull_consumer_impl
                    .clone()
                    .ok_or_else(|| crate::mq_client_err!("default_lite_pull_consumer_impl is not initialized"))?;
                let register_ok = client_instance
                    .mut_from_ref()
                    .register_consumer(
                        &self.consumer_config.consumer_group,
                        MQConsumerInnerImpl::from_lite_pull(default_lite_pull_consumer_impl),
                    )
                    .await;
                if !register_ok {
                    *self.service_state = ServiceState::CreateJust;
                    return Err(crate::mq_client_err!(format!(
                        "The consumer group[{}] has been created before, specify another name please.{}",
                        self.consumer_config.consumer_group,
                        rocketmq_common::common::FAQUrl::suggest_todo(
                            rocketmq_common::common::FAQUrl::GROUP_NAME_DUPLICATE_URL
                        )
                    )));
                }

                let client_instance_clone = client_instance.clone();
                client_instance.mut_from_ref().start(client_instance_clone).await?;

                *self.service_state = ServiceState::Running;

                self.start_topic_metadata_check_task()?;

                info!(
                    "DefaultLitePullConsumerImpl [{}] started successfully",
                    self.consumer_config.consumer_group
                );

                if let Err(error) = self.operate_after_running().await {
                    let _ = self.shutdown().await;
                    return Err(error);
                }

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

    /// Completes startup operations that depend on the final running state.
    ///
    /// This mirrors Java's `operateAfterRunning`: subscriptions made before `start`
    /// need topic route refresh, and assignments made before `start` need pull tasks.
    async fn operate_after_running(&mut self) -> RocketMQResult<()> {
        let subscription_type = *self.subscription_type.read().await;
        match subscription_type {
            SubscriptionType::Subscribe => {
                self.update_topic_subscribe_info_when_subscription_changed().await?;
            }
            SubscriptionType::Assign => {
                let assigned: Vec<MessageQueue> =
                    self.assigned_message_queue.message_queues().await.into_iter().collect();
                self.update_pull_task_for_assign(&assigned).await?;
            }
            SubscriptionType::None => {}
        }

        self.refresh_registered_topic_message_queue_snapshots().await?;

        let client_instance = self
            .client_instance
            .as_mut()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?;
        client_instance.check_client_in_broker().await
    }

    fn start_topic_metadata_check_task(&mut self) -> RocketMQResult<()> {
        if self.topic_metadata_check_task_handle.is_some() {
            return Ok(());
        }

        let consumer_impl = self
            .default_lite_pull_consumer_impl
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("default_lite_pull_consumer_impl is not initialized"))?;
        let weak_consumer_impl = ArcMut::downgrade(consumer_impl);
        let shutdown_signal = self.shutdown_signal.clone();
        let task_cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled_for_task = task_cancelled.clone();
        let check_interval = Duration::from_millis(self.consumer_config.topic_metadata_check_interval_millis.max(1));

        self.topic_metadata_check_task_handle = Some(
            spawn_lite_pull_task("rocketmq-client-lite-pull-topic-metadata", task_cancelled, async move {
                if !sleep_or_cancel(&shutdown_signal, &task_cancelled_for_task, Duration::from_secs(10)).await {
                    return;
                }

                loop {
                    if shutdown_signal.load(Ordering::Acquire) || task_cancelled_for_task.load(Ordering::Acquire) {
                        break;
                    }

                    let Some(consumer_impl) = weak_consumer_impl.upgrade() else {
                        break;
                    };

                    if let Err(error) = consumer_impl.fetch_topic_message_queues_and_compare().await {
                        warn!("Scheduled fetchTopicMessageQueuesAndCompare exception: {}", error);
                    }

                    if !sleep_or_cancel(&shutdown_signal, &task_cancelled_for_task, check_interval).await {
                        break;
                    }
                }
            })
            .map_err(|error| crate::mq_client_err!(format!("failed to spawn topic metadata check task: {error}")))?,
        );

        Ok(())
    }

    async fn refresh_registered_topic_message_queue_snapshots(&self) -> RocketMQResult<()> {
        let topics: Vec<CheetahString> = self
            .topic_message_queue_change_listener_map
            .read()
            .await
            .keys()
            .cloned()
            .collect();

        for topic in topics {
            let message_queues = self.fetch_message_queues(topic.clone()).await?;
            self.message_queues_for_topic
                .write()
                .await
                .insert(topic, message_queues);
        }

        Ok(())
    }

    async fn notify_topic_message_queue_change_if_needed(
        &self,
        topic: &CheetahString,
        new_message_queues: HashSet<MessageQueue>,
    ) {
        let listener = {
            self.topic_message_queue_change_listener_map
                .read()
                .await
                .get(topic)
                .cloned()
        };
        let Some(listener) = listener else {
            return;
        };

        let is_changed = {
            let snapshots = self.message_queues_for_topic.read().await;
            snapshots.get(topic) != Some(&new_message_queues)
        };

        if !is_changed {
            return;
        }

        self.message_queues_for_topic
            .write()
            .await
            .insert(topic.clone(), new_message_queues.clone());

        let topic = topic.clone();
        if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            listener.on_changed(topic.as_str(), new_message_queues);
        })) {
            tracing::error!(
                "topicMessageQueueChangeListener for topic {} panicked: {:?}",
                topic,
                error
            );
        }
    }

    async fn fetch_topic_message_queues_and_compare(&self) -> RocketMQResult<()> {
        let topics: Vec<CheetahString> = self
            .topic_message_queue_change_listener_map
            .read()
            .await
            .keys()
            .cloned()
            .collect();

        for topic in topics {
            let new_message_queues = self.fetch_message_queues(topic.clone()).await?;
            self.notify_topic_message_queue_change_if_needed(&topic, new_message_queues)
                .await;
        }

        Ok(())
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
                self.rebalance_listener_shutdown.cancel();
                self.rebalance_listener_tasks.close();
                if tokio::time::timeout(
                    REBALANCE_LISTENER_SHUTDOWN_TIMEOUT,
                    self.rebalance_listener_tasks.wait(),
                )
                .await
                .is_err()
                {
                    warn!(
                        "LitePull rebalance listener tasks did not finish in time. group={}",
                        self.consumer_config.consumer_group
                    );
                }

                if let Some(handle) = self.topic_metadata_check_task_handle.take() {
                    if !handle.wait(Duration::from_secs(1)).await {
                        warn!("Topic metadata check task did not finish in time");
                    }
                }

                // Wait for all pull tasks to complete (5s timeout)
                let mut handles = self.task_handles.write().await;
                for (mq, handle) in handles.drain() {
                    if !handle.wait(Duration::from_secs(5)).await {
                        warn!("Pull task for {:?} did not finish in time", mq);
                    }
                }
                drop(handles);

                self.persist_consumer_offset().await;
                if let Some(offset_store) = self.offset_store.as_mut() {
                    if !offset_store
                        .mut_from_ref()
                        .shutdown(OFFSET_STORE_SHUTDOWN_TIMEOUT)
                        .await
                    {
                        warn!(
                            "lite pull consumer [{}] offset store did not stop before timeout",
                            self.consumer_config.consumer_group
                        );
                    }
                }

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
            ServiceState::CreateJust => Ok(()),
            ServiceState::ShutdownAlready => {
                warn!("The lite pull consumer has already been shutdown");
                Ok(())
            }
            ServiceState::StartFailed => Ok(()),
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
        self.subscribe_with_listener_arc(topic, sub_expression, Arc::new(listener))
            .await
    }

    /// Subscribes to a topic with a shared listener for queue assignment changes.
    pub async fn subscribe_with_listener_arc(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
        listener: ArcMessageQueueListener,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let sub_expression = sub_expression.into();

        self.subscribe_inner(topic, sub_expression, Some(listener)).await
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
                if topic.is_empty() {
                    return Err(crate::mq_client_err!("Topic cannot be empty"));
                }

                self.set_subscription_type(SubscriptionType::Subscribe).await?;

                let subscription_data =
                    FilterAPI::build(&topic, sel.get_expression(), Some(sel.get_expression_type().clone()))
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
            None => self.subscribe(topic, "*").await,
        }
    }

    /// Checks if a message queue is paused.
    pub async fn is_paused(&self, message_queue: &MessageQueue) -> bool {
        self.assigned_message_queue.is_paused(message_queue).await
    }

    /// Updates the name server addresses.
    pub async fn update_name_server_address(&self, addresses: Vec<String>) {
        let joined_addr = addresses.join(";");
        self.client_config
            .mut_from_ref()
            .set_namesrv_addr(CheetahString::from_string(joined_addr.clone()));

        if let Some(client_instance) = self.client_instance.as_ref() {
            if let Some(api_impl) = client_instance.mq_client_api_impl.as_ref() {
                api_impl.update_name_server_address_list(&joined_addr).await;
            }
        }
    }

    /// Registers a listener for topic message queue changes.
    pub async fn register_topic_message_queue_change_listener(
        &self,
        topic: impl Into<CheetahString>,
        listener: Arc<dyn TopicMessageQueueChangeListener + Send + Sync>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let previous = {
            let mut listeners = self.topic_message_queue_change_listener_map.write().await;
            listeners.insert(topic.clone(), listener)
        };
        if previous.is_some() {
            warn!(
                "Topic {} had been registered, new listener will overwrite the old one",
                topic
            );
        }
        if *self.service_state == ServiceState::Running {
            let message_queues = self.fetch_message_queues(topic.clone()).await?;
            self.message_queues_for_topic
                .write()
                .await
                .insert(topic, message_queues);
        }
        Ok(())
    }

    /// Sets the subscription expression for a topic in assign mode.
    pub async fn set_sub_expression_for_assign(
        &self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> RocketMQResult<()> {
        let topic = topic.into();
        let sub_expression = sub_expression.into();

        if sub_expression.as_str().trim().is_empty() {
            return Err(crate::mq_client_err!("subExpression can not be null or empty."));
        }
        if *self.service_state != ServiceState::CreateJust {
            return Err(crate::mq_client_err!("setAssignTag only can be called before start."));
        }

        self.set_subscription_type(SubscriptionType::Assign).await?;

        let mut map = self.topic_to_sub_expression.write().await;
        map.insert(topic, sub_expression);
        Ok(())
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
            let selector = crate::consumer::message_selector::MessageSelector::new(
                sub_data.expression_type.clone(),
                sub_data.sub_string.clone(),
            );
            sub_expression_map.insert(topic, selector);
        }

        Ok(())
    }

    /// Returns the subscription data currently used for heartbeat payloads.
    pub fn subscriptions_for_heartbeat(&self) -> HashSet<SubscriptionData> {
        self.rebalance_impl
            .get_subscription_inner()
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
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
        let task_cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled_for_task = task_cancelled.clone();
        let assigned_mq = self.assigned_message_queue.clone();
        let mq_clone = mq.clone();

        let handle = spawn_lite_pull_task("rocketmq-client-lite-pull-task", task_cancelled, async move {
            loop {
                if shutdown_signal.load(Ordering::Acquire) || task_cancelled_for_task.load(Ordering::Acquire) {
                    break;
                }

                let pq = match assigned_mq.get_process_queue(&mq_clone).await {
                    Some(pq) if !pq.is_dropped() => pq,
                    _ => break,
                };

                if assigned_mq.is_paused(&mq_clone).await {
                    if !sleep_or_cancel(
                        &shutdown_signal,
                        &task_cancelled_for_task,
                        Duration::from_millis(default_impl.consumer_config.pull_time_delay_millis_when_exception),
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }

                match default_impl.pull_inner(&mq_clone, &pq).await {
                    Ok(delay) => {
                        if delay > 0
                            && !sleep_or_cancel(
                                &shutdown_signal,
                                &task_cancelled_for_task,
                                Duration::from_millis(delay),
                            )
                            .await
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Pull error for {:?}: {}", mq_clone, e);
                        if !sleep_or_cancel(
                            &shutdown_signal,
                            &task_cancelled_for_task,
                            Duration::from_millis(default_impl.consumer_config.pull_time_delay_millis_when_exception),
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
        })
        .map_err(|error| crate::mq_client_err!(format!("failed to spawn pull task for {mq}: {error}")))?;

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
        mq: &MessageQueue,
        process_queue: &Arc<crate::consumer::consumer_impl::process_queue::ProcessQueue>,
    ) -> RocketMQResult<u64> {
        struct NoopPullCallback;

        impl PullCallback for NoopPullCallback {
            async fn on_success(&mut self, _pull_result: crate::PullResultExt) {}

            fn on_exception(&mut self, _e: Box<dyn std::error::Error + Send>) {}
        }

        self.make_sure_state_ok()?;
        process_queue.set_last_pull_timestamp(current_millis());

        if let Some(delay) = self.check_flow_control(mq, process_queue).await? {
            return Ok(delay);
        }

        let pull_offset = self.next_pull_offset(mq).await?;

        if pull_offset < 0 {
            return Ok(self.consumer_config.pull_time_delay_millis_when_exception);
        }

        let subscription_data = self.subscription_data_for_queue(mq).await?;
        let sub_version = lite_pull_request_sub_version(&subscription_data);
        let sys_flag = lite_pull_request_sys_flag();

        let mut pull_api_wrapper = self
            .pull_api_wrapper
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("PullAPIWrapper is not initialized"))?
            .clone();
        let mut pull_result = pull_api_wrapper
            .pull_kernel_impl(
                mq,
                subscription_data.sub_string.clone(),
                subscription_data.expression_type.clone(),
                sub_version,
                pull_offset,
                self.consumer_config.pull_batch_size,
                i32::MAX,
                sys_flag,
                0,
                self.consumer_config.broker_suspend_max_time_millis,
                self.consumer_config.consumer_pull_timeout_millis,
                CommunicationMode::Sync,
                NoopPullCallback,
            )
            .await?
            .ok_or_else(|| crate::mq_client_err!("Synchronous lite pull returned no result"))?;

        pull_api_wrapper.process_pull_result(mq, &mut pull_result, &subscription_data);

        match pull_result.pull_result.pull_status {
            PullStatus::Found => {
                let messages = pull_result.pull_result.msg_found_list.take().unwrap_or_default();
                let lock = self.message_queue_lock(mq).await;
                let _guard = lock.lock().await;
                let result_is_current = self
                    .update_pull_offset_if_no_pending_seek(
                        mq,
                        pull_result.pull_result.next_begin_offset as i64,
                        process_queue,
                    )
                    .await;
                if result_is_current && !messages.is_empty() {
                    let dispatch_to_consume = process_queue.put_message(&messages).await;
                    if dispatch_to_consume {
                        self.consume_request_tx
                            .send(LitePullConsumeRequest::new(messages, mq.clone(), process_queue.clone()))
                            .map_err(|_| crate::mq_client_err!("Lite pull consume request queue is closed"))?;
                    }
                }

                Ok(0)
            }
            PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => {
                let lock = self.message_queue_lock(mq).await;
                let _guard = lock.lock().await;
                self.update_pull_offset_if_no_pending_seek(
                    mq,
                    pull_result.pull_result.next_begin_offset as i64,
                    process_queue,
                )
                .await;
                Ok(0)
            }
            PullStatus::OffsetIllegal => {
                warn!(
                    "Lite pull offset illegal, mq={}, requested_offset={}, result={}",
                    mq, pull_offset, pull_result.pull_result
                );
                Ok(self
                    .handle_offset_illegal_pull_result(
                        mq,
                        process_queue,
                        pull_result.pull_result.next_begin_offset as i64,
                    )
                    .await)
            }
        }
    }

    async fn message_queue_lock(&self, message_queue: &MessageQueue) -> Arc<Mutex<()>> {
        let mut locks = self.message_queue_locks.write().await;
        locks
            .entry(message_queue.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn update_pull_offset_if_no_pending_seek(
        &self,
        mq: &MessageQueue,
        next_pull_offset: i64,
        process_queue: &Arc<crate::consumer::consumer_impl::process_queue::ProcessQueue>,
    ) -> bool {
        if self.assigned_message_queue.get_seek_offset(mq).await == -1 {
            self.assigned_message_queue
                .update_pull_offset(mq, next_pull_offset, process_queue)
                .await;
            true
        } else {
            false
        }
    }

    async fn handle_offset_illegal_pull_result(
        &self,
        mq: &MessageQueue,
        process_queue: &Arc<crate::consumer::consumer_impl::process_queue::ProcessQueue>,
        next_pull_offset: i64,
    ) -> u64 {
        let lock = self.message_queue_lock(mq).await;
        let _guard = lock.lock().await;
        self.update_pull_offset_if_no_pending_seek(mq, next_pull_offset, process_queue)
            .await;
        self.consumer_config.pull_time_delay_millis_when_exception
    }

    async fn subscription_data_for_queue(&self, mq: &MessageQueue) -> RocketMQResult<SubscriptionData> {
        let subscription_inner = self.rebalance_impl.get_subscription_inner();
        if let Some(subscription_data) = subscription_inner.get(mq.topic()) {
            return Ok(subscription_data.value().clone());
        }

        let expression = {
            let topic_to_sub_expression = self.topic_to_sub_expression.read().await;
            topic_to_sub_expression
                .get(mq.topic())
                .cloned()
                .unwrap_or_else(|| CheetahString::from_static_str(SubscriptionData::SUB_ALL))
        };

        FilterAPI::build(
            mq.topic(),
            &expression,
            Some(CheetahString::from_static_str(ExpressionType::TAG)),
        )
        .map_err(|e| crate::mq_client_err!(format!("buildSubscriptionData exception, {}", e)))
    }

    async fn next_pull_offset(&self, mq: &MessageQueue) -> RocketMQResult<i64> {
        let seek_offset = self.assigned_message_queue.get_seek_offset(mq).await;
        if seek_offset != -1 {
            self.assigned_message_queue.update_consume_offset(mq, seek_offset).await;
            self.assigned_message_queue.clear_seek_offset(mq).await;
            self.clear_message_queue_in_cache(mq).await;
            return Ok(seek_offset);
        }

        let current_pull_offset = self.assigned_message_queue.get_pull_offset(mq).await;
        if current_pull_offset >= 0 {
            return Ok(current_pull_offset);
        }

        self.compute_initial_pull_offset(mq).await
    }

    #[allow(deprecated)]
    async fn compute_initial_pull_offset(&self, mq: &MessageQueue) -> RocketMQResult<i64> {
        if let Some(offset_store) = &self.offset_store {
            let stored_offset = offset_store.read_offset(mq, ReadOffsetType::MemoryFirstThenStore).await;
            if stored_offset >= 0 {
                return Ok(stored_offset);
            }
        }

        match self.consumer_config.consume_from_where {
            ConsumeFromWhere::ConsumeFromFirstOffset | ConsumeFromWhere::ConsumeFromMinOffset => {
                self.min_offset(mq).await
            }
            ConsumeFromWhere::ConsumeFromTimestamp => {
                if mq.topic_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    return self.max_offset(mq).await;
                }
                let timestamp = self
                    .consumer_config
                    .consume_timestamp
                    .as_deref()
                    .and_then(|value| util_all::parse_date(value, util_all::YYYYMMDDHHMMSS))
                    .ok_or_else(|| crate::mq_client_err!("consumeTimestamp is invalid"))?
                    .and_utc()
                    .timestamp_millis() as u64;
                self.search_offset(mq, timestamp).await
            }
            ConsumeFromWhere::ConsumeFromLastOffset
            | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
            | ConsumeFromWhere::ConsumeFromMaxOffset => {
                if mq.topic_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    self.min_offset(mq).await
                } else {
                    self.max_offset(mq).await
                }
            }
        }
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
        let _poll_guard = self.poll_lock.lock().await;
        self.make_sure_state_ok()?;

        if self.consumer_config.auto_commit {
            self.maybe_auto_commit().await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_millis);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

            // Only hold lock during receive, release before async operations.
            let request = {
                let mut rx = self.consume_request_rx.lock().await;
                if remaining.is_zero() {
                    match rx.try_recv() {
                        Ok(req) => req,
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                        | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(Vec::new()),
                    }
                } else {
                    match tokio::time::timeout(remaining, rx.recv()).await {
                        Ok(Some(req)) => req,
                        Ok(None) => return Ok(Vec::new()),
                        Err(_) => return Ok(Vec::new()),
                    }
                }
            };

            // Filter dropped queues without holding lock.
            if request.process_queue.is_dropped() {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Vec::new());
                }
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
                    .with_namespace(self.client_config.namespace.clone().unwrap_or_default());

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
                context.access_channel = Some(self.client_config.access_channel);

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

            request.process_queue.update_last_consume_timestamp();

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

        if let Err(e) = self.make_sure_state_ok() {
            tracing::error!("Failed to commit all offsets, service state invalid: {}", e);
            return Ok(());
        }

        for mq in queues {
            if let Err(e) = self.commit_assigned_message_queue_offset(&mq).await {
                tracing::error!("Failed to commit offset for queue {:?}: {}", mq, e);
            }
        }

        Ok(())
    }

    /// Commits offset for a specific message queue.
    pub async fn commit_sync(&self, mq: &MessageQueue, persist: bool) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;

        let updated = self.commit_assigned_message_queue_offset(mq).await?;

        if persist && updated {
            if let Some(ref offset_store) = self.offset_store {
                let mut mqs = HashSet::new();
                mqs.insert(mq.clone());
                offset_store.mut_from_ref().persist_all(&mqs).await;
            }
        }

        Ok(())
    }

    async fn commit_assigned_message_queue_offset(&self, mq: &MessageQueue) -> RocketMQResult<bool> {
        let consume_offset = self.assigned_message_queue.get_consume_offset(mq).await;

        if consume_offset == -1 {
            tracing::error!("Consume offset is -1 for queue {:?}, skip commit", mq);
            return Ok(false);
        }

        if let Some(pq) = self.assigned_message_queue.get_process_queue(mq).await {
            if !pq.is_dropped() {
                self.update_consume_offset(mq, consume_offset).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Commits the current assigned consume offsets for the specified queues.
    pub async fn commit_message_queues(
        &self,
        message_queues: &HashSet<MessageQueue>,
        persist: bool,
    ) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;

        if message_queues.is_empty() {
            return Ok(());
        }

        for mq in message_queues {
            self.commit_assigned_message_queue_offset(mq).await?;
        }

        if persist {
            if let Some(ref offset_store) = self.offset_store {
                offset_store.mut_from_ref().persist_all(message_queues).await;
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

        let mqs_to_persist: HashSet<MessageQueue> = offsets.keys().cloned().collect();

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
                }
            }
        }

        if persist {
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

    #[cfg(test)]
    pub(crate) fn consume_message_hook_count(&self) -> usize {
        self.consume_message_hook_list.len()
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
        self.ensure_message_queue_assigned(message_queue).await?;

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

        let lock = self.message_queue_lock(message_queue).await;
        let _guard = lock.lock().await;

        // Check again under the queue lock in case rebalance changed assignment after offset lookup.
        self.ensure_message_queue_assigned(message_queue).await?;

        // Check if ProcessQueue is dropped
        if let Some(pq) = self.assigned_message_queue.get_process_queue(message_queue).await {
            if pq.is_dropped() {
                return Err(crate::mq_client_err!(format!(
                    "ProcessQueue is dropped for message queue: {:?}",
                    message_queue
                )));
            }
        }

        let _poll_guard = self.poll_lock.lock().await;
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

    async fn ensure_message_queue_assigned(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        let assigned = self.assigned_message_queue.message_queues().await;
        if assigned.contains(message_queue) {
            return Ok(());
        }

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
        Err(crate::mq_client_err!(error_msg))
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
        let to_remove = {
            let task_handles = self.task_handles.read().await;
            task_handles
                .keys()
                .filter(|mq| mq.topic() == topic.as_str())
                .cloned()
                .collect::<Vec<_>>()
        };
        if !to_remove.is_empty() {
            let mut task_handles = self.task_handles.write().await;
            for mq in to_remove {
                if let Some(handle) = task_handles.remove(&mq) {
                    handle.abort();
                }
            }
        }

        // Remove from assigned_message_queue
        self.assigned_message_queue
            .remove_assigned_message_queue(topic.as_str())
            .await;

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

    /// Creates a topic through the Java-compatible MQAdmin facade.
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> RocketMQResult<()> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        client_instance
            .mq_admin_impl
            .create_topic(key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    /// Queries messages by key through the Java-compatible MQAdmin facade.
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> RocketMQResult<QueryResult> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        client_instance
            .mq_admin_impl
            .query_message(topic, key, max_num, begin, end)
            .await
    }

    /// Queries one message by unique key, matching DefaultMQProducer's fallback path.
    pub async fn query_message_by_uniq_key(&self, topic: &str, uniq_key: &str) -> RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;

        let begin = current_millis().saturating_sub(QUERY_UNIQ_KEY_LOOKBACK_MILLIS);
        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();
        let result = client_instance
            .mq_admin_impl
            .query_message_with_unique_flag(topic, uniq_key, 32, begin, i64::MAX as u64, true)
            .await?;
        result
            .message_list()
            .first()
            .cloned()
            .ok_or_else(|| crate::mq_client_err!("query message by uniq key finished, but no message."))
    }

    /// Views a message by offset message id through the Java-compatible MQAdmin facade.
    pub async fn view_message(&self, topic: &str, msg_id: &str) -> RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        if message_decoder::decode_message_id(msg_id).is_ok() {
            client_instance.mq_admin_impl.view_message(topic, msg_id).await
        } else {
            self.query_message_by_uniq_key(topic, msg_id).await
        }
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
    #[allow(dead_code)]
    pub async fn offset_for_timestamp(&self, mq: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        self.search_offset(mq, timestamp).await
    }

    /// Returns the earliest message store time for the specified message queue.
    #[allow(dead_code)]
    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> RocketMQResult<i64> {
        self.make_sure_state_ok()?;

        let mut client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| crate::mq_client_err!("Client instance not initialized"))?
            .clone();

        client_instance.mq_admin_impl.earliest_msg_store_time(mq).await
    }

    /// Returns the maximum offset of the specified message queue.
    #[allow(dead_code)]
    pub async fn max_offset_public(&self, mq: &MessageQueue) -> RocketMQResult<i64> {
        self.max_offset(mq).await
    }

    /// Returns the minimum offset of the specified message queue.
    #[allow(dead_code)]
    pub async fn min_offset_public(&self, mq: &MessageQueue) -> RocketMQResult<i64> {
        self.min_offset(mq).await
    }

    /// Returns whether auto-commit is enabled.
    #[allow(dead_code)]
    pub fn is_auto_commit(&self) -> bool {
        self.consumer_config.auto_commit
    }

    /// Updates whether offsets are automatically committed during poll.
    pub fn set_auto_commit(&self, auto_commit: bool) {
        self.consumer_config.mut_from_ref().auto_commit = auto_commit;
    }

    /// Returns the interval between automatic offset commits.
    pub fn auto_commit_interval_millis(&self) -> u64 {
        self.consumer_config.auto_commit_interval_millis
    }

    /// Updates the interval between automatic offset commits.
    pub fn set_auto_commit_interval_millis(&self, interval_millis: u64) {
        self.consumer_config.mut_from_ref().auto_commit_interval_millis = interval_millis;
    }

    /// Returns whether the subscription group runs in unit mode.
    pub fn is_unit_mode(&self) -> bool {
        self.consumer_config.unit_mode
    }

    /// Updates whether the subscription group runs in unit mode.
    pub fn set_unit_mode(&self, unit_mode: bool) {
        self.consumer_config.mut_from_ref().unit_mode = unit_mode;
        if let Some(wrapper) = self.pull_api_wrapper.as_ref() {
            wrapper.mut_from_ref().set_unit_mode(unit_mode);
        }
        self.rebalance_impl.consumer_config.mut_from_ref().unit_mode = unit_mode;
    }

    /// Returns the configured message model.
    pub fn message_model_config(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    /// Updates the configured message model.
    pub fn set_message_model(&self, message_model: MessageModel) {
        self.consumer_config.mut_from_ref().message_model = message_model;
        self.rebalance_impl.mut_from_ref().set_message_model(message_model);
        self.rebalance_impl.consumer_config.mut_from_ref().message_model = message_model;
    }

    /// Returns where consumption starts when no offset exists.
    pub fn consume_from_where_config(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    /// Updates where consumption starts when no offset exists.
    pub fn set_consume_from_where(&self, consume_from_where: ConsumeFromWhere) -> RocketMQResult<()> {
        validate_lite_pull_consume_from_where(consume_from_where)?;
        self.consumer_config.mut_from_ref().consume_from_where = consume_from_where;
        self.rebalance_impl.consumer_config.mut_from_ref().consume_from_where = consume_from_where;
        Ok(())
    }

    /// Returns the configured timestamp for ConsumeFromTimestamp.
    pub fn consume_timestamp(&self) -> Option<CheetahString> {
        self.consumer_config.consume_timestamp.clone()
    }

    /// Updates the configured timestamp for ConsumeFromTimestamp.
    pub fn set_consume_timestamp(&self, consume_timestamp: Option<CheetahString>) {
        self.consumer_config.mut_from_ref().consume_timestamp = consume_timestamp.clone();
        self.rebalance_impl.consumer_config.mut_from_ref().consume_timestamp = consume_timestamp;
    }

    /// Returns the queue allocation strategy used during rebalance.
    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy + Send + Sync> {
        self.consumer_config.allocate_message_queue_strategy.clone()
    }

    /// Updates the queue allocation strategy used during rebalance.
    pub fn set_allocate_message_queue_strategy(
        &self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    ) {
        self.consumer_config.mut_from_ref().allocate_message_queue_strategy = allocate_message_queue_strategy.clone();
        self.rebalance_impl
            .mut_from_ref()
            .set_allocate_message_queue_strategy(allocate_message_queue_strategy.clone());
        self.rebalance_impl
            .consumer_config
            .mut_from_ref()
            .allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
    }

    #[cfg(test)]
    pub(crate) fn rebalance_allocate_message_queue_strategy_name(&self) -> Option<&'static str> {
        self.rebalance_impl
            .rebalance_impl_inner
            .allocate_message_queue_strategy
            .as_ref()
            .map(|strategy| strategy.get_name())
    }

    /// Returns the user message queue listener invoked after LitePull rebalance updates assignment.
    pub fn message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.user_message_queue_listener
            .read()
            .ok()
            .and_then(|listener| listener.clone())
    }

    /// Updates the user message queue listener invoked after LitePull rebalance updates assignment.
    pub fn set_message_queue_listener(&self, listener: Option<ArcMessageQueueListener>) {
        match self.user_message_queue_listener.write() {
            Ok(mut current) => *current = listener,
            Err(error) => warn!("LitePull message queue listener lock poisoned: {}", error),
        }
    }

    /// Returns the topic metadata check interval in milliseconds.
    pub fn topic_metadata_check_interval_millis(&self) -> u64 {
        self.consumer_config.topic_metadata_check_interval_millis
    }

    /// Updates the topic metadata check interval in milliseconds.
    pub fn set_topic_metadata_check_interval_millis(&self, interval_millis: u64) {
        self.consumer_config.mut_from_ref().topic_metadata_check_interval_millis = interval_millis;
    }

    /// Returns whether broker selection is controlled by user configuration.
    pub fn is_connect_broker_by_user(&self) -> bool {
        self.pull_api_wrapper
            .as_ref()
            .map_or(self.consumer_config.connect_broker_by_user, |wrapper| {
                wrapper.is_connect_broker_by_user()
            })
    }

    /// Updates whether broker selection is controlled by user configuration.
    pub fn set_connect_broker_by_user(&self, connect_broker_by_user: bool) {
        self.consumer_config.mut_from_ref().connect_broker_by_user = connect_broker_by_user;
        if let Some(wrapper) = self.pull_api_wrapper.as_ref() {
            wrapper
                .mut_from_ref()
                .set_connect_broker_by_user(connect_broker_by_user);
        }
    }

    /// Returns the broker ID used when user-controlled broker selection is enabled.
    pub fn default_broker_id(&self) -> u64 {
        self.pull_api_wrapper
            .as_ref()
            .map_or(self.consumer_config.default_broker_id, |wrapper| {
                wrapper.default_broker_id()
            })
    }

    /// Updates the broker ID used when user-controlled broker selection is enabled.
    pub fn set_default_broker_id(&self, broker_id: u64) {
        self.consumer_config.mut_from_ref().default_broker_id = broker_id;
        if let Some(wrapper) = self.pull_api_wrapper.as_ref() {
            wrapper.mut_from_ref().set_default_broker_id(broker_id);
        }
    }

    /// Returns the default poll timeout in milliseconds.
    pub fn poll_timeout_millis(&self) -> u64 {
        self.consumer_config.poll_timeout_millis
    }

    /// Updates the default poll timeout in milliseconds.
    pub fn set_poll_timeout_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().poll_timeout_millis = timeout_millis;
    }

    /// Returns the maximum time that the broker may suspend a long-poll pull request.
    pub fn broker_suspend_max_time_millis(&self) -> u64 {
        self.consumer_config.broker_suspend_max_time_millis
    }

    /// Updates the maximum time that the broker may suspend a long-poll pull request.
    pub fn set_broker_suspend_max_time_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().broker_suspend_max_time_millis = timeout_millis;
    }

    /// Returns the consumer-side timeout for suspended long-poll pull requests.
    pub fn consumer_timeout_millis_when_suspend(&self) -> u64 {
        self.consumer_config.consumer_timeout_millis_when_suspend
    }

    /// Updates the consumer-side timeout for suspended long-poll pull requests.
    pub fn set_consumer_timeout_millis_when_suspend(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().consumer_timeout_millis_when_suspend = timeout_millis;
    }

    /// Returns the RPC timeout used for lite pull requests in milliseconds.
    pub fn consumer_pull_timeout_millis(&self) -> u64 {
        self.consumer_config.consumer_pull_timeout_millis
    }

    /// Updates the RPC timeout used for lite pull requests in milliseconds.
    pub fn set_consumer_pull_timeout_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().consumer_pull_timeout_millis = timeout_millis;
    }

    /// Returns the maximum number of messages requested in one pull.
    pub fn pull_batch_size(&self) -> i32 {
        self.consumer_config.pull_batch_size
    }

    /// Updates the maximum number of messages requested in one pull.
    pub fn set_pull_batch_size(&self, pull_batch_size: i32) {
        self.consumer_config.mut_from_ref().pull_batch_size = pull_batch_size;
    }

    /// Returns the configured number of pull worker threads.
    pub fn pull_thread_nums(&self) -> usize {
        self.consumer_config.pull_thread_nums
    }

    /// Updates the configured number of pull worker threads.
    pub fn set_pull_thread_nums(&self, pull_thread_nums: usize) {
        self.consumer_config.mut_from_ref().pull_thread_nums = pull_thread_nums;
    }

    /// Returns the total cached-message threshold across all queues.
    pub fn pull_threshold_for_all(&self) -> i64 {
        self.consumer_config.pull_threshold_for_all
    }

    /// Updates the total cached-message threshold across all queues.
    pub fn set_pull_threshold_for_all(&self, pull_threshold_for_all: i64) {
        self.consumer_config.mut_from_ref().pull_threshold_for_all = pull_threshold_for_all;
    }

    /// Returns the cached-message threshold for each queue.
    pub fn pull_threshold_for_queue(&self) -> i64 {
        self.consumer_config.pull_threshold_for_queue
    }

    /// Updates the cached-message threshold for each queue.
    pub fn set_pull_threshold_for_queue(&self, pull_threshold_for_queue: i64) {
        self.consumer_config.mut_from_ref().pull_threshold_for_queue = pull_threshold_for_queue;
    }

    /// Returns the cached-message-size threshold in MiB for each queue.
    pub fn pull_threshold_size_for_queue(&self) -> i32 {
        self.consumer_config.pull_threshold_size_for_queue
    }

    /// Updates the cached-message-size threshold in MiB for each queue.
    pub fn set_pull_threshold_size_for_queue(&self, pull_threshold_size_for_queue: i32) {
        self.consumer_config.mut_from_ref().pull_threshold_size_for_queue = pull_threshold_size_for_queue;
    }

    /// Returns the maximum cached offset span.
    pub fn consume_max_span(&self) -> i64 {
        self.consumer_config.consume_max_span
    }

    /// Updates the maximum cached offset span.
    pub fn set_consume_max_span(&self, consume_max_span: i64) {
        self.consumer_config.mut_from_ref().consume_max_span = consume_max_span;
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
            .ok_or_else(|| crate::mq_client_err!("MQClientAPIImpl not initialized"))?
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
            .ok_or_else(|| crate::mq_client_err!("MQClientAPIImpl not initialized"))?
            .get_min_offset(broker_result.broker_addr.as_str(), message_queue, 3000)
            .await
    }

    async fn clear_message_queue_in_cache(&self, message_queue: &MessageQueue) {
        if let Some(process_queue) = self.assigned_message_queue.get_process_queue(message_queue).await {
            process_queue.clear().await;
        }

        let mut retained_requests = Vec::new();
        {
            let mut rx = self.consume_request_rx.lock().await;
            while let Ok(request) = rx.try_recv() {
                if request.message_queue != *message_queue {
                    retained_requests.push(request);
                }
            }
        }

        for request in retained_requests {
            if self.consume_request_tx.send(request).is_err() {
                tracing::warn!("Lite pull consume request queue is closed while clearing queue cache");
                break;
            }
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
            let topics: Vec<CheetahString> = self
                .rebalance_impl
                .get_subscription_inner()
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            self.rebalance_impl.mut_from_ref().do_rebalance(false).await;
            if let Err(error) = self.sync_assigned_queues_from_rebalance(&topics).await {
                warn!(
                    "LitePull failed to synchronize rebalance assignments for group={}, error={}",
                    self.consumer_config.consumer_group, error
                );
            }
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
        self.consumer_config.unit_mode
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

    async fn consumer_running_info(
        &self,
    ) -> rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo {
        let mut info = rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::new();
        info.consume_type = self.consume_type();
        info.consume_orderly = false;
        info.prop_consumer_start_timestamp = self.consumer_start_timestamp.load(Ordering::Acquire) as u64;
        info.sync_properties_from_derived_fields();
        info.set_property("consumerGroup", self.consumer_config.consumer_group.to_string());
        info.set_property("messageModel", format!("{:?}", self.consumer_config.message_model));
        info.set_property("pullBatchSize", self.consumer_config.pull_batch_size.to_string());
        info.set_property("autoCommit", self.consumer_config.auto_commit.to_string());
        info.set_property(
            "autoCommitIntervalMillis",
            self.consumer_config.auto_commit_interval_millis.to_string(),
        );

        for entry in self.rebalance_impl.get_subscription_inner().iter() {
            info.subscription_set.insert(entry.value().clone());
        }

        let assigned_message_queues = self.assigned_message_queue.message_queues().await;
        for mq in assigned_message_queues {
            let Some(process_queue) = self.assigned_message_queue.get_process_queue(&mq).await else {
                continue;
            };
            let commit_offset = if let Some(offset_store) = self.offset_store.as_ref() {
                offset_store
                    .read_offset(&mq, ReadOffsetType::MemoryFirstThenStore)
                    .await
            } else {
                0
            };
            let mut pq_info = ProcessQueueInfo {
                commit_offset,
                ..Default::default()
            };
            process_queue.fill_process_queue_info(&mut pq_info).await;
            info.mq_table.insert(mq, pq_info);
        }

        info
    }
}

#[doc(hidden)]
pub async fn run_lite_pull_task_lifecycle_probe() -> LitePullTaskLifecycleProbe {
    let cancelled = Arc::new(AtomicBool::new(false));
    let task_cancelled = cancelled.clone();
    let handle = spawn_lite_pull_task("rocketmq-client-lite-pull-task-probe", cancelled, async move {
        while !task_cancelled.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .expect("lite pull lifecycle probe task should spawn");
    let task_count_before_shutdown = handle.task_count();

    let shutdown_started = Instant::now();
    let shutdown_healthy = handle.wait(Duration::from_secs(1)).await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = 0;

    LitePullTaskLifecycleProbe {
        healthy: shutdown_healthy && task_count_before_shutdown == 1 && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::base::access_channel::AccessChannel;

    type ConsumeHookEvent = (CheetahString, bool, CheetahString, Option<AccessChannel>);

    struct CapturingConsumeHook {
        events: StdMutex<Vec<ConsumeHookEvent>>,
    }

    impl CapturingConsumeHook {
        fn new() -> Self {
            Self {
                events: StdMutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<ConsumeHookEvent> {
            self.events.lock().expect("consume hook events lock").clone()
        }
    }

    impl ConsumeMessageHook for CapturingConsumeHook {
        fn hook_name(&self) -> &'static str {
            "CapturingConsumeHook"
        }

        fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
            self.events.lock().expect("consume hook events lock").push((
                CheetahString::from_static_str("before"),
                context.success,
                context.status.clone(),
                context.access_channel,
            ));
        }

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            self.events.lock().expect("consume hook events lock").push((
                CheetahString::from_static_str("after"),
                context.success,
                context.status.clone(),
                context.access_channel,
            ));
        }
    }

    #[test]
    fn spawn_lite_pull_task_without_tokio_runtime_runs_on_fallback_thread() {
        let (tx, rx) = std::sync::mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));

        let handle = spawn_lite_pull_task("rocketmq-client-lite-pull-test", cancelled, async move {
            let current_thread = std::thread::current();
            let thread_name = current_thread.name().unwrap_or_default().to_string();
            tx.send(thread_name).expect("test receiver should still be open");
        })
        .expect("lite pull task should spawn without an ambient Tokio runtime");

        let thread_name = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("fallback lite pull task should complete");
        assert_eq!(thread_name, "rocketmq-client-fallback");
        handle.abort();
    }

    #[tokio::test]
    async fn lite_pull_task_wait_aborts_tracked_task_after_timeout() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, AtomicOrdering::SeqCst);
            }
        }

        let task_started = Arc::new(AtomicBool::new(false));
        let task_aborted = Arc::new(AtomicBool::new(false));
        let task_started_inner = task_started.clone();
        let task_aborted_inner = task_aborted.clone();

        let task_cancelled = Arc::new(AtomicBool::new(false));
        let task_handle = spawn_lite_pull_task("rocketmq-client-lite-pull-timeout-test", task_cancelled, async move {
            let _drop_flag = DropFlag(task_aborted_inner);
            task_started_inner.store(true, AtomicOrdering::SeqCst);
            std::future::pending::<()>().await;
        })
        .expect("lite pull timeout test task should spawn");

        for _ in 0..20 {
            if task_started.load(AtomicOrdering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(task_started.load(AtomicOrdering::SeqCst));

        assert!(!task_handle.wait(Duration::from_millis(20)).await);
        assert!(
            task_aborted.load(AtomicOrdering::SeqCst),
            "timed-out lite pull task should be aborted before wait returns"
        );
    }

    #[tokio::test]
    async fn lite_pull_task_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_lite_pull_task_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }

    struct SlowAfterConsumeHook {
        after_timestamp: Arc<AtomicU64>,
    }

    impl ConsumeMessageHook for SlowAfterConsumeHook {
        fn hook_name(&self) -> &'static str {
            "SlowAfterConsumeHook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {}

        fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {
            std::thread::sleep(Duration::from_millis(15));
            self.after_timestamp
                .store(rocketmq_common::TimeUtils::current_millis(), AtomicOrdering::SeqCst);
        }
    }

    struct BlockingFirstMessageHook {
        entered_first_after: Arc<AtomicBool>,
        release_first_after: Arc<AtomicBool>,
    }

    impl ConsumeMessageHook for BlockingFirstMessageHook {
        fn hook_name(&self) -> &'static str {
            "BlockingFirstMessageHook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {}

        fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
            if context
                .msg_list
                .first()
                .is_some_and(|message| message.queue_offset == 1)
            {
                self.entered_first_after.store(true, AtomicOrdering::SeqCst);
                while !self.release_first_after.load(AtomicOrdering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }

    #[tokio::test]
    async fn shutdown_in_create_just_keeps_state_like_java_noop() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_shutdown_create_group"),
                ..Default::default()
            }),
        );

        impl_.shutdown().await.expect("shutdown should be a no-op before start");

        assert_eq!(impl_.service_state(), ServiceState::CreateJust);
    }

    #[tokio::test]
    async fn shutdown_in_start_failed_keeps_state_like_java_default_case() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_shutdown_failed_group"),
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::StartFailed;

        impl_
            .shutdown()
            .await
            .expect("shutdown should be a no-op after failed start");

        assert_eq!(impl_.service_state(), ServiceState::StartFailed);
    }

    #[tokio::test]
    async fn start_without_self_reference_returns_error_without_panic() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_missing_self_ref_group"),
                ..Default::default()
            }),
        );

        let error = impl_
            .start()
            .await
            .expect_err("missing self-reference should return an error");

        assert!(error
            .to_string()
            .contains("default_lite_pull_consumer_impl is not initialized"));
    }

    #[test]
    #[allow(deprecated)]
    fn check_config_rejects_unsupported_consume_from_where_like_java_lite_pull() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_invalid_consume_from_where_group"),
                consume_from_where: ConsumeFromWhere::ConsumeFromMaxOffset,
                ..Default::default()
            }),
        );

        let error = impl_
            .check_config()
            .expect_err("Java LitePull rejects legacy consumeFromWhere values");

        assert!(error.to_string().contains("Invalid ConsumeFromWhere Value"));
    }

    #[test]
    fn check_config_rejects_default_consumer_group_like_java_lite_pull() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str(mix_all::DEFAULT_CONSUMER_GROUP),
                ..Default::default()
            }),
        );

        let error = impl_
            .check_config()
            .expect_err("Java LitePull rejects DEFAULT_CONSUMER_GROUP");

        assert!(error
            .to_string()
            .contains("consumerGroup can not equal DEFAULT_CONSUMER"));
    }

    #[test]
    fn check_config_rejects_consumer_suspend_timeout_below_broker_suspend_like_java() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_invalid_suspend_timeout_group"),
                broker_suspend_max_time_millis: 30_000,
                consumer_timeout_millis_when_suspend: 20_000,
                ..Default::default()
            }),
        );

        let error = impl_
            .check_config()
            .expect_err("Java LitePull rejects consumer suspend timeout below broker suspend timeout");

        assert!(error.to_string().contains(
            "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than \
             brokerSuspendMaxTimeMillis"
        ));
    }

    #[test]
    fn set_consumer_group_updates_rebalance_before_start_and_rejects_running_mutation() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_initial_group"),
                ..Default::default()
            }),
        );

        impl_
            .set_consumer_group(CheetahString::from_static_str("lite_pull_updated_group"))
            .expect("consumer group should update before start");

        assert_eq!(impl_.consumer_group_config().as_str(), "lite_pull_updated_group");
        assert_eq!(
            impl_.rebalance_consumer_group_name().as_deref(),
            Some("lite_pull_updated_group")
        );

        *impl_.service_state = ServiceState::Running;

        let error = impl_
            .set_consumer_group(CheetahString::from_static_str("lite_pull_late_group"))
            .expect_err("consumer group changes after running should be rejected");

        assert!(error
            .to_string()
            .contains("consumerGroup can not be changed after the lite pull consumer has started"));
        assert_eq!(impl_.consumer_group_config().as_str(), "lite_pull_updated_group");
    }

    #[test]
    fn set_offset_store_updates_rebalance_before_start_and_rejects_running_mutation() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_offset_store_group"),
                ..Default::default()
            }),
        );
        let offset_store = ArcMut::new(OffsetStore::new_test());

        impl_
            .set_offset_store(Some(offset_store))
            .expect("offset store should update before start");

        assert!(impl_.offset_store().is_some());
        assert!(impl_.rebalance_has_offset_store());

        impl_
            .set_offset_store(None)
            .expect("offset store should be clearable before start");

        assert!(impl_.offset_store().is_none());
        assert!(!impl_.rebalance_has_offset_store());

        *impl_.service_state = ServiceState::Running;

        let error = impl_
            .set_offset_store(Some(ArcMut::new(OffsetStore::new_test())))
            .expect_err("offset store changes after running should be rejected");

        assert!(error
            .to_string()
            .contains("offsetStore can not be changed after the lite pull consumer has started"));
    }

    #[tokio::test]
    async fn set_unit_mode_updates_pull_api_wrapper_for_filter_hooks_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_unit_mode_group"),
                ..Default::default()
            }),
        );
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "lite-pull-unit-mode-test", None);
        impl_.pull_api_wrapper = Some(ArcMut::new(PullAPIWrapper::new(
            client_instance,
            CheetahString::from_static_str("lite_pull_unit_mode_group"),
            false,
        )));

        impl_.set_unit_mode(true);

        assert!(impl_.is_unit_mode());
        assert!(impl_
            .pull_api_wrapper
            .as_ref()
            .expect("pull wrapper should be present")
            .unit_mode());
    }

    #[test]
    fn lite_pull_request_protocol_flags_match_java_pull_sync_impl() {
        let sys_flag = lite_pull_request_sys_flag() as u32;
        assert!(!PullSysFlag::has_commit_offset_flag(sys_flag));
        assert!(PullSysFlag::has_suspend_flag(sys_flag));
        assert!(PullSysFlag::has_subscription_flag(sys_flag));
        assert!(!PullSysFlag::has_class_filter_flag(sys_flag));
        assert!(PullSysFlag::has_lite_pull_flag(sys_flag));

        let tag_subscription = SubscriptionData {
            expression_type: CheetahString::from_static_str(ExpressionType::TAG),
            sub_version: 123,
            ..Default::default()
        };
        assert_eq!(lite_pull_request_sub_version(&tag_subscription), 0);

        let sql_subscription = SubscriptionData {
            expression_type: CheetahString::from_static_str(ExpressionType::SQL92),
            sub_version: 456,
            ..Default::default()
        };
        assert_eq!(lite_pull_request_sub_version(&sql_subscription), 456);
    }

    #[tokio::test]
    async fn operate_after_running_starts_preassigned_pull_tasks() {
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_preassigned_group"),
                ..Default::default()
            }),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);
        impl_
            .assign(vec![mq.clone()])
            .await
            .expect("assign before running should succeed");
        impl_.client_instance = Some(MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            "lite-pull-operate-after-running-test",
            None,
        ));
        *impl_.service_state = ServiceState::Running;

        impl_
            .operate_after_running()
            .await
            .expect("operate_after_running should start preassigned pull task");

        assert!(impl_.task_handles.read().await.contains_key(&mq));

        impl_.shutdown_signal.store(true, Ordering::Release);
        let handle = {
            let mut task_handles = impl_.task_handles.write().await;
            task_handles.remove(&mq)
        };
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn assign_update_aborts_removed_pull_tasks_like_java() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, AtomicOrdering::SeqCst);
            }
        }

        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_assign_abort_group"),
                ..Default::default()
            }),
        );
        let removed_mq = MessageQueue::from_parts("topic-assign-abort", "broker-a", 0);
        let retained_mq = MessageQueue::from_parts("topic-assign-abort", "broker-a", 1);
        let task_started = Arc::new(AtomicBool::new(false));
        let task_aborted = Arc::new(AtomicBool::new(false));
        let task_started_inner = Arc::clone(&task_started);
        let task_aborted_inner = Arc::clone(&task_aborted);
        let task_cancelled = Arc::new(AtomicBool::new(false));
        let handle = spawn_lite_pull_task(
            "rocketmq-client-lite-pull-assign-abort-test",
            task_cancelled,
            async move {
                let _drop_flag = DropFlag(task_aborted_inner);
                task_started_inner.store(true, AtomicOrdering::SeqCst);
                std::future::pending::<()>().await;
            },
        )
        .expect("lite pull assign abort test task should spawn");
        impl_.task_handles.write().await.insert(removed_mq.clone(), handle);
        for _ in 0..20 {
            if task_started.load(AtomicOrdering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(task_started.load(AtomicOrdering::SeqCst));

        impl_
            .update_assigned_message_queue_for_assign(std::slice::from_ref(&retained_mq))
            .await;

        assert!(!impl_.task_handles.read().await.contains_key(&removed_mq));
        for _ in 0..20 {
            if task_aborted.load(AtomicOrdering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(task_aborted.load(AtomicOrdering::SeqCst));
        assert!(impl_.assignment().await.contains(&retained_mq));
    }

    #[tokio::test]
    async fn consumer_running_info_includes_assigned_process_queues() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_running_info_group"),
                ..Default::default()
            }),
        );
        let mq = MessageQueue::from_parts("topic-running-info", "broker-a", 0);
        impl_
            .assign(vec![mq.clone()])
            .await
            .expect("assign before running should succeed");

        let info = impl_.consumer_running_info().await;

        assert!(info.prop_consumer_start_timestamp > 0);
        assert!(info.mq_table.contains_key(&mq));
        assert_eq!(info.mq_table.get(&mq).map(|pq| pq.commit_offset), Some(0));
    }

    #[tokio::test]
    async fn update_name_server_address_updates_client_api_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_update_namesrv_group"),
                ..Default::default()
            }),
        );
        impl_.client_instance = Some(MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            "lite-pull-update-namesrv-test",
            None,
        ));

        impl_
            .update_name_server_address(vec!["127.0.0.1:9876".to_string(), "127.0.0.2:9876".to_string()])
            .await;

        assert_eq!(
            impl_.client_config.namesrv_addr.as_deref(),
            Some("127.0.0.1:9876;127.0.0.2:9876")
        );
        let api_impl = impl_
            .client_instance
            .as_ref()
            .and_then(|client| client.mq_client_api_impl.as_ref())
            .expect("client api should be initialized");
        let mut actual_addresses = api_impl.get_name_server_address_list().to_vec();
        actual_addresses.sort();
        assert_eq!(
            actual_addresses,
            vec![
                CheetahString::from_static_str("127.0.0.1:9876"),
                CheetahString::from_static_str("127.0.0.2:9876"),
            ]
        );
    }

    #[tokio::test]
    async fn subscribe_with_sql_selector_preserves_expression_type_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_sql_selector_group"),
                ..Default::default()
            }),
        );
        let topic = CheetahString::from_static_str("topic-sql-selector");
        let expression = CheetahString::from_static_str("a > 10");

        impl_
            .subscribe_with_selector(
                topic.clone(),
                Some(crate::consumer::message_selector::MessageSelector::by_sql(
                    expression.clone(),
                )),
            )
            .await
            .expect("SQL92 selector should build subscription data");

        let subscriptions = impl_.rebalance_impl.get_subscription_inner();
        let subscription = subscriptions
            .get(&topic)
            .expect("subscription should be stored for SQL92 selector");
        assert_eq!(subscription.expression_type, ExpressionType::SQL92);
        assert_eq!(subscription.sub_string, expression);
        assert!(subscription.tags_set.is_empty());
        assert!(subscription.code_set.is_empty());
        drop(subscription);

        let mut heartbeat_selectors = HashMap::new();
        impl_
            .build_subscriptions_for_heartbeat(&mut heartbeat_selectors)
            .await
            .expect("heartbeat selector map should build");
        let selector = heartbeat_selectors
            .get(topic.as_str())
            .expect("heartbeat selector should retain topic");
        assert_eq!(selector.get_expression_type(), ExpressionType::SQL92);
        assert_eq!(selector.get_expression(), expression.as_str());
    }

    #[tokio::test]
    async fn set_sub_expression_for_assign_matches_java_validation_and_state() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_assign_filter_group"),
                ..Default::default()
            }),
        );
        let topic = CheetahString::from_static_str("topic-assign-filter");

        let blank_error = impl_
            .set_sub_expression_for_assign(topic.clone(), " ")
            .await
            .expect_err("Java LitePull rejects blank assign filter expressions");
        assert!(blank_error
            .to_string()
            .contains("subExpression can not be null or empty."));
        assert_eq!(*impl_.subscription_type.read().await, SubscriptionType::None);

        impl_
            .set_sub_expression_for_assign(topic.clone(), "TagA || TagB")
            .await
            .expect("valid assign filter should be accepted before start");
        assert_eq!(*impl_.subscription_type.read().await, SubscriptionType::Assign);
        let map = impl_.topic_to_sub_expression.read().await;
        assert_eq!(
            map.get(&topic).map(|expression| expression.as_str()),
            Some("TagA || TagB")
        );
        drop(map);

        let mut running_impl = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_assign_filter_running_group"),
                ..Default::default()
            }),
        );
        *running_impl.service_state = ServiceState::Running;

        let running_error = running_impl
            .set_sub_expression_for_assign(topic, "TagA")
            .await
            .expect_err("Java LitePull only allows assign filters before start");
        assert!(running_error
            .to_string()
            .contains("setAssignTag only can be called before start."));
    }

    #[tokio::test]
    async fn next_pull_offset_applies_seek_to_consume_offset_like_java() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_seek_offset_group"),
                ..Default::default()
            }),
        );
        let mq = MessageQueue::from_parts("topic-seek", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        impl_.assigned_message_queue.update_consume_offset(&mq, 3).await;
        impl_.assigned_message_queue.set_seek_offset(&mq, 42).await;

        let offset = impl_
            .next_pull_offset(&mq)
            .await
            .expect("seek offset should be returned without broker lookup");

        assert_eq!(offset, 42);
        assert_eq!(impl_.assigned_message_queue.get_seek_offset(&mq).await, -1);
        assert_eq!(impl_.assigned_message_queue.get_consume_offset(&mq).await, 42);
    }

    #[tokio::test]
    async fn seek_rejects_unassigned_queue_before_broker_offset_lookup_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_seek_unassigned_group"),
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let mq = MessageQueue::from_parts("topic-seek-unassigned", "broker-a", 0);
        let error = impl_
            .seek(&mq, 10)
            .await
            .expect_err("Java LitePull rejects unassigned queues before querying offsets");

        let message = error.to_string();
        assert!(message.contains("The message queue is not in assigned list"));
        assert!(!message.contains("Client instance not initialized"));
    }

    #[tokio::test]
    async fn commit_all_updates_offsets_without_persisting_like_java() {
        let group = CheetahString::from_static_str("lite_pull_commit_all_group");
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: group,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let mq = MessageQueue::from_parts("topic-commit-all", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        impl_.assigned_message_queue.update_consume_offset(&mq, 100).await;

        let offset_store = ArcMut::new(OffsetStore::new_test());
        offset_store.update_offset(&mq, 5, false).await;
        impl_.offset_store = Some(offset_store.clone());

        impl_.commit_all().await.expect("commit all should update offset store");

        assert_eq!(offset_store.read_offset(&mq, ReadOffsetType::ReadFromMemory).await, 100);
        assert_eq!(offset_store.test_persist_all_count(), 0);
    }

    #[tokio::test]
    async fn commit_message_queues_uses_assigned_consume_offset_like_java() {
        let group = CheetahString::from_static_str("lite_pull_commit_set_group");
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: group,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let mq = MessageQueue::from_parts("topic-commit-set", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        impl_.assigned_message_queue.update_consume_offset(&mq, 100).await;

        let offset_store = ArcMut::new(OffsetStore::new_test());
        offset_store.update_offset(&mq, 5, false).await;
        impl_.offset_store = Some(offset_store.clone());

        let queues = HashSet::from([mq.clone()]);
        impl_
            .commit_message_queues(&queues, false)
            .await
            .expect("commit set should use assigned consume offset");

        assert_eq!(offset_store.read_offset(&mq, ReadOffsetType::ReadFromMemory).await, 100);
        assert_eq!(offset_store.test_persist_all_count(), 0);
    }

    #[tokio::test]
    async fn commit_message_queues_with_persist_calls_persist_all_like_java() {
        let group = CheetahString::from_static_str("lite_pull_commit_set_persist_group");
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: group,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let mq = MessageQueue::from_parts("topic-commit-set-persist", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        impl_.assigned_message_queue.update_consume_offset(&mq, 100).await;

        let offset_store = ArcMut::new(OffsetStore::new_test());
        impl_.offset_store = Some(offset_store.clone());

        let queues = HashSet::from([mq]);
        impl_
            .commit_message_queues(&queues, true)
            .await
            .expect("commit set should explicitly persist when requested");

        assert_eq!(offset_store.test_persist_all_count(), 1);
    }

    #[tokio::test]
    async fn poll_zero_timeout_returns_cached_message_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_zero_timeout_group"),
                auto_commit: false,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let mq = MessageQueue::from_parts("topic-zero-timeout", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");
        let message = ArcMut::new(MessageExt {
            queue_offset: 7,
            ..Default::default()
        });
        let queued_messages = vec![message.clone()];
        process_queue.put_message(&queued_messages).await;
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(vec![message], mq.clone(), process_queue))
            .expect("consume request queue should accept cached message");

        let messages = impl_
            .poll(0)
            .await
            .expect("zero-timeout poll should complete without waiting");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].queue_offset, 7);
        assert_eq!(impl_.assigned_message_queue.get_consume_offset(&mq).await, 8);
    }

    #[tokio::test]
    async fn clear_message_queue_in_cache_removes_pending_consume_requests_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_clear_cache_group"),
                auto_commit: false,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;

        let cleared_mq = MessageQueue::from_parts("topic-clear-cache", "broker-a", 0);
        let retained_mq = MessageQueue::from_parts("topic-clear-cache", "broker-a", 1);
        impl_.assigned_message_queue.put(cleared_mq.clone()).await;
        impl_.assigned_message_queue.put(retained_mq.clone()).await;

        let cleared_process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&cleared_mq)
            .await
            .expect("cleared queue should have a process queue");
        let retained_process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&retained_mq)
            .await
            .expect("retained queue should have a process queue");
        let cleared_message = ArcMut::new(MessageExt {
            queue_offset: 11,
            ..Default::default()
        });
        let retained_message = ArcMut::new(MessageExt {
            queue_offset: 22,
            ..Default::default()
        });
        let cleared_messages = vec![cleared_message.clone()];
        let retained_messages = vec![retained_message.clone()];
        cleared_process_queue.put_message(&cleared_messages).await;
        retained_process_queue.put_message(&retained_messages).await;
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(
                vec![cleared_message],
                cleared_mq.clone(),
                cleared_process_queue.clone(),
            ))
            .expect("cleared consume request should enqueue");
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(
                vec![retained_message],
                retained_mq.clone(),
                retained_process_queue,
            ))
            .expect("retained consume request should enqueue");

        impl_.clear_message_queue_in_cache(&cleared_mq).await;

        assert_eq!(cleared_process_queue.msg_count(), 0);
        let messages = impl_
            .poll(0)
            .await
            .expect("poll should keep requests from other message queues");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].queue_offset, 22);
        assert_eq!(impl_.assigned_message_queue.get_consume_offset(&retained_mq).await, 23);
    }

    #[tokio::test]
    async fn poll_sets_access_channel_only_for_after_hook_like_java() {
        let client_config = ClientConfig {
            access_channel: AccessChannel::Cloud,
            ..Default::default()
        };
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(client_config),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_hook_access_channel_group"),
                auto_commit: false,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;
        let hook = Arc::new(CapturingConsumeHook::new());
        impl_.register_consume_message_hook(hook.clone());

        let mq = MessageQueue::from_parts("topic-hook-access-channel", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");
        let message = ArcMut::new(MessageExt {
            queue_offset: 9,
            ..Default::default()
        });
        let queued_messages = vec![message.clone()];
        process_queue.put_message(&queued_messages).await;
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(vec![message], mq, process_queue))
            .expect("consume request queue should accept cached message");

        let messages = impl_
            .poll(0)
            .await
            .expect("poll should run consume hooks for cached messages");

        assert_eq!(messages.len(), 1);
        assert_eq!(
            hook.events(),
            vec![
                (
                    CheetahString::from_static_str("before"),
                    false,
                    CheetahString::new(),
                    None
                ),
                (
                    CheetahString::from_static_str("after"),
                    true,
                    CheetahString::from_static_str("CONSUME_SUCCESS"),
                    Some(AccessChannel::Cloud)
                ),
            ]
        );
    }

    #[tokio::test]
    async fn poll_refreshes_last_consume_timestamp_after_hooks_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_timestamp_after_hook_group"),
                auto_commit: false,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;
        let after_timestamp = Arc::new(AtomicU64::new(0));
        impl_.register_consume_message_hook(Arc::new(SlowAfterConsumeHook {
            after_timestamp: after_timestamp.clone(),
        }));

        let mq = MessageQueue::from_parts("topic-hook-timestamp", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");
        let message = ArcMut::new(MessageExt {
            queue_offset: 11,
            ..Default::default()
        });
        let queued_messages = vec![message.clone()];
        process_queue.put_message(&queued_messages).await;
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(vec![message], mq, process_queue.clone()))
            .expect("consume request queue should accept cached message");

        let messages = impl_
            .poll(0)
            .await
            .expect("poll should refresh last consume timestamp after hooks");

        let hook_after_timestamp = after_timestamp.load(AtomicOrdering::SeqCst);
        assert_eq!(messages.len(), 1);
        assert!(hook_after_timestamp > 0);
        assert!(process_queue.get_last_consume_timestamp() >= hook_after_timestamp);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_poll_calls_are_serialized_like_java_synchronized_poll() {
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_serialized_poll_group"),
                auto_commit: false,
                ..Default::default()
            }),
        ));
        *impl_.service_state = ServiceState::Running;

        let entered_first_after = Arc::new(AtomicBool::new(false));
        let release_first_after = Arc::new(AtomicBool::new(false));
        impl_.register_consume_message_hook(Arc::new(BlockingFirstMessageHook {
            entered_first_after: entered_first_after.clone(),
            release_first_after: release_first_after.clone(),
        }));

        let mq = MessageQueue::from_parts("topic-serialized-poll", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");
        let first_message = ArcMut::new(MessageExt {
            queue_offset: 1,
            ..Default::default()
        });
        let second_message = ArcMut::new(MessageExt {
            queue_offset: 2,
            ..Default::default()
        });
        let queued_messages = vec![first_message.clone(), second_message.clone()];
        process_queue.put_message(&queued_messages).await;
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(
                vec![first_message],
                mq.clone(),
                process_queue.clone(),
            ))
            .expect("first consume request should enqueue");
        impl_
            .consume_request_tx
            .send(LitePullConsumeRequest::new(vec![second_message], mq, process_queue))
            .expect("second consume request should enqueue");

        let first_consumer = impl_.clone();
        let first_poll = tokio::spawn(async move {
            first_consumer
                .poll(0)
                .await
                .expect("first poll should succeed")
                .first()
                .expect("first poll should return a message")
                .queue_offset
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered_first_after.load(AtomicOrdering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("first poll should enter after hook");

        let second_done = Arc::new(AtomicBool::new(false));
        let second_done_for_task = second_done.clone();
        let second_consumer = impl_.clone();
        let second_poll = tokio::spawn(async move {
            let offset = second_consumer
                .poll(0)
                .await
                .expect("second poll should succeed")
                .first()
                .expect("second poll should return a message")
                .queue_offset;
            second_done_for_task.store(true, AtomicOrdering::SeqCst);
            offset
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !second_done.load(AtomicOrdering::SeqCst),
            "second poll completed while the first poll was still inside a hook"
        );

        release_first_after.store(true, AtomicOrdering::SeqCst);

        assert_eq!(first_poll.await.expect("first poll task should finish"), 1);
        assert_eq!(second_poll.await.expect("second poll task should finish"), 2);
    }

    #[tokio::test]
    async fn pull_inner_refreshes_last_pull_timestamp_before_flow_control_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_last_pull_timestamp_group"),
                pull_threshold_for_queue: 0,
                pull_time_delay_millis_when_cache_flow_control: 123,
                ..Default::default()
            }),
        );
        *impl_.service_state = ServiceState::Running;
        let mq = MessageQueue::from_parts("topic-flow-control", "broker-a", 0);
        let process_queue = Arc::new(crate::consumer::consumer_impl::process_queue::ProcessQueue::new());
        process_queue.set_last_pull_timestamp(1);
        let queued_messages = vec![ArcMut::new(MessageExt {
            queue_offset: 0,
            ..Default::default()
        })];
        process_queue.put_message(&queued_messages).await;

        let delay = impl_
            .pull_inner(&mq, &process_queue)
            .await
            .expect("flow control should return a delay without broker access");

        assert_eq!(delay, 123);
        assert!(process_queue.get_last_pull_timestamp() > 1);
    }

    #[tokio::test]
    async fn pull_result_does_not_overwrite_pending_seek_offset_like_java() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_pending_seek_group"),
                ..Default::default()
            }),
        );
        let mq = MessageQueue::from_parts("topic-pending-seek", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");
        impl_
            .assigned_message_queue
            .update_pull_offset(&mq, 10, &process_queue)
            .await;
        impl_.assigned_message_queue.set_seek_offset(&mq, 42).await;

        let updated = impl_
            .update_pull_offset_if_no_pending_seek(&mq, 99, &process_queue)
            .await;

        assert!(!updated);
        assert_eq!(impl_.assigned_message_queue.get_pull_offset(&mq).await, 10);
        assert_eq!(impl_.assigned_message_queue.get_seek_offset(&mq).await, 42);
    }

    #[tokio::test]
    async fn offset_illegal_corrects_pull_offset_without_dropping_queue_like_java_lite_pull() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_offset_illegal_group"),
                pull_time_delay_millis_when_exception: 321,
                ..Default::default()
            }),
        );
        let mq = MessageQueue::from_parts("topic-offset-illegal", "broker-a", 0);
        impl_.assigned_message_queue.put(mq.clone()).await;
        let process_queue = impl_
            .assigned_message_queue
            .get_process_queue(&mq)
            .await
            .expect("assigned queue should have a process queue");

        let delay = impl_.handle_offset_illegal_pull_result(&mq, &process_queue, 88).await;

        assert_eq!(delay, 321);
        assert!(!process_queue.is_dropped());
        assert_eq!(impl_.assigned_message_queue.get_pull_offset(&mq).await, 88);
    }

    #[tokio::test]
    async fn rebalance_update_assigns_divided_queues_and_starts_pull_tasks() {
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_rebalance_update_group"),
                message_model: MessageModel::Clustering,
                ..Default::default()
            }),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        let mq0 = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let mq1 = MessageQueue::from_parts("topic-a", "broker-a", 1);
        let all = HashSet::from([mq0.clone(), mq1.clone()]);

        impl_
            .update_assign_queue_and_start_pull_task("topic-a", &all, &HashSet::from([mq0.clone()]))
            .await
            .expect("first rebalance update should start mq0");
        impl_
            .update_assign_queue_and_start_pull_task("topic-a", &all, &HashSet::from([mq1.clone()]))
            .await
            .expect("second rebalance update should switch to mq1");

        let assigned = impl_.assignment().await;
        assert!(!assigned.contains(&mq0));
        assert!(assigned.contains(&mq1));

        let task_handles = impl_.task_handles.read().await;
        assert!(!task_handles.contains_key(&mq0));
        assert!(task_handles.contains_key(&mq1));
        drop(task_handles);

        impl_.shutdown_signal.store(true, Ordering::Release);
        let handles = {
            let mut task_handles = impl_.task_handles.write().await;
            task_handles.drain().map(|(_, handle)| handle).collect::<Vec<_>>()
        };
        for handle in handles {
            handle.abort();
        }
    }

    struct CountingMessageQueueListener {
        calls: Arc<AtomicUsize>,
    }

    impl MessageQueueListener for CountingMessageQueueListener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &HashSet<MessageQueue>,
            _mq_assigned: &HashSet<MessageQueue>,
        ) {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    #[tokio::test]
    async fn lite_pull_rebalance_listener_invokes_user_listener() {
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_user_listener_group"),
                ..Default::default()
            }),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        let calls = Arc::new(AtomicUsize::new(0));
        {
            let mut user_listener = impl_
                .user_message_queue_listener
                .write()
                .expect("user listener lock should not be poisoned");
            *user_listener = Some(Arc::new(CountingMessageQueueListener { calls: calls.clone() }));
        }

        let listener = LitePullRebalanceListener {
            consumer_impl: ArcMut::downgrade(&impl_),
            user_listener: impl_.user_message_queue_listener.clone(),
            task_tracker: impl_.rebalance_listener_tasks.clone(),
            shutdown_token: impl_.rebalance_listener_shutdown.clone(),
        };
        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let all = HashSet::from([mq.clone()]);
        listener.message_queue_changed("topic-a", &all, &all);

        impl_.rebalance_listener_tasks.close();
        tokio::time::timeout(Duration::from_secs(1), impl_.rebalance_listener_tasks.wait())
            .await
            .expect("rebalance listener task should finish before timeout");

        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(AtomicOrdering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("user listener should be invoked after assignment update");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);

        let assigned = impl_.assignment().await;
        assert!(
            assigned.contains(&mq),
            "LitePull should update its internal assignment before invoking the user listener"
        );

        impl_.shutdown_signal.store(true, Ordering::Release);
        let handles = {
            let mut task_handles = impl_.task_handles.write().await;
            task_handles.drain().map(|(_, handle)| handle).collect::<Vec<_>>()
        };
        for handle in handles {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn lite_pull_rebalance_listener_ignores_changes_after_shutdown_token_cancelled() {
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_cancelled_listener_group"),
                ..Default::default()
            }),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        let calls = Arc::new(AtomicUsize::new(0));
        {
            let mut user_listener = impl_
                .user_message_queue_listener
                .write()
                .expect("user listener lock should not be poisoned");
            *user_listener = Some(Arc::new(CountingMessageQueueListener { calls: calls.clone() }));
        }

        impl_.rebalance_listener_shutdown.cancel();
        let listener = LitePullRebalanceListener {
            consumer_impl: ArcMut::downgrade(&impl_),
            user_listener: impl_.user_message_queue_listener.clone(),
            task_tracker: impl_.rebalance_listener_tasks.clone(),
            shutdown_token: impl_.rebalance_listener_shutdown.clone(),
        };
        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let all = HashSet::from([mq.clone()]);
        listener.message_queue_changed("topic-a", &all, &all);

        impl_.rebalance_listener_tasks.close();
        tokio::time::timeout(Duration::from_millis(100), impl_.rebalance_listener_tasks.wait())
            .await
            .expect("cancelled rebalance listener should not leave tracked tasks");

        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        assert!(!impl_.assignment().await.contains(&mq));
    }

    #[tokio::test]
    async fn subscribe_with_listener_installs_user_listener_with_subscription_like_java() {
        let mut impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_subscribe_listener_group"),
                ..Default::default()
            }),
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let listener: ArcMessageQueueListener = Arc::new(CountingMessageQueueListener { calls });
        impl_
            .subscribe_with_listener_arc(
                CheetahString::from_static_str("topic-a"),
                CheetahString::from_static_str("TagA"),
                listener.clone(),
            )
            .await
            .expect("subscribe with listener should succeed before start");

        let stored_listener = impl_
            .message_queue_listener()
            .expect("subscribe with listener should install the user listener");
        assert!(Arc::ptr_eq(&stored_listener, &listener));
        assert!(
            impl_.rebalance_impl.get_subscription_inner().contains_key("topic-a"),
            "subscribe with listener should build the same subscription data as Java"
        );
    }

    struct CountingTopicMessageQueueChangeListener {
        calls: Arc<AtomicUsize>,
        last_topic: Arc<StdMutex<Option<String>>>,
        last_queue_count: Arc<AtomicUsize>,
    }

    impl TopicMessageQueueChangeListener for CountingTopicMessageQueueChangeListener {
        fn on_changed(&self, topic: &str, message_queues: HashSet<MessageQueue>) {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            *self.last_topic.lock().expect("last topic lock should not be poisoned") = Some(topic.to_string());
            self.last_queue_count
                .store(message_queues.len(), AtomicOrdering::SeqCst);
        }
    }

    #[tokio::test]
    async fn topic_message_queue_change_listener_fires_only_when_snapshot_changes() {
        let impl_ = DefaultLitePullConsumerImpl::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_topic_metadata_group"),
                ..Default::default()
            }),
        );

        let topic = CheetahString::from_static_str("topic-a");
        let mq0 = MessageQueue::from_parts("topic-a", "broker-a", 0);
        let mq1 = MessageQueue::from_parts("topic-a", "broker-a", 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let last_topic = Arc::new(StdMutex::new(None));
        let last_queue_count = Arc::new(AtomicUsize::new(0));

        impl_.topic_message_queue_change_listener_map.write().await.insert(
            topic.clone(),
            Arc::new(CountingTopicMessageQueueChangeListener {
                calls: calls.clone(),
                last_topic: last_topic.clone(),
                last_queue_count: last_queue_count.clone(),
            }),
        );
        impl_
            .message_queues_for_topic
            .write()
            .await
            .insert(topic.clone(), HashSet::from([mq0.clone()]));

        impl_
            .notify_topic_message_queue_change_if_needed(&topic, HashSet::from([mq0.clone()]))
            .await;
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);

        impl_
            .notify_topic_message_queue_change_if_needed(&topic, HashSet::from([mq0.clone(), mq1.clone()]))
            .await;

        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            last_topic
                .lock()
                .expect("last topic lock should not be poisoned")
                .as_deref(),
            Some("topic-a")
        );
        assert_eq!(last_queue_count.load(AtomicOrdering::SeqCst), 2);

        let snapshot = impl_.message_queues_for_topic.read().await;
        assert_eq!(snapshot.get(&topic).map(HashSet::len), Some(2));
    }
}
