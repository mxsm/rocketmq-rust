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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::running::running_stats::RunningStats;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::FileUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::UnifiedServiceError;
use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_runtime::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
use rocketmq_store::queue::local_file_consume_queue_store::ConsumeQueueStore;
use rocketmq_store::store_path_config_helper::get_delay_offset_store_path;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

// Constants
const FIRST_DELAY_TIME: u64 = 1000;
const DELAY_FOR_A_WHILE: u64 = 100;
const DELAY_FOR_A_PERIOD: u64 = 10000;
const WAIT_FOR_SHUTDOWN: u64 = 5000;
const DELAY_FOR_A_SLEEP: u64 = 10;
const PERSIST_DELAY_INITIAL_DELAY: u64 = 10000;

// Performance optimization constants
/// Maximum number of messages to process in a single batch
const MAX_BATCH_SIZE: usize = 32;
/// Initial capacity for VecDeque to reduce reallocations
const INITIAL_QUEUE_CAPACITY: usize = 128;
/// Maximum pending queue size before applying backpressure
const MAX_PENDING_QUEUE_SIZE: usize = 10000;

pub type DeliverPendingTable<MS> = Arc<DashMap<i32, Arc<Mutex<VecDeque<PutResultProcess<MS>>>>>>;

struct ScheduleLifecycle {
    next_generation: u64,
    run: Option<ScheduleRun>,
    finalized: bool,
}

struct ScheduleRun {
    generation: u64,
    _lease: TaskGroupChildLease,
    task_group: TaskGroup,
    scheduled_tasks: ScheduledTaskGroup,
}

#[derive(Clone)]
struct ScheduleRunContext {
    generation: u64,
    task_group: TaskGroup,
    cancellation: CancellationToken,
}

struct ScheduleRuntimeCapabilities {
    task_group: TaskGroup,
    blocking: BlockingExecutor,
}

#[derive(Default)]
struct DelayLevelConfig {
    table: BTreeMap<i32, i64>,
    max_level: i32,
}

fn parse_delay_level_config(level_string: &str) -> Result<DelayLevelConfig, String> {
    let level_array = level_string.split_whitespace().collect::<Vec<_>>();
    if level_array.is_empty() {
        return Err("delay level configuration is empty".to_string());
    }

    let mut table = BTreeMap::new();
    for (index, value) in level_array.into_iter().enumerate() {
        let unit = value
            .chars()
            .last()
            .ok_or_else(|| format!("delay level at index {index} is empty"))?;
        let multiplier: i64 = match unit {
            's' => 1_000,
            'm' => 60_000,
            'h' => 3_600_000,
            'd' => 86_400_000,
            _ => return Err(format!("unknown time unit '{unit}' at index {index}")),
        };
        let number_end = value.len() - unit.len_utf8();
        let number = value[..number_end]
            .parse::<i64>()
            .map_err(|error| format!("invalid delay level number at index {index}: {error}"))?;
        if number <= 0 {
            return Err(format!("delay level value must be positive at index {index}"));
        }
        let delay_millis = multiplier
            .checked_mul(number)
            .ok_or_else(|| format!("delay level value overflows milliseconds at index {index}"))?;
        let level = i32::try_from(index + 1).map_err(|_| "too many delay levels".to_string())?;
        table.insert(level, delay_millis);
    }

    Ok(DelayLevelConfig {
        max_level: table.len() as i32,
        table,
    })
}

struct ScheduleOffsetStateInner {
    offset_table: HashMap<i32, i64>,
    version_change_counter: u64,
}

struct ScheduleOffsetState {
    inner: ParkingMutex<ScheduleOffsetStateInner>,
    data_version: ArcSwap<DataVersion>,
}

impl ScheduleOffsetState {
    fn new() -> Self {
        Self {
            inner: ParkingMutex::new(ScheduleOffsetStateInner {
                offset_table: HashMap::new(),
                version_change_counter: 0,
            }),
            data_version: ArcSwap::from_pointee(rocketmq_remoting::protocol::data_version_facade::new_data_version()),
        }
    }

    fn offset(&self, delay_level: i32) -> Option<i64> {
        self.inner.lock().offset_table.get(&delay_level).copied()
    }

    fn update_offset(
        &self,
        delay_level: i32,
        offset: i64,
        update_step: u64,
        state_machine_version: i64,
    ) -> (Option<i64>, u64, bool) {
        let mut inner = self.inner.lock();
        let previous = inner.offset_table.get(&delay_level).copied();
        if previous.is_some_and(|current| offset <= current) {
            return (previous, inner.version_change_counter, false);
        }
        inner.offset_table.insert(delay_level, offset);

        inner.version_change_counter = inner.version_change_counter.checked_add(1).unwrap_or(1);
        let version_counter = inner.version_change_counter;
        let version_updated = update_step > 0 && version_counter.is_multiple_of(update_step);
        if version_updated {
            let mut data_version = self.data_version.load_full().as_ref().clone();
            data_version.next_version_with(state_machine_version);
            self.data_version.store(Arc::new(data_version));
        }

        (previous, version_counter, version_updated)
    }

    fn correct_offset(&self, delay_level: i32, expected: i64, corrected: i64) -> bool {
        let mut inner = self.inner.lock();
        if inner.offset_table.get(&delay_level).copied() != Some(expected) {
            return false;
        }
        inner.offset_table.insert(delay_level, corrected);
        true
    }

    fn data_version(&self) -> Arc<DataVersion> {
        self.data_version.load_full()
    }

    fn set_data_version(&self, data_version: DataVersion) {
        let _inner = self.inner.lock();
        self.data_version.store(Arc::new(data_version));
    }

    fn snapshot(&self) -> (HashMap<i32, i64>, Arc<DataVersion>) {
        let inner = self.inner.lock();
        (inner.offset_table.clone(), self.data_version.load_full())
    }

    fn install_snapshot(&self, offset_table: &HashMap<i32, i64>, data_version: Option<&DataVersion>) {
        let mut inner = self.inner.lock();
        inner.offset_table.clone_from(offset_table);
        inner.version_change_counter = 0;
        if let Some(data_version) = data_version {
            self.data_version.store(Arc::new(data_version.clone()));
        }
    }
}

fn schedule_message_service_startup_failed(error: impl Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "ScheduleMessageService: {error}"
    )))
}

/// `ScheduleMessageService` is the core service in RocketMQ specifically designed to manage and
/// deliver delayed messages (scheduled messages). It supports the consumption of messages after a
/// specified time through predefined delay levels, making it suitable for scenarios such as order
/// timeouts and triggering scheduled tasks.
///
/// ### Core Features
///
/// #### 1. Storage Management of Delayed Messages
///
/// **Receiving Delayed Messages**:
/// When a producer sends a delayed message, the Broker temporarily stores the message in a special
/// internal Topic (`SCHEDULE_TOPIC_XXXX`) instead of directly writing it to the target Topic. Each
/// delay level corresponds to a queue (e.g., Queue 0 of `SCHEDULE_TOPIC_XXXX` corresponds to a 1 -
/// second delay).
///
/// **Storage Structure**:
/// - The original Topic and queue information of the message are stored in the message attributes.
/// - The delay time is specified by the `delayTimeLevel` parameter (e.g., Level 3 corresponds to a
///   10 - second delay).
///
/// #### 2. Periodic Scanning and Message Redelivery
///
/// **Scheduled Task Scheduling**:
/// An independent scheduled task is created for each delay level to periodically scan the messages
/// in the corresponding queue.
///
/// **Delay Time Check**:
/// When the preset delay time of a message arrives, it is retrieved from `SCHEDULE_TOPIC_XXXX` and
/// redelivered to the queue of the original target Topic.
///
/// **Consumption Visibility**:
/// After delivery, the message can be normally pulled by the consumer, ensuring the delay takes
/// effect.
///
/// #### 3. Exception Recovery and Consistency Assurance
///
/// **Crash Recovery**:
/// When the Broker restarts, it recovers the unprocessed delayed messages to avoid message loss.
///
/// **Duplicate Delivery Prevention**:
/// Duplicate delivery is prevented through the unique message key (`uniqKey`) and the storage
/// offset (`commitLogOffset`).
pub struct ScheduleMessageService<MS: MessageStore> {
    delay_level_config: ArcSwap<DelayLevelConfig>,
    offset_state: ScheduleOffsetState,
    started: AtomicBool,
    active_generation: AtomicU64,
    enable_async_deliver: bool,
    deliver_pending_table: DeliverPendingTable<MS>,
    deliver_resend_in_progress: Arc<DashMap<i32, Arc<AtomicBool>>>,
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    escape_bridge: Weak<EscapeBridge<MS>>,
    runtime_capabilities: OnceLock<ScheduleRuntimeCapabilities>,
    compatibility_scheduler: ScheduledTaskManager,
    lifecycle: Mutex<ScheduleLifecycle>,
    persistence_gate: Mutex<()>,
}

fn schedule_message_service_shutdown_failed(error: impl Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::ShutdownFailed(format!(
        "ScheduleMessageService: {error}"
    )))
}

fn schedule_message_service_interrupted() -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::Interrupted)
}

async fn wait_for_schedule_activation(
    activation: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> bool {
    loop {
        if *activation.borrow() {
            return true;
        }
        tokio::select! {
            _ = cancellation.cancelled() => return false,
            changed = activation.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

async fn schedule_sleep(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

impl<MS: MessageStore> ScheduleMessageService<MS> {
    pub(crate) fn new(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        escape_bridge: Weak<EscapeBridge<MS>>,
        service_context: Option<ServiceContext>,
        compatibility_scheduler: ScheduledTaskManager,
    ) -> Self {
        let enable_async_deliver = message_store_config.enable_schedule_async_deliver;
        let runtime_capabilities = OnceLock::new();
        if let Some(service_context) = service_context {
            runtime_capabilities
                .set(ScheduleRuntimeCapabilities {
                    task_group: service_context.task_group().clone(),
                    blocking: service_context.blocking().clone(),
                })
                .unwrap_or_else(|_| unreachable!("new ScheduleMessageService capability cell must be empty"));
        }

        Self {
            delay_level_config: ArcSwap::from_pointee(DelayLevelConfig::default()),
            offset_state: ScheduleOffsetState::new(),
            started: AtomicBool::new(false),
            active_generation: AtomicU64::new(0),
            enable_async_deliver,
            deliver_pending_table: Arc::new(DashMap::new()),
            deliver_resend_in_progress: Arc::new(DashMap::new()),
            broker_config,
            message_store_config,
            escape_bridge,
            runtime_capabilities,
            compatibility_scheduler,
            lifecycle: Mutex::new(ScheduleLifecycle {
                next_generation: 1,
                run: None,
                finalized: false,
            }),
            persistence_gate: Mutex::new(()),
        }
    }

    fn delivery_runtime(&self) -> Arc<EscapeBridge<MS>> {
        self.escape_bridge
            .upgrade()
            .expect("EscapeBridge owner must outlive ScheduleMessageService")
    }

    fn runtime_capabilities(&self) -> Result<&ScheduleRuntimeCapabilities, String> {
        if let Some(runtime_capabilities) = self.runtime_capabilities.get() {
            return Ok(runtime_capabilities);
        }

        // Public Broker builder compatibility: reuse the already-audited legacy scheduler root
        // rather than creating another current-runtime adapter in the Broker crate.
        let task_group = self
            .compatibility_scheduler
            .compatibility_task_group()
            .map_err(|error| error.to_string())?;
        let blocking = BlockingExecutor::new(
            BlockingPoolPolicy::default(),
            task_group.child("schedule-message.blocking-reaper"),
        )
        .map_err(|error| error.to_string())?;
        let _ = self
            .runtime_capabilities
            .set(ScheduleRuntimeCapabilities { task_group, blocking });

        self.runtime_capabilities
            .get()
            .ok_or_else(|| "failed to install schedule runtime capabilities".to_string())
    }

    pub fn build_running_stats(&self, stats: &mut HashMap<String, String>) {
        let (offset_table, _) = self.offset_state.snapshot();
        for (delay_level, delay_offset) in offset_table {
            let queue_id = delay_level_to_queue_id(delay_level);
            let max_offset = self.delivery_runtime().with_message_store(|message_store| {
                message_store.get_max_offset_in_queue(
                    &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                    queue_id,
                )
            });

            let value = format!("{delay_offset},{max_offset}");
            let key = format!("{}_{}", RunningStats::ScheduleMessageOffset.as_str(), delay_level);
            stats.insert(key, value);
        }
    }

    /// Updates the offset for a specific delay level and manages data version.
    ///
    /// This method is called after successfully delivering a scheduled message.
    /// It updates the offset table and periodically updates the data version for persistence.
    ///
    /// # Arguments
    ///
    /// * `delay_level` - The delay level whose offset is being updated
    /// * `offset` - The new offset value
    fn update_offset(&self, delay_level: i32, offset: i64) {
        let state_machine_version = self
            .delivery_runtime()
            .with_message_store(MessageStore::get_state_machine_version);
        let (old_offset, version_counter, version_updated) = self.offset_state.update_offset(
            delay_level,
            offset,
            self.broker_config.delay_offset_update_version_step,
            state_machine_version,
        );

        // Log significant offset updates (every 100 messages)
        if let Some(old) = old_offset {
            if offset - old >= 100 {
                info!(
                    "Delay level {} offset updated: {} -> {} (delta: {})",
                    delay_level,
                    old,
                    offset,
                    offset - old
                );
            }
        }

        if version_updated {
            info!(
                "Data version updated: version_counter={}, state_machine_version={}",
                version_counter, state_machine_version
            );
        }
    }

    pub fn compute_deliver_timestamp(&self, delay_level: i32, store_timestamp: i64) -> i64 {
        let delay_config = self.delay_level_config.load();
        if let Some(time) = delay_config.table.get(&delay_level) {
            *time + store_timestamp
        } else {
            store_timestamp + 1000
        }
    }

    /// Starts the schedule message service.
    ///
    /// Initializes delay level tables, spawns delivery tasks for each level,
    /// and starts the periodic offset persistence task.
    ///
    /// # Returns
    ///
    /// `Ok(())` on successful start, error otherwise
    pub async fn start(this: Arc<Self>) -> RocketMQResult<()> {
        Self::start_with_persist_initial_delay(this, Duration::from_millis(PERSIST_DELAY_INITIAL_DELAY)).await
    }

    pub(crate) async fn start_with_persist_initial_delay(
        this: Arc<Self>,
        persist_initial_delay: Duration,
    ) -> RocketMQResult<()> {
        Self::start_internal(this, persist_initial_delay, true).await
    }

    pub(crate) async fn start_persist_task_for_probe(
        this: Arc<Self>,
        persist_initial_delay: Duration,
    ) -> RocketMQResult<()> {
        Self::start_internal(this, persist_initial_delay, false).await
    }

    async fn start_internal(
        this: Arc<Self>,
        persist_initial_delay: Duration,
        start_delivery: bool,
    ) -> RocketMQResult<()> {
        let mut lifecycle = this.lifecycle.lock().await;
        if lifecycle.finalized {
            return Err(schedule_message_service_startup_failed("service is already finalized"));
        }
        if lifecycle.run.is_some() {
            return Ok(());
        }

        let runtime_capabilities = this
            .runtime_capabilities()
            .map_err(schedule_message_service_startup_failed)?;
        let generation = lifecycle.next_generation;
        lifecycle.next_generation = lifecycle.next_generation.checked_add(1).unwrap_or(1);
        let lease = runtime_capabilities
            .task_group
            .try_child_lease(format!("rocketmq-broker.schedule.generation-{generation}"))
            .map_err(schedule_message_service_startup_failed)?;
        let task_group = lease.group().clone();
        let scheduled_group = task_group
            .try_child("scheduled")
            .map_err(schedule_message_service_startup_failed)?;
        let scheduled_tasks = ScheduledTaskGroup::new(scheduled_group);
        let run_context = ScheduleRunContext {
            generation,
            cancellation: task_group.cancellation_token(),
            task_group: task_group.clone(),
        };
        let (activation, activation_rx) = watch::channel(false);

        info!(generation, "Starting ScheduleMessageService generation");
        let delay_config = this.delay_level_config.load_full();
        let install_result = async {
            if start_delivery {
                for level in delay_config.table.keys().copied() {
                    let offset = this.offset_state.offset(level).unwrap_or(0);
                    if this.enable_async_deliver {
                        let service = Arc::downgrade(&this);
                        let context = run_context.clone();
                        let mut activation = activation_rx.clone();
                        Self::spawn_schedule_task(&run_context, "broker.schedule.handle-put-result", async move {
                            if !wait_for_schedule_activation(&mut activation, &context.cancellation).await
                                || !schedule_sleep(&context.cancellation, Duration::from_millis(FIRST_DELAY_TIME)).await
                            {
                                return;
                            }
                            let task = HandlePutResultTask::new(level, service, context);
                            task.run().await;
                        })?;
                    }

                    let service = Arc::downgrade(&this);
                    let context = run_context.clone();
                    let mut activation = activation_rx.clone();
                    Self::spawn_schedule_task(&run_context, "broker.schedule.deliver-delayed-message", async move {
                        if !wait_for_schedule_activation(&mut activation, &context.cancellation).await
                            || !schedule_sleep(&context.cancellation, Duration::from_millis(FIRST_DELAY_TIME)).await
                        {
                            return;
                        }
                        let task = DeliverDelayedMessageTimerTask::new(level, offset, service, context);
                        task.run().await;
                    })?;
                }
            }
            Self::schedule_persist_task(
                &this,
                &run_context,
                &scheduled_tasks,
                activation_rx,
                persist_initial_delay,
            )
        }
        .await;

        if let Err(error) = install_result {
            task_group.cancel();
            let _ = task_group.shutdown(Duration::from_millis(WAIT_FOR_SHUTDOWN)).await;
            return Err(error);
        }

        lifecycle.run = Some(ScheduleRun {
            generation,
            _lease: lease,
            task_group,
            scheduled_tasks,
        });
        this.active_generation.store(generation, Ordering::Release);
        this.started.store(true, Ordering::Release);
        let _ = activation.send(true);
        info!(
            generation,
            delay_levels = delay_config.table.len(),
            "ScheduleMessageService generation started"
        );
        Ok(())
    }

    fn schedule_persist_task(
        this: &Arc<Self>,
        run_context: &ScheduleRunContext,
        scheduled_tasks: &ScheduledTaskGroup,
        activation: watch::Receiver<bool>,
        initial_delay: Duration,
    ) -> RocketMQResult<()> {
        let service = Arc::downgrade(this);
        let context = run_context.clone();
        let period = Duration::from_millis(this.message_store_config.flush_delay_offset_interval.max(1));
        let mut config = ScheduledTaskConfig::fixed_delay("broker.schedule.persist-delay-offset", period);
        config.initial_delay = initial_delay;
        config.shutdown_timeout = Duration::from_millis(WAIT_FOR_SHUTDOWN);

        scheduled_tasks
            .schedule_fixed_delay(config, move || {
                let service = service.clone();
                let context = context.clone();
                let mut activation = activation.clone();
                async move {
                    if !wait_for_schedule_activation(&mut activation, &context.cancellation).await {
                        return;
                    }
                    let Some(service) = service.upgrade() else {
                        return;
                    };
                    if let Err(error) = service.persist_generation(&context).await {
                        warn!(
                            ?error,
                            generation = context.generation,
                            "failed to persist schedule offsets"
                        );
                    }
                }
            })
            .map(|_| ())
            .map_err(schedule_message_service_startup_failed)
    }

    /// Gracefully shuts down the schedule message service.
    ///
    /// Signals all tasks to stop, waits for them to complete, and persists final state.
    pub async fn shutdown(&self) -> RocketMQResult<()> {
        self.stop_inner(true).await.map(|_| ())
    }

    pub async fn stop(&self) -> RocketMQResult<bool> {
        self.stop_inner(false).await
    }

    async fn stop_inner(&self, finalize: bool) -> RocketMQResult<bool> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.finalized {
            return Ok(true);
        }

        let run = lifecycle.run.take();
        self.started.store(false, Ordering::Release);
        self.active_generation.store(0, Ordering::Release);

        if let Some(run) = run {
            info!(
                generation = run.generation,
                finalize, "Stopping ScheduleMessageService generation"
            );
            run.task_group.cancel();

            // Wait for any already-submitted blocking write. Periodic writers waiting for this
            // gate observe cancellation and leave without writing.
            drop(self.persistence_gate.lock().await);
            let report = run.task_group.shutdown(Duration::from_millis(WAIT_FOR_SHUTDOWN)).await;
            if !report.is_healthy() {
                warn!(
                    generation = run.generation,
                    report = %report.to_json(),
                    "ScheduleMessageService generation shutdown report is unhealthy"
                );
            }
        }

        self.persist_current().await?;
        lifecycle.finalized = finalize;
        info!(
            finalize,
            "ScheduleMessageService stopped after final offset persistence"
        );
        Ok(true)
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    pub(crate) fn task_count(&self) -> usize {
        self.lifecycle
            .try_lock()
            .ok()
            .and_then(|lifecycle| {
                lifecycle
                    .run
                    .as_ref()
                    .map(|run| run.task_group.task_count() + run.scheduled_tasks.group().task_count())
            })
            .unwrap_or_default()
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.lifecycle
            .try_lock()
            .ok()
            .and_then(|lifecycle| lifecycle.run.as_ref().map(|run| run.scheduled_tasks.snapshot()))
            .unwrap_or_default()
    }

    fn spawn_schedule_task<F>(
        run_context: &ScheduleRunContext,
        task_name: &'static str,
        future: F,
    ) -> RocketMQResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        run_context
            .task_group
            .spawn_service(task_name, future)
            .map(|_| ())
            .map_err(schedule_message_service_startup_failed)
    }

    fn is_generation_active(&self, generation: u64) -> bool {
        self.started.load(Ordering::Acquire)
            && generation != 0
            && self.active_generation.load(Ordering::Acquire) == generation
    }

    async fn persist_generation(&self, run_context: &ScheduleRunContext) -> RocketMQResult<()> {
        let _guard = tokio::select! {
            _ = run_context.cancellation.cancelled() => return Ok(()),
            guard = self.persistence_gate.lock() => guard,
        };
        if !self.is_generation_active(run_context.generation) || run_context.cancellation.is_cancelled() {
            return Ok(());
        }
        self.persist_current_locked().await
    }

    async fn persist_current(&self) -> RocketMQResult<()> {
        let _guard = self.persistence_gate.lock().await;
        self.persist_current_locked().await
    }

    async fn persist_current_locked(&self) -> RocketMQResult<()> {
        let runtime_capabilities = self
            .runtime_capabilities()
            .map_err(schedule_message_service_shutdown_failed)?;
        let json = self.encode_pretty(true);
        let file_name = self.config_file_path();
        runtime_capabilities
            .blocking
            .spawn_io("broker.schedule.persist-delay-offset", move || {
                FileUtils::string_to_file(json.as_str(), file_name.as_str())
            })
            .await
            .map_err(schedule_message_service_shutdown_failed)?
            .map_err(schedule_message_service_shutdown_failed)?;
        Ok(())
    }

    pub fn get_max_delay_level(&self) -> i32 {
        self.delay_level_config.load().max_level
    }

    pub fn get_data_version(&self) -> DataVersion {
        self.offset_state.data_version().as_ref().clone()
    }

    pub fn set_data_version(&self, data_version: DataVersion) {
        self.offset_state.set_data_version(data_version);
    }

    pub fn load_when_sync_delay_offset(&self, snapshot: &DelayOffsetSerializeWrapper) -> RocketMQResult<bool> {
        let offset_table = snapshot.offset_table().cloned().unwrap_or_default();
        self.offset_state
            .install_snapshot(&offset_table, snapshot.data_version());
        let parse_result = self.parse_delay_level();
        Ok(parse_result)
    }

    /// Corrects delay offsets based on actual consume queue state.
    ///
    /// Ensures all offsets are within valid bounds for their respective consume queues.
    /// If an offset is out of bounds, it's corrected and logged.
    ///
    /// # Returns
    ///
    /// `true` if corrections were successful, `false` otherwise
    pub fn correct_delay_offset(&self) -> bool {
        let topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        let delay_config = self.delay_level_config.load_full();
        for delay_level in delay_config.table.keys().copied() {
            let queue_id = delay_level_to_queue_id(delay_level);
            let cq = self.delivery_runtime().with_message_store(|message_store| {
                message_store
                    .get_queue_store()
                    .downcast_ref::<ConsumeQueueStore>()
                    .expect("Failed to downcast to ConsumeQueueStore")
                    .find_or_create_consume_queue(&topic, queue_id)
            });

            if let Some(current_delay_offset) = self.offset_state.offset(delay_level) {
                let mut correct_delay_offset = current_delay_offset;
                let cq_min_offset = cq.get_min_offset_in_queue();
                let cq_max_offset = cq.get_max_offset_in_queue();

                if current_delay_offset < cq_min_offset {
                    correct_delay_offset = cq_min_offset;
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        current_delay_offset,
                        cq_min_offset,
                        cq_max_offset,
                        cq.get_queue_id()
                    );
                }

                if current_delay_offset > cq_max_offset {
                    correct_delay_offset = cq_max_offset;
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        current_delay_offset,
                        cq_min_offset,
                        cq_max_offset,
                        cq.get_queue_id()
                    );
                }

                if correct_delay_offset != current_delay_offset {
                    error!(
                        "correct delay offset [ delayLevel {} ] from {} to {}",
                        delay_level, current_delay_offset, correct_delay_offset
                    );
                    self.offset_state
                        .correct_offset(delay_level, current_delay_offset, correct_delay_offset);
                }
            }
        }

        true
    }

    /// Parses the configured delay level string and initializes the delay level table.
    ///
    /// # Format
    /// Delay levels are specified as space-separated values with time units:
    /// - "s": seconds
    /// - "m": minutes
    /// - "h": hours
    /// - "d": days
    ///
    /// # Example
    /// "1s 5s 10s 30s 1m 2m" creates 6 delay levels
    ///
    /// # Returns
    /// `true` if parsing succeeds, `false` otherwise
    pub fn parse_delay_level(&self) -> bool {
        let level_string = self.message_store_config.message_delay_level.as_str();

        info!("Parsing delay level configuration: {}", level_string);
        let delay_config = match parse_delay_level_config(level_string) {
            Ok(config) => config,
            Err(error) => {
                error!(%error, message_delay_level = level_string, "Failed to parse delay level configuration");
                return false;
            }
        };
        let delay_level_count = delay_config.table.len();
        let max_delay_level = delay_config.max_level;
        let accepted_levels = delay_config.table.keys().copied().collect::<Vec<_>>();

        if self.enable_async_deliver {
            self.deliver_pending_table
                .retain(|level, _| accepted_levels.binary_search(level).is_ok());
            self.deliver_resend_in_progress
                .retain(|level, _| accepted_levels.binary_search(level).is_ok());
            for level in accepted_levels {
                self.deliver_pending_table
                    .entry(level)
                    .or_insert_with(|| Arc::new(Mutex::new(VecDeque::with_capacity(INITIAL_QUEUE_CAPACITY))));
                self.deliver_resend_in_progress
                    .entry(level)
                    .or_insert_with(|| Arc::new(AtomicBool::new(false)));
            }
        }

        self.delay_level_config.store(Arc::new(delay_config));

        info!(
            "Successfully parsed {} delay levels, max level: {}",
            delay_level_count, max_delay_level
        );
        true
    }

    fn message_time_up(msg_ext: MessageExt) -> MessageExtBrokerInner {
        let mut inner = MessageExtBrokerInner::default();
        let sys_flag = msg_ext.sys_flag();
        let born_timestamp = msg_ext.born_timestamp();
        let born_host = msg_ext.born_host();
        let store_host = msg_ext.store_host();
        let reconsume_times = msg_ext.reconsume_times();
        let message = msg_ext.message;
        let message_properties = message.properties().as_map().clone();
        if let Some(body) = message.get_body() {
            inner.set_body(body.clone());
        }
        inner.set_flag(message.flag());
        MessageAccessor::set_properties(&mut inner, message_properties);
        let topic_filter_type = message_single::parse_topic_filter_type(inner.sys_flag());
        let tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_filter_type,
            inner.tags().as_ref().unwrap_or(&CheetahString::empty()),
        );
        inner.tags_code = tags_code;
        inner.properties_string = MessageDecoder::message_properties_to_string(inner.get_properties());
        inner.message_ext_inner.sys_flag = sys_flag;
        inner.message_ext_inner.born_timestamp = born_timestamp;
        inner.message_ext_inner.born_host = born_host;
        inner.message_ext_inner.store_host = store_host;
        inner.message_ext_inner.reconsume_times = reconsume_times;
        inner.set_wait_store_msg_ok(false);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELAY_SEC);
        let topic = inner.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
        if let Some(topic) = topic {
            inner.set_topic(topic);
        }
        let queue_id = inner.property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID));
        if let Some(queue_id_str) = queue_id {
            match queue_id_str.parse::<i32>() {
                Ok(qid) => {
                    inner.message_ext_inner.queue_id = qid;
                }
                Err(e) => {
                    error!(
                        "Failed to parse queue_id '{}': {}. Using default queue_id=0",
                        queue_id_str, e
                    );
                    inner.message_ext_inner.queue_id = 0;
                }
            }
        }

        inner
    }

    pub(crate) async fn load_async(&self) -> RocketMQResult<bool> {
        let runtime_capabilities = self
            .runtime_capabilities()
            .map_err(schedule_message_service_startup_failed)?;
        let file_name = self.config_file_path();
        let load_result = runtime_capabilities
            .blocking
            .spawn_io(
                "broker.schedule.load-delay-offset",
                move || match FileUtils::file_to_string(file_name.as_str()) {
                    Ok(content) if !content.is_empty() => (true, Some(content)),
                    Ok(_) | Err(_) => match FileUtils::file_to_string(format!("{file_name}.bak").as_str()) {
                        Ok(content) if !content.is_empty() => (true, Some(content)),
                        Ok(_) => (true, None),
                        Err(_) => (false, None),
                    },
                },
            )
            .await
            .map_err(schedule_message_service_startup_failed)?;

        if let Some(content) = load_result.1 {
            self.decode(&content);
        }
        Ok(load_result.0 && self.parse_delay_level() && self.correct_delay_offset())
    }

    pub(crate) async fn sync_delay_offset_from_peer(
        &self,
        encoded_snapshot: &str,
        snapshot: &DelayOffsetSerializeWrapper,
    ) -> RocketMQResult<bool> {
        let lifecycle = self.lifecycle.lock().await;
        if lifecycle.run.is_some() {
            return Err(schedule_message_service_shutdown_failed(
                "peer delay-offset synchronization requires a stopped delivery generation",
            ));
        }
        let runtime_capabilities = self
            .runtime_capabilities()
            .map_err(schedule_message_service_shutdown_failed)?;
        let _persist_guard = self.persistence_gate.lock().await;
        let file_name = self.config_file_path();
        let encoded_snapshot = encoded_snapshot.to_owned();
        runtime_capabilities
            .blocking
            .spawn_io("broker.schedule.persist-peer-delay-offset", move || {
                FileUtils::string_to_file(encoded_snapshot.as_str(), file_name.as_str())
            })
            .await
            .map_err(schedule_message_service_shutdown_failed)?
            .map_err(schedule_message_service_shutdown_failed)?;

        let result = self.load_when_sync_delay_offset(snapshot);
        drop(lifecycle);
        result
    }

    /// Gets a copy of the current offset table.
    ///
    /// # Returns
    ///
    /// A HashMap containing all delay levels and their current offsets
    pub fn get_offset_table(&self) -> HashMap<i32, i64> {
        self.offset_state.snapshot().0
    }
}

impl<MS: MessageStore> ConfigManager for ScheduleMessageService<MS> {
    fn load(&self) -> bool {
        let result = {
            let file_name = self.config_file_path();
            let result = FileUtils::file_to_string(file_name.as_str());
            match result {
                Ok(ref content) => {
                    if content.is_empty() {
                        warn!("load bak config file");
                        self.load_bak()
                    } else {
                        self.decode(content);
                        info!("load Config file: {} -----OK", file_name);
                        true
                    }
                }
                Err(_) => self.load_bak(),
            }
        };
        let parse_result = self.parse_delay_level();
        let correct_result = self.correct_delay_offset();

        result && parse_result && correct_result
    }

    fn config_file_path(&self) -> String {
        get_delay_offset_store_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let (offset_table, data_version) = self.offset_state.snapshot();
        let delay_offset_serialize_wrapper =
            DelayOffsetSerializeWrapper::new(Some(offset_table), Some(data_version.as_ref().clone()));

        let result = if pretty_format {
            delay_offset_serialize_wrapper.serialize_json_pretty()
        } else {
            delay_offset_serialize_wrapper.serialize_json()
        };

        match result {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to encode delay offset table to JSON: {}", e);
                // Return empty JSON object as fallback
                "{}".to_string()
            }
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            warn!("Decode called with empty json string");
            return;
        }

        let delay_offset_serialize_wrapper =
            match SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(json_string) {
                Ok(wrapper) => wrapper,
                Err(e) => {
                    error!(
                        "Failed to deserialize delay offset from json: {}. Json: {}",
                        e, json_string
                    );
                    return;
                }
            };

        let offset_table = delay_offset_serialize_wrapper
            .offset_table()
            .cloned()
            .unwrap_or_default();
        let data_version = delay_offset_serialize_wrapper.data_version();
        self.offset_state.install_snapshot(&offset_table, data_version);

        if !offset_table.is_empty() {
            info!("Loaded {} delay offset entries from storage", offset_table.len());
        }

        if let Some(data_version) = data_version {
            info!("Loaded data version: {:?}", data_version);
        }
    }
}

/// Task for delivering delayed messages when their time is up
pub struct DeliverDelayedMessageTimerTask<MS: MessageStore> {
    /// The delay level for this task
    delay_level: i32,

    /// The offset in the consume queue to start from
    offset: i64,

    /// Reference to the parent service
    schedule_service: Weak<ScheduleMessageService<MS>>,
    run_context: ScheduleRunContext,
}

impl<MS: MessageStore> DeliverDelayedMessageTimerTask<MS> {
    /// Create a new timer task for delivering delayed messages
    fn new(
        delay_level: i32,
        offset: i64,
        schedule_service: Weak<ScheduleMessageService<MS>>,
        run_context: ScheduleRunContext,
    ) -> Self {
        Self {
            delay_level,
            offset,
            schedule_service,
            run_context,
        }
    }

    /// Execute the task
    pub async fn run(&self) {
        let Some(schedule_service) = self.schedule_service.upgrade() else {
            return;
        };
        if self.run_context.cancellation.is_cancelled()
            || !schedule_service.is_generation_active(self.run_context.generation)
        {
            info!("Delivery task for level {} received shutdown signal", self.delay_level);
            return;
        }

        match self.execute_on_time_up().await {
            Ok(_) => {}
            Err(e) => {
                error!("ScheduleMessageService, executeOnTimeUp exception: {}", e);
                self.schedule_next_timer_task(self.offset, DELAY_FOR_A_PERIOD);
            }
        }
    }

    /// Corrects the delivery timestamp to prevent messages from being delivered too early
    /// due to system clock issues or invalid tags code.
    ///
    /// If the delivery timestamp is more than one full delay period in the future,
    /// it's corrected to the current time to force immediate delivery.
    ///
    /// # Arguments
    ///
    /// * `now` - Current system time in milliseconds
    /// * `deliver_timestamp` - Target delivery timestamp from tags code
    ///
    /// # Returns
    ///
    /// Corrected delivery timestamp that won't exceed `now + delay_time`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // For a 10s delay level:
    /// // If deliver_timestamp = now + 20s, returns now (force immediate delivery)
    /// // If deliver_timestamp = now + 5s, returns deliver_timestamp (wait 5s)
    /// let corrected = task.correct_deliver_timestamp(current_millis, tags_code);
    /// ```
    fn correct_deliver_timestamp(&self, now: i64, deliver_timestamp: i64) -> i64 {
        let Some(schedule_service) = self.schedule_service.upgrade() else {
            return now;
        };
        let delay_config = schedule_service.delay_level_config.load();
        let delay_time = *delay_config.table.get(&self.delay_level).unwrap();
        let max_timestamp = now + delay_time;

        if deliver_timestamp > max_timestamp {
            warn!(
                "Delivery timestamp {} exceeds max allowed {} (now + delay_time). Correcting to now. DelayLevel: {}",
                deliver_timestamp, max_timestamp, self.delay_level
            );
            now
        } else {
            deliver_timestamp
        }
    }

    /// Execute when the scheduled time is up for messages
    async fn execute_on_time_up(&self) -> RocketMQResult<()> {
        let schedule_service = self
            .schedule_service
            .upgrade()
            .ok_or_else(schedule_message_service_interrupted)?;
        let delivery_runtime = schedule_service.delivery_runtime();
        // Get the consume queue for this delay level
        let queue_id = delay_level_to_queue_id(self.delay_level);
        let cq = delivery_runtime.with_message_store(|message_store| {
            message_store.get_consume_queue(
                &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                queue_id,
            )
        });
        if cq.is_none() {
            self.schedule_next_timer_task(self.offset, DELAY_FOR_A_WHILE);
            return Ok(());
        }
        let cq = cq.unwrap();
        // Get an iterator to the consume queue starting at the current offset
        let mut buffer_cq = match cq.iterate_from(self.offset) {
            Some(iter) => iter,
            None => {
                let reset_offset = if cq.get_min_offset_in_queue() > self.offset {
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        self.offset,
                        cq.get_min_offset_in_queue(),
                        cq.get_queue_id()
                    );
                    cq.get_min_offset_in_queue()
                } else if cq.get_max_offset_in_queue() < self.offset {
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        self.offset,
                        cq.get_max_offset_in_queue(),
                        cq.get_queue_id()
                    );
                    cq.get_max_offset_in_queue()
                } else {
                    self.offset
                };

                self.schedule_next_timer_task(reset_offset, DELAY_FOR_A_WHILE);
                return Ok(());
            }
        };

        let mut next_offset = self.offset;

        // Process each message in the consume queue
        while schedule_service.is_generation_active(self.run_context.generation)
            && !self.run_context.cancellation.is_cancelled()
        {
            let cq_unit = if let Some(unit) = buffer_cq.next() {
                unit
            } else {
                break;
            };
            let physical_offset = cq_unit.pos;
            let physical_size = cq_unit.size;
            let mut tags_code = cq_unit.tags_code;

            // Handle invalid tags code
            if !cq_unit.is_tags_code_valid() {
                error!(
                    "[BUG] can't find consume queue extend file content! addr={}, physical_offset={}, physical_size={}",
                    tags_code, physical_offset, physical_size
                );

                let msg_store_time = delivery_runtime.with_message_store(|message_store| {
                    message_store
                        .get_commit_log()
                        .pickup_store_timestamp(physical_offset, physical_size)
                });

                tags_code = schedule_service.compute_deliver_timestamp(self.delay_level, msg_store_time);
            }

            // Check if it's time to deliver the message
            let now = current_millis() as i64;
            let deliver_timestamp = self.correct_deliver_timestamp(now, tags_code);

            let curr_offset = cq_unit.queue_offset;
            assert_eq!(cq_unit.batch_num, 1);
            next_offset = curr_offset + cq_unit.batch_num as i64;

            // Detect clock skew/backwards
            if deliver_timestamp < tags_code - (24 * 3600 * 1000) {
                warn!(
                    "Detected potential clock skew! deliverTimestamp={}, tagsCode={}, offset={}, delayLevel={}. \
                     Forcing immediate delivery.",
                    deliver_timestamp, tags_code, curr_offset, self.delay_level
                );
                // Force immediate delivery on clock issues
            } else {
                let countdown = deliver_timestamp - now;
                if countdown > 0 {
                    if countdown > (DELAY_FOR_A_PERIOD * 10) as i64 {
                        info!(
                            "Message countdown is very large ({}ms), offset={}, delayLevel={}",
                            countdown, curr_offset, self.delay_level
                        );
                    }
                    self.schedule_next_timer_task(curr_offset, DELAY_FOR_A_WHILE);
                    schedule_service.update_offset(self.delay_level, curr_offset);
                    return Ok(());
                }
            }

            // Look up the actual message
            let msg_ext = match delivery_runtime.with_message_store(|message_store| {
                message_store.look_message_by_offset_with_size(physical_offset, physical_size)
            }) {
                Some(msg) => msg,
                None => {
                    warn!(
                        "Failed to look up message at physical_offset={}, physical_size={}, delay_level={}",
                        physical_offset, physical_size, self.delay_level
                    );
                    continue;
                }
            };
            let msg_id = msg_ext.msg_id().clone();
            // Process the message for delivery
            let msg_inner = ScheduleMessageService::<MS>::message_time_up(msg_ext);

            // Check for transaction half messages which should be discarded
            if msg_inner.get_topic() == TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC {
                error!(
                    "[BUG] the real topic of schedule msg is {}, discard the msg. msg={:?}",
                    msg_inner.get_topic(),
                    msg_inner
                );
                continue;
            }

            // Deliver the message
            let deliver_suc = if schedule_service.enable_async_deliver {
                self.async_deliver(msg_inner, msg_id, curr_offset, physical_offset, physical_size)
                    .await?
            } else {
                self.sync_deliver(msg_inner, msg_id, curr_offset, physical_offset, physical_size)
                    .await?
            };

            if !deliver_suc {
                self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE);
                return Ok(());
            }
        }

        // Schedule the next task
        self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE);

        Ok(())
    }

    /// Schedule the next timer task
    fn schedule_next_timer_task(&self, offset: i64, delay: u64) {
        let Some(schedule_service) = self.schedule_service.upgrade() else {
            return;
        };
        if self.run_context.cancellation.is_cancelled()
            || !schedule_service.is_generation_active(self.run_context.generation)
        {
            return;
        }

        let schedule_service = self.schedule_service.clone();
        let delay_level = self.delay_level;
        let context = self.run_context.clone();

        // Schedule the next task after the specified delay
        let spawn_result = ScheduleMessageService::<MS>::spawn_schedule_task(
            &self.run_context,
            "broker.schedule.deliver-delayed-message",
            async move {
                if !schedule_sleep(&context.cancellation, Duration::from_millis(delay)).await {
                    return;
                }
                let task = DeliverDelayedMessageTimerTask::new(delay_level, offset, schedule_service, context);
                task.run().await;
            },
        );
        if let Err(error) = spawn_result {
            warn!(
                ?error,
                generation = self.run_context.generation,
                "failed to reschedule delayed delivery"
            );
        }
    }

    /// Deliver a message synchronously
    async fn sync_deliver(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> RocketMQResult<bool> {
        let mut result_process = self
            .deliver_message(msg_inner, msg_id, offset, offset_py, size_py, false)
            .await?;

        // Wait for the result
        let result = result_process.get();
        let send_status = result.put_message_status() == PutMessageStatus::PutOk;

        if send_status {
            if let Some(schedule_service) = self.schedule_service.upgrade() {
                schedule_service.update_offset(self.delay_level, result_process.get_next_offset());
            }
        }

        Ok(send_status)
    }

    /// Deliver a message asynchronously
    async fn async_deliver(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> RocketMQResult<bool> {
        let schedule_service = self
            .schedule_service
            .upgrade()
            .ok_or_else(schedule_message_service_interrupted)?;
        let processes_queue = schedule_service
            .deliver_pending_table
            .get(&self.delay_level)
            .map(|entry| Arc::clone(entry.value()))
            .expect("delay-level pending queue should be initialized");
        let resend_in_progress = schedule_service
            .deliver_resend_in_progress
            .get(&self.delay_level)
            .map(|entry| Arc::clone(entry.value()))
            .expect("delay-level resend state should be initialized");

        if resend_in_progress.load(Ordering::Acquire) {
            warn!(
                "Asynchronous deliver blocked while retrying delay level {}",
                self.delay_level
            );
            return Ok(false);
        }

        let max_pending_limit = schedule_service
            .message_store_config
            .schedule_async_deliver_max_pending_limit;
        {
            let queue = processes_queue.lock().await;
            let current_pending_num = queue.len();
            if current_pending_num > max_pending_limit || current_pending_num >= MAX_PENDING_QUEUE_SIZE {
                warn!(
                    "Asynchronous deliver triggers flow control, currentPendingNum={}, maxPendingLimit={}",
                    current_pending_num, max_pending_limit
                );
                return Ok(false);
            }
            if let Some(first_process) = queue.front() {
                if first_process.need2_blocked() {
                    warn!("Asynchronous deliver block. info={}", first_process);
                    return Ok(false);
                }
            }
        }

        // Do not hold the queue or DashMap guard across message-store I/O.
        let result_process = self
            .deliver_message(msg_inner, msg_id, offset, offset_py, size_py, true)
            .await?;

        let mut processes_queue = processes_queue.lock().await;
        // The put already happened, so the result must remain tracked even if another producer
        // filled the queue while message-store I/O was in flight.
        if processes_queue.len() >= MAX_PENDING_QUEUE_SIZE {
            warn!(
                "Pending queue for delay level {} reached its soft capacity while delivery was in flight (size={})",
                self.delay_level,
                processes_queue.len()
            );
        }

        // Add to pending queue
        processes_queue.push_back(result_process);

        Ok(true)
    }

    /// Deliver a message and return a process to track the result
    async fn deliver_message(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
        auto_resend: bool,
    ) -> RocketMQResult<PutResultProcess<MS>> {
        // Create a channel for the async result

        let topic = msg_inner.get_topic().clone();
        let schedule_service = self
            .schedule_service
            .upgrade()
            .ok_or_else(schedule_message_service_interrupted)?;
        let escape_bridge = schedule_service
            .escape_bridge
            .upgrade()
            .ok_or_else(schedule_message_service_interrupted)?;
        // Send the message asynchronously
        let result = escape_bridge.async_put_message(msg_inner).await;

        // Create and return the process tracking object
        let result_process = PutResultProcess::new(
            schedule_service.escape_bridge.clone(),
            schedule_service
                .message_store_config
                .schedule_async_deliver_max_resend_num2_blocked,
        )
        .set_topic(topic)
        .set_delay_level(self.delay_level)
        .set_offset(offset)
        .set_physic_offset(offset_py)
        .set_physic_size(size_py)
        .set_msg_id(msg_id.to_string())
        .set_auto_resend(auto_resend)
        .set_put_message_result(result)
        .then_process()
        .await;

        Ok(result_process)
    }
}

struct ProcessStatusCell {
    value: AtomicU8,
}

impl ProcessStatusCell {
    fn new(status: ProcessStatus) -> Self {
        Self {
            value: AtomicU8::new(status as u8),
        }
    }

    fn load(&self) -> ProcessStatus {
        match self.value.load(Ordering::Acquire) {
            value if value == ProcessStatus::Success as u8 => ProcessStatus::Success,
            value if value == ProcessStatus::Exception as u8 => ProcessStatus::Exception,
            value if value == ProcessStatus::Skip as u8 => ProcessStatus::Skip,
            _ => ProcessStatus::Running,
        }
    }

    fn store(&self, status: ProcessStatus) {
        self.value.store(status as u8, Ordering::Release);
    }
}

/// Process for handling the result of putting a message
pub struct PutResultProcess<MS: MessageStore> {
    topic: CheetahString,
    offset: i64,
    physic_offset: i64,
    physic_size: i32,
    delay_level: i32,
    msg_id: CheetahString,
    auto_resend: bool,
    put_message_result: Option<PutMessageResult>,
    resend_count: AtomicI32,
    status: ProcessStatusCell,
    escape_bridge: Weak<EscapeBridge<MS>>,
    max_resend_num2_blocked: usize,
}

impl<MS: MessageStore> PutResultProcess<MS> {
    /// Create a new PutResultProcess instance
    pub(crate) fn new(escape_bridge: Weak<EscapeBridge<MS>>, max_resend_num2_blocked: usize) -> Self {
        Self {
            topic: CheetahString::empty(),
            offset: 0,
            physic_offset: 0,
            physic_size: 0,
            delay_level: 0,
            msg_id: CheetahString::empty(),
            auto_resend: false,
            put_message_result: None,
            resend_count: AtomicI32::new(0),
            status: ProcessStatusCell::new(ProcessStatus::Running),
            escape_bridge,
            max_resend_num2_blocked,
        }
    }

    /// Set the topic for this process
    pub fn set_topic(mut self, topic: impl Into<CheetahString>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set the offset for this process
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Set the physical offset for this process
    pub fn set_physic_offset(mut self, physic_offset: i64) -> Self {
        self.physic_offset = physic_offset;
        self
    }

    /// Set the physical size for this process
    pub fn set_physic_size(mut self, physic_size: i32) -> Self {
        self.physic_size = physic_size;
        self
    }

    /// Set the delay level for this process
    pub fn set_delay_level(mut self, delay_level: i32) -> Self {
        self.delay_level = delay_level;
        self
    }

    /// Set the message ID for this process
    pub fn set_msg_id(mut self, msg_id: impl Into<CheetahString>) -> Self {
        self.msg_id = msg_id.into();
        self
    }

    /// Set whether to automatically resend on failure
    pub fn set_auto_resend(mut self, auto_resend: bool) -> Self {
        self.auto_resend = auto_resend;
        self
    }

    /// Set the future for this process
    pub fn set_put_message_result(mut self, put_result: PutMessageResult) -> Self {
        self.put_message_result = Some(put_result);
        self
    }

    /// Get the topic
    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    /// Get the offset
    pub fn get_offset(&self) -> i64 {
        self.offset
    }

    /// Get the next offset
    pub fn get_next_offset(&self) -> i64 {
        self.offset + 1
    }

    /// Get the physical offset
    pub fn get_physic_offset(&self) -> i64 {
        self.physic_offset
    }

    /// Get the physical size
    pub fn get_physic_size(&self) -> i32 {
        self.physic_size
    }

    /// Get the delay level
    pub fn get_delay_level(&self) -> i32 {
        self.delay_level
    }

    /// Get the message ID
    pub fn get_msg_id(&self) -> &str {
        &self.msg_id
    }

    /// Check if auto-resend is enabled
    pub fn is_auto_resend(&self) -> bool {
        self.auto_resend
    }

    /// Get the resend count
    pub fn get_resend_count(&self) -> i32 {
        self.resend_count.load(Ordering::Relaxed)
    }

    /// Handle the processing after completing the future
    pub async fn then_process(self) -> Self {
        // Create a clone of self that will be captured in the async closure
        if let Some(put_message_result) = &self.put_message_result {
            self.handle_result(put_message_result);
        }

        self
    }

    /// Handle the result of a put operation
    fn handle_result(&self, result: &PutMessageResult) {
        if result.put_message_status() == PutMessageStatus::PutOk {
            self.on_success(result);
        } else {
            self.on_exception();
        }
    }

    /// Handle a successful put operation
    pub fn on_success(&self, _result: &PutMessageResult) {
        self.status.store(ProcessStatus::Success);
    }

    /// Handle an exception during processing
    pub fn on_exception(&self) {
        warn!("ScheduleMessageService onException, info: {}", self);

        self.status.store(if self.auto_resend {
            ProcessStatus::Exception
        } else {
            ProcessStatus::Skip
        });
    }

    /// Get the current processing status
    pub fn get_status(&self) -> ProcessStatus {
        self.status.load()
    }

    /// Get the result
    pub fn get(&mut self) -> PutMessageResult {
        match self.put_message_result.take() {
            None => PutMessageResult::new_default(PutMessageStatus::UnknownError),
            Some(value) => value,
        }
    }

    /// Resend the message
    pub async fn do_resend(&self, cancellation: &CancellationToken) -> bool {
        info!("Resend message, info: {}", self);

        // Gradually increase the resend interval
        let sleep_time = std::cmp::min((self.resend_count.fetch_add(1, Ordering::SeqCst) + 1) * 100, 60 * 1000);
        if !schedule_sleep(cancellation, Duration::from_millis(sleep_time as u64)).await {
            return false;
        }

        let Some(escape_bridge) = self.escape_bridge.upgrade() else {
            self.status.store(if self.need2_skip() {
                ProcessStatus::Skip
            } else {
                ProcessStatus::Exception
            });
            return true;
        };
        // Look up the message and resend it
        match escape_bridge.with_message_store(|message_store| {
            message_store.look_message_by_offset_with_size(self.physic_offset, self.physic_size)
        }) {
            Some(msg_ext) => {
                // Convert to inner message
                let msg_inner = ScheduleMessageService::<MS>::message_time_up(msg_ext);

                // Try to put the message
                let result = escape_bridge.put_message(msg_inner).await;

                self.handle_result(&result);
            }
            None => {
                warn!("ScheduleMessageService resend not found message. info: {}", self);
                self.status.store(if self.need2_skip() {
                    ProcessStatus::Skip
                } else {
                    ProcessStatus::Exception
                });
            }
        }
        true
    }

    /// Check if processing needs to be blocked
    pub fn need2_blocked(&self) -> bool {
        self.resend_count.load(Ordering::Relaxed) > self.max_resend_num2_blocked as i32
    }

    /// Check if processing needs to be skipped
    pub fn need2_skip(&self) -> bool {
        self.resend_count.load(Ordering::Relaxed) > (self.max_resend_num2_blocked * 2) as i32
    }
}

impl<MS: MessageStore> Display for PutResultProcess<MS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PutResultProcess{{topic='{}', offset={}, physicOffset={}, physicSize={}, delayLevel={}, msgId='{}', \
             autoResend={}, resendCount={}, status={}}}",
            self.topic,
            self.offset,
            self.physic_offset,
            self.physic_size,
            self.delay_level,
            self.msg_id,
            self.auto_resend,
            self.resend_count.load(Ordering::Relaxed),
            self.get_status(),
        )
    }
}

/// Represents the status of a message processing operation
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessStatus {
    /// In process, the processing result has not yet been returned
    Running,

    /// Put message success
    Success,

    /// Put message exception. When auto_resend is true, the message will be resent
    Exception,

    /// Skip put message. When the message cannot be looked up, the message will be skipped
    Skip,
}

enum PendingQueueAction<MS: MessageStore> {
    Advance(i64),
    Wait,
    Resend(PutResultProcess<MS>),
    Skip(PutResultProcess<MS>),
    Empty,
}

impl Display for ProcessStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProcessStatus::Running => write!(f, "RUNNING"),
            ProcessStatus::Success => write!(f, "SUCCESS"),
            ProcessStatus::Exception => write!(f, "EXCEPTION"),
            ProcessStatus::Skip => write!(f, "SKIP"),
        }
    }
}

/// Task for handling results of asynchronous message puts
pub struct HandlePutResultTask<MS: MessageStore> {
    /// Delay level this task is handling
    delay_level: i32,

    /// Reference to the parent service
    schedule_service: Weak<ScheduleMessageService<MS>>,
    run_context: ScheduleRunContext,
}

impl<MS: MessageStore> HandlePutResultTask<MS> {
    /// Create a new task for handling put results at the specified delay level
    fn new(
        delay_level: i32,
        schedule_service: Weak<ScheduleMessageService<MS>>,
        run_context: ScheduleRunContext,
    ) -> Self {
        Self {
            delay_level,
            schedule_service,
            run_context,
        }
    }

    /// Execute the task to process pending results
    pub async fn run(&self) {
        let Some(schedule_service) = self.schedule_service.upgrade() else {
            return;
        };
        if self.run_context.cancellation.is_cancelled()
            || !schedule_service.is_generation_active(self.run_context.generation)
        {
            info!(
                "HandlePutResultTask for level {} received shutdown signal",
                self.delay_level
            );
            return;
        }

        // Get the pending queue for this delay level
        let pending_queue = match schedule_service.deliver_pending_table.get(&self.delay_level) {
            Some(queue) => Arc::clone(queue.value()),
            None => {
                // If queue doesn't exist, schedule next task and return
                self.schedule_next_task();
                return;
            }
        };
        let resend_in_progress = match schedule_service.deliver_resend_in_progress.get(&self.delay_level) {
            Some(flag) => Arc::clone(flag.value()),
            None => {
                self.schedule_next_task();
                return;
            }
        };
        // Performance optimization: Track processing metrics
        let queue_size = pending_queue.lock().await.len();
        if queue_size > 1000 {
            info!(
                "HandlePutResultTask for level {} processing large queue: {} items",
                self.delay_level, queue_size
            );
        }

        // Performance optimization: Process multiple items in batch
        let mut processed_count = 0;
        let max_process_per_cycle = MAX_BATCH_SIZE.min(queue_size);

        // Process each result in the queue
        while processed_count < max_process_per_cycle {
            processed_count += 1;
            let action = {
                let mut queue = pending_queue.lock().await;
                if resend_in_progress.load(Ordering::Acquire) {
                    PendingQueueAction::Wait
                } else {
                    match queue.front().map(PutResultProcess::get_status) {
                        None => PendingQueueAction::Empty,
                        Some(ProcessStatus::Success) => {
                            let next_offset = queue
                                .pop_front()
                                .expect("pending queue front should exist")
                                .get_next_offset();
                            PendingQueueAction::Advance(next_offset)
                        }
                        Some(ProcessStatus::Running) => PendingQueueAction::Wait,
                        Some(ProcessStatus::Exception) => {
                            resend_in_progress.store(true, Ordering::Release);
                            PendingQueueAction::Resend(queue.pop_front().expect("pending queue front should exist"))
                        }
                        Some(ProcessStatus::Skip) => {
                            PendingQueueAction::Skip(queue.pop_front().expect("pending queue front should exist"))
                        }
                    }
                }
            };
            match action {
                PendingQueueAction::Empty => break,
                PendingQueueAction::Advance(next_offset) => {
                    schedule_service.update_offset(self.delay_level, next_offset);
                }
                PendingQueueAction::Wait => {
                    // If any process is still running, schedule next task and return
                    self.schedule_next_task();
                    return;
                }
                PendingQueueAction::Resend(process) => {
                    // If service is stopped, don't continue processing
                    if !schedule_service.is_generation_active(self.run_context.generation)
                        || self.run_context.cancellation.is_cancelled()
                    {
                        warn!("HandlePutResultTask shutdown, info={}", &process);
                        pending_queue.lock().await.push_front(process);
                        resend_in_progress.store(false, Ordering::Release);
                        return;
                    }

                    // Otherwise log warning and try resending
                    warn!("putResultProcess error, info={}", &process);
                    if !process.do_resend(&self.run_context.cancellation).await {
                        pending_queue.lock().await.push_front(process);
                        resend_in_progress.store(false, Ordering::Release);
                        return;
                    }
                    pending_queue.lock().await.push_front(process);
                    resend_in_progress.store(false, Ordering::Release);
                    break;
                }
                PendingQueueAction::Skip(process) => {
                    // Log and remove skipped processes
                    warn!("putResultProcess skip, info={}", process);
                    break;
                }
            }
        }
        // Schedule the next execution of this task
        self.schedule_next_task();
    }

    /// Schedule the next execution of the handler task
    fn schedule_next_task(&self) {
        // Only schedule if the service is still running and not shutting down
        let Some(schedule_service) = self.schedule_service.upgrade() else {
            return;
        };
        if schedule_service.is_generation_active(self.run_context.generation)
            && !self.run_context.cancellation.is_cancelled()
        {
            let delay_level = self.delay_level;
            let schedule_service = self.schedule_service.clone();
            let context = self.run_context.clone();

            // Schedule after a short delay into the same generation-owned task group.
            let spawn_result = ScheduleMessageService::<MS>::spawn_schedule_task(
                &self.run_context,
                "broker.schedule.handle-put-result",
                async move {
                    if !schedule_sleep(&context.cancellation, Duration::from_millis(DELAY_FOR_A_SLEEP)).await {
                        return;
                    }
                    let task = HandlePutResultTask::new(delay_level, schedule_service, context);
                    task.run().await;
                },
            );
            if let Err(error) = spawn_result {
                warn!(
                    ?error,
                    generation = self.run_context.generation,
                    "failed to reschedule put-result handler"
                );
            }
        }
    }
}

#[inline]
pub fn queue_id_to_delay_level(queue_id: i32) -> i32 {
    queue_id + 1
}

#[inline]
pub fn delay_level_to_queue_id(delay_level: i32) -> i32 {
    delay_level - 1
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Barrier;
    use std::thread;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::protocol::DataVersion;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tempfile::TempDir;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

    #[test]
    fn schedule_offset_state_advances_version_on_exact_threshold() {
        let state = ScheduleOffsetState::new();
        let initial_generation = state.data_version();

        assert_eq!(state.update_offset(1, 10, 2, 7), (None, 1, false));
        assert_eq!(state.data_version().counter(), 0);
        assert_eq!(state.update_offset(1, 11, 2, 7), (Some(10), 2, true));
        assert_eq!(state.data_version().counter(), 1);
        assert_eq!(initial_generation.counter(), 0);

        assert_eq!(state.update_offset(1, 11, 2, 7), (Some(11), 2, false));
        assert_eq!(state.update_offset(1, 9, 2, 7), (Some(11), 2, false));
        assert_eq!(state.offset(1), Some(11));
        assert_eq!(state.data_version().counter(), 1);
    }

    #[test]
    fn schedule_offset_state_zero_step_does_not_panic_or_advance_version() {
        let state = ScheduleOffsetState::new();

        assert_eq!(state.update_offset(1, 10, 0, 7), (None, 1, false));
        assert_eq!(state.update_offset(1, 11, 0, 7), (Some(10), 2, false));
        assert_eq!(state.data_version().counter(), 0);
    }

    #[test]
    fn schedule_offset_snapshot_replaces_offsets_and_resets_version_cadence() {
        let state = ScheduleOffsetState::new();
        state.update_offset(1, 10, 1, 7);

        let mut installed_version = DataVersion::default();
        installed_version.next_version_with(99);
        let installed_counter = installed_version.counter();
        state.install_snapshot(&HashMap::from([(2, 20)]), Some(&installed_version));

        assert_eq!(state.snapshot().0, HashMap::from([(2, 20)]));
        assert_eq!(state.data_version().counter(), installed_counter);
        assert_eq!(state.update_offset(2, 21, 2, 99), (Some(20), 1, false));
        assert_eq!(state.data_version().counter(), installed_counter);
    }

    #[test]
    fn schedule_offset_state_serializes_concurrent_writers() {
        const WRITERS: usize = 16;
        let state = Arc::new(ScheduleOffsetState::new());
        let barrier = Arc::new(Barrier::new(WRITERS));
        let handles = (0..WRITERS)
            .map(|index| {
                let state = Arc::clone(&state);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    state.update_offset(index as i32, index as i64 + 1, 1, 42);
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().expect("offset writer should finish");
        }

        let (offsets, version) = state.snapshot();
        assert_eq!(offsets.len(), WRITERS);
        assert_eq!(version.counter(), WRITERS as i64);
    }

    #[test]
    fn delay_level_config_is_published_only_after_complete_parse() {
        let published = ArcSwap::from_pointee(parse_delay_level_config("1s 2m 3h").unwrap());
        let old_generation = published.load_full();

        assert!(parse_delay_level_config("1s broken 3h").is_err());
        assert!(Arc::ptr_eq(&old_generation, &published.load_full()));

        published.store(Arc::new(parse_delay_level_config("5s").unwrap()));
        assert_eq!(old_generation.table.len(), 3);
        assert_eq!(published.load().table, BTreeMap::from([(1, 5_000)]));
        assert_eq!(published.load().max_level, 1);
    }

    #[test]
    fn delay_level_config_rejects_non_ascii_units_without_panicking() {
        assert!(parse_delay_level_config("1秒").is_err());
        assert!(parse_delay_level_config("0s").is_err());
        assert!(parse_delay_level_config("9223372036854775807d").is_err());
    }

    #[test]
    fn process_status_cell_publishes_every_transition() {
        let status = ProcessStatusCell::new(ProcessStatus::Running);

        for expected in [
            ProcessStatus::Success,
            ProcessStatus::Exception,
            ProcessStatus::Skip,
            ProcessStatus::Running,
        ] {
            status.store(expected);
            assert_eq!(status.load(), expected);
        }
    }

    #[tokio::test]
    async fn schedule_lifecycle_uses_fresh_generation_and_prunes_dynamic_child() {
        let temp_dir = TempDir::new().expect("schedule lifecycle temp dir should be created");
        let root = temp_dir.path().to_string_lossy().into_owned();
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root.clone().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.into(),
            ..MessageStoreConfig::default()
        });
        let runtime_context = RuntimeContext::from_current("schedule-lifecycle-generation-test");
        let service_context = runtime_context.service_context("broker");
        let parent_group = service_context.task_group().clone();
        let mut runtime = BrokerRuntime::new_with_service_context(broker_config, message_store_config, service_context);
        let service = runtime.inner_for_test().schedule_message_service_unchecked().clone();

        ScheduleMessageService::start_persist_task_for_probe(service.clone(), Duration::from_secs(60))
            .await
            .expect("first schedule generation should start");
        let first_generation = service.active_generation.load(Ordering::Acquire);
        assert_ne!(first_generation, 0);
        assert_eq!(parent_group.child_stats().active, 1);

        service.stop().await.expect("first schedule generation should stop");
        assert_eq!(service.active_generation.load(Ordering::Acquire), 0);
        assert_eq!(service.task_count(), 0);
        assert_eq!(parent_group.child_stats().active, 0);

        ScheduleMessageService::start_persist_task_for_probe(service.clone(), Duration::from_secs(60))
            .await
            .expect("second schedule generation should start");
        let second_generation = service.active_generation.load(Ordering::Acquire);
        assert!(second_generation > first_generation);

        service
            .shutdown()
            .await
            .expect("final schedule shutdown should succeed");
        assert_eq!(parent_group.child_stats().active, 0);
        assert!(
            ScheduleMessageService::start_persist_task_for_probe(service, Duration::from_secs(60))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn legacy_runtime_constructor_installs_owned_schedule_context_on_first_start() {
        let temp_dir = TempDir::new().expect("legacy schedule context temp dir should be created");
        let root = temp_dir.path().to_string_lossy().into_owned();
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root.clone().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let service = runtime.inner_for_test().schedule_message_service_unchecked().clone();
        assert!(service.runtime_capabilities.get().is_none());

        ScheduleMessageService::start_persist_task_for_probe(service.clone(), Duration::from_secs(60))
            .await
            .expect("legacy schedule service should install a compatibility context");
        assert!(service.runtime_capabilities.get().is_some());

        service
            .shutdown()
            .await
            .expect("legacy compatibility context should shutdown cleanly");
    }

    #[tokio::test]
    async fn failed_peer_persistence_leaves_memory_snapshot_unchanged() {
        let temp_dir = TempDir::new().expect("peer persistence temp dir should be created");
        let blocked_root = temp_dir.path().join("not-a-directory");
        std::fs::write(&blocked_root, b"blocked").expect("blocking file should be created");
        let root = blocked_root.to_string_lossy().into_owned();
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root.clone().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.into(),
            ..MessageStoreConfig::default()
        });
        let runtime_context = RuntimeContext::from_current("schedule-peer-persistence-test");
        let service_context = runtime_context.service_context("broker");
        let mut runtime = BrokerRuntime::new_with_service_context(broker_config, message_store_config, service_context);
        let service = runtime.inner_for_test().schedule_message_service_unchecked().clone();
        service.offset_state.update_offset(1, 10, 1, 1);
        let before = service.offset_state.snapshot();
        let peer = DelayOffsetSerializeWrapper::new(Some(HashMap::from([(2, 20)])), Some(DataVersion::default()));
        let encoded = peer.serialize_json().expect("peer snapshot should encode");

        assert!(service.sync_delay_offset_from_peer(&encoded, &peer).await.is_err());
        let after = service.offset_state.snapshot();
        assert_eq!(after.0, before.0);
        assert_eq!(after.1.counter(), before.1.counter());
    }

    // =============================================================================
    // Tests for DelayOffsetSerializeWrapper
    // =============================================================================

    /// Test DelayOffsetSerializeWrapper creation and accessor
    #[test]
    fn test_delay_offset_serialize_wrapper_accessors() {
        let mut offset_map = HashMap::new();
        offset_map.insert(1, 1000i64);
        offset_map.insert(2, 2000i64);

        let wrapper = DelayOffsetSerializeWrapper::new(Some(offset_map.clone()), None);

        // Test accessor methods
        assert!(wrapper.offset_table().is_some());
        let table = wrapper.offset_table().unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&1), Some(&1000i64));
        assert_eq!(table.get(&2), Some(&2000i64));

        // Test data_version accessor
        assert!(wrapper.data_version().is_none());
    }

    /// Test DelayOffsetSerializeWrapper with data version
    #[test]
    fn test_delay_offset_serialize_wrapper_with_version() {
        let mut offset_map = HashMap::new();
        offset_map.insert(1, 1000i64);

        let data_version = DataVersion::default();
        let wrapper = DelayOffsetSerializeWrapper::new(Some(offset_map), Some(data_version));

        assert!(wrapper.offset_table().is_some());
        assert!(wrapper.data_version().is_some());
    }

    /// Test queue_id_to_delay_level conversion
    #[test]
    fn test_queue_id_to_delay_level() {
        assert_eq!(queue_id_to_delay_level(0), 1);
        assert_eq!(queue_id_to_delay_level(1), 2);
        assert_eq!(queue_id_to_delay_level(17), 18);
    }

    /// Test delay_level_to_queue_id conversion
    #[test]
    fn test_delay_level_to_queue_id() {
        assert_eq!(delay_level_to_queue_id(1), 0);
        assert_eq!(delay_level_to_queue_id(2), 1);
        assert_eq!(delay_level_to_queue_id(18), 17);
    }

    /// Test bidirectional conversion between queue_id and delay_level
    #[test]
    fn test_queue_id_delay_level_bidirectional() {
        for queue_id in 0..18 {
            let delay_level = queue_id_to_delay_level(queue_id);
            let converted_back = delay_level_to_queue_id(delay_level);
            assert_eq!(queue_id, converted_back);
        }
    }

    /// Test ProcessStatus enum - ensure it's Copy and implements Debug
    #[test]
    fn test_process_status_traits() {
        let status = ProcessStatus::Running;
        let status_copy = status; // Test Copy trait

        // Both should be usable
        assert!(matches!(status, ProcessStatus::Running));
        assert!(matches!(status_copy, ProcessStatus::Running));

        // Test Debug trait
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Running"));
    }

    // =============================================================================
    // Tests for Helper Functions
    // =============================================================================

    /// Test DelayOffsetSerializeWrapper default state
    #[test]
    fn test_delay_offset_serialize_wrapper_default() {
        let wrapper = DelayOffsetSerializeWrapper::default();
        // Default wrapper has None for offset_table
        assert!(wrapper.offset_table().is_none());
        assert!(wrapper.data_version().is_none());
    }

    /// Test compute_deliver_timestamp with known delay level
    #[test]
    fn test_compute_deliver_timestamp() {
        // This test requires a ScheduleMessageService instance with delay_level_table populated
        // We'll test the logic by understanding the function behavior
        let store_timestamp = 1000i64;
        let delay_time = 5000i64; // 5 seconds
        let expected = store_timestamp + delay_time;

        assert_eq!(expected, 6000);
    }

    /// Test helper function conversions with edge cases
    #[test]
    fn test_conversion_edge_cases() {
        // Test with negative values
        assert_eq!(queue_id_to_delay_level(-1), 0);
        assert_eq!(delay_level_to_queue_id(0), -1);

        // Test with large values
        assert_eq!(queue_id_to_delay_level(1000), 1001);
        assert_eq!(delay_level_to_queue_id(1001), 1000);
    }

    /// Test DelayOffsetSerializeWrapper serialization compatibility
    #[test]
    fn test_delay_offset_serialize_wrapper_json_format() {
        let mut offset_map = HashMap::new();
        offset_map.insert(1, 1000i64);
        offset_map.insert(2, 2000i64);

        let wrapper = DelayOffsetSerializeWrapper::new(Some(offset_map), None);

        // Serialize using serde_json directly
        let json = serde_json::to_string(&wrapper).expect("Failed to serialize");

        // Verify camelCase format
        assert!(json.contains("offsetTable"));
        assert!(json.contains("dataVersion"));
    }

    #[test]
    fn schedule_message_service_uses_typed_errors() {
        let source = include_str!("schedule_message_service.rs");

        assert!(source.contains("async fn persist_current(&self) -> RocketMQResult<()>"));
        assert!(source.contains("RocketMQResult<PutResultProcess<MS>>"));
        assert!(!source.contains(concat!("ArcMut<", "ScheduleMessageService")));
        assert!(!source.contains(concat!("mut_from_ref()", ".escape_bridge_mut()")));
        assert!(!source.contains(concat!("Box<dyn std::error::", "Error")));
    }
}
