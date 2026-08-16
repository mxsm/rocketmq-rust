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

#![allow(deprecated)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::Mutex;

use crate::producer::local_transaction_state::LocalTransactionState;
use crate::runtime::schedule_client_fixed_delay_task_with_context;
use crate::runtime::ClientRuntime;
use crate::runtime::ClientScheduledTaskHandle;

pub use crate::consumer::consumer_impl::default_mq_pull_consumer_impl::DefaultMQPullConsumerImpl;
pub use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
pub use crate::consumer::default_mq_pull_consumer::MQPullConsumer;

const MODERN_TRANSACTION_LISTENER: &str = "TransactionListener";
const MODERN_TRACE_HOOKS: &str = "RocketMQ trace hooks";

fn unsupported_legacy_api(api: &str, replacement: &str) -> RocketMQError {
    RocketMQError::illegal_argument(format!(
        "{api} is deprecated in the RocketMQ Java client and is not supported by rocketmq-client-rust; use \
         {replacement} instead"
    ))
}

fn unsupported_impl_api(api: &str) -> RocketMQError {
    RocketMQError::illegal_argument(format!(
        "{api} is a RocketMQ Java impl-package type and is not part of the rocketmq-client-rust public API"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleServiceState {
    Created,
    Running,
    Failed,
    Shutdown,
}

struct PullScheduleCore {
    consumer: DefaultMQPullConsumer,
    service_context: ChildServiceContext,
    callbacks: StdRwLock<HashMap<CheetahString, Arc<dyn PullTaskCallback>>>,
    refresh_interval: StdRwLock<Duration>,
    state: Mutex<ScheduleServiceState>,
    coordinator: StdMutex<Option<ClientScheduledTaskHandle>>,
}

impl PullScheduleCore {
    fn callback_snapshot(&self) -> HashMap<CheetahString, Arc<dyn PullTaskCallback>> {
        self.callbacks
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn start(self: &Arc<Self>) -> RocketMQResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ScheduleServiceState::Created => {}
            ScheduleServiceState::Running => {
                return Err(crate::mq_client_err!("MQPullConsumerScheduleService already started"));
            }
            ScheduleServiceState::Failed => {
                return Err(crate::mq_client_err!(
                    "MQPullConsumerScheduleService start previously failed; create a new service"
                ));
            }
            ScheduleServiceState::Shutdown => {
                return Err(crate::mq_client_err!(
                    "MQPullConsumerScheduleService has been shut down"
                ));
            }
        }
        if self.callback_snapshot().is_empty() {
            *state = ScheduleServiceState::Failed;
            return Err(crate::mq_client_err!("no pull task callback is registered"));
        }
        if let Err(error) = self.consumer.start().await {
            *state = ScheduleServiceState::Failed;
            return Err(error);
        }
        if let Err(error) = self.spawn_coordinator() {
            self.consumer.shutdown().await?;
            *state = ScheduleServiceState::Failed;
            return Err(error);
        }
        *state = ScheduleServiceState::Running;
        Ok(())
    }

    fn spawn_coordinator(self: &Arc<Self>) -> RocketMQResult<()> {
        let core = self.clone();
        let due = Arc::new(Mutex::new(HashMap::<MessageQueue, Instant>::new()));
        let refresh_interval = *self
            .refresh_interval
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let handle = schedule_client_fixed_delay_task_with_context(
            &self.service_context,
            "rocketmq-client-classic-pull-schedule",
            refresh_interval,
            refresh_interval,
            Duration::from_secs(5),
            move || {
                let core = core.clone();
                let due = due.clone();
                async move {
                    let mut due = due.lock().await;
                let mut active_queues = HashSet::new();
                for (topic, callback) in core.callback_snapshot() {
                    let queues = match core.consumer.fetch_subscribe_message_queues(topic.as_str()).await {
                        Ok(queues) => queues,
                        Err(error) => {
                            tracing::warn!(topic = %topic, error = %error, "scheduled Classic Pull route refresh failed");
                            continue;
                        }
                    };
                    for queue in queues {
                        active_queues.insert(queue.clone());
                        let now = Instant::now();
                        if due.get(&queue).is_some_and(|deadline| *deadline > now) {
                            continue;
                        }
                        let mut context = PullTaskContext::with_pull_consumer(core.consumer.clone());
                        match callback.do_pull_task(&queue, &mut context).await {
                            Ok(()) => {
                                let delay = u64::try_from(context.pull_next_delay_time_millis.max(0)).unwrap_or(0);
                                due.insert(queue, now + Duration::from_millis(delay));
                            }
                            Err(error) => {
                                tracing::warn!(message_queue = %queue, error = %error, "scheduled Classic Pull callback failed");
                                due.insert(queue, now + Duration::from_millis(3000));
                            }
                        }
                    }
                }
                due.retain(|queue, _| active_queues.contains(queue));
                }
            },
        )
        .map_err(RocketMQError::from)?;
        *self.coordinator.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
        Ok(())
    }

    async fn shutdown(&self) -> RocketMQResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ScheduleServiceState::Shutdown => return Ok(()),
            ScheduleServiceState::Created => {
                *state = ScheduleServiceState::Shutdown;
                return Ok(());
            }
            ScheduleServiceState::Running | ScheduleServiceState::Failed => {}
        }
        let coordinator = self
            .coordinator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let stopped = match coordinator {
            Some(coordinator) => coordinator.shutdown(Duration::from_secs(5)).await.is_healthy(),
            None => true,
        };
        self.consumer.shutdown().await?;
        *state = ScheduleServiceState::Shutdown;
        if !stopped {
            return Err(RocketMQError::Timeout {
                operation: "classic pull schedule shutdown",
                timeout_ms: 5000,
            });
        }
        Ok(())
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Classic Pull scheduling is retained for compatibility; prefer application-owned scheduling for new code"
)]
#[derive(Clone)]
pub struct MQPullConsumerScheduleService {
    consumer_group: CheetahString,
    core: Option<Arc<PullScheduleCore>>,
}

impl MQPullConsumerScheduleService {
    /// Creates a detached compatibility value.
    ///
    /// Use [`Self::with_client_runtime`] for a runnable schedule service.
    pub fn new(consumer_group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: consumer_group.into(),
            core: None,
        }
    }

    /// Creates a schedule service backed by an application-owned client runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer group or underlying Classic Pull configuration is
    /// invalid.
    pub fn with_client_runtime(
        client_runtime: Arc<ClientRuntime>,
        consumer_group: impl Into<CheetahString>,
    ) -> RocketMQResult<Self> {
        let consumer_group = consumer_group.into();
        let consumer = DefaultMQPullConsumer::builder(client_runtime.clone())
            .consumer_group(consumer_group.clone())
            .build()?;
        Ok(Self {
            consumer_group,
            core: Some(Arc::new(PullScheduleCore {
                consumer,
                service_context: client_runtime.component("classic-pull-schedule"),
                callbacks: StdRwLock::new(HashMap::new()),
                refresh_interval: StdRwLock::new(Duration::from_secs(1)),
                state: Mutex::new(ScheduleServiceState::Created),
                coordinator: StdMutex::new(None),
            })),
        })
    }

    fn core(&self) -> RocketMQResult<&Arc<PullScheduleCore>> {
        self.core.as_ref().ok_or_else(|| {
            RocketMQError::not_initialized(
                "MQPullConsumerScheduleService has no ClientRuntime; use with_client_runtime",
            )
        })
    }

    /// Returns the configured consumer group.
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    /// Starts the consumer and its runtime-owned schedule coordinator.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value, a stable lifecycle error for a
    /// repeated or invalid start, or an underlying consumer or task-spawn error.
    pub async fn start(&self) -> RocketMQResult<()> {
        self.core()?.start().await
    }

    /// Cancels the coordinator, awaits it, and shuts down the consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value, an underlying consumer shutdown
    /// error, or a timeout when the coordinator does not stop within the shutdown bound.
    pub async fn shutdown(&self) -> RocketMQResult<()> {
        self.core()?.shutdown().await
    }

    /// Sets how frequently the coordinator refreshes registered topic routes.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero duration or a detached schedule service.
    pub fn set_refresh_interval(&self, refresh_interval: Duration) -> RocketMQResult<()> {
        if refresh_interval.is_zero() {
            return Err(crate::mq_client_err!("schedule refresh interval must be positive"));
        }
        *self
            .core()?
            .refresh_interval
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = refresh_interval;
        Ok(())
    }

    /// Registers or replaces the callback for a topic.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank topic or a detached schedule service.
    pub fn register_pull_task_callback<C>(&self, topic: impl Into<CheetahString>, callback: C) -> RocketMQResult<()>
    where
        C: PullTaskCallback,
    {
        let topic = topic.into();
        if topic.trim().is_empty() {
            return Err(crate::mq_client_err!("topic is blank"));
        }
        let core = self.core()?;
        core.callbacks
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(topic, Arc::new(callback));
        Ok(())
    }

    /// Returns the schedule service's Classic Pull consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached schedule service.
    pub fn default_mq_pull_consumer(&self) -> RocketMQResult<DefaultMQPullConsumer> {
        Ok(self.core()?.consumer.clone())
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskCallback is deprecated with MQPullConsumerScheduleService"
)]
pub trait PullTaskCallback: Send + Sync + 'static {
    /// Runs one scheduled pull task for a currently readable queue.
    ///
    /// The callback may set the next delay through `context`. Returning an error applies the
    /// schedule service's bounded retry delay.
    fn do_pull_task<'a>(
        &'a self,
        _message_queue: &'a MessageQueue,
        _context: &'a mut PullTaskContext,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskContext is deprecated with MQPullConsumerScheduleService"
)]
#[derive(Clone)]
pub struct PullTaskContext {
    pull_next_delay_time_millis: i32,
    pull_consumer: Option<DefaultMQPullConsumer>,
}

impl std::fmt::Debug for PullTaskContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PullTaskContext")
            .field("pull_next_delay_time_millis", &self.pull_next_delay_time_millis)
            .field("has_pull_consumer", &self.pull_consumer.is_some())
            .finish()
    }
}

impl Default for PullTaskContext {
    fn default() -> Self {
        Self {
            pull_next_delay_time_millis: 200,
            pull_consumer: None,
        }
    }
}

impl PullTaskContext {
    /// Creates a context without an attached consumer.
    pub fn new() -> Self {
        Self::default()
    }

    fn with_pull_consumer(pull_consumer: DefaultMQPullConsumer) -> Self {
        Self {
            pull_consumer: Some(pull_consumer),
            ..Self::default()
        }
    }

    /// Returns the configured delay before this queue's next callback.
    pub fn pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis
    }

    /// Java-compatible alias for [`Self::pull_next_delay_time_millis`].
    pub fn get_pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis()
    }

    /// Sets the delay before this queue's next callback.
    pub fn set_pull_next_delay_time_millis(&mut self, pull_next_delay_time_millis: i32) {
        self.pull_next_delay_time_millis = pull_next_delay_time_millis;
    }

    /// Returns the consumer attached by the schedule service.
    ///
    /// # Errors
    ///
    /// Returns an initialization error when the context was constructed directly and no consumer
    /// has been attached.
    pub fn get_pull_consumer(&self) -> RocketMQResult<DefaultMQPullConsumer> {
        self.pull_consumer
            .clone()
            .ok_or_else(|| RocketMQError::not_initialized("PullTaskContext has no pull consumer"))
    }

    /// Replaces the consumer attached to this context.
    pub fn set_pull_consumer(&mut self, pull_consumer: DefaultMQPullConsumer) {
        self.pull_consumer = Some(pull_consumer);
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskImpl is deprecated; use MQPullConsumerScheduleService"
)]
#[derive(Debug, Clone, Default)]
pub struct PullTaskImpl {
    message_queue: Option<MessageQueue>,
}

impl PullTaskImpl {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue: Some(message_queue),
        }
    }

    pub fn message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn run(&self) -> RocketMQResult<()> {
        self.message_queue
            .as_ref()
            .map(|_| ())
            .ok_or_else(|| RocketMQError::not_initialized("PullTaskImpl has no message queue"))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java RebalancePullImpl is deprecated with DefaultMQPullConsumer"
)]
#[derive(Debug, Clone, Default)]
pub struct RebalancePullImpl;

impl RebalancePullImpl {
    pub fn new() -> RocketMQResult<Self> {
        Ok(Self)
    }
}

#[deprecated(since = "0.9.0", note = "Java MQHelper depends on DefaultMQPullConsumer")]
#[derive(Debug, Clone, Default)]
pub struct MQHelper;

impl MQHelper {
    /// Retains the Java-shaped helper signature without creating a hidden runtime.
    ///
    /// Use [`Self::reset_offset_by_timestamp_with_client_runtime`] for the runnable equivalent.
    ///
    /// # Errors
    ///
    /// Always returns an initialization error because this compatibility signature has no
    /// application-owned [`ClientRuntime`].
    pub fn reset_offset_by_timestamp(
        _message_model: impl Into<CheetahString>,
        _consumer_group: impl Into<CheetahString>,
        _topic: impl Into<CheetahString>,
        _timestamp: u64,
    ) -> RocketMQResult<()> {
        Err(RocketMQError::not_initialized(
            "MQHelper requires an application-owned ClientRuntime; use reset_offset_by_timestamp_with_client_runtime",
        ))
    }

    /// Resets every queue offset for a topic to the position nearest the timestamp.
    ///
    /// The temporary Classic Pull consumer persists updated offsets during its bounded shutdown.
    ///
    /// # Errors
    ///
    /// Returns a configuration, startup, route, offset-query, offset-store, or shutdown error.
    pub async fn reset_offset_by_timestamp_with_client_runtime(
        client_runtime: Arc<ClientRuntime>,
        message_model: MessageModel,
        consumer_group: impl Into<CheetahString>,
        topic: impl Into<CheetahString>,
        timestamp: u64,
    ) -> RocketMQResult<()> {
        let consumer = DefaultMQPullConsumer::builder(client_runtime)
            .consumer_group(consumer_group)
            .message_model(message_model)
            .build()?;
        let topic = topic.into();
        consumer.start().await?;

        let operation = async {
            let mut queues = consumer.fetch_subscribe_message_queues(topic.as_str()).await?;
            queues.sort();
            for queue in queues {
                let offset = consumer.search_offset(&queue, timestamp).await?;
                if offset >= 0 {
                    consumer.update_consume_offset(&queue, offset).await?;
                }
            }
            Ok(())
        }
        .await;
        let shutdown = consumer.shutdown().await;

        match operation {
            Ok(()) => shutdown,
            Err(error) => {
                if let Err(shutdown_error) = shutdown {
                    tracing::warn!(%shutdown_error, "MQHelper consumer shutdown failed after offset reset error");
                }
                Err(error)
            }
        }
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java TransactionCheckListener is deprecated; use TransactionListener"
)]
pub trait TransactionCheckListener {
    fn check_local_transaction_state(&self, _message: &MessageExt) -> RocketMQResult<LocalTransactionState> {
        Err(unsupported_legacy_api(
            "TransactionCheckListener",
            MODERN_TRANSACTION_LISTENER,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct SendMessageOpenTracingHookImpl;

impl SendMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "SendMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "SendMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct ConsumeMessageOpenTracingHookImpl;

impl ConsumeMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "ConsumeMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "ConsumeMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct EndTransactionOpenTracingHookImpl;

impl EndTransactionOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "EndTransactionOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "EndTransactionOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct DoNothingClientRemotingProcessor;

impl DoNothingClientRemotingProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_request(&self) -> Option<()> {
        None
    }
}

#[derive(Debug, Clone, Default)]
pub struct RebalanceImpl;

impl RebalanceImpl {
    pub fn new() -> RocketMQResult<Self> {
        Err(unsupported_impl_api("RebalanceImpl"))
    }

    pub fn do_rebalance(&self) -> RocketMQResult<()> {
        Err(unsupported_impl_api("RebalanceImpl"))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumeRequest;

impl ConsumeRequest {
    pub fn new() -> Self {
        Self
    }

    pub fn run(&self) -> RocketMQResult<()> {
        Err(unsupported_impl_api("ConsumeRequest"))
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_pull_consumer_fails_closed_without_runtime() {
        let consumer = DefaultMQPullConsumer::with_consumer_group("LegacyGroup");

        assert_eq!(
            consumer.consumer_group().map(|group| group.as_str()),
            Some("LegacyGroup")
        );
        let error = consumer
            .start()
            .await
            .expect_err("detached pull consumer should not start");

        assert!(error.to_string().contains("DefaultMQPullConsumer"));
        assert!(error.to_string().contains("builder"));
        assert!(!error.to_string().contains("not supported"));
    }

    #[test]
    fn pull_task_context_preserves_java_default_delay() {
        let mut context = PullTaskContext::new();

        assert_eq!(context.get_pull_next_delay_time_millis(), 200);
        context.set_pull_next_delay_time_millis(500);
        assert_eq!(context.pull_next_delay_time_millis(), 500);
    }

    #[test]
    fn detached_schedule_service_requires_runtime_for_callbacks() {
        struct Callback;
        impl PullTaskCallback for Callback {}

        let service = MQPullConsumerScheduleService::new("LegacyGroup");
        let error = service
            .register_pull_task_callback("TopicA", Callback)
            .expect_err("detached schedule service should require runtime");

        assert!(error.to_string().contains("with_client_runtime"));
        assert!(!error.to_string().contains("not supported"));
    }

    #[test]
    fn legacy_open_tracing_hooks_keep_java_hook_names() {
        assert_eq!(
            SendMessageOpenTracingHookImpl::new(()).hook_name(),
            "SendMessageOpenTracingHook"
        );
        assert_eq!(
            ConsumeMessageOpenTracingHookImpl::new(()).hook_name(),
            "ConsumeMessageOpenTracingHook"
        );
        assert_eq!(
            EndTransactionOpenTracingHookImpl::new(()).hook_name(),
            "EndTransactionOpenTracingHook"
        );
    }

    #[test]
    fn transaction_check_listener_returns_typed_unsupported_error() {
        struct Listener;
        impl TransactionCheckListener for Listener {}

        let error = Listener
            .check_local_transaction_state(&MessageExt::default())
            .expect_err("deprecated transaction listener should reject checks");

        assert!(error.to_string().contains("TransactionCheckListener"));
        assert!(error.to_string().contains("TransactionListener"));
    }

    #[test]
    fn impl_package_markers_do_not_panic() {
        assert!(DoNothingClientRemotingProcessor::new().process_request().is_none());

        let rebalance_error = RebalanceImpl::new().expect_err("impl-package RebalanceImpl should be unsupported");
        assert!(rebalance_error.to_string().contains("impl-package type"));

        let consume_error = ConsumeRequest::new()
            .run()
            .expect_err("impl-package ConsumeRequest should be unsupported");
        assert!(consume_error.to_string().contains("ConsumeRequest"));
    }
}
