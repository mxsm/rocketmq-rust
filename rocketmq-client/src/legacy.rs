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

use crate::ClientError;
use crate::ClientResult;
use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::Mutex;

use crate::runtime::schedule_client_fixed_delay_task_with_context;
use crate::runtime::ClientRuntime;
use crate::runtime::ClientScheduledTaskHandle;

pub use crate::consumer::consumer_impl::default_mq_pull_consumer_impl::DefaultMQPullConsumerImpl;
pub use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
pub use crate::consumer::default_mq_pull_consumer::MQPullConsumer;

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

    async fn start(self: &Arc<Self>) -> ClientResult<()> {
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

    fn spawn_coordinator(self: &Arc<Self>) -> ClientResult<()> {
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
        .map_err(|source| ClientError::service_source("classic_pull_schedule", source))?;
        *self.coordinator.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
        Ok(())
    }

    async fn shutdown(&self) -> ClientResult<()> {
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
            return Err(ClientError::timeout("classic pull schedule shutdown", 5000));
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
    ) -> ClientResult<Self> {
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

    fn core(&self) -> ClientResult<&Arc<PullScheduleCore>> {
        self.core.as_ref().ok_or_else(|| {
            ClientError::not_initialized("MQPullConsumerScheduleService has no ClientRuntime; use with_client_runtime")
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
    pub async fn start(&self) -> ClientResult<()> {
        self.core()?.start().await
    }

    /// Cancels the coordinator, awaits it, and shuts down the consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value, an underlying consumer shutdown
    /// error, or a timeout when the coordinator does not stop within the shutdown bound.
    pub async fn shutdown(&self) -> ClientResult<()> {
        self.core()?.shutdown().await
    }

    /// Sets how frequently the coordinator refreshes registered topic routes.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero duration or a detached schedule service.
    pub fn set_refresh_interval(&self, refresh_interval: Duration) -> ClientResult<()> {
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
    pub fn register_pull_task_callback<C>(&self, topic: impl Into<CheetahString>, callback: C) -> ClientResult<()>
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
    pub fn default_mq_pull_consumer(&self) -> ClientResult<DefaultMQPullConsumer> {
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
    ) -> Pin<Box<dyn Future<Output = ClientResult<()>> + Send + 'a>> {
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
    pub fn get_pull_consumer(&self) -> ClientResult<DefaultMQPullConsumer> {
        self.pull_consumer
            .clone()
            .ok_or_else(|| ClientError::not_initialized("PullTaskContext has no pull consumer"))
    }

    /// Replaces the consumer attached to this context.
    pub fn set_pull_consumer(&mut self, pull_consumer: DefaultMQPullConsumer) {
        self.pull_consumer = Some(pull_consumer);
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
    ) -> ClientResult<()> {
        Err(ClientError::not_initialized(
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
    ) -> ClientResult<()> {
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

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::test_support::error_assertions::assert_not_initialized;

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

        assert_not_initialized(
            &error,
            "DefaultMQPullConsumer has no ClientRuntime; create it with DefaultMQPullConsumer::builder",
        );
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

        assert_not_initialized(
            &error,
            "MQPullConsumerScheduleService has no ClientRuntime; use with_client_runtime",
        );
    }
}
