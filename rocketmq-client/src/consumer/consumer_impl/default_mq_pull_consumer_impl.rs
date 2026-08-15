// Copyright 2026 The RocketMQ Rust Authors
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
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use tokio::sync::Mutex;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::ServiceState;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::default_mq_pull_consumer::ClassicPullCallback;
use crate::consumer::default_mq_pull_consumer::PullOptions;
use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use crate::consumer::pull_result::PullResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassicPullServiceState {
    Created,
    Running,
    Failed,
    Shutdown,
}

struct ClassicPullCore {
    lite_consumer: DefaultLitePullConsumer,
    consumer_pull_timeout: Duration,
    broker_suspend_timeout: Duration,
    consumer_timeout_when_suspend: Duration,
    state: Mutex<ClassicPullServiceState>,
    listeners: Arc<StdRwLock<HashMap<CheetahString, ArcMessageQueueListener>>>,
}

struct ClassicPullQueueListener {
    listeners: Arc<StdRwLock<HashMap<CheetahString, ArcMessageQueueListener>>>,
    namespace: Option<CheetahString>,
}

impl ClassicPullQueueListener {
    fn new(listeners: Arc<StdRwLock<HashMap<CheetahString, ArcMessageQueueListener>>>) -> Self {
        Self {
            listeners,
            namespace: None,
        }
    }

    fn with_namespace(mut self, namespace: Option<CheetahString>) -> Self {
        self.namespace = namespace.filter(|namespace| !namespace.is_empty());
        self
    }

    fn without_namespace(&self, resource: &str) -> CheetahString {
        match &self.namespace {
            Some(namespace) => NamespaceUtil::without_namespace_with_namespace(resource, namespace.as_str()).into(),
            None => NamespaceUtil::without_namespace(resource).into(),
        }
    }

    fn queues_without_namespace(&self, queues: &HashSet<MessageQueue>) -> HashSet<MessageQueue> {
        queues
            .iter()
            .map(|queue| {
                MessageQueue::from_parts(
                    self.without_namespace(queue.topic()),
                    queue.broker_name().clone(),
                    queue.queue_id(),
                )
            })
            .collect()
    }
}

impl crate::consumer::message_queue_listener::MessageQueueListener for ClassicPullQueueListener {
    fn message_queue_changed(&self, topic: &str, mq_all: &HashSet<MessageQueue>, mq_divided: &HashSet<MessageQueue>) {
        let topic = self.without_namespace(topic);
        let listener = self
            .listeners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&topic)
            .cloned();
        if let Some(listener) = listener {
            let mq_all = self.queues_without_namespace(mq_all);
            let mq_divided = self.queues_without_namespace(mq_divided);
            listener.message_queue_changed(topic.as_str(), &mq_all, &mq_divided);
        }
    }
}

impl ClassicPullCore {
    async fn start(self: &Arc<Self>) -> RocketMQResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ClassicPullServiceState::Created => {}
            ClassicPullServiceState::Running => {
                return Err(crate::mq_client_err!("DefaultMQPullConsumer already started"));
            }
            ClassicPullServiceState::Failed => {
                return Err(crate::mq_client_err!(
                    "DefaultMQPullConsumer start previously failed; create a new consumer"
                ));
            }
            ClassicPullServiceState::Shutdown => {
                return Err(crate::mq_client_err!(
                    "DefaultMQPullConsumer has been shut down; create a new consumer"
                ));
            }
        }

        let topics = self.listener_snapshot().keys().cloned().collect::<Vec<_>>();
        for topic in topics {
            let topic = self.lite_consumer.classic_topic_with_namespace(topic.as_str());
            self.lite_consumer.register_classic_pull_subscription(&topic).await?;
        }
        if let Err(error) = self.lite_consumer.start().await {
            *state = ClassicPullServiceState::Failed;
            return Err(error);
        }
        *state = ClassicPullServiceState::Running;
        Ok(())
    }

    fn listener_snapshot(&self) -> HashMap<CheetahString, ArcMessageQueueListener> {
        self.listeners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn shutdown(&self) -> RocketMQResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ClassicPullServiceState::Shutdown => return Ok(()),
            ClassicPullServiceState::Created => {
                *state = ClassicPullServiceState::Shutdown;
                return Ok(());
            }
            ClassicPullServiceState::Running | ClassicPullServiceState::Failed => {}
        }
        self.lite_consumer.shutdown().await;
        *state = ClassicPullServiceState::Shutdown;
        Ok(())
    }

    async fn ensure_running(&self) -> RocketMQResult<()> {
        if *self.state.lock().await != ClassicPullServiceState::Running {
            return Err(RocketMQError::not_initialized(
                "DefaultMQPullConsumer not started. Call start() first.",
            ));
        }
        self.lite_consumer.try_impl_()?.ensure_classic_pull_running()
    }
}

/// Functional implementation handle retained under the Java compatibility name.
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
#[derive(Clone, Default)]
pub struct DefaultMQPullConsumerImpl {
    core: Option<Arc<ClassicPullCore>>,
}

#[allow(deprecated)]
impl DefaultMQPullConsumerImpl {
    /// Creates a detached implementation marker for source compatibility.
    pub fn new() -> RocketMQResult<Self> {
        Ok(Self::default())
    }

    pub(crate) fn from_lite_consumer(
        lite_consumer: DefaultLitePullConsumer,
        consumer_pull_timeout: Duration,
        broker_suspend_timeout: Duration,
        consumer_timeout_when_suspend: Duration,
    ) -> RocketMQResult<Self> {
        let listeners = Arc::new(StdRwLock::new(HashMap::new()));
        let namespace = lite_consumer.client_config().resolved_namespace();
        lite_consumer.set_classic_pull_message_queue_listener(Some(Arc::new(
            ClassicPullQueueListener::new(listeners.clone()).with_namespace(namespace),
        )))?;
        Ok(Self {
            core: Some(Arc::new(ClassicPullCore {
                lite_consumer,
                consumer_pull_timeout,
                broker_suspend_timeout,
                consumer_timeout_when_suspend,
                state: Mutex::new(ClassicPullServiceState::Created),
                listeners,
            })),
        })
    }

    fn core(&self) -> RocketMQResult<&Arc<ClassicPullCore>> {
        self.core.as_ref().ok_or_else(|| {
            RocketMQError::not_initialized(
                "DefaultMQPullConsumerImpl is detached; create a consumer with DefaultMQPullConsumer::builder",
            )
        })
    }

    /// Starts the implementation.
    ///
    /// # Errors
    ///
    /// Returns an initialization, lifecycle, or underlying client startup error.
    pub async fn start(&self) -> RocketMQResult<()> {
        self.core()?.start().await
    }

    /// Shuts the implementation down and awaits its runtime-owned client tasks.
    ///
    /// # Errors
    ///
    /// Returns an initialization or underlying client shutdown error.
    pub async fn shutdown(&self) -> RocketMQResult<()> {
        self.core()?.shutdown().await
    }

    /// Returns whether the implementation is running.
    pub async fn is_running(&self) -> bool {
        let Ok(core) = self.core() else {
            return false;
        };
        *core.state.lock().await == ClassicPullServiceState::Running
            && core
                .lite_consumer
                .try_impl_()
                .is_ok_and(|implementation| implementation.service_state() == ServiceState::Running)
    }

    pub(crate) fn consumer_pull_timeout(&self) -> Duration {
        self.core
            .as_ref()
            .map_or(Duration::from_secs(10), |core| core.consumer_pull_timeout)
    }

    pub(crate) fn client_config(&self) -> RocketMQResult<Arc<ClientConfig>> {
        Ok(self.core()?.lite_consumer.client_config())
    }

    pub(crate) fn broker_suspend_timeout(&self) -> Duration {
        self.core
            .as_ref()
            .map_or(Duration::from_secs(20), |core| core.broker_suspend_timeout)
    }

    pub(crate) fn consumer_timeout_when_suspend(&self) -> Duration {
        self.core
            .as_ref()
            .map_or(Duration::from_secs(30), |core| core.consumer_timeout_when_suspend)
    }

    pub(crate) async fn pull_with_options(&self, options: PullOptions) -> RocketMQResult<PullResult> {
        let core = self.core()?;
        core.ensure_running().await?;
        let message_queue = core.lite_consumer.queue_with_namespace(options.message_queue());
        let options = options.with_message_queue(message_queue);
        core.lite_consumer.try_impl_()?.classic_pull(&options).await
    }

    pub(crate) async fn pull_async_with_options<C>(&self, options: PullOptions, callback: C) -> RocketMQResult<()>
    where
        C: ClassicPullCallback,
    {
        let core = self.core()?;
        core.ensure_running().await?;
        let message_queue = core.lite_consumer.queue_with_namespace(options.message_queue());
        let options = options.with_message_queue(message_queue);
        core.lite_consumer
            .try_impl_()?
            .classic_pull_async(options, callback)
            .await
    }

    pub(crate) async fn fetch_subscribe_message_queues(&self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.fetch_message_queues(topic).await
    }

    pub(crate) async fn register_message_queue_listener(
        &self,
        topic: &str,
        listener: ArcMessageQueueListener,
    ) -> RocketMQResult<()> {
        if topic.trim().is_empty() {
            return Err(crate::mq_client_err!("topic is blank"));
        }
        let core = self.core()?;
        let topic = CheetahString::from_slice(topic);
        core.listeners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(topic.clone(), listener);
        if *core.state.lock().await == ClassicPullServiceState::Running {
            let wrapped_topic = core.lite_consumer.classic_topic_with_namespace(topic.as_str());
            core.lite_consumer
                .register_classic_pull_subscription(&wrapped_topic)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn update_consume_offset(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(message_queue);
        core.lite_consumer
            .try_impl_()?
            .update_classic_pull_offset(&queue, offset)
            .await
    }

    pub(crate) async fn fetch_consume_offset(
        &self,
        message_queue: &MessageQueue,
        from_store: bool,
    ) -> RocketMQResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        let queue = core.lite_consumer.queue_with_namespace(message_queue);
        core.lite_consumer
            .try_impl_()?
            .fetch_classic_pull_offset(&queue, from_store)
            .await
    }

    pub(crate) async fn search_offset(&self, message_queue: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.offset_for_timestamp(message_queue, timestamp).await
    }

    pub(crate) async fn max_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.max_offset(message_queue).await
    }

    pub(crate) async fn min_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let core = self.core()?;
        core.ensure_running().await?;
        core.lite_consumer.min_offset(message_queue).await
    }

    /// Returns a live rebalance compatibility marker.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached implementation marker.
    pub fn rebalance_impl(&self) -> RocketMQResult<crate::legacy::RebalancePullImpl> {
        self.core()?;
        Ok(crate::legacy::RebalancePullImpl)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::consumer::message_queue_listener::MessageQueueListener;

    struct AssignmentListener {
        calls: Arc<AtomicUsize>,
        assigned: Arc<AtomicUsize>,
    }

    impl MessageQueueListener for AssignmentListener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &HashSet<MessageQueue>,
            mq_divided: &HashSet<MessageQueue>,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.assigned.store(mq_divided.len(), Ordering::SeqCst);
        }
    }

    #[test]
    fn classic_queue_listener_forwards_the_rebalance_assignment_for_its_topic() {
        let listeners = Arc::new(StdRwLock::new(HashMap::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let assigned = Arc::new(AtomicUsize::new(0));
        listeners.write().expect("listener registry should be writable").insert(
            CheetahString::from_static_str("TopicA"),
            Arc::new(AssignmentListener {
                calls: calls.clone(),
                assigned: assigned.clone(),
            }) as ArcMessageQueueListener,
        );
        let dispatcher = ClassicPullQueueListener::new(listeners);
        let all = HashSet::from([
            MessageQueue::from_parts("TopicA", "broker-a", 0),
            MessageQueue::from_parts("TopicA", "broker-a", 1),
        ]);
        let divided = HashSet::from([MessageQueue::from_parts("TopicA", "broker-a", 1)]);

        dispatcher.message_queue_changed("TopicA", &all, &divided);
        dispatcher.message_queue_changed("OtherTopic", &all, &divided);

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(assigned.load(Ordering::SeqCst), 1);
    }
}
