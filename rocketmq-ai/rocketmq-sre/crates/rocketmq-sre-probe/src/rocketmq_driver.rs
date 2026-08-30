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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ConsumeConcurrentlyContext;
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::LocalTransactionState;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerConcurrently;
use rocketmq_client_rust::TransactionListener;
use rocketmq_client_rust::TransactionMQProducer;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_sre_probe::ProbeAclConfig;
use rocketmq_sre_probe::ProbePlan;
use rocketmq_sre_probe::cleanup::ProbeCleanupResult;
use rocketmq_sre_probe::consumer::ProbeConsumeObservation;
use rocketmq_sre_probe::consumer::ProbeConsumerMode;
use rocketmq_sre_probe::producer::ProbeMessageBatch;
use rocketmq_sre_probe::producer::ProbeSendMode;
use rocketmq_sre_probe::producer::ProbeSendObservation;
use rocketmq_sre_probe::scenario::ProbeDriver;
use rocketmq_sre_probe::scenario::ProbeDriverError;
use tokio::sync::Notify;

const SEND_TIMEOUT_MILLIS: u64 = 2_000;
const DELAY_MILLIS: u64 = 500;
const DRIVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct RocketMqScenarioDriver {
    client_runtime: Arc<ClientRuntime>,
    endpoint: String,
    acl_config: Option<ProbeAclConfig>,
    consumer: Option<DefaultMQPushConsumer>,
    observed: Arc<AtomicUsize>,
    notification: Arc<Notify>,
    expected_key_prefix: Option<Arc<str>>,
    producer_stopped: bool,
    consumer_stopped: bool,
}

impl RocketMqScenarioDriver {
    pub(crate) fn new(
        client_runtime: Arc<ClientRuntime>,
        endpoint: String,
        acl_config: Option<ProbeAclConfig>,
    ) -> Self {
        Self {
            client_runtime,
            endpoint,
            acl_config,
            consumer: None,
            observed: Arc::new(AtomicUsize::new(0)),
            notification: Arc::new(Notify::new()),
            expected_key_prefix: None,
            producer_stopped: true,
            consumer_stopped: true,
        }
    }

    async fn send_standard(
        &mut self,
        plan: &ProbePlan,
        mode: ProbeSendMode,
        batch: &ProbeMessageBatch,
    ) -> Result<ProbeSendObservation, ProbeDriverError> {
        let builder = DefaultMQProducer::builder(Arc::clone(&self.client_runtime))
            .producer_group(plan.identity.producer_group.clone())
            .name_server_addr(self.endpoint.clone());
        let builder = match &self.acl_config {
            Some(config) => builder.rpc_hook(Arc::new(config.rpc_hook())),
            None => builder,
        };
        let mut producer = builder.build();
        self.producer_stopped = false;
        producer
            .start()
            .await
            .map_err(|_| ProbeDriverError::new("producer_start_failed"))?;

        let payload = vec![b'x'; batch.payload_bytes as usize];
        let mut accepted = 0_u16;
        let operation = async {
            for sequence in 0..batch.count {
                let mut builder = Message::builder()
                    .topic(plan.identity.topic.clone())
                    .tags(batch.tag)
                    .keys(vec![format!("{}-{sequence}", batch.key_prefix)])
                    .body_slice(&payload);
                if mode == ProbeSendMode::DelayedTimer {
                    builder = builder.delay_millis(DELAY_MILLIS);
                }
                producer
                    .send_with_timeout(builder.build_unchecked(), SEND_TIMEOUT_MILLIS)
                    .await
                    .map_err(|_| ProbeDriverError::new("message_send_failed"))?;
                accepted += 1;
                if sequence + 1 < batch.count {
                    tokio::time::sleep(Duration::from_millis(batch.minimum_interval_millis)).await;
                }
            }
            Ok::<(), ProbeDriverError>(())
        }
        .await;
        producer.shutdown().await;
        self.producer_stopped = true;
        operation?;
        Ok(ProbeSendObservation {
            accepted_messages: accepted,
        })
    }

    async fn send_transaction(
        &mut self,
        plan: &ProbePlan,
        batch: &ProbeMessageBatch,
    ) -> Result<ProbeSendObservation, ProbeDriverError> {
        let builder = TransactionMQProducer::builder(Arc::clone(&self.client_runtime))
            .producer_group(plan.identity.producer_group.clone())
            .name_server_addr(self.endpoint.clone())
            .topics(vec![plan.identity.topic.clone()])
            .transaction_listener(CommitTransactionListener);
        let builder = match &self.acl_config {
            Some(config) => builder.rpc_hook(Arc::new(config.rpc_hook())),
            None => builder,
        };
        let mut producer = builder.build();
        self.producer_stopped = false;
        producer
            .start()
            .await
            .map_err(|_| ProbeDriverError::new("transaction_producer_start_failed"))?;

        let payload = vec![b'x'; batch.payload_bytes as usize];
        let mut accepted = 0_u16;
        let operation = async {
            for sequence in 0..batch.count {
                let message = Message::builder()
                    .topic(plan.identity.topic.clone())
                    .tags(batch.tag)
                    .keys(vec![format!("{}-{sequence}", batch.key_prefix)])
                    .body_slice(&payload)
                    .build_unchecked();
                producer
                    .send_message_in_transaction::<(), _>(message, None)
                    .await
                    .map_err(|_| ProbeDriverError::new("transaction_send_failed"))?;
                accepted += 1;
                if sequence + 1 < batch.count {
                    tokio::time::sleep(Duration::from_millis(batch.minimum_interval_millis)).await;
                }
            }
            Ok::<(), ProbeDriverError>(())
        }
        .await;
        producer.shutdown().await;
        self.producer_stopped = true;
        operation?;
        Ok(ProbeSendObservation {
            accepted_messages: accepted,
        })
    }
}

impl ProbeDriver for RocketMqScenarioDriver {
    fn set_expected_key_prefix(&mut self, key_prefix: &str) {
        self.expected_key_prefix = Some(Arc::from(format!("{key_prefix}-")));
    }

    async fn start_consumer(&mut self, plan: &ProbePlan, _mode: ProbeConsumerMode) -> Result<(), ProbeDriverError> {
        let expected_key_prefix = self
            .expected_key_prefix
            .clone()
            .ok_or_else(|| ProbeDriverError::new("consumer_key_filter_missing"))?;
        let listener = CountingListener {
            observed: Arc::clone(&self.observed),
            notification: Arc::clone(&self.notification),
            expected_key_prefix,
        };
        let builder = DefaultMQPushConsumer::builder(Arc::clone(&self.client_runtime))
            .consumer_group(plan.identity.consumer_group.clone())
            .name_server_addr(self.endpoint.clone());
        let builder = match &self.acl_config {
            Some(config) => builder.rpc_hook(Some(Arc::new(config.rpc_hook()))),
            None => builder,
        };
        let mut consumer = builder.build();
        consumer
            .subscribe(&plan.identity.topic, "*")
            .await
            .map_err(|_| ProbeDriverError::new("consumer_subscribe_failed"))?;
        consumer.register_message_listener_concurrently(listener);
        self.consumer_stopped = false;
        consumer
            .start()
            .await
            .map_err(|_| ProbeDriverError::new("consumer_start_failed"))?;
        self.consumer = Some(consumer);
        Ok(())
    }

    async fn send(
        &mut self,
        plan: &ProbePlan,
        mode: ProbeSendMode,
        batch: &ProbeMessageBatch,
    ) -> Result<ProbeSendObservation, ProbeDriverError> {
        match mode {
            ProbeSendMode::TransactionCommit => self.send_transaction(plan, batch).await,
            ProbeSendMode::Standard
            | ProbeSendMode::ProxyPath
            | ProbeSendMode::DelayedTimer
            | ProbeSendMode::PopSeed => self.send_standard(plan, mode, batch).await,
        }
    }

    async fn await_acknowledgements(&mut self, expected: u16) -> Result<ProbeConsumeObservation, ProbeDriverError> {
        let expected = usize::from(expected);
        loop {
            let notified = self.notification.notified();
            if self.observed.load(Ordering::Acquire) >= expected {
                break;
            }
            notified.await;
        }
        let observed = self.observed.load(Ordering::Acquire).min(usize::from(u16::MAX)) as u16;
        Ok(ProbeConsumeObservation {
            received_messages: observed,
            acknowledged_messages: observed,
        })
    }

    async fn cleanup(&mut self) -> ProbeCleanupResult {
        let mut warnings = Vec::new();
        if let Some(mut consumer) = self.consumer.take() {
            match tokio::time::timeout(DRIVER_SHUTDOWN_TIMEOUT, consumer.shutdown()).await {
                Ok(()) => self.consumer_stopped = true,
                Err(_) => warnings.push("consumer_shutdown_timeout".to_owned()),
            }
        }
        ProbeCleanupResult::bounded(self.producer_stopped, self.consumer_stopped, true, warnings)
    }
}

#[derive(Debug)]
struct CommitTransactionListener;

impl TransactionListener for CommitTransactionListener {
    fn execute_local_transaction(
        &self,
        _message: &dyn MessageTrait,
        _argument: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }

    fn check_local_transaction(&self, _message: &MessageExt) -> LocalTransactionState {
        LocalTransactionState::CommitMessage
    }
}

#[derive(Clone)]
struct CountingListener {
    observed: Arc<AtomicUsize>,
    notification: Arc<Notify>,
    expected_key_prefix: Arc<str>,
}

impl MessageListenerConcurrently for CountingListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        let matched = messages
            .iter()
            .filter(|message| {
                message
                    .get_keys_ref()
                    .is_some_and(|keys| contains_expected_key(keys.as_str(), self.expected_key_prefix.as_ref()))
            })
            .count();
        if matched > 0 {
            self.observed.fetch_add(matched, Ordering::AcqRel);
            self.notification.notify_waiters();
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

fn contains_expected_key(keys: &str, expected_prefix: &str) -> bool {
    keys.split_whitespace().any(|key| key.starts_with(expected_prefix))
}

#[cfg(test)]
mod tests {
    use super::contains_expected_key;

    #[test]
    fn message_filter_counts_only_the_current_invocation_prefix() {
        assert!(contains_expected_key("probe-current-0 unrelated", "probe-current-"));
        assert!(!contains_expected_key("probe-previous-0", "probe-current-"));
        assert!(!contains_expected_key("probe-current", "probe-current-"));
    }
}
