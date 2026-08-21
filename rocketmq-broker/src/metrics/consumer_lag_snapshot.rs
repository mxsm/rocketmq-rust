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
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use futures::future::join_all;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_store::BrokerReadWriteStore;
use tokio::time::Instant;
use tracing::warn;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupOutcome;
use crate::offset::manager::consumer_offset_manager::ConsumerLagAdjustment;
use crate::offset::manager::consumer_offset_manager::ConsumerLagAdjustments;
use crate::offset::manager::consumer_offset_manager::ConsumerLagObservation;
use crate::offset::manager::consumer_offset_manager::ConsumerLagTarget;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pop_message_processor::PopMessageProcessor;

const POP_REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ConsumerLagSnapshotService<MS: BrokerReadWriteStore> {
    offsets: Arc<ConsumerOffsetManager<MS>>,
    consumers: ConsumerManager,
    pop_processor: Arc<PopMessageProcessor<MS>>,
    notify_before_pop_calculate_lag: bool,
    cardinality_limit: usize,
    refresh_timeout: Duration,
    current: ArcSwap<Vec<ConsumerLagObservation>>,
}

impl<MS: BrokerReadWriteStore> ConsumerLagSnapshotService<MS> {
    pub(crate) fn new(
        offsets: Arc<ConsumerOffsetManager<MS>>,
        consumers: ConsumerManager,
        pop_processor: Arc<PopMessageProcessor<MS>>,
        notify_before_pop_calculate_lag: bool,
        cardinality_limit: usize,
    ) -> Self {
        Self {
            offsets,
            consumers,
            pop_processor,
            notify_before_pop_calculate_lag,
            cardinality_limit,
            refresh_timeout: POP_REFRESH_TIMEOUT,
            // Fail closed until the owned refresh task has sampled POP state.
            current: ArcSwap::from_pointee(Vec::new()),
        }
    }

    pub(crate) fn current(&self) -> Arc<Vec<ConsumerLagObservation>> {
        self.current.load_full()
    }

    pub(crate) async fn refresh(&self) {
        let pop_processor = &self.pop_processor;

        let targets = self.offsets.consumer_lag_targets(self.cardinality_limit);
        let pop_targets = targets
            .iter()
            .filter(|target| self.is_pop_consumer(target))
            .collect::<Vec<_>>();
        let mut failed_targets = HashSet::new();

        if self.notify_before_pop_calculate_lag {
            let completions = pop_targets.iter().filter_map(|target| {
                pop_processor
                    .notify_message_arriving_before_lag(&target.topic, &target.consumer_group)
                    .map(|completion| (target.topic_group.clone(), completion))
            });
            failed_targets = await_pop_refreshes(completions.collect(), self.refresh_timeout).await;
        }

        let mut adjustments = ConsumerLagAdjustments::new();
        for target in pop_targets {
            let queue_adjustments = if failed_targets.contains(&target.topic_group) {
                target
                    .queue_offsets
                    .iter()
                    .map(|(queue_id, _)| {
                        (
                            *queue_id,
                            ConsumerLagAdjustment {
                                pull_offset: -1,
                                inflight_messages: 0,
                            },
                        )
                    })
                    .collect::<HashMap<_, _>>()
            } else {
                let mut queue_adjustments = HashMap::with_capacity(target.queue_offsets.len());
                for (queue_id, committed_offset) in &target.queue_offsets {
                    if let Some(adjustment) = pop_processor
                        .consumer_lag_adjustment(&target.topic, &target.consumer_group, *queue_id, *committed_offset)
                        .await
                    {
                        queue_adjustments.insert(*queue_id, adjustment);
                    }
                }
                queue_adjustments
            };
            if !queue_adjustments.is_empty() {
                adjustments.insert(target.topic_group.clone(), queue_adjustments);
            }
        }

        self.current.store(Arc::new(
            self.offsets
                .consumer_lag_snapshot_for_targets_with_adjustments(&targets, &adjustments),
        ));
    }

    fn is_pop_consumer(&self, target: &ConsumerLagTarget) -> bool {
        is_pop_consumer(&self.consumers, target)
    }
}

fn is_pop_consumer(consumers: &ConsumerManager, target: &ConsumerLagTarget) -> bool {
    consumers
        .get_consumer_group_info_internal(&target.consumer_group, true)
        .is_some_and(|info| info.get_consume_type() == ConsumeType::ConsumePop)
}

async fn await_pop_refreshes(
    completions: Vec<(
        CheetahString,
        crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupCompletion,
    )>,
    timeout: Duration,
) -> HashSet<CheetahString> {
    let deadline = Instant::now() + timeout;
    let completions = completions.into_iter().map(|(topic_group, completion)| async move {
        let outcome = tokio::time::timeout_at(deadline, completion).await;
        (topic_group, outcome)
    });
    let mut failed = HashSet::new();
    for (topic_group, outcome) in join_all(completions).await {
        match outcome {
            Ok(Ok(PopWakeupOutcome::ProcessingCompleted)) => {}
            Ok(Ok(other)) => {
                warn!(?other, topic_group = %topic_group, "POP lag refresh wake-up did not complete");
                failed.insert(topic_group);
            }
            Ok(Err(_)) => {
                warn!(topic_group = %topic_group, "POP lag refresh completion sender was dropped");
                failed.insert(topic_group);
            }
            Err(_) => {
                warn!(topic_group = %topic_group, "POP lag refresh timed out");
                failed.insert(topic_group);
            }
        }
    }
    failed
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use super::await_pop_refreshes;
    use super::is_pop_consumer;
    use crate::client::consumer_group_event::ConsumerGroupEvent;
    use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    use crate::client::manager::consumer_manager::ConsumerManager;
    use crate::long_polling::long_polling_service::pop_long_polling_service::PopWakeupOutcome;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;

    struct NoopListener;

    impl ConsumerIdsChangeListener for NoopListener {
        fn handle(&self, _event: ConsumerGroupEvent, _group: &str, _args: &[&dyn Any]) {}

        fn shutdown(&self) {}
    }

    #[test]
    fn pop_refresh_targets_only_pop_consumers() {
        let consumers = ConsumerManager::new(Arc::new(NoopListener), 60_000);
        let pop_group = CheetahString::from_static_str("pop-group");
        let pull_group = CheetahString::from_static_str("pull-group");
        consumers.compensate_basic_consumer_info(&pop_group, ConsumeType::ConsumePop, MessageModel::Clustering);
        consumers.compensate_basic_consumer_info(&pull_group, ConsumeType::ConsumePassively, MessageModel::Clustering);
        let pop_target = crate::offset::manager::consumer_offset_manager::ConsumerLagTarget {
            topic_group: CheetahString::from_static_str("topic-a@pop-group"),
            topic: CheetahString::from_static_str("topic-a"),
            consumer_group: pop_group,
            queue_offsets: vec![(0, 0)],
        };
        let pull_target = crate::offset::manager::consumer_offset_manager::ConsumerLagTarget {
            topic_group: CheetahString::from_static_str("topic-a@pull-group"),
            topic: CheetahString::from_static_str("topic-a"),
            consumer_group: pull_group,
            queue_offsets: vec![(0, 0)],
        };

        assert!(is_pop_consumer(&consumers, &pop_target));
        assert!(!is_pop_consumer(&consumers, &pull_target));
    }

    #[tokio::test]
    async fn pop_refresh_wait_is_bounded_and_marks_non_success_outcomes_failed() {
        let (success_tx, success_rx) = tokio::sync::oneshot::channel();
        success_tx
            .send(PopWakeupOutcome::ProcessingCompleted)
            .expect("send success completion");
        let (failed_tx, failed_rx) = tokio::sync::oneshot::channel();
        failed_tx
            .send(PopWakeupOutcome::ProcessingFailed)
            .expect("send failure completion");
        let (timeout_tx, timeout_rx) = tokio::sync::oneshot::channel();

        let failed = await_pop_refreshes(
            vec![
                (CheetahString::from_static_str("success"), success_rx),
                (CheetahString::from_static_str("failed"), failed_rx),
                (CheetahString::from_static_str("timeout"), timeout_rx),
            ],
            std::time::Duration::ZERO,
        )
        .await;
        drop(timeout_tx);

        assert_eq!(
            failed,
            std::collections::HashSet::from([
                CheetahString::from_static_str("failed"),
                CheetahString::from_static_str("timeout"),
            ])
        );
    }
}
