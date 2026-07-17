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

//! Deterministic static-topic allocation and epoch policy.

use serde::Deserialize;
use serde::Serialize;

use crate::core::clock::Clock;
use crate::core::error::required;
use crate::core::AdminError;
use crate::core::AdminResult;

const EPOCH_STEP_MILLIS: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticTopicPlanRequest {
    topic: String,
    queue_count: u32,
    target_brokers: Vec<String>,
    existing_epochs: Vec<u64>,
    existing_queue_count: Option<u32>,
}

impl StaticTopicPlanRequest {
    pub fn try_new(
        topic: impl Into<String>,
        queue_count: u32,
        target_brokers: impl IntoIterator<Item = String>,
        existing_epochs: impl IntoIterator<Item = u64>,
        existing_queue_count: Option<u32>,
    ) -> AdminResult<Self> {
        if queue_count == 0 {
            return Err(AdminError::invalid_argument("queueCount", "must be greater than zero"));
        }
        if let Some(existing) = existing_queue_count {
            if queue_count <= existing {
                return Err(AdminError::invalid_argument(
                    "queueCount",
                    format!("must be greater than existing queue count {existing}"),
                ));
            }
        }

        let mut target_brokers = target_brokers
            .into_iter()
            .filter_map(|broker| {
                let broker = broker.trim().to_string();
                (!broker.is_empty()).then_some(broker)
            })
            .collect::<Vec<_>>();
        target_brokers.sort();
        target_brokers.dedup();
        if target_brokers.is_empty() {
            return Err(AdminError::invalid_argument(
                "targetBrokers",
                "at least one broker must be provided",
            ));
        }

        Ok(Self {
            topic: required("topic", topic)?,
            queue_count,
            target_brokers,
            existing_epochs: existing_epochs.into_iter().collect(),
            existing_queue_count,
        })
    }

    pub fn plan(&self, clock: &dyn Clock) -> AdminResult<StaticTopicPlan> {
        self.plan_at(clock.now_millis())
    }

    pub fn plan_at(&self, now_millis: u64) -> AdminResult<StaticTopicPlan> {
        let old_epoch = self.existing_epochs.iter().copied().max().unwrap_or(now_millis);
        let new_epoch = if self.existing_epochs.is_empty() {
            now_millis.checked_add(EPOCH_STEP_MILLIS)
        } else {
            old_epoch
                .checked_add(EPOCH_STEP_MILLIS)
                .map(|next_existing_epoch| next_existing_epoch.max(now_millis))
        }
        .ok_or_else(|| AdminError::invalid_argument("epoch", "epoch overflow"))?;

        let assignments = (0..self.queue_count)
            .map(|global_id| StaticTopicAssignment {
                global_id,
                broker_name: self.target_brokers[global_id as usize % self.target_brokers.len()].clone(),
            })
            .collect();

        Ok(StaticTopicPlan {
            topic: self.topic.clone(),
            old_epoch,
            new_epoch,
            queue_count: self.queue_count,
            target_brokers: self.target_brokers.clone(),
            assignments,
            extends_existing_mapping: self.existing_queue_count.is_some(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticTopicAssignment {
    pub global_id: u32,
    pub broker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticTopicPlan {
    pub topic: String,
    pub old_epoch: u64,
    pub new_epoch: u64,
    pub queue_count: u32,
    pub target_brokers: Vec<String>,
    pub assignments: Vec<StaticTopicAssignment>,
    pub extends_existing_mapping: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    struct CountingClock {
        now: u64,
        calls: AtomicUsize,
    }

    impl Clock for CountingClock {
        fn now_millis(&self) -> u64 {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.now
        }
    }

    #[test]
    fn new_mapping_samples_clock_once_and_is_deterministic() {
        let clock = CountingClock {
            now: 10_000,
            calls: AtomicUsize::new(0),
        };
        let request = StaticTopicPlanRequest::try_new(
            "TopicA",
            5,
            ["broker-b".to_string(), "broker-a".to_string(), "broker-a".to_string()],
            [],
            None,
        )
        .unwrap();

        let plan = request.plan(&clock).unwrap();

        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(plan.old_epoch, 10_000);
        assert_eq!(plan.new_epoch, 11_000);
        assert_eq!(plan.target_brokers, vec!["broker-a", "broker-b"]);
        assert_eq!(
            plan.assignments
                .iter()
                .map(|assignment| assignment.broker_name.as_str())
                .collect::<Vec<_>>(),
            vec!["broker-a", "broker-b", "broker-a", "broker-b", "broker-a"]
        );
    }

    #[test]
    fn existing_mapping_advances_from_largest_epoch() {
        let request =
            StaticTopicPlanRequest::try_new("TopicA", 8, ["broker-a".to_string()], [30_000, 20_000], Some(4)).unwrap();

        let plan = request.plan_at(25_000).unwrap();

        assert_eq!(plan.old_epoch, 30_000);
        assert_eq!(plan.new_epoch, 31_000);
        assert!(plan.extends_existing_mapping);
    }

    #[test]
    fn existing_mapping_rejects_non_increasing_queue_count() {
        let error =
            StaticTopicPlanRequest::try_new("TopicA", 4, ["broker-a".to_string()], [30_000], Some(4)).unwrap_err();

        assert!(error.to_string().contains("greater than existing queue count"));
    }

    #[test]
    fn epoch_overflow_is_reported() {
        let request = StaticTopicPlanRequest::try_new("TopicA", 1, ["broker-a".to_string()], [], None).unwrap();

        assert!(request.plan_at(u64::MAX).is_err());
    }
}
