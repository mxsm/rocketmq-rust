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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use thiserror::Error;

#[derive(Clone, Copy, Debug)]
pub(crate) enum EscapeTargetPreference<'a> {
    Any,
    Broker(&'a CheetahString),
    Stable(u64),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum EscapeTargetError {
    #[error("topic has no writable remote route")]
    RouteUnavailable,
    #[error("topic route contains only the local broker")]
    SelfOnlyRoute,
    #[error("the requested escape broker is the local broker")]
    SelfTarget,
    #[error("the requested escape broker is unavailable")]
    RequestedBrokerUnavailable,
}

#[derive(Debug, Default)]
pub(crate) struct EscapeTargetResolver {
    next_queue: AtomicUsize,
}

impl EscapeTargetResolver {
    pub(crate) fn resolve(
        &self,
        queues: &[MessageQueue],
        local_broker: &CheetahString,
        preference: EscapeTargetPreference<'_>,
    ) -> Result<MessageQueue, EscapeTargetError> {
        if queues.is_empty() {
            return Err(EscapeTargetError::RouteUnavailable);
        }
        if matches!(preference, EscapeTargetPreference::Broker(broker) if broker == local_broker) {
            return Err(EscapeTargetError::SelfTarget);
        }

        let remote_queues = queues
            .iter()
            .filter(|queue| queue.broker_name() != local_broker)
            .collect::<Vec<_>>();
        if remote_queues.is_empty() {
            return Err(EscapeTargetError::SelfOnlyRoute);
        }

        let selected = match preference {
            EscapeTargetPreference::Any => {
                let index = self.next_queue.fetch_add(1, Ordering::Relaxed) % remote_queues.len();
                remote_queues[index]
            }
            EscapeTargetPreference::Broker(broker) => remote_queues
                .into_iter()
                .find(|queue| queue.broker_name() == broker)
                .ok_or(EscapeTargetError::RequestedBrokerUnavailable)?,
            EscapeTargetPreference::Stable(key) => remote_queues[(key % remote_queues.len() as u64) as usize],
        };
        Ok(selected.clone())
    }
}
