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

//! What a slave learns of its master's native deliveries.
//!
//! A delivered native timer's message carries the delivery token the Broker gave the timer.
//! While this Broker does not deliver, Reput hands every dispatched message here, and the
//! tokens of native deliveries are kept. The slave then takes its due timers in each lane's
//! order as the master does, and counts one delivered only once its token is here, so its
//! watermark never passes a timer the master did not write. A slave promoted to master also
//! finds here the timers its old master already wrote.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::message::MessageConst;
use rocketmq_store_api::TimerEngineId;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::timer::timer_message_store::TIMER_TOPIC;

/// Tokens kept at most. Past it a slave falls behind its master, and a promotion writes again
/// what the slave did not match.
const FEED_CAPACITY: usize = 1 << 20;

/// Delivery tokens of the native timers whose message Reput dispatched while this Broker did
/// not deliver.
#[derive(Default)]
pub(crate) struct NativeDeliveryFeed {
    observing: AtomicBool,
    tokens: Mutex<HashSet<CheetahString>>,
    full: AtomicBool,
}

impl NativeDeliveryFeed {
    /// Starts or stops keeping tokens: a Broker that delivers writes the messages itself.
    pub(crate) fn set_observing(&self, observing: bool) {
        self.observing.store(observing, Ordering::Release);
    }

    /// Takes `token` when the message it names was dispatched.
    pub(crate) fn take(&self, token: &CheetahString) -> bool {
        self.tokens.lock().remove(token)
    }

    fn record(&self, request: &DispatchRequest) {
        if !self.observing.load(Ordering::Acquire) || !request.success || request.topic.as_str() == TIMER_TOPIC {
            // A timer in the Timer topic carries the same token as the message it becomes.
            return;
        }
        let Some(token) = request
            .properties_map
            .as_ref()
            .and_then(|properties| properties.get(MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN))
        else {
            return;
        };
        if !is_native_token(token) {
            return;
        }
        let mut tokens = self.tokens.lock();
        if tokens.len() >= FEED_CAPACITY {
            if !self.full.swap(true, Ordering::AcqRel) {
                warn!("the native delivery feed is full; a promotion may deliver native timers again");
            }
            return;
        }
        tokens.insert(token.clone());
    }
}

/// Whether `token` is one the Broker gave a native timer: `N:<format>:...`.
fn is_native_token(token: &str) -> bool {
    token
        .split_once(':')
        .is_some_and(|(engine, _)| engine == TimerEngineId::Native.as_str())
}

impl CommitLogDispatcher for NativeDeliveryFeed {
    fn supports_parallel_dispatch(&self) -> bool {
        true
    }

    fn dispatch(&self, request: &mut DispatchRequest) {
        self.record(request);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn dispatched(topic: &str, token: &str) -> DispatchRequest {
        DispatchRequest {
            topic: CheetahString::from_slice(topic),
            success: true,
            properties_map: Some(HashMap::from([(
                CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN),
                CheetahString::from_slice(token),
            )])),
            ..DispatchRequest::default()
        }
    }

    #[test]
    fn only_native_deliveries_seen_while_observing_are_kept() {
        let feed = NativeDeliveryFeed::default();
        feed.dispatch(&mut dispatched("TopicA", "N:1:early:0:0"));
        feed.set_observing(true);
        for (topic, token) in [
            ("TopicA", "N:1:k0:0:0"),
            ("TopicA", "F:1:k1:0:0"),
            (TIMER_TOPIC, "N:1:k2:0:0"),
        ] {
            feed.dispatch(&mut dispatched(topic, token));
        }
        let token = |token: &'static str| CheetahString::from_static_str(token);
        assert!(!feed.take(&token("N:1:early:0:0")));
        assert!(feed.take(&token("N:1:k0:0:0")));
        assert!(!feed.take(&token("N:1:k0:0:0")), "a token is taken once");
        assert!(!feed.take(&token("F:1:k1:0:0")));
        assert!(!feed.take(&token("N:1:k2:0:0")));
    }
}
