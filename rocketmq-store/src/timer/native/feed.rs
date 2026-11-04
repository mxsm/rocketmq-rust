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
//!
//! The tokens live in memory. Each checkpoint leaves a mark in the CommitLog, below every
//! delivery the next start may still need, and that start reads the deliveries back from there.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
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

/// File under the store's `config` directory that holds the mark.
const FEED_MARK_FILE: &str = "timer-native-feed.meta";

/// The dispatched messages that carry one token. A client that sends a timer again gives both
/// copies the same token, and each copy needs one message.
struct KeptToken {
    lowest_offset: i64,
    highest_offset: i64,
    messages: u32,
}

/// Delivery tokens of the native timers whose message Reput dispatched while this Broker did
/// not deliver.
#[derive(Default)]
pub(crate) struct NativeDeliveryFeed {
    observing: AtomicBool,
    tokens: Mutex<HashMap<CheetahString, KeptToken>>,
    full: AtomicBool,
}

impl NativeDeliveryFeed {
    /// Starts or stops keeping tokens: a Broker that delivers writes the messages itself.
    pub(crate) fn set_observing(&self, observing: bool) {
        self.observing.store(observing, Ordering::Release);
    }

    /// Tokens kept and not yet taken.
    pub(crate) fn len(&self) -> usize {
        self.tokens.lock().len()
    }

    /// Takes `token` when a message it names was dispatched.
    pub(crate) fn take(&self, token: &CheetahString) -> bool {
        let mut tokens = self.tokens.lock();
        let Some(kept) = tokens.get_mut(token) else {
            return false;
        };
        kept.messages -= 1;
        if kept.messages == 0 {
            tokens.remove(token);
        }
        true
    }

    /// Drops the tokens of what was dispatched from the CommitLog at or past `offset`, which is
    /// being cut off: this Broker's new master has not delivered those timers.
    pub(crate) fn forget_from(&self, offset: i64) {
        self.tokens.lock().retain(|_, kept| {
            if kept.highest_offset >= offset && kept.lowest_offset < offset {
                // Where the copies between them lie is not kept; the lowest is sure to stay.
                kept.highest_offset = kept.lowest_offset;
                kept.messages = 1;
            }
            kept.lowest_offset < offset
        });
    }

    /// The lowest CommitLog offset a kept token came from.
    pub(crate) fn lowest_offset(&self) -> Option<i64> {
        self.tokens.lock().values().map(|kept| kept.lowest_offset).min()
    }

    /// Keeps the token of a message a restart read back from the CommitLog, in whichever role
    /// this Broker comes up: a master finds there what it wrote before it went down.
    pub(crate) fn replay(&self, request: &DispatchRequest) {
        self.keep(request);
    }

    fn record(&self, request: &DispatchRequest) {
        if self.observing.load(Ordering::Acquire) {
            self.keep(request);
        }
    }

    fn keep(&self, request: &DispatchRequest) {
        if !request.success || request.topic.as_str() == TIMER_TOPIC {
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
        let offset = request.commit_log_offset;
        let mut tokens = self.tokens.lock();
        if let Some(kept) = tokens.get_mut(token) {
            // Reput dispatches again what a restart read back; its copies only count once.
            if offset > kept.highest_offset {
                kept.highest_offset = offset;
                kept.messages += 1;
            }
            kept.lowest_offset = kept.lowest_offset.min(offset);
            return;
        }
        if tokens.len() >= FEED_CAPACITY {
            if !self.full.swap(true, Ordering::AcqRel) {
                warn!("the native delivery feed is full; a promotion may deliver native timers again");
            }
            return;
        }
        tokens.insert(
            token.clone(),
            KeptToken {
                lowest_offset: offset,
                highest_offset: offset,
                messages: 1,
            },
        );
    }
}

/// The CommitLog offset a start reads the deliveries back from.
pub(crate) struct NativeFeedMark {
    path: PathBuf,
}

impl NativeFeedMark {
    pub(crate) fn new(store_root: impl AsRef<Path>) -> Self {
        Self {
            path: store_root.as_ref().join("config").join(FEED_MARK_FILE),
        }
    }

    /// The mark an earlier run left, if it left a readable one.
    pub(crate) fn read(&self) -> Option<i64> {
        fs::read_to_string(&self.path).ok()?.trim().parse().ok()
    }

    /// Replaces the mark, durably.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of writing the file; the old mark stays then.
    pub(crate) fn write(&self, offset: i64) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("meta.tmp");
        let mut file = fs::File::create(&temp)?;
        file.write_all(offset.to_string().as_bytes())?;
        file.sync_all()?;
        fs::rename(&temp, &self.path)
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

    fn dispatched_at(offset: i64, token: &str) -> DispatchRequest {
        DispatchRequest {
            commit_log_offset: offset,
            ..dispatched("TopicA", token)
        }
    }

    #[test]
    fn a_cut_drops_the_tokens_of_what_it_cuts_off() {
        let feed = NativeDeliveryFeed::default();
        feed.set_observing(true);
        for (offset, token) in [
            (10, "N:1:kept:0:0"),
            (50, "N:1:cut:0:0"),
            (20, "N:1:both:0:0"),
            (60, "N:1:both:0:0"),
        ] {
            feed.dispatch(&mut dispatched_at(offset, token));
        }

        feed.forget_from(50);

        let token = |token: &'static str| CheetahString::from_static_str(token);
        assert!(!feed.take(&token("N:1:cut:0:0")));
        assert!(feed.take(&token("N:1:kept:0:0")));
        assert!(feed.take(&token("N:1:both:0:0")));
        assert!(!feed.take(&token("N:1:both:0:0")), "only the copy behind the cut stays");
        assert_eq!(feed.len(), 0);
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
