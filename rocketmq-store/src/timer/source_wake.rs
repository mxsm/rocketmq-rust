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

use std::sync::Arc;
use std::sync::Weak;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Wakes the timer source stages once Reput has dispatched Timer topic records.
///
/// Reput calls [`TimerSourceWake::wake`] after every dispatcher has finished a batch that holds
/// a Timer topic record, so the records are in the Timer consume queue when a stage reads it. A
/// wake only shortens a stage's wait: each stage still runs on its own interval, and wakes that
/// arrive while a stage is busy coalesce into one.
#[derive(Debug, Default)]
pub(crate) struct TimerSourceWake {
    subscribers: Mutex<Vec<Weak<Notify>>>,
}

impl TimerSourceWake {
    /// Returns the `Notify` one stage waits on. Dropping it unsubscribes the stage.
    pub(crate) fn subscribe(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let mut subscribers = self.subscribers.lock();
        subscribers.retain(|subscriber| subscriber.strong_count() > 0);
        subscribers.push(Arc::downgrade(&notify));
        notify
    }

    /// Wakes every subscribed stage. A stage that is not waiting finds the wake when it next
    /// waits.
    pub(crate) fn wake(&self) {
        for subscriber in self.subscribers.lock().iter().filter_map(Weak::upgrade) {
            subscriber.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;

    use super::TimerSourceWake;

    #[test]
    fn a_wake_reaches_each_subscriber_once_and_skips_dropped_ones() {
        let wake = TimerSourceWake::default();
        let first = wake.subscribe();
        let dropped = wake.subscribe();
        drop(dropped);
        let second = wake.subscribe();
        assert!(first.notified().now_or_never().is_none());

        wake.wake();
        wake.wake();
        assert!(first.notified().now_or_never().is_some());
        assert!(second.notified().now_or_never().is_some());
        // Wakes that arrived before the stage waited coalesce into one.
        assert!(first.notified().now_or_never().is_none());
        assert_eq!(wake.subscribers.lock().len(), 2);
    }
}
