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

use std::sync::Arc;
use std::sync::Weak;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

/// Opaque, low-cost identity for requests that must execute in arrival order.
///
/// Ordering is scoped to one transport session. A hash collision can only
/// serialize additional work; it cannot allow ordered work to overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestOrderingKey(u64);

impl RequestOrderingKey {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Declares whether a request can execute concurrently within its session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RequestOrdering {
    #[default]
    Concurrent,
    Ordered(RequestOrderingKey),
}

#[derive(Clone, Default)]
pub(crate) struct RequestSequencer {
    locks: Arc<DashMap<RequestOrderingKey, Weak<tokio::sync::Mutex<()>>>>,
}

impl RequestSequencer {
    pub(crate) async fn acquire(&self, ordering: RequestOrdering) -> RequestOrderingGuard {
        let RequestOrdering::Ordered(key) = ordering else {
            return RequestOrderingGuard::concurrent();
        };

        let lock = match self.locks.entry(key) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        };
        let guard = lock.clone().lock_owned().await;
        RequestOrderingGuard {
            key: Some(key),
            lock: Some(lock),
            guard: Some(guard),
            locks: Some(self.locks.clone()),
        }
    }

    #[cfg(test)]
    fn retained_key_count(&self) -> usize {
        self.locks.len()
    }
}

pub(crate) struct RequestOrderingGuard {
    key: Option<RequestOrderingKey>,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    locks: Option<Arc<DashMap<RequestOrderingKey, Weak<tokio::sync::Mutex<()>>>>>,
}

impl RequestOrderingGuard {
    fn concurrent() -> Self {
        Self {
            key: None,
            lock: None,
            guard: None,
            locks: None,
        }
    }
}

impl Drop for RequestOrderingGuard {
    fn drop(&mut self) {
        self.guard.take();
        let Some(lock) = self.lock.take() else {
            return;
        };
        let Some(key) = self.key else {
            return;
        };
        let Some(locks) = self.locks.as_ref() else {
            return;
        };
        let owned_lock = Arc::downgrade(&lock);
        drop(lock);
        locks.remove_if(&key, |_, current| {
            Weak::ptr_eq(current, &owned_lock) && current.strong_count() == 0
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    #[tokio::test]
    async fn equal_keys_are_serialized_and_unrelated_keys_are_independent() {
        let sequencer = RequestSequencer::default();
        let key = RequestOrderingKey::new(7);
        let first = sequencer.acquire(RequestOrdering::Ordered(key)).await;

        let same_key_entered = Arc::new(tokio::sync::Notify::new());
        let same_key_sequencer = sequencer.clone();
        let same_key_signal = same_key_entered.clone();
        let same_key = tokio::spawn(async move {
            let _guard = same_key_sequencer.acquire(RequestOrdering::Ordered(key)).await;
            same_key_signal.notify_one();
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), same_key_entered.notified())
                .await
                .is_err()
        );

        let unrelated = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            sequencer.acquire(RequestOrdering::Ordered(RequestOrderingKey::new(8))),
        )
        .await
        .expect("unrelated key must not wait");
        drop(unrelated);

        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), same_key_entered.notified())
            .await
            .expect("same key proceeds after predecessor");
        same_key.await.expect("same-key task");
    }

    #[tokio::test]
    async fn completed_keys_do_not_accumulate_in_the_sequencer() {
        let sequencer = RequestSequencer::default();
        let completed = AtomicUsize::new(0);
        for key in 0..128 {
            let guard = sequencer
                .acquire(RequestOrdering::Ordered(RequestOrderingKey::new(key)))
                .await;
            completed.fetch_add(1, Ordering::Relaxed);
            drop(guard);
        }

        assert_eq!(completed.load(Ordering::Relaxed), 128);
        assert_eq!(sequencer.retained_key_count(), 0);
    }
}
