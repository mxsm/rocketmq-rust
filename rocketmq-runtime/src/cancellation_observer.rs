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

//! Lock-free checks and waits on a [`CancellationToken`].

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::WaitForCancellationFutureOwned;

/// Waiter lists per observer. Each thread uses one of them, so waits that start on different
/// worker threads do not share a lock.
const WAITER_SHARDS: usize = 8;

/// Observes one [`CancellationToken`] without taking its lock.
///
/// `CancellationToken::is_cancelled` locks the token's tree node, and every wait on the token
/// locks it on each poll. When many tasks on different threads check one token, such as all
/// requests of a connection, that lock moves between cores on every check. An observer reads an
/// atomic flag instead, and its waits register in per-thread waiter lists.
///
/// The flag is set while the token is being cancelled: the observer registers a waker with the
/// token, which [`CancellationToken::cancel`] calls before it returns, also when the cancellation
/// comes from a parent token. A thread that knows a cancellation has returned therefore sees
/// [`is_cancelled`](Self::is_cancelled) return `true`, as it would on the token itself.
///
/// Cloning an observer shares its flag and waiter lists.
#[derive(Clone)]
pub struct CancellationObserver {
    inner: Arc<ObserverInner>,
}

struct ObserverInner {
    cancelled: AtomicBool,
    token: CancellationToken,
    /// Registered with `token` and polled with a [`BridgeWaker`] until it completes.
    bridge: parking_lot::Mutex<Pin<Box<WaitForCancellationFutureOwned>>>,
    waiters: [WaiterShard; WAITER_SHARDS],
}

/// Keeps each waiter list on its own cache lines.
#[repr(align(128))]
struct WaiterShard(Notify);

impl CancellationObserver {
    /// Creates an observer of `token`.
    pub fn new(token: CancellationToken) -> Self {
        let inner = Arc::new(ObserverInner {
            cancelled: AtomicBool::new(false),
            bridge: parking_lot::Mutex::new(Box::pin(token.clone().cancelled_owned())),
            token,
            waiters: std::array::from_fn(|_| WaiterShard(Notify::new())),
        });
        let waker = Waker::from(Arc::new(BridgeWaker {
            observer: Arc::downgrade(&inner),
        }));
        inner.poll_bridge(&waker);
        Self { inner }
    }

    /// Returns whether the token is cancelled, with one atomic load.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Waits until the token is cancelled.
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + '_ {
        let inner = &*self.inner;
        async move {
            loop {
                if inner.cancelled.load(Ordering::Acquire) {
                    return;
                }
                let notified = inner.waiters[current_shard()].0.notified();
                // A cancellation that sets the flag after this load notifies the shard after
                // `notified` was created, which completes it.
                if inner.cancelled.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }
    }

    /// Returns the observed token.
    pub fn token(&self) -> &CancellationToken {
        &self.inner.token
    }
}

impl fmt::Debug for CancellationObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationObserver")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl ObserverInner {
    /// Polls the token's cancellation future and sets the flag once it completes.
    ///
    /// Called on creation and from the token's cancellation, which tokio-util runs after it
    /// released the token's locks, so polling the token here cannot deadlock.
    fn poll_bridge(&self, waker: &Waker) {
        let mut bridge = self.bridge.lock();
        // Set under the lock, so a completed future is not polled again.
        if self.cancelled.load(Ordering::Relaxed) || bridge.as_mut().poll(&mut Context::from_waker(waker)).is_pending()
        {
            return;
        }
        // Stored before the waiters are notified: a waiter that misses the flag created its
        // `Notified` before these notifications.
        self.cancelled.store(true, Ordering::Release);
        drop(bridge);
        for shard in &self.waiters {
            shard.0.notify_waiters();
        }
    }
}

/// The waker registered with the observed token. It holds the observer weakly, so a token that
/// is never cancelled does not keep a dropped observer alive.
struct BridgeWaker {
    observer: Weak<ObserverInner>,
}

impl Wake for BridgeWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(observer) = self.observer.upgrade() {
            observer.poll_bridge(&Waker::from(Arc::clone(self)));
        }
    }
}

/// Returns this thread's waiter shard.
fn current_shard() -> usize {
    static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % WAITER_SHARDS;
    }
    SHARD.with(|shard| *shard)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_new_observer_of_a_cancelled_token_is_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(CancellationObserver::new(token).is_cancelled());
    }

    #[test]
    fn cancelling_the_token_sets_the_flag_before_cancel_returns() {
        let token = CancellationToken::new();
        let observer = CancellationObserver::new(token.clone());
        assert!(!observer.is_cancelled());
        token.cancel();
        assert!(observer.is_cancelled());
    }

    #[test]
    fn cancelling_a_parent_sets_the_flag_of_an_observed_child_before_cancel_returns() {
        let parent = CancellationToken::new();
        let observer = CancellationObserver::new(parent.child_token().child_token());
        assert!(!observer.is_cancelled());
        parent.cancel();
        assert!(observer.is_cancelled());
        assert!(observer.token().is_cancelled());
    }

    #[test]
    fn cancelling_a_child_does_not_cancel_an_observed_parent() {
        let parent = CancellationToken::new();
        let observer = CancellationObserver::new(parent.clone());
        parent.child_token().cancel();
        assert!(!observer.is_cancelled());
    }

    #[test]
    fn a_dropped_observer_is_freed_while_its_token_stays_alive() {
        let token = CancellationToken::new();
        let observer = CancellationObserver::new(token.clone());
        let weak = Arc::downgrade(&observer.inner);
        drop(observer);
        assert!(weak.upgrade().is_none());
        // Cancelling after the observer is gone wakes no one and does not panic.
        token.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_waiter_wakes_when_the_token_is_cancelled() {
        let token = CancellationToken::new();
        let observer = CancellationObserver::new(token.clone());
        let waiters: Vec<_> = (0..64)
            .map(|_| {
                let observer = observer.clone();
                tokio::spawn(async move { observer.cancelled().await })
            })
            .collect();
        tokio::task::yield_now().await;
        token.cancel();
        for waiter in waiters {
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("the waiter wakes")
                .unwrap();
        }
        observer.cancelled().await;
    }

    #[test]
    fn waits_that_race_the_cancellation_all_complete() {
        // Repeats the race between a waiter that has not yet registered and the cancellation.
        for _ in 0..200 {
            let token = CancellationToken::new();
            let observer = CancellationObserver::new(token.clone());
            let waiter = std::thread::spawn({
                let observer = observer.clone();
                move || futures::executor::block_on(observer.cancelled())
            });
            token.cancel();
            waiter.join().unwrap();
            assert!(observer.is_cancelled());
        }
    }
}
