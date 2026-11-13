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

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::oneshot;

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

/// Gives the requests of one ordering key their turns in the order they were claimed.
///
/// A request claims its turn with [`Self::claim`] where requests arrive, in arrival order, and
/// waits for it in its own task with [`OrderingTicket::acquire`]. The turn order is therefore
/// fixed before any request task runs; the order in which the runtime starts those tasks does
/// not matter.
#[derive(Clone, Default)]
pub(crate) struct RequestSequencer {
    keys: Arc<DashMap<RequestOrderingKey, KeyTurns>>,
}

/// The ticket holding one key's turn and the tickets waiting for it, in claim order.
///
/// An entry exists while some ticket holds or waits for the key. Every change happens under the
/// key's map shard lock, so a claim either finds the key free or queues behind a holder that will
/// pass the turn on.
struct KeyTurns {
    holder: u64,
    waiting: VecDeque<(u64, oneshot::Sender<()>)>,
    next_ticket: u64,
}

impl RequestSequencer {
    /// Claims the next turn of `ordering`'s key; call it in arrival order.
    ///
    /// A concurrent request claims nothing. The returned ticket keeps its place until it is
    /// acquired and released, or dropped.
    pub(crate) fn claim(&self, ordering: RequestOrdering) -> OrderingTicket {
        let RequestOrdering::Ordered(key) = ordering else {
            return OrderingTicket {
                state: TicketState::Concurrent,
            };
        };
        match self.keys.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(KeyTurns {
                    holder: 0,
                    waiting: VecDeque::new(),
                    next_ticket: 1,
                });
                OrderingTicket {
                    state: TicketState::Granted(self.turn(key, 0)),
                }
            }
            Entry::Occupied(mut entry) => {
                let turns = entry.get_mut();
                let id = turns.next_ticket;
                turns.next_ticket = turns.next_ticket.wrapping_add(1);
                let (granted, wait) = oneshot::channel();
                turns.waiting.push_back((id, granted));
                OrderingTicket {
                    state: TicketState::Waiting {
                        turn: self.turn(key, id),
                        wait,
                    },
                }
            }
        }
    }

    fn turn(&self, key: RequestOrderingKey, id: u64) -> Turn {
        Turn {
            key,
            id,
            keys: Arc::clone(&self.keys),
        }
    }

    /// Claims and waits for a turn at once, for tests that model one request at a time.
    #[cfg(test)]
    pub(crate) async fn acquire(&self, ordering: RequestOrdering) -> RequestOrderingGuard {
        self.claim(ordering).acquire().await
    }

    #[cfg(test)]
    fn retained_key_count(&self) -> usize {
        self.keys.len()
    }
}

/// A request's claimed place in its key's turn order.
pub(crate) struct OrderingTicket {
    state: TicketState,
}

enum TicketState {
    Concurrent,
    Granted(Turn),
    Waiting { turn: Turn, wait: oneshot::Receiver<()> },
}

impl OrderingTicket {
    /// Waits until every request that claimed the key earlier has released it.
    ///
    /// Dropping the ticket, or this future before it completes, gives up the place: a waiting
    /// ticket leaves the queue and a ticket that was already granted passes the turn on.
    pub(crate) async fn acquire(self) -> RequestOrderingGuard {
        match self.state {
            TicketState::Concurrent => RequestOrderingGuard::concurrent(),
            TicketState::Granted(turn) => RequestOrderingGuard::holding(turn),
            TicketState::Waiting { turn, wait } => {
                // The holder sends before it drops the sender, and only this ticket's own turn
                // removes the sender otherwise, so the wait ends only with the grant.
                let granted = wait.await;
                debug_assert!(granted.is_ok(), "a waiting ordering ticket ends only with its grant");
                RequestOrderingGuard::holding(turn)
            }
        }
    }
}

/// One ticket's turn on a key. Dropping it withdraws a waiting ticket from the queue, or passes a
/// held turn to the next waiting ticket and retires the key when none waits.
struct Turn {
    key: RequestOrderingKey,
    id: u64,
    keys: Arc<DashMap<RequestOrderingKey, KeyTurns>>,
}

impl Drop for Turn {
    fn drop(&mut self) {
        let id = self.id;
        self.keys.remove_if_mut(&self.key, |_, turns| {
            if let Some(position) = turns.waiting.iter().position(|(waiting, _)| *waiting == id) {
                turns.waiting.remove(position);
                return false;
            }
            if turns.holder != id {
                return false;
            }
            while let Some((next, granted)) = turns.waiting.pop_front() {
                turns.holder = next;
                if granted.send(()).is_ok() {
                    return false;
                }
            }
            true
        });
    }
}

pub(crate) struct RequestOrderingGuard {
    /// Held for its drop, which passes the turn to the next same-key request.
    _turn: Option<Turn>,
}

impl RequestOrderingGuard {
    fn concurrent() -> Self {
        Self { _turn: None }
    }

    fn holding(turn: Turn) -> Self {
        Self { _turn: Some(turn) }
    }
}

tokio::task_local! {
    static HELD_ORDERING: Cell<Option<RequestOrderingGuard>>;
}

/// Runs an admitted request while it holds its ordering slot.
///
/// The slot is released when `request` completes or is dropped, or earlier through
/// [`release_request_ordering`].
///
/// It returns the task-local scope future itself: an `async fn` wrapper would hold `request` both
/// as its argument and inside the scope it awaits, doubling the request future.
pub(crate) fn run_ordered<F>(guard: RequestOrderingGuard, request: F) -> impl Future<Output = F::Output>
where
    F: Future,
{
    HELD_ORDERING.scope(Cell::new(Some(guard)), request)
}

/// Ends the ordered part of the request running on the current task.
///
/// The next request with the same ordering key may start once this returns, while the current
/// request keeps running. A handler calls it once the work that must not overlap a later
/// same-key request is complete, for example after a send is appended but before it waits for the
/// disk flush. Calls after the first one, outside a request, or from another task do nothing.
pub fn release_request_ordering() {
    let _ = HELD_ORDERING.try_with(|guard| drop(guard.take()));
}

/// Returns whether the request running on the current task still holds its ordering slot.
#[cfg(any(test, feature = "test-support"))]
pub fn request_ordering_held() -> bool {
    HELD_ORDERING
        .try_with(|guard| {
            let held = guard.take();
            let is_held = held.is_some();
            guard.set(held);
            is_held
        })
        .unwrap_or(false)
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

    #[tokio::test(start_paused = true)]
    async fn released_ordering_admits_the_next_same_key_request_while_the_first_runs() {
        let sequencer = RequestSequencer::default();
        let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(9));
        let released = Arc::new(tokio::sync::Notify::new());
        let finish = Arc::new(tokio::sync::Notify::new());
        let first = tokio::spawn(run_ordered(sequencer.acquire(ordering).await, {
            let released = released.clone();
            let finish = finish.clone();
            async move {
                release_request_ordering();
                released.notify_one();
                finish.notified().await;
            }
        }));
        released.notified().await;

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), sequencer.acquire(ordering))
            .await
            .expect("a released ordering admits the next same-key request");

        assert!(!first.is_finished());
        drop(second);
        finish.notify_one();
        first.await.expect("first request");
    }

    #[tokio::test(start_paused = true)]
    async fn unreleased_ordering_is_held_until_the_request_completes() {
        let sequencer = RequestSequencer::default();
        let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(10));
        let finish = Arc::new(tokio::sync::Notify::new());
        let first = tokio::spawn(run_ordered(sequencer.acquire(ordering).await, {
            let finish = finish.clone();
            async move { finish.notified().await }
        }));

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), sequencer.acquire(ordering))
                .await
                .is_err()
        );
        finish.notify_one();
        first.await.expect("first request");
        drop(sequencer.acquire(ordering).await);
    }

    #[test]
    fn release_outside_a_request_is_a_no_op() {
        release_request_ordering();
    }

    #[tokio::test]
    async fn turns_follow_claim_order_even_when_later_claims_are_polled_first() {
        let sequencer = RequestSequencer::default();
        let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(11));
        let tickets: Vec<_> = (0..4).map(|_| sequencer.claim(ordering)).collect();
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        // Start the waiting tickets newest first, as a runtime may.
        for (index, ticket) in tickets.into_iter().enumerate().rev() {
            let entered = entered_tx.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = ticket.acquire().await;
                entered.send(index).expect("order observer");
                tokio::task::yield_now().await;
            }));
        }
        drop(entered_tx);

        let mut order = Vec::new();
        while let Some(index) = entered_rx.recv().await {
            order.push(index);
        }
        assert_eq!(order, vec![0, 1, 2, 3]);
        for task in tasks {
            task.await.expect("ordered task");
        }
        assert_eq!(sequencer.retained_key_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_ticket_gives_up_its_place_without_letting_a_successor_overtake() {
        let sequencer = RequestSequencer::default();
        let ordering = RequestOrdering::Ordered(RequestOrderingKey::new(12));
        let first = sequencer.claim(ordering);
        let abandoned_waiting = sequencer.claim(ordering);
        let third = sequencer.claim(ordering);

        let held = first.acquire().await;
        drop(abandoned_waiting);
        let mut third = Box::pin(third.acquire());
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut third)
                .await
                .is_err(),
            "the first request still holds the key"
        );
        drop(held);
        let third = tokio::time::timeout(std::time::Duration::from_secs(1), third)
            .await
            .expect("the dropped ticket passes over to the next claim");

        // A ticket granted the turn but never acquired passes it on when dropped.
        let granted_never_acquired = sequencer.claim(ordering);
        drop(third);
        let next = sequencer.claim(ordering);
        drop(granted_never_acquired);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), next.acquire())
                .await
                .expect("an unused grant passes on"),
        );
        assert_eq!(sequencer.retained_key_count(), 0);
    }

    #[tokio::test]
    async fn ordered_request_holds_its_future_once() {
        let sequencer = RequestSequencer::default();
        let guard = sequencer.acquire(RequestOrdering::Concurrent).await;
        let request = async {
            let state = [7_u8; 4096];
            tokio::task::yield_now().await;
            std::hint::black_box(state);
        };
        let request_size = std::mem::size_of_val(&request);

        let ordered = run_ordered(guard, request);

        assert!(
            std::mem::size_of_val(&ordered) < request_size + 256,
            "ordered request is {} bytes for a {request_size}-byte request",
            std::mem::size_of_val(&ordered)
        );
        ordered.await;
    }
}
