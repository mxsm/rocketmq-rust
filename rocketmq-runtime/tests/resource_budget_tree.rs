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

use std::future::pending;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::MonotonicClock;
use rocketmq_runtime::PermitRebindOutcome;
use rocketmq_runtime::QueuePushOutcome;
use rocketmq_runtime::QueuePushRejection;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::RuntimeContractViolation;

#[derive(Default)]
struct ManualClock {
    millis: AtomicU64,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        self.millis.fetch_add(
            duration.as_millis().try_into().expect("test duration fits u64"),
            Ordering::AcqRel,
        );
    }
}

impl MonotonicClock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::Acquire))
    }
}

fn limit(count: usize, bytes: usize, policy: FullPolicy) -> BudgetLimit {
    BudgetLimit::new(count, bytes, policy)
}

fn accepted<T>(outcome: QueuePushOutcome<T>) -> QueuePushOutcome<T> {
    assert!(
        !matches!(&outcome, QueuePushOutcome::Rejected { .. }),
        "queue item should be admitted"
    );
    outcome
}

fn rejected<T>(outcome: QueuePushOutcome<T>) -> (T, QueuePushRejection) {
    match outcome {
        QueuePushOutcome::Rejected { item, rejection } => (item, rejection),
        _ => panic!("queue item should be rejected"),
    }
}

#[test]
fn child_permit_reserves_every_ancestor_and_releases_on_drop() {
    let tree = ResourceBudgetTree::new("process", limit(4, 400, FullPolicy::Reject)).expect("root budget");
    let broker = tree
        .root()
        .child("broker", limit(3, 300, FullPolicy::Reject))
        .expect("broker budget");
    let queue = broker
        .child("events", limit(2, 200, FullPolicy::Reject))
        .expect("event budget");

    let permit = queue.try_acquire_data(80).expect("first event");
    assert_eq!(tree.root().snapshot().current_count, 1);
    assert_eq!(broker.snapshot().current_bytes, 80);
    assert_eq!(queue.snapshot().current_count, 1);

    drop(permit);
    assert_eq!(tree.root().snapshot().current_count, 0);
    assert_eq!(broker.snapshot().current_bytes, 0);
    assert_eq!(queue.snapshot().released_count, 1);
}

#[test]
fn deep_child_permit_preserves_ancestors_beyond_inline_storage() {
    let tree = ResourceBudgetTree::new("process", limit(8, 800, FullPolicy::Reject)).expect("root budget");
    let mut levels = vec![tree.root()];
    for index in 0..6 {
        let child = levels
            .last()
            .expect("parent budget")
            .child(format!("level-{index}"), limit(8, 800, FullPolicy::Reject))
            .expect("child budget");
        levels.push(child);
    }

    let permit = levels
        .last()
        .expect("deepest budget")
        .try_acquire_data(64)
        .expect("deep permit");
    assert!(levels.iter().all(|budget| {
        let snapshot = budget.snapshot();
        snapshot.current_count == 1 && snapshot.current_bytes == 64
    }));

    drop(permit);
    assert!(levels.iter().all(|budget| {
        let snapshot = budget.snapshot();
        snapshot.current_count == 0 && snapshot.current_bytes == 0
    }));
}

#[test]
fn parent_budget_bounds_the_sum_of_independent_children() {
    let tree = ResourceBudgetTree::new("process", limit(2, 100, FullPolicy::Reject)).expect("root budget");
    let first = tree
        .root()
        .child("first", limit(2, 100, FullPolicy::Reject))
        .expect("first budget");
    let second = tree
        .root()
        .child("second", limit(2, 100, FullPolicy::Reject))
        .expect("second budget");

    let first_permit = first.try_acquire_data(60).expect("first reservation");
    let error = second
        .try_acquire_data(60)
        .expect_err("root byte limit must cover siblings");
    assert_eq!(error.dimension(), BudgetDimension::Bytes);
    assert_eq!(error.path(), "process/second");
    assert_eq!(error.exhausted_path(), "process");
    assert_eq!(second.snapshot().rejected_count, 1);
    drop(first_permit);
    assert!(second.try_acquire_data(60).is_ok());
}

#[test]
fn rebind_between_siblings_preserves_common_ancestor_accounting() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let source = tree
        .root()
        .child("producer", limit(2, 32, FullPolicy::Reject))
        .expect("source budget");
    let target = tree
        .root()
        .child("transport", limit(2, 32, FullPolicy::Reject))
        .expect("target budget");
    let mut permit = source.try_acquire_data(8).expect("source permit");

    assert_eq!(
        permit.try_rebind(&target).expect("same-tree rebind"),
        PermitRebindOutcome::Rebound
    );

    assert_eq!(tree.root().snapshot().current_count, 1);
    assert_eq!(tree.root().snapshot().current_bytes, 8);
    assert_eq!(source.snapshot().current_count, 0);
    assert_eq!(source.snapshot().current_bytes, 0);
    assert_eq!(target.snapshot().current_count, 1);
    assert_eq!(target.snapshot().current_bytes, 8);

    drop(permit);
    assert_eq!(tree.root().snapshot().current_count, 0);
    assert_eq!(target.snapshot().current_count, 0);
}

#[test]
fn promoting_a_permit_releases_data_reserve_without_releasing_total_capacity() {
    let limit = limit(2, 20, FullPolicy::Reject).with_control_reserve(BudgetCapacity::new(1, 10));
    let tree = ResourceBudgetTree::new("process", limit).expect("root budget");
    let root = tree.root();
    let mut promoted = root.try_acquire_data(10).expect("data permit");
    assert!(root.try_acquire_data(1).is_err());

    promoted.promote_to_control();

    assert_eq!(promoted.class(), BudgetClass::Control);
    let data = root
        .try_acquire_data(10)
        .expect("promoted permit should release data-only capacity");
    assert_eq!(root.snapshot().current_count, 2);
    assert_eq!(root.snapshot().current_bytes, 20);
    drop((promoted, data));
    assert_eq!(root.snapshot().current_count, 0);
    assert_eq!(root.snapshot().current_bytes, 0);
}

#[test]
fn failed_rebind_keeps_the_source_permit_valid() {
    let tree = ResourceBudgetTree::new("process", limit(3, 64, FullPolicy::Reject)).expect("root budget");
    let source = tree
        .root()
        .child("producer", limit(2, 32, FullPolicy::Reject))
        .expect("source budget");
    let target = tree
        .root()
        .child("transport", limit(1, 32, FullPolicy::Reject))
        .expect("target budget");
    let target_owner = target.try_acquire_data(8).expect("fill target");
    let mut source_owner = source.try_acquire_data(8).expect("source permit");

    let outcome = source_owner
        .try_rebind(&target)
        .expect("same-tree rebind must return an outcome");
    assert!(matches!(
        outcome,
        PermitRebindOutcome::Rejected(ref error) if error.dimension() == BudgetDimension::Count
    ));
    assert_eq!(source.snapshot().current_count, 1);
    assert_eq!(source.snapshot().current_bytes, 8);
    assert_eq!(target.snapshot().current_count, 1);
    assert_eq!(tree.root().snapshot().current_count, 2);
    assert_eq!(tree.root().snapshot().current_bytes, 16);

    drop((source_owner, target_owner));
    assert_eq!(tree.root().snapshot().current_count, 0);
    assert_eq!(tree.root().snapshot().current_bytes, 0);
}

#[test]
fn rebind_rejects_a_target_from_another_tree() {
    let source_tree = ResourceBudgetTree::new("source-process", limit(1, 16, FullPolicy::Reject)).expect("source tree");
    let target_tree = ResourceBudgetTree::new("target-process", limit(1, 16, FullPolicy::Reject)).expect("target tree");
    let source = source_tree.root();
    let mut permit = source.try_acquire_data(8).expect("source permit");

    let error = permit
        .try_rebind(&target_tree.root())
        .expect_err("cross-tree rebind must fail");

    assert_eq!(error, RuntimeContractViolation::PermitTargetInDifferentTree);
    assert_eq!(source.snapshot().current_count, 1);
    assert_eq!(source.snapshot().current_bytes, 8);
    drop(permit);
    assert_eq!(source.snapshot().current_count, 0);
}

#[test]
fn control_reserve_survives_data_plane_overload() {
    let limit = limit(3, 30, FullPolicy::Reject).with_control_reserve(BudgetCapacity::new(1, 10));
    let tree = ResourceBudgetTree::new("process", limit).expect("root budget");
    let root = tree.root();

    let first = root.try_acquire_data(10).expect("first data permit");
    let second = root.try_acquire_data(10).expect("second data permit");
    assert!(root.try_acquire_data(1).is_err());
    let control = root.try_acquire_control(10).expect("reserved control capacity");
    assert!(root.try_acquire_control(1).is_err());

    drop((first, second, control));
    assert_eq!(root.snapshot().current_count, 0);
}

#[test]
fn rate_limit_uses_injected_monotonic_time_and_preserves_control_tokens() {
    let clock = Arc::new(ManualClock::default());
    let limit = limit(8, 800, FullPolicy::Reject)
        .with_rate(RateLimit::new(4, 4))
        .with_control_reserve(BudgetCapacity::new(1, 100).with_rate(RateLimit::new(1, 1)));
    let tree = ResourceBudgetTree::with_clock("process", limit, clock.clone()).expect("root budget");
    let root = tree.root();

    let data = (0..3)
        .map(|_| root.try_acquire_data(1).expect("data burst permit"))
        .collect::<Vec<_>>();
    assert_eq!(
        root.try_acquire_data(1)
            .expect_err("data burst must retain one control token")
            .dimension(),
        BudgetDimension::Rate
    );
    let control = root.try_acquire_control(1).expect("control rate reserve");
    assert!(root.try_acquire_control(1).is_err());

    clock.advance(Duration::from_secs(1));
    assert!(root.try_acquire_control(1).is_ok());
    drop((data, control));
}

#[test]
fn child_limits_cannot_escape_parent_hard_limits() {
    let tree = ResourceBudgetTree::new(
        "process",
        limit(4, 400, FullPolicy::Reject)
            .with_rate(RateLimit::new(10, 10))
            .with_max_age(Duration::from_secs(10)),
    )
    .expect("root budget");

    assert!(tree
        .root()
        .child(
            "too-large",
            limit(5, 400, FullPolicy::Reject)
                .with_rate(RateLimit::new(10, 10))
                .with_max_age(Duration::from_secs(10)),
        )
        .is_err());
    assert!(tree
        .root()
        .child(
            "unbounded-rate",
            limit(4, 400, FullPolicy::Reject).with_max_age(Duration::from_secs(10)),
        )
        .is_err());
    assert!(tree
        .root()
        .child(
            "unbounded-age",
            limit(4, 400, FullPolicy::Reject).with_rate(RateLimit::new(10, 10)),
        )
        .is_err());
}

#[test]
fn reject_policy_keeps_depth_and_bytes_bounded_at_two_times_overload() {
    let tree = ResourceBudgetTree::new("process", limit(4, 40, FullPolicy::Reject)).expect("root budget");
    let queue = BudgetedQueue::new(tree.root());
    let mut rejected = Vec::new();

    for item in 0..8 {
        if let QueuePushOutcome::Rejected { item, .. } = queue.try_push_data(item, 10) {
            rejected.push(item);
        }
    }

    assert_eq!(queue.len(), 4);
    assert_eq!(rejected, vec![4, 5, 6, 7]);
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.retained_bytes, 40);
    assert_eq!(snapshot.rejected_count, 4);
}

#[test]
fn coalesce_latest_replaces_pending_state_and_releases_old_permits() {
    let tree = ResourceBudgetTree::new("process", limit(1, 16, FullPolicy::CoalesceLatest)).expect("root budget");
    let queue = BudgetedQueue::new(tree.root());

    assert!(matches!(
        accepted(queue.try_push_data("old", 8)),
        QueuePushOutcome::Enqueued
    ));
    assert!(matches!(
        accepted(queue.try_push_data("new", 8)),
        QueuePushOutcome::Coalesced { replaced: 1 }
    ));
    assert_eq!(queue.try_pop(), Some("new"));
    assert_eq!(queue.snapshot().coalesced_count, 1);
    assert_eq!(queue.snapshot().retained_bytes, 0);
}

#[test]
fn coalesce_latest_preserves_pending_state_when_an_ancestor_is_exhausted() {
    let tree = ResourceBudgetTree::new("process", limit(2, 32, FullPolicy::Reject)).expect("root budget");
    let coalescing = tree
        .root()
        .child("latest-state", limit(2, 32, FullPolicy::CoalesceLatest))
        .expect("coalescing child");
    let sibling = tree
        .root()
        .child("sibling", limit(1, 16, FullPolicy::Reject))
        .expect("sibling child");
    let queue = BudgetedQueue::new(coalescing);

    accepted(queue.try_push_data("pending", 8));
    let _sibling_permit = sibling.try_acquire_data(8).expect("sibling reservation");
    let (item, _rejection) = rejected(queue.try_push_data("replacement", 8));

    assert_eq!(item, "replacement");
    assert_eq!(queue.try_pop(), Some("pending"));
    assert_eq!(queue.snapshot().coalesced_count, 0);
}

#[test]
fn coalesce_latest_preserves_pending_state_when_replacement_cannot_fit() {
    let tree = ResourceBudgetTree::new("coalesce-oversized", limit(2, 16, FullPolicy::CoalesceLatest)).expect("tree");
    let queue = BudgetedQueue::new(tree.root());

    accepted(queue.try_push_data("retained", 8));
    let (_item, rejection) = rejected(queue.try_push_data("oversized", 17));

    assert!(matches!(rejection, QueuePushRejection::BudgetExhausted(_)));
    assert_eq!(queue.try_pop(), Some("retained"));
    assert_eq!(queue.snapshot().coalesced_count, 0);
}

#[test]
fn retain_preserves_order_and_releases_removed_item_permits() {
    let tree = ResourceBudgetTree::new("process", limit(4, 40, FullPolicy::Reject)).expect("root budget");
    let queue = BudgetedQueue::new(tree.root());
    for item in 0..4 {
        accepted(queue.try_push_data(item, 10));
    }

    assert_eq!(queue.retain(|item| item % 2 == 0), 2);
    assert_eq!(queue.try_pop(), Some(0));
    assert_eq!(queue.try_pop(), Some(2));
    assert_eq!(queue.snapshot().retained_bytes, 0);
}

#[test]
fn drop_stale_policy_uses_virtual_time_and_reports_oldest_age() {
    let clock = Arc::new(ManualClock::default());
    let tree = ResourceBudgetTree::with_clock(
        "process",
        limit(1, 16, FullPolicy::DropStale).with_max_age(Duration::from_secs(5)),
        clock.clone(),
    )
    .expect("root budget");
    let queue = BudgetedQueue::new(tree.root());

    accepted(queue.try_push_data("stale", 8));
    clock.advance(Duration::from_secs(6));
    assert!(matches!(
        accepted(queue.try_push_data("fresh", 8)),
        QueuePushOutcome::DroppedStale { dropped: 1 }
    ));
    assert_eq!(queue.try_pop(), Some("fresh"));
    assert_eq!(queue.snapshot().dropped_count, 1);
}

#[test]
fn reject_policy_never_discards_aged_work_silently() {
    let clock = Arc::new(ManualClock::default());
    let tree = ResourceBudgetTree::with_clock(
        "reject-aged-work",
        limit(2, 16, FullPolicy::Reject).with_max_age(Duration::from_secs(1)),
        clock.clone(),
    )
    .expect("tree");
    let queue = BudgetedQueue::new(tree.root());

    accepted(queue.try_push_data("required", 8));
    clock.advance(Duration::from_secs(2));

    assert_eq!(queue.try_pop(), Some("required"));
    assert_eq!(queue.snapshot().dropped_count, 0);
}

#[test]
fn close_slow_consumer_policy_closes_when_oldest_item_exceeds_max_age() {
    let clock = Arc::new(ManualClock::default());
    let tree = ResourceBudgetTree::with_clock(
        "slow-consumer-age",
        limit(2, 16, FullPolicy::CloseSlowConsumer).with_max_age(Duration::from_secs(1)),
        clock.clone(),
    )
    .expect("tree");
    let queue = BudgetedQueue::new(tree.root());

    accepted(queue.try_push_data("stale", 8));
    clock.advance(Duration::from_secs(2));

    assert_eq!(queue.try_pop(), None);
    assert!(queue.is_closed());
    assert_eq!(queue.snapshot().dropped_count, 1);
    assert_eq!(queue.snapshot().closed_slow_consumer_count, 1);
}

#[test]
fn slow_consumer_policy_closes_and_drains_the_queue() {
    let tree = ResourceBudgetTree::new("process", limit(1, 16, FullPolicy::CloseSlowConsumer)).expect("root budget");
    let queue = BudgetedQueue::new(tree.root());

    accepted(queue.try_push_data("first", 8));
    let (item, rejection) = rejected(queue.try_push_data("second", 8));
    assert_eq!(rejection, QueuePushRejection::SlowConsumerClosed);
    assert_eq!(item, "second");
    assert!(queue.is_closed());
    assert!(queue.is_empty());
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.closed_slow_consumer_count, 1);
    assert_eq!(snapshot.dropped_count, 1);
    assert_eq!(snapshot.retained_bytes, 0);
}

#[tokio::test]
async fn aborted_owner_releases_raii_permit() {
    let tree = ResourceBudgetTree::new("process", limit(1, 16, FullPolicy::Reject)).expect("root budget");
    let budget = tree.root();
    let task_budget = budget.clone();
    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _permit = task_budget.try_acquire(8, BudgetClass::Data).expect("task permit");
        acquired_tx.send(()).expect("signal acquisition");
        pending::<()>().await;
    });

    acquired_rx.await.expect("task acquired permit");
    assert_eq!(budget.snapshot().current_count, 1);
    task.abort();
    let _ = task.await;
    assert_eq!(budget.snapshot().current_count, 0);
    assert!(budget.try_acquire_data(8).is_ok());
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_observes_item_capacity_release() {
    let tree = ResourceBudgetTree::new("wait-count", limit(1, 16, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    accepted(queue.try_push_data("held", 1));

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until(
                "waiting",
                1,
                BudgetClass::Data,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    });
    tokio::task::yield_now().await;

    let waiting = queue.snapshot();
    assert_eq!(waiting.waiters, 1);
    assert_eq!(waiting.wait_count, 1);
    assert_eq!(queue.try_pop(), Some("held"));
    assert!(matches!(
        accepted(waiter.await.expect("join waiter")),
        QueuePushOutcome::Enqueued
    ));
    assert_eq!(queue.try_pop(), Some("waiting"));
    assert_eq!(queue.snapshot().reserved_count, 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_observes_byte_capacity_release() {
    let tree = ResourceBudgetTree::new("wait-bytes", limit(2, 8, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    accepted(queue.try_push_data("held", 8));

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until(
                "waiting",
                1,
                BudgetClass::Data,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    });
    tokio::task::yield_now().await;

    assert_eq!(queue.snapshot().waiters, 1);
    assert_eq!(queue.try_pop(), Some("held"));
    accepted(waiter.await.expect("join waiter"));
    assert_eq!(queue.try_pop(), Some("waiting"));
    assert_eq!(queue.snapshot().retained_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_returns_original_item_when_deadline_wins() {
    let tree =
        ResourceBudgetTree::new("wait-deadline", limit(1, 8, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    accepted(queue.try_push_data("held", 8));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until("original", 1, BudgetClass::Data, deadline)
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;

    let (item, rejection) = rejected(waiter.await.expect("join waiter"));
    assert_eq!(rejection, QueuePushRejection::DeadlineExceeded);
    assert_eq!(item, "original");
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.waiters, 0);
    assert_eq!(snapshot.wait_count, 1);
    assert_eq!(snapshot.deadline_exceeded_count, 1);
    assert_eq!(queue.try_pop(), Some("held"));
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_close_wakes_waiter_and_returns_original_item() {
    let tree = ResourceBudgetTree::new("wait-close", limit(1, 8, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    accepted(queue.try_push_data("held", 8));

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until(
                "original",
                1,
                BudgetClass::Data,
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().waiters, 1);
    queue.close();

    let (item, rejection) = rejected(waiter.await.expect("join waiter"));
    assert_eq!(rejection, QueuePushRejection::Closed);
    assert_eq!(item, "original");
    assert_eq!(queue.snapshot().waiters, 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_oversized_item_fails_without_waiting() {
    let tree =
        ResourceBudgetTree::new("wait-oversized", limit(2, 8, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    let before = tokio::time::Instant::now();

    let (item, rejection) = rejected(
        queue
            .push_until("oversized", 9, BudgetClass::Data, before + Duration::from_secs(30))
            .await,
    );

    assert!(matches!(rejection, QueuePushRejection::BudgetExhausted(_)));
    assert_eq!(item, "oversized");
    assert_eq!(tokio::time::Instant::now(), before);
    assert_eq!(queue.snapshot().wait_count, 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_rejects_item_that_cannot_fit_ancestor_data_reserve() {
    let root_limit = limit(2, 8, FullPolicy::Reject).with_control_reserve(BudgetCapacity::new(1, 4));
    let tree = ResourceBudgetTree::new("reserved-root", root_limit).expect("root budget");
    let child = tree
        .root()
        .child("waiting", limit(2, 8, FullPolicy::WaitUntilDeadline))
        .expect("waiting child");
    let queue = BudgetedQueue::new(child);
    let before = tokio::time::Instant::now();

    let (item, rejection) = rejected(
        queue
            .push_until(
                "too-large-for-data",
                5,
                BudgetClass::Data,
                before + Duration::from_secs(30),
            )
            .await,
    );

    match rejection {
        QueuePushRejection::BudgetExhausted(error) => {
            assert_eq!(error.dimension(), BudgetDimension::Bytes);
            assert_eq!(error.exhausted_path(), "reserved-root");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(item, "too-large-for-data");
    assert_eq!(tokio::time::Instant::now(), before);
    assert_eq!(queue.snapshot().wait_count, 0);
}

#[tokio::test(start_paused = true)]
async fn wait_until_deadline_returns_rate_exhaustion_without_capacity_wait() {
    let clock = Arc::new(ManualClock::default());
    let wait_limit = limit(1, 8, FullPolicy::WaitUntilDeadline).with_rate(RateLimit::new(1, 1));
    let tree = ResourceBudgetTree::with_clock("rate-wait", wait_limit, clock).expect("budget tree");
    let queue = BudgetedQueue::new(tree.root());
    accepted(queue.try_push_data("first", 1));
    assert_eq!(queue.try_pop(), Some("first"));
    let before = tokio::time::Instant::now();

    let (item, rejection) = rejected(
        queue
            .push_until("second", 1, BudgetClass::Data, before + Duration::from_secs(30))
            .await,
    );

    match rejection {
        QueuePushRejection::BudgetExhausted(error) => {
            assert_eq!(error.dimension(), BudgetDimension::Rate);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(item, "second");
    assert_eq!(tokio::time::Instant::now(), before);
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.wait_count, 0);
    assert_eq!(snapshot.throttled_count, 1);
    assert_eq!(snapshot.rejected_count, 1);
}

#[tokio::test(start_paused = true)]
async fn ancestor_release_wakes_waiting_child() {
    let tree = ResourceBudgetTree::new("shared", limit(1, 8, FullPolicy::Reject)).expect("root budget");
    let waiting_budget = tree
        .root()
        .child("waiting", limit(1, 8, FullPolicy::WaitUntilDeadline))
        .expect("waiting child");
    let sibling = tree
        .root()
        .child("sibling", limit(1, 8, FullPolicy::Reject))
        .expect("sibling child");
    let sibling_permit = sibling.try_acquire_data(8).expect("fill ancestor from sibling");
    let queue = BudgetedQueue::new(waiting_budget);

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until(
                "child",
                8,
                BudgetClass::Data,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().waiters, 1);
    drop(sibling_permit);

    accepted(waiter.await.expect("join waiter"));
    assert_eq!(queue.try_pop(), Some("child"));
    assert_eq!(tree.root().snapshot().current_count, 0);
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_and_panicking_owner_restore_metrics_and_permits() {
    let tree = ResourceBudgetTree::new("wait-cancel", limit(1, 8, FullPolicy::WaitUntilDeadline)).expect("budget tree");
    let budget = tree.root();
    let queue = BudgetedQueue::new(budget.clone());
    accepted(queue.try_push_data("held", 8));

    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move {
        waiting_queue
            .push_until(
                "cancelled",
                1,
                BudgetClass::Data,
                tokio::time::Instant::now() + Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(queue.snapshot().waiters, 1);
    waiter.abort();
    assert!(waiter.await.expect_err("waiter must be cancelled").is_cancelled());
    assert_eq!(queue.snapshot().waiters, 0);
    assert_eq!(budget.snapshot().current_count, 1);

    let owned = queue.try_pop_budgeted().expect("take owned permit");
    let panicking_owner = tokio::spawn(async move {
        let _owned = owned;
        panic!("injected owner panic");
    });
    assert!(panicking_owner.await.expect_err("owner must panic").is_panic());
    assert_eq!(budget.snapshot().current_count, 0);
    assert_eq!(budget.snapshot().current_bytes, 0);
    assert_eq!(budget.snapshot().admitted_count, budget.snapshot().released_count);
}
