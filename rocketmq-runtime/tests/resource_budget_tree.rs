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
use rocketmq_runtime::BudgetRejectionReason;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::DynamicKeyAdmissionRejection;
use rocketmq_runtime::DynamicKeyRegistrationFailure;
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
    assert_eq!(error.dimension(), Some(BudgetDimension::Bytes));
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
        PermitRebindOutcome::Rejected(ref error) if error.dimension() == Some(BudgetDimension::Count)
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
        Some(BudgetDimension::Rate)
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
            assert_eq!(error.dimension(), Some(BudgetDimension::Bytes));
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
            assert_eq!(error.dimension(), Some(BudgetDimension::Rate));
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

#[tokio::test]
async fn retiring_a_dynamic_key_waits_for_the_real_reservation() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let key = tree
        .root()
        .register_dynamic_child("ordering-key", limit(2, 32, FullPolicy::Reject))
        .expect("dynamic key");
    let permit = key.try_acquire(8, BudgetClass::Data).expect("key permit");
    let budget = key.budget();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let key_clone = key.clone();
    let retirement = tokio::spawn(async move { key_clone.retire_until(deadline).await });
    // Let the retirement observe the live reservation and park on the release
    // notification instead of racing the drop below.
    tokio::task::yield_now().await;
    assert!(
        !retirement.is_finished(),
        "a closed handle is not proof that the work stopped, so retirement must still be waiting"
    );

    drop(permit);
    let outcome = retirement.await.expect("retirement task");
    assert!(
        outcome.released,
        "retirement must be woken by the real release instead of assuming the closed handle stopped the work"
    );
    assert_eq!(outcome.outstanding_reservations, 0);
    assert_eq!(budget.snapshot().current_count, 0);
    assert!(key.is_closed());
}

#[tokio::test]
async fn a_retired_key_reports_outstanding_work_and_keeps_its_name_reserved() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let key = tree
        .root()
        .register_dynamic_child("ordering-key", limit(2, 32, FullPolicy::Reject))
        .expect("dynamic key");
    let permit = key.try_acquire(8, BudgetClass::Data).expect("key permit");

    // An elapsed deadline reports the real remaining work instead of releasing
    // the registration.
    let outstanding = key.retire_until(tokio::time::Instant::now()).await;
    assert!(!outstanding.released);
    assert_eq!(outstanding.outstanding_reservations, 1);
    assert_eq!(outstanding.outstanding_bytes, 8);

    // The name stays reserved, so the next generation cannot run beside the one
    // it would replace.
    assert!(matches!(
        tree.root()
            .register_dynamic_child("ordering-key", limit(2, 32, FullPolicy::Reject)),
        Err(DynamicKeyRegistrationFailure::NameInUse { .. })
    ));

    drop(permit);
    let retired = key.retire_until(tokio::time::Instant::now()).await;
    assert!(retired.released);
    assert_eq!(retired.outstanding_reservations, 0);

    // The name is free again, and the replacement is a new identity.
    let replacement = tree
        .root()
        .register_dynamic_child("ordering-key", limit(2, 32, FullPolicy::Reject))
        .expect("the released name should be reusable");
    assert_ne!(replacement.generation(), key.generation());
}

#[tokio::test]
async fn a_closed_dynamic_key_refuses_new_admissions() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let key = tree
        .root()
        .register_dynamic_child("ordering-key", limit(2, 32, FullPolicy::Reject))
        .expect("dynamic key");
    assert!(!key.is_closed());
    assert!(key.try_acquire(8, BudgetClass::Data).is_ok());

    key.close();
    assert!(key.is_closed());
    assert!(matches!(
        key.try_acquire(8, BudgetClass::Data),
        Err(DynamicKeyAdmissionRejection::Closed)
    ));
}

#[tokio::test]
async fn the_dynamic_key_registry_is_bounded() {
    let tree = ResourceBudgetTree::with_clock_and_key_capacity(
        "process",
        limit(4, 64, FullPolicy::Reject),
        Arc::new(ManualClock::default()),
        1,
    )
    .expect("root budget");
    let first = tree
        .root()
        .register_dynamic_child("first", limit(1, 16, FullPolicy::Reject))
        .expect("first key");

    assert!(matches!(
        tree.root()
            .register_dynamic_child("second", limit(1, 16, FullPolicy::Reject)),
        Err(DynamicKeyRegistrationFailure::CapacityExhausted { max_entries: 1 })
    ));

    // Retiring the first key releases its slot instead of leaving an
    // unreachable registration behind.
    assert!(first.retire_until(tokio::time::Instant::now()).await.released);
    assert!(tree
        .root()
        .register_dynamic_child("second", limit(1, 16, FullPolicy::Reject))
        .is_ok());
}

#[tokio::test]
async fn retired_generation_closes_escaped_budgets_descendants_and_rebinds() {
    let tree = ResourceBudgetTree::new("process", limit(16, 160, FullPolicy::Reject)).unwrap();
    let root = tree.root();
    let key = root
        .register_dynamic_child("key", limit(8, 80, FullPolicy::Reject))
        .unwrap();
    let old = key.budget();
    let left = old.child("left", limit(4, 40, FullPolicy::Reject)).unwrap();
    let right = old.child("right", limit(4, 40, FullPolicy::Reject)).unwrap();
    let mut same = old.try_acquire_control(8).unwrap();
    let mut sibling = left.try_acquire_data(8).unwrap();
    key.close();
    for budget in [&old, &left, &right] {
        for class in [BudgetClass::Data, BudgetClass::Control] {
            let rejection = budget.try_acquire(1, class).unwrap_err();
            assert_eq!(rejection.reason(), BudgetRejectionReason::Closed);
            assert_eq!(rejection.dimension(), None);
            assert_eq!(rejection.exhausted_path(), old.path());
        }
    }
    for result in [same.try_rebind(&old).unwrap(), sibling.try_rebind(&right).unwrap()] {
        assert!(matches!(result, PermitRebindOutcome::Rejected(error) if error.is_closed()));
    }
    assert_eq!(root.snapshot().current_count, 2);
    assert_eq!(old.snapshot().current_bytes, 16);
    assert!(matches!(
        old.register_dynamic_child("nested", limit(1, 1, FullPolicy::Reject)),
        Err(DynamicKeyRegistrationFailure::Closed)
    ));
    assert!(old
        .child("closed-child", limit(1, 1, FullPolicy::Reject))
        .unwrap()
        .try_acquire_data(1)
        .unwrap_err()
        .is_closed());
    assert!(!key.retire_until(tokio::time::Instant::now()).await.released);
    // Migration out preserves the root reservation while draining the old key.
    assert_eq!(same.try_rebind(&root).unwrap(), PermitRebindOutcome::Rebound);
    drop(sibling);
    assert!(key.retire_until(tokio::time::Instant::now()).await.released);
    let next = root
        .register_dynamic_child("key", limit(8, 80, FullPolicy::Reject))
        .unwrap();
    let next_permit = next.try_acquire(8, BudgetClass::Data).unwrap();
    assert!(!key.retire_until(tokio::time::Instant::now()).await.released);
    assert!(old.try_acquire_control(8).unwrap_err().is_closed());
    assert_eq!(root.snapshot().current_count, 2);
    drop((same, next_permit));
    assert!(next.retire_until(tokio::time::Instant::now()).await.released);
    let snapshot = root.snapshot();
    assert_eq!(snapshot.current_count, 0);
    assert_eq!(snapshot.admitted_count, snapshot.released_count);
}

#[test]
fn closure_does_not_run_destructive_queue_policies_or_retain_rejected_permits() {
    for policy in [
        FullPolicy::Reject,
        FullPolicy::WaitUntilDeadline,
        FullPolicy::CoalesceLatest,
        FullPolicy::DropStale,
        FullPolicy::CloseSlowConsumer,
    ] {
        let clock = Arc::new(ManualClock::default());
        let tree = ResourceBudgetTree::with_clock("process", limit(8, 80, FullPolicy::Reject), clock.clone()).unwrap();
        let key = tree
            .root()
            .register_dynamic_child("key", limit(2, 20, policy).with_max_age(Duration::from_secs(1)))
            .unwrap();
        let queue = BudgetedQueue::new(key.budget());
        accepted(queue.try_push_data("accepted", 8));
        let permit = key.budget().try_acquire_data(8).unwrap();
        key.close();
        clock.advance(Duration::from_secs(2));
        assert_eq!(
            rejected(queue.try_push_data("rejected", 8)),
            ("rejected", QueuePushRejection::Closed)
        );
        assert_eq!(
            rejected(queue.try_push_budgeted("budgeted", permit).unwrap()),
            ("budgeted", QueuePushRejection::Closed)
        );
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.depth, 1);
        assert_eq!(snapshot.reserved_count, 1);
        assert_eq!(
            snapshot.dropped_count + snapshot.coalesced_count + snapshot.closed_slow_consumer_count,
            0
        );
        let foreign = ResourceBudgetTree::new("foreign", limit(1, 8, FullPolicy::Reject)).unwrap();
        let rejected = queue
            .try_push_budgeted("foreign", foreign.root().try_acquire_data(8).unwrap())
            .unwrap_err();
        assert_eq!(rejected.item, "foreign");
        assert_eq!(foreign.root().snapshot().current_bytes, 8);
        drop(rejected);
        drop(queue);
        assert_eq!(tree.root().snapshot().current_count, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn dynamic_close_wakes_queue_waiters_and_receivers_without_deadline_expiry() {
    let tree = ResourceBudgetTree::new("process", limit(4, 32, FullPolicy::Reject)).unwrap();
    let key = tree
        .root()
        .register_dynamic_child("key", limit(1, 8, FullPolicy::WaitUntilDeadline))
        .unwrap();
    let queue = BudgetedQueue::new(key.budget());
    accepted(queue.try_push_data("first", 8));
    let waiting = queue.push_until(
        "waiting",
        8,
        BudgetClass::Data,
        tokio::time::Instant::now() + Duration::from_secs(100),
    );
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    assert_eq!(queue.snapshot().waiters, 1);
    key.close();
    assert_eq!(rejected(waiting.await), ("waiting", QueuePushRejection::Closed));
    assert_eq!(queue.snapshot().deadline_exceeded_count, 0);
    assert_eq!(queue.recv().await, Some("first"));
    assert_eq!(queue.recv().await, None);

    let other = tree
        .root()
        .register_dynamic_child("other", limit(1, 8, FullPolicy::Reject))
        .unwrap();
    let empty = BudgetedQueue::<()>::new(other.budget());
    let receiving = empty.recv();
    tokio::pin!(receiving);
    assert!(futures::poll!(&mut receiving).is_pending());
    other.close();
    assert_eq!(receiving.await, None);
}

#[tokio::test]
async fn concurrent_close_and_escaped_acquisition_cannot_escape_retirement_accounting() {
    let tree = ResourceBudgetTree::new("process", limit(4, 32, FullPolicy::Reject)).unwrap();
    for _ in 0..64 {
        let key = tree
            .root()
            .register_dynamic_child("key", limit(2, 16, FullPolicy::Reject))
            .unwrap();
        let escaped = key.budget();
        let barrier = std::sync::Barrier::new(2);
        let acquired = std::thread::scope(|scope| {
            let acquiring = scope.spawn(|| {
                barrier.wait();
                escaped.try_acquire_data(8)
            });
            barrier.wait();
            key.close();
            acquiring.join().unwrap()
        });
        let before_drop = key.retire_until(tokio::time::Instant::now()).await;
        assert_eq!(before_drop.outstanding_reservations, usize::from(acquired.is_ok()));
        assert_eq!(before_drop.released, acquired.is_err());
        assert!(escaped.try_acquire_data(1).unwrap_err().is_closed());
        drop(acquired);
        if !before_drop.released {
            assert!(key.retire_until(tokio::time::Instant::now()).await.released);
        }
        assert_eq!(tree.root().snapshot().current_count, 0);
    }
}

#[test]
fn coalesced_item_destructors_can_close_the_dynamic_key_after_admission_unlocks() {
    struct CloseOnDrop(Option<rocketmq_runtime::DynamicBudgetKey>);
    impl Drop for CloseOnDrop {
        fn drop(&mut self) {
            if let Some(key) = self.0.take() {
                key.close();
            }
        }
    }
    let (finished, result) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let root = ResourceBudgetTree::new("root", limit(2, 16, FullPolicy::Reject))
            .unwrap()
            .root();
        let key = root
            .register_dynamic_child("key", limit(1, 8, FullPolicy::CoalesceLatest))
            .unwrap();
        let queue = BudgetedQueue::new(key.budget());
        accepted(queue.try_push_data(CloseOnDrop(Some(key.clone())), 8));
        assert!(matches!(
            queue.try_push_data(CloseOnDrop(None), 8),
            QueuePushOutcome::Coalesced { replaced: 1 }
        ));
        assert!(key.is_closed());
        assert_eq!(queue.len(), 1);
        drop(queue);
        assert_eq!(root.snapshot().current_count, 0);
        finished.send(()).unwrap();
    });
    result
        .recv_timeout(Duration::from_secs(5))
        .expect("item destructor must not run under an admission gate");
    worker.join().unwrap();
}

#[test]
fn budgeted_push_charges_the_shared_ancestor_exactly_once() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let component = tree
        .root()
        .child("component", limit(4, 32, FullPolicy::Reject))
        .expect("component budget");
    let queue = BudgetedQueue::new(component.clone());

    // The caller admits the payload at its own boundary before the item exists
    // in the queue.
    let permit = component.try_acquire_data(12).expect("ingress permit");
    queue
        .try_push_budgeted("payload", permit)
        .expect("same-tree push should not report a foreign permit");

    assert_eq!(tree.root().snapshot().current_bytes, 12);
    assert_eq!(component.snapshot().current_bytes, 12);
    assert_eq!(queue.snapshot().retained_bytes, 12);

    // The pop path carries the same charge, so consuming the item releases it
    // once rather than twice.
    let owned = queue.try_pop_budgeted().expect("owned item");
    assert_eq!(owned.retained_bytes(), 12);
    assert_eq!(tree.root().snapshot().current_bytes, 12);
    drop(owned);
    assert_eq!(tree.root().snapshot().current_bytes, 0);
    assert_eq!(component.snapshot().current_bytes, 0);
}

#[test]
fn budgeted_push_rebinds_a_permit_acquired_from_an_ancestor() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let component = tree
        .root()
        .child("component", limit(4, 32, FullPolicy::Reject))
        .expect("component budget");
    let queue = BudgetedQueue::new(component.clone());

    let permit = tree.root().try_acquire_data(9).expect("ancestor permit");
    queue
        .try_push_budgeted("payload", permit)
        .expect("an ancestor permit belongs to the same tree");

    // The reservation now covers the component as well, and the shared root is
    // still charged once.
    assert_eq!(tree.root().snapshot().current_bytes, 9);
    assert_eq!(component.snapshot().current_bytes, 9);

    drop(queue.try_pop_budgeted().expect("owned item"));
    assert_eq!(tree.root().snapshot().current_bytes, 0);
    assert_eq!(component.snapshot().current_bytes, 0);
}

#[test]
fn budgeted_push_returns_a_foreign_permit_unchanged() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let component = tree
        .root()
        .child("component", limit(4, 32, FullPolicy::Reject))
        .expect("component budget");
    let queue = BudgetedQueue::new(component);

    let other = ResourceBudgetTree::new("other", limit(4, 64, FullPolicy::Reject)).expect("other budget");
    let foreign = other.root().try_acquire_data(7).expect("foreign permit");

    let returned = queue
        .try_push_budgeted("payload", foreign)
        .expect_err("a permit from another tree must be reported");
    assert_eq!(returned.item, "payload");
    // The caller still owns the charge, so refusing the push must not release it.
    assert_eq!(returned.permit.bytes(), 7);
    assert_eq!(other.root().snapshot().current_bytes, 7);
    assert!(queue.is_empty());

    drop(returned);
    assert_eq!(other.root().snapshot().current_bytes, 0);
}

#[test]
fn budgeted_push_reports_a_rejection_without_retaining_a_charge() {
    let tree = ResourceBudgetTree::new("process", limit(4, 64, FullPolicy::Reject)).expect("root budget");
    let component = tree
        .root()
        .child("component", limit(1, 8, FullPolicy::Reject))
        .expect("component budget");
    let queue = BudgetedQueue::new(component.clone());

    // The component's only byte of capacity is already committed, so the
    // rebind to the queue's own budget has nothing left to move into.
    let held = component.try_acquire_data(8).expect("hold component capacity");
    let permit = tree.root().try_acquire_data(4).expect("ancestor permit");

    let (item, rejection) = rejected(
        queue
            .try_push_budgeted("payload", permit)
            .expect("the permit belongs to this tree"),
    );
    assert_eq!(item, "payload");
    assert!(matches!(rejection, QueuePushRejection::BudgetExhausted(_)));
    assert!(queue.is_empty());
    // The rejected push released its transferred charge, so only the held
    // reservation remains.
    assert_eq!(tree.root().snapshot().current_bytes, 8);
    drop(held);
    assert_eq!(tree.root().snapshot().current_bytes, 0);
}
