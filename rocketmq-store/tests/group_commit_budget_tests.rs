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

use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushOutcome;
use rocketmq_runtime::QueuePushRejection;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_store_local::flush::group_commit::GroupCommitQueue;
use rocketmq_store_local::flush::group_commit::GroupCommitRequest;

fn group_commit_queue<E>(name: &str, count: usize, bytes: usize, policy: FullPolicy) -> GroupCommitQueue<E> {
    let budget = ResourceBudgetTree::new(name, BudgetLimit::new(count, bytes, policy))
        .expect("group commit test budget")
        .root();
    GroupCommitQueue::new(budget)
}

fn admitted<T>(outcome: QueuePushOutcome<T>) {
    assert!(
        !matches!(outcome, QueuePushOutcome::Rejected { .. }),
        "group commit request should be admitted"
    );
}

fn rejection<T>(outcome: QueuePushOutcome<T>) -> QueuePushRejection {
    match outcome {
        QueuePushOutcome::Rejected { rejection, .. } => rejection,
        QueuePushOutcome::Enqueued | QueuePushOutcome::Coalesced { .. } | QueuePushOutcome::DroppedStale { .. } => {
            panic!("group commit request should be rejected")
        }
    }
}

#[test]
fn group_commit_queue_enforces_count_and_holds_permit_until_owner_drop() {
    let queue = group_commit_queue::<()>("group-count", 1, 4_096, FullPolicy::Reject);
    let (first, _first_response) = GroupCommitRequest::new(10, 5_000);
    let (second, _second_response) = GroupCommitRequest::new(20, 5_000);
    let first_bytes = first.retained_bytes();
    let second_bytes = second.retained_bytes();

    admitted(queue.try_push_data(first, first_bytes));
    let error = rejection(queue.try_push_data(second, second_bytes));
    assert!(matches!(
        error,
        QueuePushRejection::BudgetExhausted(error)
            if error.dimension() == BudgetDimension::Count
    ));

    let owned = queue.try_pop_budgeted().expect("accepted request");
    assert_eq!(queue.snapshot().reserved_count, 1);
    drop(owned);
    assert_eq!(queue.snapshot().reserved_count, 0);
    assert_eq!(queue.snapshot().retained_bytes, 0);
}

#[test]
fn group_commit_queue_enforces_retained_bytes_before_count() {
    let (first, _first_response) = GroupCommitRequest::<()>::new(10, 5_000);
    let retained_bytes = first.retained_bytes();
    let queue = group_commit_queue("group-bytes", 2, retained_bytes + 1, FullPolicy::Reject);
    let (second, _second_response) = GroupCommitRequest::new(20, 5_000);
    let second_bytes = second.retained_bytes();

    admitted(queue.try_push_data(first, retained_bytes));
    let error = rejection(queue.try_push_data(second, second_bytes));
    assert!(matches!(
        error,
        QueuePushRejection::BudgetExhausted(error)
            if error.dimension() == BudgetDimension::Bytes
    ));

    drop(queue.try_pop_budgeted());
    assert_eq!(queue.snapshot().retained_bytes, 0);
}

#[test]
fn group_commit_queue_preserves_fifo_and_close_releases_drained_permits() {
    let queue = group_commit_queue::<()>("group-fifo", 2, 4_096, FullPolicy::Reject);
    let (first, _first_response) = GroupCommitRequest::new(10, 5_000);
    let (second, _second_response) = GroupCommitRequest::new(20, 5_000);
    let first_bytes = first.retained_bytes();
    let second_bytes = second.retained_bytes();
    admitted(queue.try_push_data(first, first_bytes));
    admitted(queue.try_push_data(second, second_bytes));
    queue.close();

    let first = queue.try_pop_budgeted().expect("first request").into_item();
    let second = queue.try_pop_budgeted().expect("second request").into_item();
    assert_eq!(first.next_offset(), 10);
    assert_eq!(second.next_offset(), 20);
    assert_eq!(queue.snapshot().reserved_count, 0);
}

#[tokio::test]
async fn group_commit_admission_deadline_is_typed_and_does_not_leak_capacity() {
    let queue = group_commit_queue::<()>("group-deadline", 1, 4_096, FullPolicy::WaitUntilDeadline);
    let (first, _first_response) = GroupCommitRequest::new(10, 5_000);
    let (second, _second_response) = GroupCommitRequest::new(20, 0);
    let first_bytes = first.retained_bytes();
    let second_bytes = second.retained_bytes();
    admitted(queue.try_push_data(first, first_bytes));

    let error = rejection(
        queue
            .push_until(second, second_bytes, BudgetClass::Data, tokio::time::Instant::now())
            .await,
    );
    assert_eq!(error, QueuePushRejection::DeadlineExceeded);
    assert_eq!(queue.snapshot().reserved_count, 1);

    drop(queue.try_pop_budgeted());
    assert_eq!(queue.snapshot().reserved_count, 0);
}
