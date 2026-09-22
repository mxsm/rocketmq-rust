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

use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushOutcome;
use rocketmq_runtime::QueuePushRejection;
use rocketmq_runtime::ResourceBudgetTree;
use tokio::task::JoinSet;
use tokio::time::Instant;

fn queue(capacity: usize, policy: FullPolicy) -> BudgetedQueue<usize> {
    let budget = ResourceBudgetTree::new("budgeted-queue-bench", BudgetLimit::new(capacity, capacity, policy))
        .expect("benchmark budget should be valid");
    BudgetedQueue::new(budget.root())
}

fn reject_batch(queue: &BudgetedQueue<usize>, size: usize) {
    for item in 0..size {
        let outcome = queue.try_push_data(black_box(item), 1);
        assert!(!matches!(&outcome, QueuePushOutcome::Rejected { .. }));
        black_box(outcome);
    }
    let rejected = queue.try_push_data(size, 1);
    assert!(matches!(
        &rejected,
        QueuePushOutcome::Rejected {
            rejection: QueuePushRejection::BudgetExhausted(_),
            ..
        }
    ));
    black_box(rejected);
    for _ in 0..size {
        black_box(queue.try_pop().expect("queued item"));
    }
}

async fn wait_release_batch(queue: &BudgetedQueue<usize>, size: usize) {
    let prior_waits = queue.snapshot().wait_count;
    for item in 0..size {
        assert!(!matches!(
            queue.try_push_data(item, 1),
            QueuePushOutcome::Rejected { .. }
        ));
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut producers = JoinSet::new();
    for item in size..size.saturating_mul(2) {
        let queue = BudgetedQueue::clone(queue);
        producers.spawn(async move {
            assert!(
                !matches!(
                    queue.push_until(item, 1, BudgetClass::Data, deadline).await,
                    QueuePushOutcome::Rejected { .. }
                ),
                "released capacity should admit waiter"
            );
        });
    }

    while queue.snapshot().waiters < size {
        tokio::task::yield_now().await;
    }
    for _ in 0..size {
        black_box(queue.try_pop().expect("initial queued item"));
    }
    while let Some(result) = producers.join_next().await {
        result.expect("benchmark producer should complete");
    }
    for _ in 0..size {
        black_box(queue.try_pop().expect("waiter queued item"));
    }

    let snapshot = queue.snapshot();
    assert_eq!(snapshot.wait_count, prior_waits + size as u64);
    assert_eq!(snapshot.reserved_count, 0);
    assert_eq!(snapshot.retained_bytes, 0);
}

/// Runs steady-state cycles at capacity: fill the queue, then replace every
/// item one at a time, so admission and release stay interleaved instead of
/// draining between batches.
fn churn_cycles(queue: &BudgetedQueue<usize>, size: usize, cycles: usize) {
    let mut next = size;
    for _ in 0..cycles {
        for _ in 0..size {
            black_box(queue.try_pop().expect("queued item"));
            assert!(!matches!(
                queue.try_push_data(next, 1),
                QueuePushOutcome::Rejected { .. }
            ));
            next += 1;
        }
    }
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.retained_bytes, size);
    assert_eq!(snapshot.wait_count, 0);
    assert_eq!(snapshot.rejected_count, 0);
}

fn bench_budgeted_queue(criterion: &mut Criterion) {
    let mut reject = criterion.benchmark_group("budgeted_queue_reject");
    for size in [128usize, 1024] {
        reject.bench_with_input(BenchmarkId::new("push_pop", size), &size, |bencher, size| {
            let queue = queue(*size, FullPolicy::Reject);
            bencher.iter(|| reject_batch(&queue, black_box(*size)));
        });
    }
    reject.finish();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut wait = criterion.benchmark_group("budgeted_queue_wait_release");
    for size in [128usize, 1024] {
        wait.bench_with_input(BenchmarkId::new("contended", size), &size, |bencher, size| {
            let queue = queue(*size, FullPolicy::WaitUntilDeadline);
            bencher.iter(|| runtime.block_on(wait_release_batch(&queue, black_box(*size))));
        });
    }
    wait.finish();

    let mut churn = criterion.benchmark_group("budgeted_queue_churn");
    for (size, cycles) in [(128usize, 4usize), (1024, 2)] {
        churn.bench_with_input(
            BenchmarkId::new("replace_at_capacity", size),
            &(size, cycles),
            |bencher, (size, cycles)| {
                let queue = queue(*size, FullPolicy::Reject);
                for item in 0..*size {
                    assert!(!matches!(
                        queue.try_push_data(item, 1),
                        QueuePushOutcome::Rejected { .. }
                    ));
                }
                bencher.iter(|| churn_cycles(&queue, black_box(*size), black_box(*cycles)));
            },
        );
    }
    churn.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(250))
        .measurement_time(Duration::from_millis(500));
    targets = bench_budgeted_queue
}
criterion_main!(benches);
