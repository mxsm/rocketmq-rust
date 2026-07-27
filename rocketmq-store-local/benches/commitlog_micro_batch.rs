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

use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencer;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerConfig;
use tokio_util::sync::CancellationToken;

const REQUESTS_PER_ITERATION: usize = 32;
const RETAINED_BYTES_PER_REQUEST: usize = 1024;

async fn run_sequencer(policy: MicroBatchPolicy) {
    let config = AppendSequencerConfig {
        queue_capacity: REQUESTS_PER_ITERATION,
        queue_bytes: REQUESTS_PER_ITERATION * RETAINED_BYTES_PER_REQUEST,
        micro_batch: policy,
    };
    let (sender, mut receiver) = AppendSequencer::bounded(config).expect("sequencer");
    for request in 0..REQUESTS_PER_ITERATION {
        sender
            .try_submit(request, RETAINED_BYTES_PER_REQUEST)
            .expect("admission");
    }
    sender.close();
    let cancellation = CancellationToken::new();
    let mut completed = 0;
    while let Some(batch) = receiver.next_batch(&cancellation).await {
        completed += batch.len();
    }
    assert_eq!(completed, REQUESTS_PER_ITERATION);
}

fn benchmark_commitlog_micro_batch(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = criterion.benchmark_group("commitlog_append_sequencer");
    group.bench_function("disabled_32_requests", |bencher| {
        bencher.iter(|| {
            runtime.block_on(run_sequencer(
                MicroBatchPolicy::disabled(REQUESTS_PER_ITERATION * RETAINED_BYTES_PER_REQUEST)
                    .expect("benchmark policy"),
            ));
        });
    });
    group.bench_function("enabled_32_requests", |bencher| {
        bencher.iter(|| {
            runtime.block_on(run_sequencer(
                MicroBatchPolicy::try_new(
                    REQUESTS_PER_ITERATION,
                    REQUESTS_PER_ITERATION * RETAINED_BYTES_PER_REQUEST,
                    Duration::ZERO,
                )
                .expect("benchmark policy"),
            ));
        });
    });
    group.finish();
}

criterion_group!(benches, benchmark_commitlog_micro_batch);
criterion_main!(benches);
