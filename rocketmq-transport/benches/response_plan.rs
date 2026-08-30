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

use std::hint::black_box;

use bytes::Bytes;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::ResponsePlan;
use rocketmq_transport::benchmark_support::ResponsePlanPreparationHarness;

#[path = "support/criterion_profile.rs"]
mod criterion_profile;

use criterion_profile::apply_remoting_command_baseline_profile;

fn response_head() -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(0).set_remark("response-plan-benchmark")
}

fn segments(payload: &Bytes) -> Vec<Bytes> {
    let quarter = payload.len() / 4;
    (0..4)
        .map(|index| {
            let start = index * quarter;
            let end = if index == 3 { payload.len() } else { start + quarter };
            payload.slice(start..end)
        })
        .collect()
}

fn benchmark_response_plan(criterion: &mut Criterion) {
    let harness = ResponsePlanPreparationHarness::new();
    let mut group = criterion.benchmark_group("transport_response_plan");
    apply_remoting_command_baseline_profile(&mut group);

    for body_bytes in [128, 4 * 1024, 256 * 1024] {
        let payload = Bytes::from(vec![0x5a; body_bytes]);
        group.throughput(Throughput::Bytes(body_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("legacy_materialize_and_contiguous_encode", body_bytes),
            &payload,
            |bencher, payload| {
                bencher.iter(|| {
                    let command = response_head()
                        .set_opaque(811)
                        .set_body(Bytes::copy_from_slice(payload.as_ref()));
                    let frame = EncodedFrame::from_command(command).expect("legacy-shaped response encode");
                    black_box((frame.encoded_len(), frame.into_bytes()));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("canonical_bytes_prepare_zero_copy_body", body_bytes),
            &payload,
            |bencher, payload| {
                bencher.iter(|| {
                    let plan =
                        ResponsePlan::bytes(response_head(), payload.clone()).expect("canonical byte response plan");
                    black_box(harness.prepare(plan, 811));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("canonical_segmented_prepare_zero_copy_body", body_bytes),
            &payload,
            |bencher, payload| {
                let body_segments = segments(payload);
                bencher.iter(|| {
                    let plan = ResponsePlan::segments(response_head(), body_segments.clone())
                        .expect("canonical segmented response plan");
                    black_box(harness.prepare(plan, 811));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_response_plan);
criterion_main!(benches);
