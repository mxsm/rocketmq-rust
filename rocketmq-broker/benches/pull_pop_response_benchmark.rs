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
use std::time::Duration;

use bytes::Bytes;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::benchmark_support::RemotingResponsePreparationHarness;

fn pull_head() -> RemotingCommand {
    RemotingCommand::create_success_response_command_with_header(PullMessageResponseHeader {
        suggest_which_broker_id: 0,
        next_begin_offset: 4_096,
        min_offset: 0,
        max_offset: 8_192,
        offset_delta: Some(0),
        topic_sys_flag: None,
        group_sys_flag: None,
        forbidden_type: None,
    })
}

fn pop_head() -> RemotingCommand {
    RemotingCommand::create_success_response_command_with_header(PopMessageResponseHeader {
        pop_time: 1_725_000_000_000,
        invisible_time: 30_000,
        revive_qid: 0,
        rest_num: 16,
        start_offset_info: Some("0 0 4096".into()),
        msg_offset_info: Some("0 0 4096,4097,4098,4099".into()),
        order_count_info: None,
    })
}

fn four_segments(payload: &Bytes) -> Vec<Bytes> {
    let quarter = payload.len() / 4;
    (0..4)
        .map(|index| {
            let start = index * quarter;
            let end = if index == 3 { payload.len() } else { start + quarter };
            payload.slice(start..end)
        })
        .collect()
}

fn benchmark_pull_pop_response(criterion: &mut Criterion) {
    let preparation = RemotingResponsePreparationHarness::new();
    let mut group = criterion.benchmark_group("broker_pull_pop_response");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for body_bytes in [4 * 1024, 256 * 1024] {
        let payload = Bytes::from(vec![0x5a; body_bytes]);
        let pop_segments = four_segments(&payload);
        group.throughput(Throughput::Bytes(body_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("pull_legacy_materialize_and_encode", body_bytes),
            &payload,
            |bencher, payload| {
                bencher.iter(|| {
                    let response = pull_head()
                        .set_opaque(901)
                        .set_body(Bytes::copy_from_slice(payload.as_ref()));
                    let frame = EncodedFrame::from_command(response).expect("legacy-shaped Pull response encode");
                    black_box((frame.encoded_len(), frame.into_bytes()));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("pull_canonical_bytes_prepare", body_bytes),
            &payload,
            |bencher, payload| {
                bencher.iter(|| {
                    let plan = RemotingResponse::bytes(pull_head(), payload.clone())
                        .expect("canonical Pull remoting response");
                    black_box(preparation.prepare(plan, 901));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("pop_legacy_materialize_and_encode", body_bytes),
            &payload,
            |bencher, payload| {
                bencher.iter(|| {
                    let response = pop_head()
                        .set_opaque(902)
                        .set_body(Bytes::copy_from_slice(payload.as_ref()));
                    let frame = EncodedFrame::from_command(response).expect("legacy-shaped Pop response encode");
                    black_box((frame.encoded_len(), frame.into_bytes()));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("pop_canonical_segmented_prepare", body_bytes),
            &pop_segments,
            |bencher, pop_segments| {
                bencher.iter(|| {
                    let plan = RemotingResponse::segments(pop_head(), pop_segments.clone())
                        .expect("canonical Pop remoting response");
                    black_box(preparation.prepare(plan, 902));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_pull_pop_response);
criterion_main!(benches);
