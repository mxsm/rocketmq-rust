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

#[path = "request_header_codec/counting_allocator.rs"]
mod counting_allocator;

use std::alloc::System as SystemAllocator;
use std::collections::HashMap;
use std::fs::File;
use std::hint::black_box;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use counting_allocator::AllocationSnapshot;
use counting_allocator::CountingAllocator;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::EncodedFrame;
use rocketmq_protocol::RemotingCommand;
use serde::Serialize;
use sysinfo::Pid;
use sysinfo::ProcessesToUpdate;
use sysinfo::System as ProcessSystem;

#[global_allocator]
static ALLOCATOR: CountingAllocator<SystemAllocator> = CountingAllocator::new(SystemAllocator);

const CONSTRUCTS_PER_ROUND: usize = 512;
const DECODES_PER_ROUND: usize = 512;
const BODY_SIZES: [usize; 6] = [0, 128, 4 * 1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024];
const EXT_FIELD_COUNTS: [usize; 7] = [0, 1, 8, 16, 32, 128, 256];

fn protocols() -> [(SerializeType, &'static str); 2] {
    [(SerializeType::JSON, "json"), (SerializeType::ROCKETMQ, "rocketmq")]
}

fn extension_fields(count: usize) -> HashMap<CheetahString, CheetahString> {
    (0..count)
        .map(|index| {
            (
                CheetahString::from_string(format!("benchmarkKey{index:03}")),
                CheetahString::from_string(format!("benchmark-value-{index:03}")),
            )
        })
        .collect()
}

fn command(
    serialize_type: SerializeType,
    ext_fields: usize,
    remark: Option<&str>,
    body_bytes: usize,
) -> RemotingCommand {
    let mut command = RemotingCommand::create_remoting_command(RequestCode::SendMessage)
        .set_version(501)
        .set_opaque(7)
        .set_serialize_type(serialize_type);
    if ext_fields > 0 {
        command = command.set_ext_fields(extension_fields(ext_fields));
    }
    if let Some(remark) = remark {
        command = command.set_remark(remark);
    }
    if body_bytes > 0 {
        command = command.set_body(Bytes::from(vec![0x5a; body_bytes]));
    }
    command
}

fn typed_command(serialize_type: SerializeType) -> RemotingCommand {
    RemotingCommand::create_request_command_with_defaults(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new("BenchmarkTopic", Some(true)),
        501,
        serialize_type,
    )
    .set_opaque(11)
}

fn encoded(command: RemotingCommand) -> Bytes {
    EncodedFrame::from_command(command)
        .expect("benchmark command must encode")
        .into_bytes()
}

fn decode_complete(frame: &Bytes) -> RemotingCommand {
    let mut input = BytesMut::from(frame.as_ref());
    let command = RemotingCommand::decode(&mut input)
        .expect("benchmark frame must decode")
        .expect("benchmark frame must be complete");
    assert!(input.is_empty(), "benchmark decode left trailing bytes");
    command
}

fn decode_fragmented(frame: &Bytes, chunks: &[usize]) -> RemotingCommand {
    let mut input = BytesMut::new();
    let mut offset = 0;
    let mut chunk_index = 0;
    while offset < frame.len() {
        let chunk = chunks[chunk_index % chunks.len()].min(frame.len() - offset);
        input.extend_from_slice(&frame[offset..offset + chunk]);
        offset += chunk;
        chunk_index += 1;
        if let Some(command) = RemotingCommand::decode(&mut input).expect("fragmented frame must remain valid") {
            assert_eq!(offset, frame.len(), "decoder completed before the final fragment");
            assert!(input.is_empty(), "fragmented decode left trailing bytes");
            return command;
        }
    }
    panic!("complete fragmented frame did not decode")
}

struct ConstructContentionHarness {
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl ConstructContentionHarness {
    fn new(worker_count: usize) -> Self {
        let start = Arc::new(Barrier::new(worker_count + 1));
        let finish = Arc::new(Barrier::new(worker_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..worker_count)
            .map(|_| {
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..CONSTRUCTS_PER_ROUND {
                        black_box(RemotingCommand::create_remoting_command(RequestCode::SendMessage));
                    }
                    finish.wait();
                })
            })
            .collect();
        Self {
            start,
            finish,
            stop,
            workers,
        }
    }

    fn run(&self) {
        self.start.wait();
        self.finish.wait();
    }
}

struct DecodeContentionHarness {
    start: Arc<Barrier>,
    finish: Arc<Barrier>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl DecodeContentionHarness {
    fn new(worker_count: usize, ext_fields: usize) -> Self {
        let frame = encoded(command(SerializeType::ROCKETMQ, ext_fields, None, 0));
        let start = Arc::new(Barrier::new(worker_count + 1));
        let finish = Arc::new(Barrier::new(worker_count + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..worker_count)
            .map(|_| {
                let frame = frame.clone();
                let start = Arc::clone(&start);
                let finish = Arc::clone(&finish);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || loop {
                    start.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for _ in 0..DECODES_PER_ROUND {
                        black_box(decode_complete(&frame));
                    }
                    finish.wait();
                })
            })
            .collect();
        Self {
            start,
            finish,
            stop,
            workers,
        }
    }

    fn run(&self) {
        self.start.wait();
        self.finish.wait();
    }
}

impl Drop for DecodeContentionHarness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("decode contention worker must stop");
        }
    }
}

impl Drop for ConstructContentionHarness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.start.wait();
        for worker in self.workers.drain(..) {
            worker.join().expect("benchmark worker must stop cleanly");
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AllocationCase {
    id: String,
    allocations: u64,
    allocated_bytes: u64,
    output_len: usize,
    output_capacity: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObjectFootprint {
    size_of_bytes: usize,
    object_count: usize,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    rss_delta_bytes: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AllocationEvidence {
    schema_version: u32,
    cases: Vec<AllocationCase>,
    object_footprint: ObjectFootprint,
}

fn allocation_case<T>(
    id: &str,
    operation: impl FnOnce() -> T,
    output_shape: impl FnOnce(&T) -> (usize, Option<usize>),
) -> AllocationCase {
    let (
        output,
        AllocationSnapshot {
            allocations,
            allocated_bytes,
        },
    ) = ALLOCATOR.measure(operation);
    let (output_len, output_capacity) = output_shape(&output);
    black_box(&output);
    AllocationCase {
        id: id.to_owned(),
        allocations,
        allocated_bytes,
        output_len,
        output_capacity,
    }
}

fn process_rss_bytes() -> u64 {
    let pid = Pid::from_u32(std::process::id());
    let mut system = ProcessSystem::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map_or(0, |process| process.memory())
}

fn object_footprint() -> ObjectFootprint {
    const OBJECT_COUNT: usize = 100_000;
    let rss_before_bytes = process_rss_bytes();
    let commands = (0..OBJECT_COUNT)
        .map(|_| RemotingCommand::create_remoting_command(RequestCode::SendMessage))
        .collect::<Vec<_>>();
    black_box(&commands);
    let rss_after_bytes = process_rss_bytes();
    drop(commands);
    ObjectFootprint {
        size_of_bytes: size_of::<RemotingCommand>(),
        object_count: OBJECT_COUNT,
        rss_before_bytes,
        rss_after_bytes,
        rss_delta_bytes: rss_after_bytes.saturating_sub(rss_before_bytes),
    }
}

fn write_allocation_evidence() {
    let Some(path) = std::env::var_os("ROCKETMQ_REMOTING_COMMAND_EVIDENCE") else {
        return;
    };
    let mut cases = Vec::new();
    cases.push(allocation_case(
        "construct-json-ext-32-body-4096",
        || command(SerializeType::JSON, 32, None, 4096),
        |command| (command.body().map_or(0, Bytes::len), None),
    ));
    for (serialize_type, label) in protocols() {
        cases.push(allocation_case(
            &format!("header-encode-{label}-ext-32"),
            || {
                let mut command = command(serialize_type, 32, None, 0);
                let mut output = BytesMut::new();
                command
                    .try_fast_header_encode(&mut output)
                    .expect("benchmark header must encode");
                output
            },
            |output| (output.len(), Some(output.capacity())),
        ));
        cases.push(allocation_case(
            &format!("frame-assemble-{label}-body-1048576"),
            || encoded(command(serialize_type, 8, None, 1024 * 1024)),
            |output| (output.len(), None),
        ));
        let frame = encoded(command(serialize_type, 32, None, 64 * 1024));
        cases.push(allocation_case(
            &format!("envelope-decode-{label}-body-65536"),
            || decode_complete(&frame),
            |output| (output.body().map_or(0, Bytes::len), None),
        ));
    }
    cases.push(allocation_case(
        "clone-ext-128-body-1048576",
        || command(SerializeType::ROCKETMQ, 128, None, 1024 * 1024).clone(),
        |output| (output.body().map_or(0, Bytes::len), None),
    ));
    cases.push(allocation_case(
        "display-ext-32",
        || command(SerializeType::JSON, 32, Some("benchmark"), 0).to_string(),
        |output| (output.len(), Some(output.capacity())),
    ));

    let evidence = AllocationEvidence {
        schema_version: 1,
        cases,
        object_footprint: object_footprint(),
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create remoting command evidence directory");
    }
    let file = File::create(path).expect("create remoting command allocation evidence");
    serde_json::to_writer_pretty(file, &evidence).expect("write remoting command allocation evidence");
}

fn benchmark_construct(c: &mut Criterion) {
    write_allocation_evidence();

    let mut group = c.benchmark_group("remoting_command/construct");
    for (serialize_type, protocol) in protocols() {
        for ext_fields in [0, 8, 32, 128] {
            group.bench_with_input(
                BenchmarkId::new(protocol, format!("ext-{ext_fields}")),
                &(serialize_type, ext_fields),
                |benchmark, &(serialize_type, ext_fields)| {
                    benchmark.iter(|| black_box(command(serialize_type, ext_fields, None, 0)));
                },
            );
        }
    }
    group.finish();

    let mut contention = c.benchmark_group("remoting_command/construct_contention");
    for thread_count in [1, 2, 4, 8, 16, 32] {
        let harness = ConstructContentionHarness::new(thread_count);
        contention.throughput(Throughput::Elements((thread_count * CONSTRUCTS_PER_ROUND) as u64));
        contention.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            &harness,
            |benchmark, harness| {
                benchmark.iter(|| harness.run());
            },
        );
    }
    contention.finish();
}

fn benchmark_encode_and_object_operations(c: &mut Criterion) {
    let mut header_encode = c.benchmark_group("remoting_command/header_encode");
    for (serialize_type, protocol) in protocols() {
        for ext_fields in EXT_FIELD_COUNTS {
            header_encode.bench_with_input(
                BenchmarkId::new(protocol, format!("ext-{ext_fields}")),
                &(serialize_type, ext_fields),
                |benchmark, &(serialize_type, ext_fields)| {
                    benchmark.iter_batched(
                        || command(serialize_type, ext_fields, None, 0),
                        |mut command| {
                            let mut output = BytesMut::new();
                            command
                                .try_fast_header_encode(&mut output)
                                .expect("benchmark header must encode");
                            black_box(output);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
        for (remark, value) in [
            ("ascii-32", "a".repeat(32)),
            ("unicode-escaped-4096", "消息🚀\\\"".repeat(512)),
        ] {
            header_encode.bench_with_input(
                BenchmarkId::new(protocol, remark),
                &(serialize_type, value),
                |benchmark, (serialize_type, value)| {
                    benchmark.iter_batched(
                        || command(*serialize_type, 8, Some(value), 0),
                        |mut command| {
                            let mut output = BytesMut::new();
                            command
                                .try_fast_header_encode(&mut output)
                                .expect("benchmark remark header must encode");
                            black_box(output);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    header_encode.finish();

    let mut frame_assemble = c.benchmark_group("remoting_command/frame_assemble");
    for (serialize_type, protocol) in protocols() {
        for body_bytes in BODY_SIZES {
            frame_assemble.throughput(Throughput::Bytes(body_bytes.max(1) as u64));
            frame_assemble.bench_with_input(
                BenchmarkId::new(protocol, format!("body-{body_bytes}")),
                &(serialize_type, body_bytes),
                |benchmark, &(serialize_type, body_bytes)| {
                    benchmark.iter_batched(
                        || command(serialize_type, 8, None, body_bytes),
                        |command| black_box(EncodedFrame::from_command(command).expect("benchmark frame must encode")),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    frame_assemble.finish();

    let mut clone_group = c.benchmark_group("remoting_command/clone");
    for ext_fields in [0, 8, 32, 128, 256] {
        for body_bytes in [0, 1024 * 1024] {
            let command = command(SerializeType::ROCKETMQ, ext_fields, None, body_bytes);
            clone_group.bench_with_input(
                BenchmarkId::new(format!("ext-{ext_fields}"), format!("body-{body_bytes}")),
                &command,
                |benchmark, command| benchmark.iter(|| black_box(command.clone())),
            );
        }
    }
    clone_group.finish();

    let mut display = c.benchmark_group("remoting_command/display");
    for ext_fields in [0, 32, 128] {
        let command = command(SerializeType::JSON, ext_fields, Some("display-benchmark"), 0);
        display.bench_with_input(
            BenchmarkId::from_parameter(ext_fields),
            &command,
            |benchmark, command| {
                benchmark.iter(|| black_box(command.to_string()));
            },
        );
    }
    display.finish();
}

fn benchmark_decode_and_round_trip(c: &mut Criterion) {
    let mut decode = c.benchmark_group("remoting_command/envelope_decode");
    for (serialize_type, protocol) in protocols() {
        for body_bytes in BODY_SIZES {
            let frame = encoded(command(serialize_type, 32, None, body_bytes));
            decode.throughput(Throughput::Bytes(frame.len() as u64));
            decode.bench_with_input(
                BenchmarkId::new(protocol, format!("body-{body_bytes}")),
                &frame,
                |benchmark, frame| {
                    benchmark.iter_batched(
                        || BytesMut::from(frame.as_ref()),
                        |mut input| {
                            black_box(
                                RemotingCommand::decode(&mut input)
                                    .expect("benchmark frame must decode")
                                    .expect("benchmark frame must be complete"),
                            );
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    decode.finish();

    let mut contention = c.benchmark_group("remoting_command/decode_contention");
    for ext_fields in [0, 8, 32, 128] {
        for thread_count in [1, 8, 16, 32] {
            let harness = DecodeContentionHarness::new(thread_count, ext_fields);
            contention.throughput(Throughput::Elements((thread_count * DECODES_PER_ROUND) as u64));
            contention.bench_with_input(
                BenchmarkId::new(format!("ext-{ext_fields}"), format!("threads-{thread_count}")),
                &harness,
                |benchmark, harness| benchmark.iter(|| harness.run()),
            );
        }
    }
    contention.finish();

    let mut typed_decode = c.benchmark_group("remoting_command/typed_decode");
    for (serialize_type, protocol) in protocols() {
        let decoded = decode_complete(&encoded(typed_command(serialize_type)));
        typed_decode.bench_with_input(BenchmarkId::from_parameter(protocol), &decoded, |benchmark, command| {
            benchmark.iter(|| {
                black_box(
                    command
                        .decode_command_custom_header::<GetRouteInfoRequestHeader>()
                        .expect("typed benchmark header must decode"),
                );
            });
        });
    }
    typed_decode.finish();

    let mut round_trip = c.benchmark_group("remoting_command/round_trip");
    for (serialize_type, protocol) in protocols() {
        for body_bytes in [0, 4 * 1024, 64 * 1024, 1024 * 1024] {
            round_trip.throughput(Throughput::Bytes(body_bytes.max(1) as u64));
            round_trip.bench_with_input(
                BenchmarkId::new(protocol, format!("body-{body_bytes}")),
                &(serialize_type, body_bytes),
                |benchmark, &(serialize_type, body_bytes)| {
                    benchmark.iter_batched(
                        || command(serialize_type, 32, None, body_bytes),
                        |command| {
                            let frame = encoded(command);
                            black_box(decode_complete(&frame));
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    round_trip.finish();

    let mut raw_forward = c.benchmark_group("remoting_command/raw_forward");
    for (serialize_type, protocol) in protocols() {
        for body_bytes in [0, 64 * 1024, 1024 * 1024] {
            let decoded = decode_complete(&encoded(command(serialize_type, 32, None, body_bytes)));
            raw_forward.throughput(Throughput::Bytes(body_bytes.max(1) as u64));
            raw_forward.bench_with_input(
                BenchmarkId::new(protocol, format!("body-{body_bytes}")),
                &decoded,
                |benchmark, decoded| {
                    benchmark.iter_batched(
                        || decoded.clone(),
                        |command| black_box(EncodedFrame::from_command(command).expect("raw forward must encode")),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    raw_forward.finish();
}

fn benchmark_fragmentation_and_limits(c: &mut Criterion) {
    let frame = encoded(command(SerializeType::ROCKETMQ, 8, None, 128));
    let mut fragmentation = c.benchmark_group("remoting_command/input_shape");
    for (name, chunks) in [
        ("one_byte_fragmentation", vec![1]),
        ("prefix_boundary", vec![4, 4, usize::MAX]),
        ("random_fragments", vec![1, 7, 3, 31, 2, 64, 5, 19]),
    ] {
        fragmentation.bench_with_input(BenchmarkId::from_parameter(name), &chunks, |benchmark, chunks| {
            benchmark.iter(|| black_box(decode_fragmented(&frame, chunks)));
        });
    }

    let mut consecutive = BytesMut::with_capacity(frame.len() * 32);
    for _ in 0..32 {
        consecutive.extend_from_slice(&frame);
    }
    fragmentation.throughput(Throughput::Elements(32));
    fragmentation.bench_function("consecutive_32_frames", |benchmark| {
        benchmark.iter_batched(
            || consecutive.clone(),
            |mut input| {
                for _ in 0..32 {
                    black_box(
                        RemotingCommand::decode(&mut input)
                            .expect("consecutive frame must decode")
                            .expect("consecutive frame must be complete"),
                    );
                }
                assert!(input.is_empty());
            },
            BatchSize::SmallInput,
        );
    });
    fragmentation.finish();

    let mut limits = c.benchmark_group("remoting_command/limits_rejection");
    let announced_payload = 1021_i32.to_be_bytes();
    let mut oversized = [0_u8; 8];
    oversized[..4].copy_from_slice(&announced_payload);
    limits.bench_function("announced_max_plus_one", |benchmark| {
        benchmark.iter_batched(
            || BytesMut::from(&oversized[..]),
            |mut input| {
                let error = match RemotingCommand::decode_with_max_frame_bytes(&mut input, 1024) {
                    Err(error) => error,
                    Ok(_) => panic!("oversized announcement must be rejected"),
                };
                black_box(error);
            },
            BatchSize::SmallInput,
        );
    });
    limits.finish();
}

fn criterion_config() -> Criterion {
    let warmup = std::env::var("ROCKETMQ_REMOTING_COMMAND_WARMUP_SECONDS")
        .ok()
        .map_or(5, |value| value.parse().expect("valid warmup seconds"));
    let measurement = std::env::var("ROCKETMQ_REMOTING_COMMAND_MEASUREMENT_SECONDS")
        .ok()
        .map_or(10, |value| value.parse().expect("valid measurement seconds"));
    let samples = std::env::var("ROCKETMQ_REMOTING_COMMAND_SAMPLES")
        .ok()
        .map_or(100, |value| value.parse().expect("valid sample count"));
    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_secs(warmup))
        .measurement_time(Duration::from_secs(measurement))
        .sample_size(samples);
    if let Some(output) = std::env::var_os("ROCKETMQ_REMOTING_COMMAND_CRITERION_DIR") {
        let output = PathBuf::from(output);
        criterion = criterion.output_directory(&output);
    }
    criterion
}

criterion_group! {
    name = remoting_command_hot_paths;
    config = criterion_config();
    targets =
        benchmark_construct,
        benchmark_encode_and_object_operations,
        benchmark_decode_and_round_trip,
        benchmark_fragmentation_and_limits
}
criterion_main!(remoting_command_hot_paths);
