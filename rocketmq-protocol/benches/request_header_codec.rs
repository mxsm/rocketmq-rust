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

#[path = "request_header_codec/counting_allocator.rs"]
mod counting_allocator;

use std::alloc::System;
use std::collections::HashMap;
use std::fs::File;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use counting_allocator::CountingAllocator;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::Criterion;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::command_custom_header::FromMap;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::EncodedFrame;
use rocketmq_protocol::RemotingCommand;
use serde::Deserialize;
use serde::Serialize;

#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

const CORPUS_JSON: &str = include_str!("../../scripts/request-header-codec/perf-corpus-v1.json");

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Corpus {
    corpus_version: String,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    id: String,
    operation: Operation,
    header: String,
    request_code: i32,
    fields: HashMap<String, String>,
    serialize_type: WireFormat,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Encode,
    Decode,
}

#[derive(Debug, Clone, Copy, Deserialize)]
enum WireFormat {
    #[serde(rename = "JSON")]
    Json,
    #[serde(rename = "ROCKETMQ")]
    Rocketmq,
}

impl From<WireFormat> for SerializeType {
    fn from(value: WireFormat) -> Self {
        match value {
            WireFormat::Json => SerializeType::JSON,
            WireFormat::Rocketmq => SerializeType::ROCKETMQ,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AllocationEvidence {
    schema_version: u32,
    corpus_version: String,
    samples_per_case: usize,
    cases: Vec<AllocationCase>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AllocationCase {
    id: String,
    allocations: Vec<u64>,
    allocated_bytes: Vec<u64>,
}

fn fields(case: &Case) -> HashMap<CheetahString, CheetahString> {
    case.fields
        .iter()
        .map(|(key, value)| {
            (
                CheetahString::from_string(key.clone()),
                CheetahString::from_string(value.clone()),
            )
        })
        .collect()
}

fn fresh_command<T>(case: &Case, fields: &HashMap<CheetahString, CheetahString>) -> RemotingCommand
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Send + Sync + 'static,
{
    let header = T::from(fields).unwrap_or_else(|error| panic!("{} header construction failed: {error}", case.id));
    RemotingCommand::create_request_command_with_defaults(case.request_code, header, 501, case.serialize_type.into())
        .set_language(LanguageCode::RUST)
        .set_opaque(7)
}

fn encode_once<T>(case: &Case, fields: &HashMap<CheetahString, CheetahString>) -> Vec<u8>
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Send + Sync + 'static,
{
    EncodedFrame::from_command(fresh_command::<T>(case, fields))
        .unwrap_or_else(|error| panic!("{} frame encode failed: {error}", case.id))
        .into_bytes()
        .to_vec()
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn decode_normal<T>(command: &RemotingCommand) -> rocketmq_error::RocketMQResult<T>
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
{
    command.decode_command_custom_header::<T>()
}

fn decode_fast<T>(command: &RemotingCommand) -> rocketmq_error::RocketMQResult<T>
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Default,
{
    command.decode_command_custom_header_fast::<T>()
}

fn verify_frame<T, D>(case: &Case, fields: &HashMap<CheetahString, CheetahString>, frame: &[u8], decode: D)
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader,
    D: Fn(&RemotingCommand) -> rocketmq_error::RocketMQResult<T>,
{
    let mut input = BytesMut::from(frame);
    let command = RemotingCommand::decode(&mut input)
        .unwrap_or_else(|error| panic!("{} envelope decode failed: {error}", case.id))
        .unwrap_or_else(|| panic!("{} frame was incomplete", case.id));
    assert!(input.is_empty(), "{} left trailing frame bytes", case.id);
    let header = decode(&command).unwrap_or_else(|error| panic!("{} typed decode failed: {error}", case.id));
    assert_eq!(
        header.to_map().expect("benchmark header must re-encode"),
        *fields,
        "{} changed its canonical logical map",
        case.id
    );
}

fn register<T, D>(criterion: &mut Criterion, case: &Case, allocations: &mut Vec<AllocationCase>, decode: D)
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Send + Sync + 'static,
    D: Fn(&RemotingCommand) -> rocketmq_error::RocketMQResult<T> + Copy,
{
    let canonical_fields = fields(case);
    let reference_frame = encode_once::<T>(case, &canonical_fields);
    verify_frame::<T, D>(case, &canonical_fields, &reference_frame, decode);
    let reference_length = reference_frame.len();
    let reference_checksum = fnv1a64(&reference_frame);

    match case.operation {
        Operation::Encode => {
            criterion.bench_function(&case.id, |bencher| {
                bencher.iter_batched_ref(
                    || fresh_command::<T>(case, &canonical_fields),
                    |command| {
                        let mut output = BytesMut::new();
                        command.fast_header_encode(&mut output);
                        black_box(output)
                    },
                    BatchSize::SmallInput,
                );
            });

            if let Ok(sample_count) = std::env::var("ROCKETMQ_HEADER_CODEC_ALLOC_SAMPLES") {
                let sample_count = sample_count.parse::<usize>().expect("valid allocation sample count");
                let mut allocation_counts = Vec::with_capacity(sample_count);
                let mut allocated_bytes = Vec::with_capacity(sample_count);
                for _ in 0..sample_count {
                    let mut command = fresh_command::<T>(case, &canonical_fields);
                    let (output, snapshot) = ALLOCATOR.measure(|| {
                        let mut output = BytesMut::new();
                        command.fast_header_encode(&mut output);
                        black_box(output.len());
                        output
                    });
                    assert!(
                        !output.is_empty(),
                        "{} allocation probe encoded an empty frame",
                        case.id
                    );
                    allocation_counts.push(snapshot.allocations);
                    allocated_bytes.push(snapshot.allocated_bytes);
                }
                allocations.push(AllocationCase {
                    id: case.id.clone(),
                    allocations: allocation_counts,
                    allocated_bytes,
                });
            }
        }
        Operation::Decode => {
            criterion.bench_function(&case.id, |bencher| {
                bencher.iter_batched_ref(
                    || BytesMut::from(reference_frame.as_slice()),
                    |input| {
                        let command = RemotingCommand::decode(input)
                            .expect("benchmark frame envelope must decode")
                            .expect("benchmark frame must be complete");
                        let header = decode(&command).expect("benchmark typed header must decode");
                        black_box((command, header))
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    let replay = encode_once::<T>(case, &canonical_fields);
    assert_eq!(replay.len(), reference_length, "{} frame length drifted", case.id);
    assert_eq!(
        fnv1a64(&replay),
        reference_checksum,
        "{} frame checksum drifted",
        case.id
    );
}

fn benchmark_request_headers(criterion: &mut Criterion) {
    let corpus: Corpus = serde_json::from_str(CORPUS_JSON).expect("valid checked-in performance corpus");
    let mut allocations = Vec::new();
    for case in &corpus.cases {
        match case.header.as_str() {
            "rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader" => {
                register::<PullMessageRequestHeader, _>(criterion, case, &mut allocations, decode_fast)
            }
            "rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader" => {
                register::<PullMessageResponseHeader, _>(criterion, case, &mut allocations, decode_fast)
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader" => {
                register::<SendMessageRequestHeader, _>(criterion, case, &mut allocations, decode_fast)
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2" => {
                register::<SendMessageRequestHeaderV2, _>(criterion, case, &mut allocations, decode_fast)
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader" => {
                register::<SendMessageResponseHeader, _>(criterion, case, &mut allocations, decode_fast)
            }
            "rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader" => {
                register::<GetConsumerStatusRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader" => {
                register::<QueryConsumeQueueRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader" => {
                register::<DeleteTopicFromNamesrvRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader" => {
                register::<ConsumeMessageDirectlyResultRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader" => {
                register::<GetLiteClientInfoRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader" => {
                register::<CleanBrokerDataRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            "rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader" => {
                register::<NotificationRequestHeader, _>(criterion, case, &mut allocations, decode_normal)
            }
            other => panic!("unregistered request-header benchmark type: {other}"),
        }
    }

    if let Some(output) = std::env::var_os("ROCKETMQ_HEADER_CODEC_ALLOC_OUTPUT") {
        let output = PathBuf::from(output);
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).expect("create allocation evidence directory");
        }
        let evidence = AllocationEvidence {
            schema_version: 1,
            corpus_version: corpus.corpus_version,
            samples_per_case: allocations.first().map_or(0, |case| case.allocations.len()),
            cases: allocations,
        };
        let mut file = File::create(output).expect("create allocation evidence");
        serde_json::to_writer_pretty(&mut file, &evidence).expect("write allocation evidence");
        use std::io::Write;
        writeln!(file).expect("terminate allocation evidence");
    }
}

fn criterion_config() -> Criterion {
    let warmup = std::env::var("ROCKETMQ_HEADER_CODEC_WARMUP_SECONDS")
        .ok()
        .map_or(5, |value| value.parse().expect("valid warmup seconds"));
    let measurement = std::env::var("ROCKETMQ_HEADER_CODEC_MEASUREMENT_SECONDS")
        .ok()
        .map_or(10, |value| value.parse().expect("valid measurement seconds"));
    let samples = std::env::var("ROCKETMQ_HEADER_CODEC_SAMPLES")
        .ok()
        .map_or(100, |value| value.parse().expect("valid sample count"));
    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_secs(warmup))
        .measurement_time(Duration::from_secs(measurement))
        .sample_size(samples);
    if let Some(output) = std::env::var_os("ROCKETMQ_HEADER_CODEC_CRITERION_DIR") {
        criterion = criterion.output_directory(PathBuf::from(output).as_path());
    }
    criterion
}

criterion_group! {
    name = request_header_codec;
    config = criterion_config();
    targets = benchmark_request_headers
}
criterion_main!(request_header_codec);
