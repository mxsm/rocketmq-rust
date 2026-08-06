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

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::command_custom_header::FromMap;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader;
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::EncodedFrame;
use rocketmq_protocol::RemotingCommand;
use serde_json::Value;

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/request_header_codec");

fn decode_base64(value: &str) -> Vec<u8> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let compact = value
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(compact.len() % 4, 0, "invalid base64 fixture length");
    let mut decoded = Vec::with_capacity(compact.len() / 4 * 3);
    for chunk in compact.chunks_exact(4) {
        let a = sextet(chunk[0]).expect("base64 first sextet");
        let b = sextet(chunk[1]).expect("base64 second sextet");
        let c = if chunk[2] == b'=' {
            0
        } else {
            sextet(chunk[2]).expect("base64 third sextet")
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            sextet(chunk[3]).expect("base64 fourth sextet")
        };
        decoded.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            decoded.push((b << 4) | (c >> 2));
        }
        if chunk[3] != b'=' {
            decoded.push((c << 6) | d);
        }
    }
    decoded
}

fn expected_map(fixture: &Value) -> HashMap<CheetahString, CheetahString> {
    fixture["canonicalExtFields"]
        .as_object()
        .expect("canonicalExtFields object")
        .iter()
        .map(|(key, value)| {
            (
                CheetahString::from_string(key.clone()),
                CheetahString::from_string(value.as_str().expect("canonical string value").to_owned()),
            )
        })
        .collect()
}

fn assert_header_map<T>(command: &RemotingCommand, expected: &HashMap<CheetahString, CheetahString>)
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader,
{
    let header = command
        .decode_command_custom_header::<T>()
        .expect("Java fixture must decode into the Rust header");
    assert_eq!(header.to_map().expect("Rust header must re-encode"), *expected);
}

fn assert_fast_header_map<T>(command: &RemotingCommand, expected: &HashMap<CheetahString, CheetahString>)
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Default,
{
    let header = command
        .decode_command_custom_header_fast::<T>()
        .expect("Java fast fixture must decode through the Rust fast entrypoint");
    assert_eq!(header.to_map().expect("Rust fast header must re-encode"), *expected);
}

fn encode_rust_header<T>(
    code: i32,
    serialize_type: SerializeType,
    expected: &HashMap<CheetahString, CheetahString>,
) -> Vec<u8>
where
    T: FromMap<Target = T, Error = rocketmq_error::RocketMQError> + CommandCustomHeader + Send + Sync + 'static,
{
    let header = T::from(expected).expect("canonical Java map must construct the Rust header");
    let command = RemotingCommand::create_request_command_with_defaults(code, header, 501, serialize_type)
        .set_language(LanguageCode::RUST)
        .set_opaque(7);
    EncodedFrame::from_command(command)
        .expect("Rust header must encode through the production entrypoint")
        .into_bytes()
        .to_vec()
}

fn encode_registered_header(
    type_id: &str,
    code: i32,
    serialize_type: SerializeType,
    expected: &HashMap<CheetahString, CheetahString>,
) -> Vec<u8> {
    match type_id {
        "rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader" => {
            encode_rust_header::<PullMessageRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader" => {
            encode_rust_header::<SendMessageRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2" => {
            encode_rust_header::<SendMessageRequestHeaderV2>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader" => {
            encode_rust_header::<SendMessageResponseHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader" => {
            encode_rust_header::<GetConsumerStatusRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader" => {
            encode_rust_header::<QueryConsumeQueueRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader" => {
            encode_rust_header::<DeleteTopicFromNamesrvRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader" => {
            encode_rust_header::<ConsumeMessageDirectlyResultRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader" => {
            encode_rust_header::<GetLiteClientInfoRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader" => {
            encode_rust_header::<CleanBrokerDataRequestHeader>(code, serialize_type, expected)
        }
        "rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader" => {
            encode_rust_header::<NotificationRequestHeader>(code, serialize_type, expected)
        }
        other => panic!("unregistered golden fixture type: {other}"),
    }
}

#[test]
fn pinned_java_frames_decode_and_reencode_to_the_canonical_logical_map() {
    let index: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(FIXTURE_ROOT).join("golden/index.json")).expect("checked-in golden index"),
    )
    .expect("valid golden index JSON");

    for entry in index["fixtures"].as_array().expect("fixture index array") {
        let fixture_path = Path::new(FIXTURE_ROOT)
            .join("golden")
            .join(entry["file"].as_str().expect("fixture file"));
        let fixture: Value = serde_json::from_str(&fs::read_to_string(&fixture_path).expect("golden fixture"))
            .expect("valid golden fixture JSON");
        let frame = decode_base64(fixture["frameBase64"].as_str().expect("base64 frame"));
        assert_eq!(frame.len(), fixture["frameLength"].as_u64().unwrap() as usize);

        let mut input = BytesMut::from(frame.as_slice());
        let command = RemotingCommand::decode(&mut input)
            .unwrap_or_else(|error| panic!("{} failed envelope decode: {error}", fixture_path.display()))
            .unwrap_or_else(|| panic!("{} is incomplete", fixture_path.display()));
        assert!(input.is_empty(), "{} left trailing bytes", fixture_path.display());
        assert_eq!(command.code(), fixture["wireCodeValue"].as_i64().unwrap() as i32);
        assert_eq!(command.opaque(), 7);
        assert_eq!(command.version(), 501);

        let expected = expected_map(&fixture);
        let actual = command.ext_fields().expect("Java frame extension fields");
        assert_eq!(actual, &expected, "{} envelope map differs", fixture_path.display());

        match fixture["rustTypeId"].as_str().expect("Rust type ID") {
            "rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader" => {
                assert_header_map::<PullMessageRequestHeader>(&command, &expected);
                assert_fast_header_map::<PullMessageRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader" => {
                assert_header_map::<SendMessageRequestHeader>(&command, &expected);
                assert_fast_header_map::<SendMessageRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2" => {
                assert_header_map::<SendMessageRequestHeaderV2>(&command, &expected);
                assert_fast_header_map::<SendMessageRequestHeaderV2>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader" => {
                assert_header_map::<SendMessageResponseHeader>(&command, &expected);
                assert_fast_header_map::<SendMessageResponseHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader" => {
                assert_header_map::<GetConsumerStatusRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader" => {
                assert_header_map::<QueryConsumeQueueRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader" => {
                assert_header_map::<DeleteTopicFromNamesrvRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader" => {
                assert_header_map::<ConsumeMessageDirectlyResultRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader" => {
                assert_header_map::<GetLiteClientInfoRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanBrokerDataRequestHeader" => {
                assert_header_map::<CleanBrokerDataRequestHeader>(&command, &expected);
            }
            "rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader" => {
                assert_header_map::<NotificationRequestHeader>(&command, &expected);
            }
            other => panic!("unregistered golden fixture type: {other}"),
        }
    }
}

#[test]
fn fixture_manifest_pins_schema_and_empty_value_policy() {
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(FIXTURE_ROOT).join("manifest.json")).expect("checked-in manifest"),
    )
    .expect("valid fixture manifest JSON");

    assert_eq!(manifest["schema"]["mappedHeaderCount"], 143);
    assert_eq!(manifest["goldenIndex"]["fixtureCount"], 22);
    assert_eq!(manifest["legacyEmptyHeaders"].as_array().unwrap().len(), 1);
    assert_eq!(manifest["wirePolicies"]["logicalMapEmpty"], "preserve");
    assert_eq!(manifest["wirePolicies"]["jsonEmpty"], "preserve");
    assert_eq!(manifest["wirePolicies"]["rocketmqBinaryEmpty"], "normalize-to-absent");
}

#[test]
fn intentional_empty_headers_are_explicit_contract_entries() {
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(FIXTURE_ROOT).join("manifest.json")).expect("checked-in manifest"),
    )
    .expect("valid fixture manifest JSON");
    let entry = &manifest["legacyEmptyHeaders"][0];
    assert_eq!(
        entry["rustTypeId"],
        "rocketmq_protocol::protocol::header::empty_header::EmptyHeader"
    );
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(FIXTURE_ROOT).join(entry["file"].as_str().unwrap()))
            .expect("empty-header fixture"),
    )
    .expect("valid empty-header fixture JSON");
    assert_eq!(fixture["classification"], "intentional-empty-header");
    assert!(fixture["logicalMap"].is_null());
    assert_eq!(fixture["jsonObject"], serde_json::json!({}));
    assert!(EmptyHeader::default().to_map().is_none());
}

#[test]
fn rust_production_frames_preserve_the_java_canonical_maps() {
    let index: Value = serde_json::from_str(
        &fs::read_to_string(Path::new(FIXTURE_ROOT).join("golden/index.json")).expect("checked-in golden index"),
    )
    .expect("valid golden index JSON");
    let output_directory = std::env::var_os("ROCKETMQ_RUST_HEADER_GOLDEN_OUTPUT").map(std::path::PathBuf::from);
    if let Some(output) = &output_directory {
        fs::create_dir_all(output).expect("create Rust golden output directory");
    }

    for entry in index["fixtures"].as_array().expect("fixture index array") {
        let fixture_path = Path::new(FIXTURE_ROOT)
            .join("golden")
            .join(entry["file"].as_str().expect("fixture file"));
        let fixture: Value = serde_json::from_str(&fs::read_to_string(&fixture_path).expect("golden fixture"))
            .expect("valid golden fixture JSON");
        let expected = expected_map(&fixture);
        let serialize_type = match fixture["serializeType"].as_str().expect("serialize type") {
            "ROCKETMQ" => SerializeType::ROCKETMQ,
            "JSON" => SerializeType::JSON,
            other => panic!("unsupported serialize type: {other}"),
        };
        let frame = encode_registered_header(
            fixture["rustTypeId"].as_str().expect("Rust type ID"),
            fixture["requestCodeValue"].as_i64().unwrap() as i32,
            serialize_type,
            &expected,
        );

        if fixture["actualPath"] == "fast" && serialize_type == SerializeType::ROCKETMQ {
            assert_eq!(
                frame,
                decode_base64(fixture["frameBase64"].as_str().expect("Java fast frame")),
                "{} Rust direct frame differs from Java fast encoding",
                fixture_path.display()
            );
        }

        let mut input = BytesMut::from(frame.as_slice());
        let decoded = RemotingCommand::decode(&mut input)
            .expect("Rust frame decode")
            .expect("complete Rust frame");
        assert_eq!(decoded.ext_fields().expect("Rust frame fields"), &expected);
        assert!(input.is_empty());

        if let Some(output) = &output_directory {
            let id = fixture["id"].as_str().expect("fixture id");
            fs::write(output.join(format!("{id}.bin")), &frame).expect("write Rust golden frame");
        }
    }
}
