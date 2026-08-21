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

//! Public-path, defaults, flags, and decoder cursor contracts for remoting commands.

use std::any::TypeId;
use std::collections::HashMap;

use bytes::BufMut;
use bytes::BytesMut;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand as CanonicalRemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::RemotingCommand as ProtocolRemotingCommand;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::RemotingCommand as RootRemotingCommand;

const TRAILING_BYTES: &[u8] = &[0xde, 0xad, 0xbe, 0xef, 0x7f];
const FALLBACK_ONLY_JSON_HEADER: &[u8; 151] =
    b"{\"code\":10,\"language\":\"RUST\",\"version\":501,\"opaque\":7,\"flag\":0,\"remark\":null,\"extFields\":{\"queueId\":\"1\"},\"extra\":true,\"serializeTypeCurrentRPC\":\"JSON\"}";
const FALLBACK_ONLY_JSON_FRAME: &[u8; 159] = b"\x00\x00\x00\x9b\x00\x00\x00\x97\
{\"code\":10,\"language\":\"RUST\",\"version\":501,\"opaque\":7,\"flag\":0,\"remark\":null,\"extFields\":{\"queueId\":\"1\"},\"extra\":true,\"serializeTypeCurrentRPC\":\"JSON\"}";

fn complete_frame(marked_header_length: i32, header: &[u8]) -> Vec<u8> {
    let mut frame = BytesMut::with_capacity(8 + header.len());
    frame.put_i32(i32::try_from(4 + header.len()).expect("test frame length must fit i32"));
    frame.put_i32(marked_header_length);
    frame.extend_from_slice(header);
    frame.to_vec()
}

fn complete_frame_for(header: &[u8], serialize_type: SerializeType) -> Vec<u8> {
    let header_length = i32::try_from(header.len()).expect("test header length must fit i32");
    complete_frame(
        CanonicalRemotingCommand::mark_serialize_type(header_length, serialize_type),
        header,
    )
}

fn binary_header(extension_fields: &[u8]) -> Vec<u8> {
    let mut header = BytesMut::new();
    header.put_i16(1);
    header.put_u8(LanguageCode::RUST.get_code());
    header.put_i16(0);
    header.put_i32(7);
    header.put_i32(0);
    header.put_i32(0);
    header.put_i32(i32::try_from(extension_fields.len()).expect("test extension fields must fit i32"));
    header.extend_from_slice(extension_fields);
    header.to_vec()
}

fn assert_complete_frame_is_consumed_on_error(name: &str, frame: &[u8]) {
    let mut input = BytesMut::with_capacity(frame.len() + TRAILING_BYTES.len());
    input.extend_from_slice(frame);
    input.extend_from_slice(TRAILING_BYTES);

    let error = match CanonicalRemotingCommand::decode(&mut input) {
        Err(error) => error,
        Ok(None) => panic!("{name} must be a complete malformed frame"),
        Ok(Some(_)) => panic!("{name} unexpectedly decoded"),
    };

    assert_eq!(input.as_ref(), TRAILING_BYTES, "{name}: {error}");
}

fn decode_json_header(header: &str) -> CanonicalRemotingCommand {
    let mut input = BytesMut::from(header.as_bytes());
    let header_length = input.len();
    let command = CanonicalRemotingCommand::header_decode(&mut input, header_length, SerializeType::JSON)
        .expect("JSON header must decode")
        .expect("JSON header must produce a command");
    assert!(input.is_empty());
    command
}

#[test]
fn public_paths_name_the_same_remoting_command_type_and_behavior() {
    assert_eq!(
        TypeId::of::<RootRemotingCommand>(),
        TypeId::of::<CanonicalRemotingCommand>()
    );
    assert_eq!(
        TypeId::of::<ProtocolRemotingCommand>(),
        TypeId::of::<CanonicalRemotingCommand>()
    );

    let root = RootRemotingCommand::with_resolved_defaults(501, SerializeType::JSON)
        .set_code(105)
        .set_opaque(0x1122_3344);
    let canonical: CanonicalRemotingCommand = root.clone();
    let protocol: ProtocolRemotingCommand = canonical.clone();

    for command in [&root, &canonical, &protocol] {
        assert_eq!(command.code(), 105);
        assert_eq!(command.version(), 501);
        assert_eq!(command.opaque(), 0x1122_3344);
        assert_eq!(command.flag(), 0);
    }
}

#[test]
fn explicit_defaults_serde_names_and_flag_bits_are_stable() {
    let command = CanonicalRemotingCommand::with_resolved_defaults(0, SerializeType::JSON).set_opaque(0x1122_3344);

    assert_eq!(command.code(), 0);
    assert_eq!(command.language(), LanguageCode::RUST);
    assert_eq!(command.version(), 0);
    assert_eq!(command.opaque(), 0x1122_3344);
    assert_eq!(command.flag(), 0);
    assert!(command.remark().is_none());
    assert!(command.ext_fields().is_none());
    assert!(command.body().is_none());
    assert_eq!(command.get_serialize_type(), SerializeType::JSON);
    assert_eq!(
        serde_json::to_string(&command).expect("default command must serialize"),
        r#"{"code":0,"language":"RUST","version":0,"opaque":287454020,"flag":0,"remark":null,"extFields":null,"serializeTypeCurrentRPC":"JSON"}"#
    );

    let response = command.clone().mark_response_type();
    let oneway = command.clone().mark_oneway_rpc();
    let combined = command.mark_response_type().mark_oneway_rpc();

    assert_eq!(response.flag(), 1);
    assert!(response.is_response_type());
    assert!(!response.is_oneway_rpc());
    assert_eq!(oneway.flag(), 2);
    assert!(!oneway.is_response_type());
    assert!(oneway.is_oneway_rpc());
    assert_eq!(combined.flag(), 3);
    assert!(combined.is_response_type());
    assert!(combined.is_oneway_rpc());
}

#[test]
fn json_absent_null_and_empty_extension_fields_remain_distinct() {
    let absent = decode_json_header(
        r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"serializeTypeCurrentRPC":"JSON"}"#,
    );
    let null = decode_json_header(
        r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":null,"serializeTypeCurrentRPC":"JSON"}"#,
    );
    let empty = decode_json_header(
        r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{},"serializeTypeCurrentRPC":"JSON"}"#,
    );

    assert!(absent.remark().is_none());
    assert!(absent.ext_fields().is_none());
    assert!(null.remark().is_none());
    assert!(null.ext_fields().is_none());
    assert_eq!(empty.ext_fields(), Some(&HashMap::new()));
}

#[test]
fn complete_malformed_frames_with_serialization_header_consume_only_the_declared_frame() {
    let invalid_marked_header_length = complete_frame(
        CanonicalRemotingCommand::mark_serialize_type(1, SerializeType::JSON),
        &[],
    );
    let invalid_serialize_type = complete_frame(0x0200_0000, &[]);
    let malformed_json = complete_frame_for(b"{", SerializeType::JSON);

    let invalid_utf8 = binary_header(&[0x00, 0x01, 0xff, 0x00, 0x00, 0x00, 0x01, b'v']);
    let truncated_key = binary_header(&[0x00, 0x02, b'k']);
    let overlong_value = binary_header(&[0x00, 0x01, b'k', 0x00, 0x00, 0x00, 0x04, b'v']);
    let malformed_binary = [
        ("invalid UTF-8 extension field", invalid_utf8),
        ("truncated extension-field key", truncated_key),
        ("overlong extension-field value", overlong_value),
    ];

    assert_complete_frame_is_consumed_on_error("invalid marked header length", &invalid_marked_header_length);
    assert_complete_frame_is_consumed_on_error("invalid serialization type", &invalid_serialize_type);
    assert_complete_frame_is_consumed_on_error("malformed JSON header", &malformed_json);
    for (name, header) in malformed_binary {
        let frame = complete_frame_for(&header, SerializeType::ROCKETMQ);
        assert_complete_frame_is_consumed_on_error(name, &frame);
    }
}

#[test]
fn complete_outer_frames_shorter_than_serialization_header_error_without_consumption() {
    for announced_payload_size in 0_u8..=3 {
        let payload = vec![0xa0 + announced_payload_size; announced_payload_size as usize];
        let mut input = BytesMut::with_capacity(4 + payload.len() + TRAILING_BYTES.len());
        input.put_i32(i32::from(announced_payload_size));
        input.extend_from_slice(&payload);
        input.extend_from_slice(TRAILING_BYTES);
        let original = input.clone();

        // Compatibility exception: a complete envelope shorter than the four-byte serialization
        // header errors before the decoder splits the declared frame, so the full input is retained.
        assert!(CanonicalRemotingCommand::decode(&mut input).is_err());
        assert_eq!(input, original, "announced payload size {announced_payload_size}");
    }
}

#[test]
fn fallback_only_complete_json_frame_consumes_only_the_declared_frame() {
    assert_eq!(&FALLBACK_ONLY_JSON_FRAME[8..], FALLBACK_ONLY_JSON_HEADER);
    let mut input = BytesMut::with_capacity(FALLBACK_ONLY_JSON_FRAME.len() + TRAILING_BYTES.len());
    input.extend_from_slice(FALLBACK_ONLY_JSON_FRAME);
    input.extend_from_slice(TRAILING_BYTES);

    let command = CanonicalRemotingCommand::decode(&mut input)
        .expect("fallback-only complete JSON frame must decode")
        .expect("fallback-only JSON frame must be complete");

    assert_eq!(command.code(), 10);
    assert_eq!(command.language(), LanguageCode::RUST);
    assert_eq!(command.version(), 501);
    assert_eq!(command.opaque(), 7);
    assert_eq!(command.flag(), 0);
    assert!(command.remark().is_none());
    assert_eq!(
        command.ext_fields(),
        Some(&HashMap::from([("queueId".into(), "1".into())]))
    );
    assert!(command.body().is_none());
    assert_eq!(command.get_serialize_type(), SerializeType::JSON);
    assert_eq!(input.as_ref(), TRAILING_BYTES);
}

#[test]
fn direct_header_decode_fallback_cursor_difference_is_recorded() {
    let mut input = BytesMut::from(FALLBACK_ONLY_JSON_HEADER.as_slice());
    let header_length = input.len();

    let command = CanonicalRemotingCommand::header_decode(&mut input, header_length, SerializeType::JSON)
        .expect("fallback-only JSON header must decode")
        .expect("fallback-only JSON header must produce a command");

    assert_eq!(command.code(), 10);
    assert_eq!(command.language(), LanguageCode::RUST);
    assert_eq!(command.version(), 501);
    assert_eq!(command.opaque(), 7);
    assert_eq!(command.flag(), 0);
    assert!(command.remark().is_none());
    assert_eq!(
        command.ext_fields(),
        Some(&HashMap::from([("queueId".into(), "1".into())]))
    );
    assert!(command.body().is_none());
    assert_eq!(command.get_serialize_type(), SerializeType::JSON);

    // Known compatibility debt: direct fallback decoding has feature-dependent cursor behavior.
    // Outer `decode` parity, including trailing-byte preservation, is tested separately above.
    #[cfg(not(feature = "simd"))]
    assert_eq!(input.as_ref(), FALLBACK_ONLY_JSON_HEADER);
    #[cfg(feature = "simd")]
    assert!(input.is_empty());
}

#[test]
fn incomplete_outer_frame_does_not_consume_or_modify_input() {
    let mut input = BytesMut::from(&[0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, b'{'][..]);
    let original = input.clone();

    let decoded = CanonicalRemotingCommand::decode(&mut input).expect("incomplete frame must not be an error");

    assert!(decoded.is_none());
    assert_eq!(input, original);
}
