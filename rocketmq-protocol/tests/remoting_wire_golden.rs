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

//! Checked-in RocketMQ binary remoting frame compatibility fixture.

use std::collections::HashMap;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::EncodedFrame;
use rocketmq_protocol::RemotingCommand;

const JSON_REQUEST_FRAME: &[u8; 182] = b"\x00\x00\x00\xb2\x00\x00\x00\xac\
{\"code\":31,\"language\":\"JAVA\",\"version\":321,\"opaque\":16909060,\"flag\":0,\"remark\":\"json-request\",\"extFields\":{\"queueId\":\"7\",\"topic\":\"TopicA\"},\"serializeTypeCurrentRPC\":\"JSON\"}JR";
const JSON_RESPONSE_FRAME: &[u8; 178] = b"\x00\x00\x00\xae\x00\x00\x00\xa8\
{\"code\":0,\"language\":\"RUST\",\"version\":322,\"opaque\":-1234567,\"flag\":1,\"remark\":null,\"extFields\":{\"hasMsg\":\"true\",\"pollingFull\":\"false\"},\"serializeTypeCurrentRPC\":\"JSON\"}JS";
const ROCKETMQ_REQUEST_FRAME: &[u8; 77] = b"\
\x00\x00\x00\x49\x01\x00\x00\x42\
\x00\x1f\x00\x01\x43\x7f\xff\xff\xff\x00\x00\x00\x02\
\x00\x00\x00\x0ebinary-request\
\x00\x00\x00\x1f\x00\x07queueId\x00\x00\x00\x018\x00\x05topic\x00\x00\x00\x06TopicB\
\x00\xffR";
const ROCKETMQ_RESPONSE_FRAME: &[u8; 70] = b"\
\x00\x00\x00\x42\x01\x00\x00\x3c\
\x00\x00\x0c\x01\x44\x80\x00\x00\x00\x00\x00\x00\x03\
\x00\x00\x00\x00\x00\x00\x00\x27\
\x00\x06hasMsg\x00\x00\x00\x05false\
\x00\x0bpollingFull\x00\x00\x00\x05falseBR";

fn decode_hex_fixture(fixture: &str) -> Vec<u8> {
    let encoded = fixture
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(encoded.len() % 2, 0, "fixture must contain complete hex pairs");
    encoded
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).expect("hex high nibble");
            let low = (pair[1] as char).to_digit(16).expect("hex low nibble");
            ((high << 4) | low) as u8
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn next_case_value(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

#[test]
fn rocketmq_binary_frame_matches_the_checked_in_wire_contract() {
    let command = RemotingCommand::with_resolved_defaults(321, SerializeType::ROCKETMQ)
        .set_code(105)
        .set_language(LanguageCode::JAVA)
        .set_opaque(0x0102_0304)
        .set_flag(1)
        .set_remark("golden")
        .set_ext_fields(HashMap::from([
            (
                CheetahString::from_static_str("zeta"),
                CheetahString::from_static_str("last"),
            ),
            (
                CheetahString::from_static_str("alpha"),
                CheetahString::from_static_str("first"),
            ),
        ]))
        .set_body(Bytes::from_static(b"body"));
    let actual = EncodedFrame::from_command(command)
        .expect("golden command must encode")
        .into_bytes();
    let expected = decode_hex_fixture(include_str!("fixtures/remoting_command_rocketmq_v1.hex"));

    assert_eq!(
        actual.as_ref(),
        expected.as_slice(),
        "wire fixture changed; actual={}",
        encode_hex(&actual)
    );

    assert_eq!(i32::from_be_bytes(expected[0..4].try_into().unwrap()), 65);
    assert_eq!(
        u32::from_be_bytes(expected[4..8].try_into().unwrap()),
        0x0100_0039,
        "serialization type and 24-bit header length"
    );
    assert_eq!(i16::from_be_bytes(expected[8..10].try_into().unwrap()), 105);
    assert_eq!(expected[10], LanguageCode::JAVA.get_code());
    assert_eq!(i16::from_be_bytes(expected[11..13].try_into().unwrap()), 321);
    assert_eq!(i32::from_be_bytes(expected[13..17].try_into().unwrap()), 0x0102_0304);
    assert_eq!(i32::from_be_bytes(expected[17..21].try_into().unwrap()), 1);

    let mut encoded = BytesMut::from(expected.as_slice());
    let decoded = RemotingCommand::decode(&mut encoded)
        .expect("golden frame must decode")
        .expect("golden frame is complete");
    assert!(encoded.is_empty());
    assert_eq!(decoded.code(), 105);
    assert_eq!(decoded.language(), LanguageCode::JAVA);
    assert_eq!(decoded.version(), 321);
    assert_eq!(decoded.opaque(), 0x0102_0304);
    assert_eq!(decoded.flag(), 1);
    assert_eq!(decoded.remark().map(|value| value.as_str()), Some("golden"));
    let ext_fields = decoded.ext_fields().expect("golden frame ext fields");
    assert_eq!(ext_fields.get("alpha").map(CheetahString::as_str), Some("first"));
    assert_eq!(ext_fields.get("zeta").map(CheetahString::as_str), Some("last"));
    assert_eq!(decoded.body().map(Bytes::as_ref), Some(b"body".as_slice()));
    assert_eq!(decoded.get_serialize_type(), SerializeType::ROCKETMQ);
}

#[test]
fn deterministic_remoting_cases_round_trip_without_trailing_bytes() {
    const SEED: u64 = 0x524d_5150_524f_544f;
    let mut state = SEED;

    for case in 0..32 {
        let serialize_type = if case % 2 == 0 {
            SerializeType::JSON
        } else {
            SerializeType::ROCKETMQ
        };
        let version = (next_case_value(&mut state) & 0x7fff) as i32;
        let code = (next_case_value(&mut state) & 0x7fff) as i32;
        let opaque = next_case_value(&mut state) as i32;
        let flag = (next_case_value(&mut state) & 0x03) as i32;
        let body_len = (next_case_value(&mut state) & 0x3f) as usize;
        let body = (0..body_len)
            .map(|_| next_case_value(&mut state) as u8)
            .collect::<Vec<_>>();
        let remark = format!("seed-{SEED:016x}-case-{case}");
        let key = CheetahString::from_string(format!("key-{case}"));
        let value = CheetahString::from_string(format!("value-{}", next_case_value(&mut state)));
        let command = RemotingCommand::with_resolved_defaults(version, serialize_type)
            .set_code(code)
            .set_opaque(opaque)
            .set_flag(flag)
            .set_remark(remark.clone())
            .set_ext_fields(HashMap::from([(key.clone(), value.clone())]))
            .set_body(Bytes::copy_from_slice(&body));

        let frame = EncodedFrame::from_command(command)
            .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} failed to encode: {error}"))
            .into_bytes();
        let mut input = BytesMut::from(frame.as_ref());
        let decoded = RemotingCommand::decode(&mut input)
            .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} failed to decode: {error}"))
            .unwrap_or_else(|| panic!("seed={SEED:#018x} case={case} produced an incomplete frame"));

        assert!(input.is_empty(), "seed={SEED:#018x} case={case}");
        assert_eq!(decoded.version(), version, "seed={SEED:#018x} case={case}");
        assert_eq!(decoded.code(), code, "seed={SEED:#018x} case={case}");
        assert_eq!(decoded.opaque(), opaque, "seed={SEED:#018x} case={case}");
        assert_eq!(decoded.flag(), flag, "seed={SEED:#018x} case={case}");
        assert_eq!(
            decoded.remark().map(CheetahString::as_str),
            Some(remark.as_str()),
            "seed={SEED:#018x} case={case}"
        );
        assert_eq!(
            decoded.ext_fields().and_then(|fields| fields.get(&key)),
            Some(&value),
            "seed={SEED:#018x} case={case}"
        );
        assert_eq!(
            decoded.body().map(Bytes::as_ref),
            Some(body.as_slice()),
            "seed={SEED:#018x} case={case}"
        );
        assert_eq!(
            decoded.get_serialize_type(),
            serialize_type,
            "seed={SEED:#018x} case={case}"
        );
    }
}

#[test]
fn json_request_and_response_frames_match_exact_wire_bytes() {
    let request = RemotingCommand::with_resolved_defaults(321, SerializeType::JSON)
        .set_code(31)
        .set_language(LanguageCode::JAVA)
        .set_opaque(0x0102_0304)
        .set_remark("json-request")
        .set_command_custom_header(GetMinOffsetRequestHeader {
            topic: "TopicA".into(),
            queue_id: 7,
            topic_request_header: None,
        })
        .set_body(Bytes::from_static(b"JR"));
    let response = RemotingCommand::with_resolved_defaults(322, SerializeType::JSON)
        .set_code(0)
        .set_language(LanguageCode::RUST)
        .set_opaque(-1_234_567)
        .mark_response_type()
        .set_command_custom_header(NotificationResponseHeader {
            has_msg: true,
            ..Default::default()
        })
        .set_body(Bytes::from_static(b"JS"));

    let actual_request = EncodedFrame::from_command(request)
        .expect("JSON request must encode")
        .into_bytes();
    let actual_response = EncodedFrame::from_command(response)
        .expect("JSON response must encode")
        .into_bytes();

    assert_eq!(actual_request.as_ref(), JSON_REQUEST_FRAME);
    assert_eq!(actual_response.as_ref(), JSON_RESPONSE_FRAME);
    assert_eq!(&JSON_REQUEST_FRAME[4..8], &[0x00, 0x00, 0x00, 0xac]);
    assert_eq!(&JSON_RESPONSE_FRAME[4..8], &[0x00, 0x00, 0x00, 0xa8]);

    let mut request_input = BytesMut::from(JSON_REQUEST_FRAME.as_slice());
    let request = RemotingCommand::decode(&mut request_input)
        .expect("JSON request fixture must decode")
        .expect("JSON request fixture must be complete");
    assert!(request_input.is_empty());
    assert_eq!(request.code(), 31);
    assert_eq!(request.language(), LanguageCode::JAVA);
    assert_eq!(request.version(), 321);
    assert_eq!(request.opaque(), 0x0102_0304);
    assert_eq!(request.flag(), 0);
    assert_eq!(request.remark().map(CheetahString::as_str), Some("json-request"));
    assert_eq!(
        request.ext_fields(),
        Some(&HashMap::from([
            (CheetahString::from("queueId"), CheetahString::from("7")),
            (CheetahString::from("topic"), CheetahString::from("TopicA")),
        ]))
    );
    assert_eq!(request.body().map(Bytes::as_ref), Some(b"JR".as_slice()));
    assert_eq!(request.get_serialize_type(), SerializeType::JSON);
    let header = request
        .decode_command_custom_header::<GetMinOffsetRequestHeader>()
        .expect("typed JSON request header must decode");
    assert_eq!(header.topic, "TopicA");
    assert_eq!(header.queue_id, 7);

    let mut response_input = BytesMut::from(JSON_RESPONSE_FRAME.as_slice());
    let response = RemotingCommand::decode(&mut response_input)
        .expect("JSON response fixture must decode")
        .expect("JSON response fixture must be complete");
    assert!(response_input.is_empty());
    assert_eq!(response.code(), 0);
    assert_eq!(response.language(), LanguageCode::RUST);
    assert_eq!(response.version(), 322);
    assert_eq!(response.opaque(), -1_234_567);
    assert_eq!(response.flag(), 1);
    assert!(response.is_response_type());
    assert!(!response.is_oneway_rpc());
    assert!(response.remark().is_none());
    assert_eq!(
        response.ext_fields(),
        Some(&HashMap::from([
            (CheetahString::from("hasMsg"), CheetahString::from("true")),
            (CheetahString::from("pollingFull"), CheetahString::from("false"),),
        ]))
    );
    assert_eq!(response.body().map(Bytes::as_ref), Some(b"JS".as_slice()));
    assert_eq!(response.get_serialize_type(), SerializeType::JSON);
    let header = response
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("typed JSON response header must decode");
    assert!(header.has_msg);
    assert!(!header.polling_full);
}

#[test]
fn rocketmq_request_and_response_frames_match_exact_wire_bytes() {
    let request = RemotingCommand::with_resolved_defaults(323, SerializeType::ROCKETMQ)
        .set_code(31)
        .set_language(LanguageCode::JAVA)
        .set_opaque(i32::MAX)
        .mark_oneway_rpc()
        .set_remark("binary-request")
        .set_command_custom_header(GetMinOffsetRequestHeader {
            topic: "TopicB".into(),
            queue_id: 8,
            topic_request_header: None,
        })
        .set_body(Bytes::from_static(&[0x00, 0xff, 0x52]));
    let response = RemotingCommand::with_resolved_defaults(324, SerializeType::ROCKETMQ)
        .set_code(0)
        .set_language(LanguageCode::RUST)
        .set_opaque(i32::MIN)
        .mark_response_type()
        .mark_oneway_rpc()
        .set_command_custom_header(NotificationResponseHeader {
            has_msg: false,
            ..Default::default()
        })
        .set_body(Bytes::from_static(b"BR"));

    let actual_request = EncodedFrame::from_command(request)
        .expect("ROCKETMQ request must encode")
        .into_bytes();
    let actual_response = EncodedFrame::from_command(response)
        .expect("ROCKETMQ response must encode")
        .into_bytes();

    assert_eq!(actual_request.as_ref(), ROCKETMQ_REQUEST_FRAME);
    assert_eq!(actual_response.as_ref(), ROCKETMQ_RESPONSE_FRAME);
    assert_eq!(&ROCKETMQ_REQUEST_FRAME[4..8], &[0x01, 0x00, 0x00, 0x42]);
    assert_eq!(&ROCKETMQ_RESPONSE_FRAME[4..8], &[0x01, 0x00, 0x00, 0x3c]);

    let mut request_input = BytesMut::from(ROCKETMQ_REQUEST_FRAME.as_slice());
    let request = RemotingCommand::decode(&mut request_input)
        .expect("ROCKETMQ request fixture must decode")
        .expect("ROCKETMQ request fixture must be complete");
    assert!(request_input.is_empty());
    assert_eq!(request.code(), 31);
    assert_eq!(request.language(), LanguageCode::JAVA);
    assert_eq!(request.version(), 323);
    assert_eq!(request.opaque(), i32::MAX);
    assert_eq!(request.flag(), 2);
    assert!(!request.is_response_type());
    assert!(request.is_oneway_rpc());
    assert_eq!(request.remark().map(CheetahString::as_str), Some("binary-request"));
    assert_eq!(
        request.ext_fields(),
        Some(&HashMap::from([
            (CheetahString::from("queueId"), CheetahString::from("8")),
            (CheetahString::from("topic"), CheetahString::from("TopicB")),
        ]))
    );
    assert_eq!(request.body().map(Bytes::as_ref), Some([0x00, 0xff, 0x52].as_slice()));
    assert_eq!(request.get_serialize_type(), SerializeType::ROCKETMQ);
    let header = request
        .decode_command_custom_header::<GetMinOffsetRequestHeader>()
        .expect("typed ROCKETMQ request header must decode");
    assert_eq!(header.topic, "TopicB");
    assert_eq!(header.queue_id, 8);

    let mut response_input = BytesMut::from(ROCKETMQ_RESPONSE_FRAME.as_slice());
    let response = RemotingCommand::decode(&mut response_input)
        .expect("ROCKETMQ response fixture must decode")
        .expect("ROCKETMQ response fixture must be complete");
    assert!(response_input.is_empty());
    assert_eq!(response.code(), 0);
    assert_eq!(response.language(), LanguageCode::RUST);
    assert_eq!(response.version(), 324);
    assert_eq!(response.opaque(), i32::MIN);
    assert_eq!(response.flag(), 3);
    assert!(response.is_response_type());
    assert!(response.is_oneway_rpc());
    assert!(response.remark().is_none());
    assert_eq!(
        response.ext_fields(),
        Some(&HashMap::from([
            (CheetahString::from("hasMsg"), CheetahString::from("false")),
            (CheetahString::from("pollingFull"), CheetahString::from("false"),),
        ]))
    );
    assert_eq!(response.body().map(Bytes::as_ref), Some(b"BR".as_slice()));
    assert_eq!(response.get_serialize_type(), SerializeType::ROCKETMQ);
    let header = response
        .decode_command_custom_header::<NotificationResponseHeader>()
        .expect("typed ROCKETMQ response header must decode");
    assert!(!header.has_msg);
    assert!(!header.polling_full);
}
