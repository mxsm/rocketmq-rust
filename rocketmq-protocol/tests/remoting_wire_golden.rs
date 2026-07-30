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
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::EncodedFrame;
use rocketmq_protocol::RemotingCommand;

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
