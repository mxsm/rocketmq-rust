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

use std::collections::HashSet;

use super::*;

fn json_header_with_ext_fields(ext_fields: &str) -> BytesMut {
    BytesMut::from(
        format!(
            r#"{{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{ext_fields},"serializeTypeCurrentRPC":"JSON"}}"#
        )
        .as_bytes(),
    )
}

fn complete_frame(header: &[u8], serialize_type: SerializeType) -> BytesMut {
    let header_length = i32::try_from(header.len()).expect("test header length must fit the wire format");
    let total_length = header_length
        .checked_add(4)
        .expect("test payload length must fit the wire format");
    let mut frame = BytesMut::with_capacity(8 + header.len());
    frame.put_i32(total_length);
    frame.put_i32(RemotingCommand::mark_serialize_type(header_length, serialize_type));
    frame.extend_from_slice(header);
    frame
}

fn binary_header(extension_fields: &[u8]) -> Vec<u8> {
    let extension_fields_length =
        i32::try_from(extension_fields.len()).expect("test extension fields must fit the wire format");
    let mut header = BytesMut::with_capacity(21 + extension_fields.len());
    header.put_i16(10);
    header.put_u8(LanguageCode::RUST.get_code());
    header.put_i16(501);
    header.put_i32(7);
    header.put_i32(1);
    header.put_i32(0);
    header.put_i32(extension_fields_length);
    header.extend_from_slice(extension_fields);
    header.to_vec()
}

#[test]
fn request_id_sequence_is_unique_under_contention() {
    const THREADS: usize = 8;
    const IDS_PER_THREAD: usize = 1_024;
    let counter = Arc::new(AtomicI32::new(0));
    let handles = (0..THREADS)
        .map(|_| {
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                (0..IDS_PER_THREAD)
                    .map(|_| next_request_id_from(&counter))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let ids = handles
        .into_iter()
        .flat_map(|handle| handle.join().expect("request ID worker must finish"))
        .collect::<HashSet<_>>();

    assert_eq!(ids.len(), THREADS * IDS_PER_THREAD);
    assert_eq!(ids.iter().copied().min(), Some(0));
    assert_eq!(ids.iter().copied().max(), Some((THREADS * IDS_PER_THREAD - 1) as i32));
}

#[test]
fn request_id_sequence_preserves_signed_wrap_behavior() {
    let counter = AtomicI32::new(i32::MAX);

    assert_eq!(next_request_id_from(&counter), i32::MAX);
    assert_eq!(next_request_id_from(&counter), i32::MIN);
}

#[test]
fn request_id_storage_is_a_direct_static_atomic() {
    let source = include_str!("../remoting_command.rs");
    let declaration = source
        .lines()
        .find(|line| line.starts_with("static REQUEST_ID:"))
        .expect("request ID declaration");

    assert_eq!(declaration, "static REQUEST_ID: AtomicI32 = AtomicI32::new(0);");
}

#[derive(Debug, Default, PartialEq, Eq)]
struct TestCustomHeader {
    value: i32,
}

#[derive(Debug, Default, rocketmq_macros::RequestHeaderCodecV3)]
#[header(type_id = "rocketmq_protocol::tests::RawStrictAliasHeader")]
struct RawStrictAliasHeader {
    #[header(key = "canonical", alias = "legacy")]
    value: Option<CheetahString>,
}

impl CommandCustomHeader for TestCustomHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str("value"),
            CheetahString::from_string(self.value.to_string()),
        )]))
    }
}

impl FromMap for TestCustomHeader {
    type Error = rocketmq_error::RocketMQError;
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self, Self::Error> {
        let value = map
            .get(&CheetahString::from_static_str("value"))
            .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("missing value test custom header field"))?
            .parse::<i32>()
            .map_err(|error| {
                rocketmq_error::RocketMQError::illegal_argument(format!(
                    "invalid value test custom header field: {error}"
                ))
            })?;
        Ok(Self { value })
    }
}

#[derive(Debug)]
struct OtherCustomHeader;

impl CommandCustomHeader for OtherCustomHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::new())
    }
}

#[derive(Debug)]
struct NoPreflightLengthHeader;

impl CommandCustomHeader for NoPreflightLengthHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([("field".into(), "value".into())]))
    }

    fn encoded_len_hint(&self) -> usize {
        panic!("ROCKETMQ frame encoding must not preflight custom-header lengths")
    }
}

#[test]
fn test_remoting_command() {
    let command = RemotingCommand::create_remoting_command(1)
        .set_code(1)
        .set_language(LanguageCode::JAVA)
        .set_opaque(1)
        .set_flag(1)
        .set_ext_fields(HashMap::new())
        .set_remark_option(Some("remark".to_string()));

    assert_eq!(
        format!(
            "{{\"code\":1,\"language\":\"JAVA\",\"version\":{},\"opaque\":1,\"flag\":1,\"remark\":\"remark\",\
             \"extFields\":{{}},\"serializeTypeCurrentRPC\":\"JSON\"}}",
            crate::version::CURRENT_VERSION as i32
        ),
        serde_json::to_string(&command).unwrap()
    );
}

#[test]
fn add_ext_field_initializes_absent() {
    let mut command = RemotingCommand::create_success_response_command();
    assert!(command.ext_fields.is_absent());

    command.add_ext_field("key", "value");

    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("key"))
            .map(CheetahString::as_str),
        Some("value")
    );
}

#[test]
fn add_ext_field_if_not_exist_initializes_absent() {
    let mut command = RemotingCommand::create_success_response_command();
    assert!(command.ext_fields.is_absent());

    command.add_ext_field_if_not_exist("key", "first");
    command.add_ext_field_if_not_exist("key", "second");

    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("key"))
            .map(CheetahString::as_str),
        Some("first")
    );
}

#[test]
fn add_ext_field_preserves_materialized_fields() {
    let mut command = RemotingCommand::create_success_response_command().set_ext_fields(HashMap::from([(
        CheetahString::from_static_str("existing"),
        CheetahString::from_static_str("preserved"),
    )]));

    command.add_ext_field("added", "value");

    let fields = command.ext_fields().expect("extension fields should exist");
    assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
    assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
}

#[test]
fn add_ext_field_preserves_json_raw_fields() {
    let mut header = json_header_with_ext_fields(r#"{"existing":"preserved"}"#);
    let header_length = header.len();
    let mut command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
        .unwrap()
        .unwrap();
    assert!(command.ext_fields.is_json_raw());

    command.add_ext_field("added", "value");

    let fields = command.ext_fields().expect("extension fields should exist");
    assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
    assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
}

#[test]
fn add_ext_field_preserves_rocketmq_raw_fields() {
    let mut source = RemotingCommand::create_success_response_command()
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("existing"),
            CheetahString::from_static_str("preserved"),
        )]));
    let mut encoded = BytesMut::new();
    source.try_fast_header_encode(&mut encoded).unwrap();
    let mut command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
    assert!(command.ext_fields.is_rocketmq_raw());

    command.add_ext_field("added", "value");

    let fields = command.ext_fields().expect("extension fields should exist");
    assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
    assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
}

#[test]
fn add_ext_field_coexists_with_typed_header() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let mut command = RemotingCommand::create_success_response_command_with_header(TestCustomHeader { value: 7 })
            .set_serialize_type(serialize_type);
        command.add_ext_field("dynamic", "preserved");
        let mut encoded = BytesMut::new();

        command.try_fast_header_encode(&mut encoded).unwrap();
        let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("value"))
                .map(CheetahString::as_str),
            Some("7")
        );
        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("dynamic"))
                .map(CheetahString::as_str),
            Some("preserved")
        );
    }
}

#[test]
fn test_mark_serialize_type() {
    let i = RemotingCommand::mark_serialize_type(261, SerializeType::JSON);
    assert_eq!(i, 261);

    let i = RemotingCommand::mark_serialize_type(16777215, SerializeType::JSON);
    assert_eq!(i, 16777215);

    println!("i={}", RemotingCommand::default().opaque);
    println!("i={}", RemotingCommand::default().opaque);
    println!("i={}", RemotingCommand::default().opaque);
    println!("i={}", RemotingCommand::default().opaque);
}

#[test]
fn frame_length_checks_each_wire_boundary_and_limit_plus_one() {
    const MAX_HEADER_LENGTH: usize = 0x00ff_ffff;
    let (total, marked) =
        RemotingCommand::checked_frame_lengths(MAX_HEADER_LENGTH, 0, SerializeType::ROCKETMQ).unwrap();
    assert_eq!(total, 4 + MAX_HEADER_LENGTH as i32);
    assert_eq!(parse_header_length(marked), MAX_HEADER_LENGTH);
    assert!(RemotingCommand::checked_frame_lengths(MAX_HEADER_LENGTH + 1, 0, SerializeType::ROCKETMQ).is_err());

    let max_body = i32::MAX as usize - 4;
    let (total, _) = RemotingCommand::checked_frame_lengths(0, max_body, SerializeType::JSON).unwrap();
    assert_eq!(total, i32::MAX);
    assert!(RemotingCommand::checked_frame_lengths(0, max_body + 1, SerializeType::JSON).is_err());
}

#[test]
fn structured_frame_fixture_decodes_canonical_json_extension_fields() {
    let header = br#"{"code":10,"language":"RUST","version":501,"opaque":7,"flag":0,"remark":null,"extFields":{"queueId":"1"},"serializeTypeCurrentRPC":"JSON"}"#;
    let mut frame = complete_frame(header, SerializeType::JSON);
    frame.extend_from_slice(b"next-frame");

    let command = RemotingCommand::decode(&mut frame)
        .expect("canonical JSON frame must decode")
        .expect("canonical JSON frame is complete");

    assert_eq!(frame.as_ref(), b"next-frame");
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("queueId"))
            .map(CheetahString::as_str),
        Some("1")
    );
}

#[test]
fn structured_frame_fixture_decodes_flexible_json_extension_fields() {
    let header = r#" { "version" : 501, "code" : -7, "language" : "JAVA", "opaque" : 9, "flag" : 1, "remark" : "火箭", "extFields" : { "escaped\u004bey" : "quote\"slash\\solidus\/", "unicode" : "中🚀" }, "serializeTypeCurrentRPC" : "JSON" } "#;
    let mut frame = complete_frame(header.as_bytes(), SerializeType::JSON);

    let command = RemotingCommand::decode(&mut frame)
        .expect("flexible JSON frame must decode")
        .expect("flexible JSON frame is complete");

    assert!(frame.is_empty());
    assert_eq!(command.remark().map(CheetahString::as_str), Some("火箭"));
    let fields = command.ext_fields().expect("flexible JSON extension fields");
    assert_eq!(
        fields.get("escapedKey").map(CheetahString::as_str),
        Some("quote\"slash\\solidus/")
    );
    assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("中🚀"));
}

#[test]
fn structured_frame_fixture_decodes_two_rocketmq_extension_fields() {
    let extension_fields = [
        0, 5, b'a', b'l', b'p', b'h', b'a', 0, 0, 0, 5, b'f', b'i', b'r', b's', b't', 0, 4, b'z', b'e', b't', b'a', 0,
        0, 0, 4, b'l', b'a', b's', b't',
    ];
    let mut frame = complete_frame(&binary_header(&extension_fields), SerializeType::ROCKETMQ);

    let command = RemotingCommand::decode(&mut frame)
        .expect("ROCKETMQ frame must decode")
        .expect("ROCKETMQ frame is complete");

    assert!(frame.is_empty());
    let fields = command.ext_fields().expect("ROCKETMQ extension fields");
    assert_eq!(fields.get("alpha").map(CheetahString::as_str), Some("first"));
    assert_eq!(fields.get("zeta").map(CheetahString::as_str), Some("last"));
}

#[test]
fn incomplete_outer_frame_is_left_untouched() {
    let mut frame = BytesMut::from(&[0, 0, 0, 8, 0, 0, 0, 0][..]);
    let before = frame.clone();

    assert!(RemotingCommand::decode(&mut frame).unwrap().is_none());
    assert_eq!(frame, before);
}

#[test]
fn malformed_input_regressions_reject_complete_extension_field_frames() {
    let invalid_json_header = [
        br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":""#.as_slice(),
        &[0xff],
        br#""},"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
    ]
    .concat();
    let unterminated_json_header =
        br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":"value"#;
    assert_eq!(unterminated_json_header.len(), 99);
    let invalid_binary_fields = [0, 1, 0xff, 0, 0, 0, 1, b'v'];
    let overlong_binary_fields = [0, 1, b'k', 0, 0, 4, 0];
    let mut cases = [
        (
            "invalid JSON UTF-8",
            complete_frame(&invalid_json_header, SerializeType::JSON),
        ),
        (
            "unterminated JSON extension field",
            complete_frame(unterminated_json_header, SerializeType::JSON),
        ),
        (
            "invalid ROCKETMQ UTF-8",
            complete_frame(&binary_header(&invalid_binary_fields), SerializeType::ROCKETMQ),
        ),
        (
            "overlong ROCKETMQ extension-field value",
            complete_frame(&binary_header(&overlong_binary_fields), SerializeType::ROCKETMQ),
        ),
    ];

    for (name, frame) in &mut cases {
        assert!(RemotingCommand::decode(frame).is_err(), "{name}");
        assert!(frame.is_empty(), "complete malformed frame must be consumed: {name}");
    }
}

#[test]
fn rocketmq_frame_encoding_does_not_preflight_custom_header_lengths() {
    let mut command =
        RemotingCommand::create_request_command(1, NoPreflightLengthHeader).set_serialize_type(SerializeType::ROCKETMQ);
    let mut encoded = BytesMut::new();

    command.try_fast_header_encode(&mut encoded).unwrap();

    let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
    assert_eq!(
        decoded
            .ext_fields()
            .and_then(|fields| fields.get("field"))
            .map(CheetahString::as_str),
        Some("value")
    );
}

#[test]
#[allow(
    deprecated,
    reason = "verifies the deprecated unchecked compatibility alias preserves its typed error result"
)]
fn try_read_custom_header_ref_reports_missing_header() {
    let command = RemotingCommand::create_remoting_command(1);

    let error = command.try_read_custom_header_ref::<TestCustomHeader>().unwrap_err();

    assert!(error.to_string().contains("missing"));
    assert!(command.read_custom_header_ref_unchecked::<TestCustomHeader>().is_err());
}

#[test]
#[allow(
    deprecated,
    reason = "verifies the deprecated unchecked compatibility alias preserves its typed error result"
)]
fn try_read_custom_header_ref_reports_type_mismatch() {
    let command = RemotingCommand::create_request_command(1, TestCustomHeader::default());

    let error = command.try_read_custom_header_ref::<OtherCustomHeader>().unwrap_err();

    assert!(error.to_string().contains("type mismatch"));
    assert!(command.read_custom_header_ref_unchecked::<OtherCustomHeader>().is_err());
}

#[test]
fn try_read_custom_header_mut_updates_expected_header() {
    let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });

    let header = command.try_read_custom_header_mut::<TestCustomHeader>().unwrap();
    header.value = 9;

    let header = command.try_read_custom_header_ref::<TestCustomHeader>().unwrap();
    assert_eq!(header.value, 9);
}

#[test]
fn clone_preserves_concrete_header_without_eagerly_hiding_dynamic_collisions() {
    let original = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 }).set_ext_fields(
        HashMap::from([(CheetahString::from("hook-field"), CheetahString::from("preserved"))]),
    );
    let mut cloned = original.clone();

    assert_eq!(
        cloned.try_read_custom_header_ref::<TestCustomHeader>().unwrap().value,
        7
    );
    assert_eq!(
        cloned
            .ext_fields()
            .and_then(|fields| fields.get("hook-field"))
            .map(CheetahString::as_str),
        Some("preserved")
    );
    assert!(cloned.ext_fields().is_none_or(|fields| !fields.contains_key("value")));
    cloned.try_make_custom_header_to_net().unwrap();
    assert_eq!(
        cloned
            .ext_fields()
            .and_then(|fields| fields.get("value"))
            .map(CheetahString::as_str),
        Some("7")
    );
}

#[test]
fn typed_dynamic_conflict_fails_json_and_rocketmq_with_full_rollback() {
    use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let header = SendMessageResponseHeader::new("typed".into(), 1, 2, None, None, None);
        let mut command = RemotingCommand::create_success_response_command_with_header(header)
            .set_serialize_type(serialize_type)
            .set_ext_fields(HashMap::from([("msgId".into(), "dynamic".into())]));
        let mut destination = BytesMut::from(&b"prefix"[..]);

        let error = command.try_fast_header_encode(&mut destination).unwrap_err();

        assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);
        assert_eq!(destination.as_ref(), b"prefix");
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("msgId"))
                .map(CheetahString::as_str),
            Some("dynamic")
        );
    }
}

#[test]
fn generated_fast_header_streams_json_without_materializing_extension_fields() {
    use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

    let header = SendMessageResponseHeader::new(
        "msg-\"\\\n-主题".into(),
        -3,
        i64::MAX,
        Some(CheetahString::new()),
        Some("batch-a".into()),
        None,
    );
    let mut command = RemotingCommand::create_success_response_command_with_header(header)
        .set_language(LanguageCode::GO)
        .set_version(501)
        .set_opaque(7)
        .set_remark("remark-\"\\\n-主题")
        .set_serialize_type(SerializeType::JSON);
    let mut materialized = command.clone();
    materialized.try_make_custom_header_to_net().unwrap();
    let expected = serde_json::to_value(&materialized).unwrap();

    let mut encoded = BytesMut::new();
    command.try_fast_header_encode(&mut encoded).unwrap();

    assert!(!command.custom_header_to_net);
    assert!(command.ext_fields.is_absent());
    let marked_header_length = i32::from_be_bytes(encoded[4..8].try_into().unwrap());
    let header_length = (marked_header_length & 0x00ff_ffff) as usize;
    let actual: serde_json::Value = serde_json::from_slice(&encoded[8..8 + header_length]).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual["extFields"]["transactionId"], "");
    assert_eq!(actual["extFields"]["queueId"], "-3");
    assert_eq!(actual["extFields"]["queueOffset"], i64::MAX.to_string());
}

#[test]
fn present_empty_is_preserved_in_map_and_normalized_after_rocketmq_decode() {
    use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

    let header = SendMessageResponseHeader::new("msg".into(), 1, 2, Some(CheetahString::new()), None, None);
    let mut materialized = RemotingCommand::create_success_response_command_with_header(header);
    materialized.try_make_custom_header_to_net().unwrap();
    assert_eq!(
        materialized
            .ext_fields()
            .and_then(|fields| fields.get("transactionId"))
            .map(CheetahString::as_str),
        Some("")
    );

    let header = SendMessageResponseHeader::new("msg".into(), 1, 2, Some(CheetahString::new()), None, None);
    let mut command = RemotingCommand::create_success_response_command_with_header(header)
        .set_serialize_type(SerializeType::ROCKETMQ);
    let mut encoded = BytesMut::new();
    command.try_fast_header_encode(&mut encoded).unwrap();
    let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

    assert!(decoded
        .ext_fields()
        .is_some_and(|fields| !fields.contains_key("transactionId")));
}

#[test]
fn rocketmq_decode_keeps_extension_fields_raw_until_compatibility_map_access() {
    let mut command = RemotingCommand::create_remoting_command(1)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([
            (
                CheetahString::from_static_str("alpha"),
                CheetahString::from_static_str("first"),
            ),
            (
                CheetahString::from_static_str("beta"),
                CheetahString::from_static_str("second"),
            ),
        ]));
    let mut encoded = BytesMut::new();
    command.try_fast_header_encode(&mut encoded).unwrap();

    let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

    assert!(decoded.ext_fields.is_rocketmq_raw());
    assert!(!decoded.ext_fields.has_materialized_map());
    let cloned = decoded.clone();
    assert!(cloned.ext_fields.is_rocketmq_raw());
    assert!(!cloned.ext_fields.has_materialized_map());

    let fields = decoded.ext_fields().unwrap();
    assert_eq!(fields.get("alpha").map(CheetahString::as_str), Some("first"));
    assert_eq!(fields.get("beta").map(CheetahString::as_str), Some("second"));
    assert!(decoded.ext_fields.is_rocketmq_raw());
    assert!(decoded.ext_fields.has_materialized_map());
    assert!(!cloned.ext_fields.has_materialized_map());

    let json = serde_json::to_value(&cloned).unwrap();
    assert_eq!(json["extFields"]["alpha"], "first");
    assert_eq!(json["extFields"]["beta"], "second");
    assert!(cloned.ext_fields.has_materialized_map());
}

#[test]
fn json_decode_keeps_extension_fields_raw_until_compatibility_map_access() {
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    let mut header = json_header_with_ext_fields(
        r#"{"ns":"first","namespace":"legacy","ns":"last","nsd":"false","empty":"","escaped":"line\n\"quoted\"\\tail","unicode":"火箭"}"#,
    );
    let header_length = header.len();
    let command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
        .unwrap()
        .unwrap();

    assert!(command.ext_fields.is_json_raw());
    assert!(!command.ext_fields.has_materialized_map());
    let cloned = command.clone();
    assert!(cloned.ext_fields.is_json_raw());
    assert!(!cloned.ext_fields.has_materialized_map());

    let decoded = command.decode_command_custom_header::<RpcRequestHeader>().unwrap();
    assert_eq!(decoded.namespace.as_deref(), Some("last"));
    assert_eq!(decoded.namespaced, Some(false));
    assert!(!command.ext_fields.has_materialized_map());

    let fields = command.ext_fields().unwrap();
    assert_eq!(fields.get("ns").map(CheetahString::as_str), Some("last"));
    assert_eq!(fields.get("empty").map(CheetahString::as_str), Some(""));
    assert_eq!(
        fields.get("escaped").map(CheetahString::as_str),
        Some("line\n\"quoted\"\\tail")
    );
    assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("火箭"));
    assert!(command.ext_fields.is_json_raw());
    assert!(command.ext_fields.has_materialized_map());
    assert!(!cloned.ext_fields.has_materialized_map());

    let json = serde_json::to_value(&cloned).unwrap();
    assert_eq!(json["extFields"]["ns"], "last");
    assert_eq!(json["extFields"]["empty"], "");
    assert_eq!(json["extFields"]["unicode"], "火箭");
    assert!(cloned.ext_fields.is_json_raw());
    assert!(cloned.ext_fields.has_materialized_map());
}

#[test]
fn display_summarizes_raw_extension_fields_without_materializing_or_exposing_values() {
    let mut json_header = json_header_with_ext_fields(r#"{"token":"json-secret"}"#);
    let json_length = json_header.len();
    let json = RemotingCommand::header_decode(&mut json_header, json_length, SerializeType::JSON)
        .unwrap()
        .unwrap();

    let mut rocketmq_source = RemotingCommand::create_remoting_command(1)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("token"),
            CheetahString::from_static_str("rocketmq-secret"),
        )]));
    let mut encoded = BytesMut::new();
    rocketmq_source.try_fast_header_encode(&mut encoded).unwrap();
    let rocketmq = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

    let json_display = json.to_string();
    let rocketmq_display = rocketmq.to_string();

    assert!(json_display.contains("JsonRaw(count=1, materialized=false)"));
    assert!(rocketmq_display.contains("RocketMqRaw(count=1, materialized=false)"));
    assert!(!json_display.contains("json-secret"));
    assert!(!rocketmq_display.contains("rocketmq-secret"));
    assert!(!json.ext_fields.has_materialized_map());
    assert!(!rocketmq.ext_fields.has_materialized_map());
}

#[test]
fn populated_raw_cache_clone_mutation_preserves_the_original_for_both_protocols() {
    let mut json_header = json_header_with_ext_fields(r#"{"original":"json"}"#);
    let json_length = json_header.len();
    let json = RemotingCommand::header_decode(&mut json_header, json_length, SerializeType::JSON)
        .unwrap()
        .unwrap();

    let mut rocketmq_source = RemotingCommand::create_remoting_command(1)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("original"),
            CheetahString::from_static_str("rocketmq"),
        )]));
    let mut encoded = BytesMut::new();
    rocketmq_source.try_fast_header_encode(&mut encoded).unwrap();
    let rocketmq = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

    for original in [json, rocketmq] {
        assert!(original
            .ext_fields()
            .is_some_and(|fields| fields.contains_key("original")));
        assert!(original.ext_fields.has_materialized_map());

        let mut cloned = original.clone();
        cloned.add_ext_field("cloned", "only");

        assert!(original
            .ext_fields()
            .is_some_and(|fields| !fields.contains_key("cloned")));
        assert!(cloned
            .ext_fields()
            .is_some_and(|fields| fields.get("cloned").is_some_and(|value| value == "only")));
    }
}

#[test]
fn json_decode_preserves_absent_null_and_empty_extension_fields() {
    let mut missing = BytesMut::from(
        r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"serializeTypeCurrentRPC":"JSON"}"#
            .as_bytes(),
    );
    let missing_length = missing.len();
    let missing_command = RemotingCommand::header_decode(&mut missing, missing_length, SerializeType::JSON)
        .unwrap()
        .unwrap();
    assert!(missing_command.ext_fields().is_none());

    let mut null = json_header_with_ext_fields("null");
    let null_length = null.len();
    let null_command = RemotingCommand::header_decode(&mut null, null_length, SerializeType::JSON)
        .unwrap()
        .unwrap();
    assert!(null_command.ext_fields().is_none());

    let mut empty = json_header_with_ext_fields("{}");
    let empty_length = empty.len();
    let empty_command = RemotingCommand::header_decode(&mut empty, empty_length, SerializeType::JSON)
        .unwrap()
        .unwrap();
    assert!(empty_command.ext_fields.is_json_raw());
    assert!(!empty_command.ext_fields.has_materialized_map());
    assert!(empty_command.ext_fields().unwrap().is_empty());
    assert!(empty_command.ext_fields.has_materialized_map());
}

#[test]
fn v3_production_json_decode_uses_raw_fields_without_materializing_the_map() {
    use crate::protocol::header::notification_request_header::NotificationRequestHeader;

    let header = NotificationRequestHeader {
        consumer_group: "group-a".into(),
        topic: "topic-a".into(),
        queue_id: 3,
        poll_time: 15_000,
        born_time: 1_720_000_000_000,
        ..Default::default()
    };
    let mut encoded = BytesMut::new();
    RemotingCommand::create_request_command(1, header)
        .set_serialize_type(SerializeType::JSON)
        .try_fast_header_encode(&mut encoded)
        .unwrap();
    let command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
    assert!(command.ext_fields.is_json_raw());
    assert!(!command.ext_fields.has_materialized_map());

    let standard = command
        .decode_command_custom_header::<NotificationRequestHeader>()
        .unwrap();
    assert_eq!(standard.queue_id, 3);
    assert!(!command.ext_fields.has_materialized_map());

    let fast = command
        .decode_command_custom_header_fast::<NotificationRequestHeader>()
        .unwrap();
    assert_eq!(fast.queue_id, 3);
    assert!(!command.ext_fields.has_materialized_map());
}

#[test]
fn json_raw_fallback_and_mutation_materialize_the_compatibility_map() {
    let mut header = json_header_with_ext_fields(r#"{"value":"7"}"#);
    let header_length = header.len();
    let mut command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
        .unwrap()
        .unwrap();
    assert!(command.ext_fields.is_json_raw());
    assert!(!command.ext_fields.has_materialized_map());

    let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
    assert_eq!(decoded.value, 7);
    assert!(command.ext_fields.is_json_raw());
    assert!(command.ext_fields.has_materialized_map());

    command.add_ext_field("added", "field");
    assert!(!command.ext_fields.is_json_raw());
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("value"))
            .map(CheetahString::as_str),
        Some("7")
    );
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("added"))
            .map(CheetahString::as_str),
        Some("field")
    );
}

#[test]
fn json_envelope_rejects_non_string_extension_field_values() {
    for value in ["7", "true", "null", "{}", "[]"] {
        let mut header = json_header_with_ext_fields(&format!(r#"{{"value":{value}}}"#));
        let header_length = header.len();

        assert!(RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON).is_err());
    }
}

#[test]
fn v3_production_decode_uses_raw_fields_without_materializing_the_map() {
    use crate::protocol::header::notification_request_header::NotificationRequestHeader;

    let header = NotificationRequestHeader {
        consumer_group: "group-a".into(),
        topic: "topic-a".into(),
        queue_id: 3,
        poll_time: 15_000,
        born_time: 1_720_000_000_000,
        ..Default::default()
    };
    let mut encoded = BytesMut::new();
    RemotingCommand::create_request_command(1, header)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .try_fast_header_encode(&mut encoded)
        .unwrap();
    let command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
    assert!(command.ext_fields.is_rocketmq_raw());
    assert!(!command.ext_fields.has_materialized_map());

    let standard = command
        .decode_command_custom_header::<NotificationRequestHeader>()
        .unwrap();
    assert_eq!(standard.queue_id, 3);
    assert!(!command.ext_fields.has_materialized_map());

    let fast = command
        .decode_command_custom_header_fast::<NotificationRequestHeader>()
        .unwrap();
    assert_eq!(fast.queue_id, 3);
    assert!(!command.ext_fields.has_materialized_map());
}

#[test]
fn raw_direct_decode_preserves_duplicate_and_alias_precedence_without_a_map() {
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    fn append_field(out: &mut BytesMut, key: &str, value: &str) {
        out.put_u16(key.len() as u16);
        out.extend_from_slice(key.as_bytes());
        out.put_i32(value.len() as i32);
        out.extend_from_slice(value.as_bytes());
    }

    let mut payload = BytesMut::new();
    append_field(&mut payload, "ns", "first-canonical");
    append_field(&mut payload, "namespace", "legacy");
    append_field(&mut payload, "ns", "last-canonical");
    append_field(&mut payload, "nsd", "true");
    append_field(&mut payload, "nsd", "false");
    let command = RemotingCommand::create_remoting_command(1)
        .set_binary_ext_fields(BinaryHeaderFields::new(payload.freeze()).unwrap());

    let decoded = command.decode_command_custom_header::<RpcRequestHeader>().unwrap();

    assert_eq!(decoded.namespace.as_deref(), Some("last-canonical"));
    assert_eq!(decoded.namespaced, Some(false));
    assert!(command.ext_fields.is_rocketmq_raw());
    assert!(!command.ext_fields.has_materialized_map());

    let mut converged = BytesMut::new();
    append_field(&mut converged, "canonical", "superseded");
    append_field(&mut converged, "legacy", "final");
    append_field(&mut converged, "canonical", "final");
    let command = RemotingCommand::create_remoting_command(1)
        .set_binary_ext_fields(BinaryHeaderFields::new(converged.freeze()).unwrap());
    let decoded = command.decode_command_custom_header::<RawStrictAliasHeader>().unwrap();
    assert_eq!(decoded.value.as_deref(), Some("final"));
    assert!(!command.ext_fields.has_materialized_map());

    let mut conflicting = BytesMut::new();
    append_field(&mut conflicting, "canonical", "canonical-value");
    append_field(&mut conflicting, "legacy", "legacy-value");
    let command = RemotingCommand::create_remoting_command(1)
        .set_binary_ext_fields(BinaryHeaderFields::new(conflicting.freeze()).unwrap());
    assert!(command.decode_command_custom_header::<RawStrictAliasHeader>().is_err());
    assert!(!command.ext_fields.has_materialized_map());
}

#[test]
fn mutating_raw_extension_fields_materializes_and_invalidates_the_raw_view() {
    let mut command = RemotingCommand::create_remoting_command(1)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("alpha"),
            CheetahString::from_static_str("first"),
        )]));
    let mut encoded = BytesMut::new();
    command.try_fast_header_encode(&mut encoded).unwrap();
    let mut decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
    assert!(decoded.ext_fields.is_rocketmq_raw());

    decoded.add_ext_field("beta", "second");

    assert!(!decoded.ext_fields.is_rocketmq_raw());
    assert_eq!(
        decoded
            .ext_fields()
            .and_then(|fields| fields.get("alpha"))
            .map(CheetahString::as_str),
        Some("first")
    );
    assert_eq!(
        decoded
            .ext_fields()
            .and_then(|fields| fields.get("beta"))
            .map(CheetahString::as_str),
        Some("second")
    );
}

#[test]
fn rocketmq_envelope_rejects_invalid_utf8_before_returning_a_command() {
    let mut command = RemotingCommand::create_remoting_command(1)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("v"),
        )]));
    let mut encoded = BytesMut::new();
    command.try_fast_header_encode(&mut encoded).unwrap();
    let last = encoded.len() - 1;
    encoded[last] = 0xff;

    assert!(RemotingCommand::decode(&mut encoded).is_err());
}

#[test]
fn materialize_custom_header_to_ext_fields_keeps_header_decodable_and_header_object_visible() {
    let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });

    command.materialize_custom_header_to_ext_fields();

    assert!(command.command_custom_header_ref().is_some());
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("value")))
            .map(CheetahString::as_str),
        Some("7")
    );
    let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
    assert_eq!(decoded.value, 7);
}

#[test]
fn required_header_decode_preserves_valid_standard_and_fast_results() {
    let command = RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
        CheetahString::from_static_str("value"),
        CheetahString::from_static_str("7"),
    )]));

    let standard = command
        .decode_required_header::<TestCustomHeader>("decode test header")
        .unwrap();
    let fast = command
        .decode_required_header_fast::<TestCustomHeader>("decode test header")
        .unwrap();

    assert_eq!(standard, TestCustomHeader { value: 7 });
    assert_eq!(fast, standard);
}

#[test]
fn required_header_decode_maps_missing_malformed_and_overflow_to_typed_error() {
    let cases = [
        ("missing extension fields", RemotingCommand::create_remoting_command(1)),
        (
            "missing required field",
            RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::new()),
        ),
        (
            "malformed numeric field",
            RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("value"),
                CheetahString::from_static_str("not-a-number"),
            )])),
        ),
        (
            "overflowing numeric field",
            RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("value"),
                CheetahString::from_static_str("2147483648"),
            )])),
        ),
    ];

    for (case, command) in cases {
        let standard = command
            .decode_required_header::<TestCustomHeader>("decode test header")
            .expect_err(case);
        assert_eq!(
            standard.descriptor(),
            &rocketmq_error::PROTOCOL_HEADER_INVALID,
            "{case}"
        );
        assert!(std::error::Error::source(&standard).is_some(), "{case}");

        let fast = command
            .decode_required_header_fast::<TestCustomHeader>("decode test header")
            .expect_err(case);
        assert_eq!(fast.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID, "{case}");
        assert!(std::error::Error::source(&fast).is_some(), "{case}");
    }
}

#[test]
fn read_custom_header_mut_invalidates_materialized_ext_fields() {
    let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });
    command.materialize_custom_header_to_ext_fields();

    let header = command
        .read_custom_header_mut::<TestCustomHeader>()
        .expect("test header should be available");
    header.value = 9;
    command.make_custom_header_to_net();

    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("value")))
            .map(CheetahString::as_str),
        Some("9")
    );
    let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
    assert_eq!(decoded.value, 9);
}

#[test]
fn failed_shared_header_mutation_keeps_materialized_fields_intact() {
    let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });
    command.try_make_custom_header_to_net().unwrap();
    let _clone = command.clone();

    assert!(command.try_read_custom_header_mut::<TestCustomHeader>().is_err());

    assert!(command.custom_header_to_net);
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("value"))
            .map(CheetahString::as_str),
        Some("7")
    );
}

#[test]
fn fast_rocketmq_encode_frame_decodes_with_body() {
    let body = Bytes::from_static(b"rocketmq-body");
    let mut command = RemotingCommand::create_remoting_command(100)
        .set_language(LanguageCode::RUST)
        .set_opaque(7)
        .set_serialize_type(SerializeType::ROCKETMQ)
        .set_body(body.clone());

    let mut encoded = BytesMut::new();
    command.fast_header_encode(&mut encoded);
    encoded.extend_from_slice(&body);

    let decoded = RemotingCommand::decode(&mut encoded)
        .expect("fast rocketmq frame should decode")
        .expect("complete frame should produce command");

    assert_eq!(decoded.code(), 100);
    assert_eq!(decoded.opaque(), 7);
    assert_eq!(decoded.get_body(), Some(&body));
}

#[test]
#[allow(
    deprecated,
    reason = "verifies legacy header read compatibility facades during their deprecation window"
)]
fn legacy_header_read_facades_match_typed_results() {
    let command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });

    assert_eq!(
        command
            .read_custom_header_ref::<TestCustomHeader>()
            .map(|header| header.value),
        Some(7)
    );
    assert_eq!(
        command
            .read_custom_header_ref_unchecked::<TestCustomHeader>()
            .unwrap()
            .value,
        7
    );

    let missing = RemotingCommand::create_remoting_command(1);
    assert!(missing.read_custom_header_ref::<TestCustomHeader>().is_none());
    assert!(missing.read_custom_header_ref_unchecked::<TestCustomHeader>().is_err());

    assert!(command.read_custom_header_ref::<OtherCustomHeader>().is_none());
    assert!(command.read_custom_header_ref_unchecked::<OtherCustomHeader>().is_err());
}

#[test]
#[allow(
    deprecated,
    reason = "verifies legacy mutable compatibility facades preserve typed invalidation and shared-header behavior"
)]
fn legacy_mutable_header_facades_preserve_typed_invalidation_and_shared_errors() {
    let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });
    command.try_make_custom_header_to_net().unwrap();

    command
        .read_custom_header_mut::<TestCustomHeader>()
        .expect("unique legacy header should be mutable")
        .value = 9;
    assert!(!command.custom_header_to_net);
    command.make_custom_header_to_net();
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("value"))
            .map(CheetahString::as_str),
        Some("9")
    );

    let mut from_ref = RemotingCommand::create_request_command(1, TestCustomHeader { value: 3 });
    from_ref
        .read_custom_header_mut_from_ref::<TestCustomHeader>()
        .expect("legacy shared-reference compatibility name should mutate unique headers")
        .value = 4;
    assert_eq!(
        from_ref.try_read_custom_header_ref::<TestCustomHeader>().unwrap().value,
        4
    );

    let mut unchecked = RemotingCommand::create_request_command(1, TestCustomHeader { value: 5 });
    unchecked
        .read_custom_header_mut_unchecked::<TestCustomHeader>()
        .unwrap()
        .value = 6;
    assert_eq!(
        unchecked
            .try_read_custom_header_ref::<TestCustomHeader>()
            .unwrap()
            .value,
        6
    );

    let _shared = command.clone();
    assert!(command.read_custom_header_mut::<TestCustomHeader>().is_none());
    assert!(command.read_custom_header_mut_from_ref::<TestCustomHeader>().is_none());
    assert!(command.read_custom_header_mut_unchecked::<TestCustomHeader>().is_err());
}

#[test]
fn legacy_materializers_preserve_typed_failure_state() {
    use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

    for materializer in ["make", "materialize"] {
        let header = SendMessageResponseHeader::new("typed".into(), 1, 2, None, None, None);
        let mut command = RemotingCommand::create_success_response_command_with_header(header)
            .set_ext_fields(HashMap::from([("msgId".into(), "dynamic".into())]));

        match materializer {
            "make" => command.make_custom_header_to_net(),
            "materialize" => command.materialize_custom_header_to_ext_fields(),
            _ => unreachable!("test enumerates the two legacy materializers"),
        }

        assert!(!command.custom_header_to_net, "{materializer}");
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("msgId"))
                .map(CheetahString::as_str),
            Some("dynamic"),
            "{materializer}"
        );
    }
}

#[test]
fn legacy_header_and_frame_facades_match_typed_bytes() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 })
            .set_serialize_type(serialize_type);

        let mut typed_header = command.clone();
        let expected_header = typed_header.try_header_encode().unwrap();
        let mut legacy_header = command.clone();
        assert_eq!(legacy_header.header_encode(), Some(expected_header));

        let mut typed_frame = command.clone();
        let expected_frame = typed_frame.try_encode_header_with_body_length(0).unwrap();
        let mut legacy_frame = command.clone();
        assert_eq!(legacy_frame.encode_header(), Some(expected_frame.clone()));
        assert_eq!(legacy_frame.encode_header_with_body_length(0), Some(expected_frame));
    }
}

#[test]
fn legacy_header_and_fast_facades_map_typed_failures_without_mutation() {
    use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let header = SendMessageResponseHeader::new("typed".into(), 1, 2, None, None, None);
        let command = RemotingCommand::create_success_response_command_with_header(header)
            .set_serialize_type(serialize_type)
            .set_ext_fields(HashMap::from([("msgId".into(), "dynamic".into())]));

        let mut typed_header = command.clone();
        assert!(typed_header.try_header_encode().is_err());
        let mut legacy_header = command.clone();
        assert!(legacy_header.header_encode().is_none());

        let mut typed_frame = command.clone();
        assert!(typed_frame.try_encode_header_with_body_length(0).is_err());
        let mut legacy_frame = command.clone();
        assert!(legacy_frame.encode_header().is_none());

        let mut destination = BytesMut::from(&b"prefix"[..]);
        let mut legacy_fast = command.clone();
        legacy_fast.fast_header_encode(&mut destination);
        assert_eq!(destination.as_ref(), b"prefix");
    }
}

#[test]
#[allow(
    deprecated,
    reason = "verifies ambiguous response compatibility factories retain their documented SUCCESS behavior"
)]
fn legacy_response_factories_preserve_success_semantics() {
    let response = RemotingCommand::create_response_command();
    assert_eq!(
        response.code(),
        crate::code::response_code::RemotingSysResponseCode::Success as i32
    );
    assert!(response.is_response_type());

    let response_with_header = RemotingCommand::create_response_command_with_header(TestCustomHeader { value: 7 });
    assert_eq!(
        response_with_header.code(),
        crate::code::response_code::RemotingSysResponseCode::Success as i32
    );
    assert_eq!(
        response_with_header
            .try_read_custom_header_ref::<TestCustomHeader>()
            .unwrap()
            .value,
        7
    );
}
