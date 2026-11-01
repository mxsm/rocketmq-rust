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

//! Business compatibility tests for Java `RemotingCommand` behavior.
//!
//! The reference behavior is `RemotingCommand.addExtField` in Apache RocketMQ:
//! the extension-field map is initialized on the first write.

use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::RemotingCommand;

fn round_trip(mut command: RemotingCommand) -> RemotingCommand {
    let mut encoded = BytesMut::new();
    command
        .try_fast_header_encode(&mut encoded)
        .expect("command should encode");
    RemotingCommand::decode(&mut encoded)
        .expect("command should decode")
        .expect("command frame should be complete")
}

#[test]
fn add_ext_field_on_absent_matches_java() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let mut command = RemotingCommand::create_success_response_command().set_serialize_type(serialize_type);
        assert!(command.ext_fields().is_none());

        command.add_ext_field("regionId", "DefaultRegion");
        let decoded = round_trip(command);

        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("regionId"))
                .map(CheetahString::as_str),
            Some("DefaultRegion")
        );
    }
}

#[test]
fn add_ext_field_if_not_exist_on_absent_matches_java() {
    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let mut command = RemotingCommand::create_success_response_command().set_serialize_type(serialize_type);
        assert!(command.ext_fields().is_none());

        command.add_ext_field_if_not_exist("traceOn", "true");
        command.add_ext_field_if_not_exist("traceOn", "false");
        let decoded = round_trip(command);

        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("traceOn"))
                .map(CheetahString::as_str),
            Some("true")
        );
    }
}

#[test]
fn node_js_language_code_matches_java() {
    const EXISTING_CODES: [(u8, LanguageCode); 13] = [
        (0, LanguageCode::JAVA),
        (1, LanguageCode::CPP),
        (2, LanguageCode::DOTNET),
        (3, LanguageCode::PYTHON),
        (4, LanguageCode::DELPHI),
        (5, LanguageCode::ERLANG),
        (6, LanguageCode::RUBY),
        (7, LanguageCode::OTHER),
        (8, LanguageCode::HTTP),
        (9, LanguageCode::GO),
        (10, LanguageCode::PHP),
        (11, LanguageCode::OMS),
        (12, LanguageCode::RUST),
    ];

    for (code, language) in EXISTING_CODES {
        assert_eq!(language.get_code(), code);
        assert_eq!(LanguageCode::value_of(code), Some(language));
    }

    assert_eq!(LanguageCode::NODE_JS.get_code(), 13);
    assert_eq!(LanguageCode::value_of(13), Some(LanguageCode::NODE_JS));
    assert_eq!(LanguageCode::value_of(14), Some(LanguageCode::OTHER));
    assert_eq!(LanguageCode::get_code_from_name("NODE_JS"), Some(LanguageCode::NODE_JS));

    for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
        let command = RemotingCommand::create_success_response_command()
            .set_language(LanguageCode::NODE_JS)
            .set_serialize_type(serialize_type);
        let decoded = round_trip(command);

        assert_eq!(decoded.language(), LanguageCode::NODE_JS);
        assert_eq!(decoded.get_serialize_type(), serialize_type);
    }

    let serde_fallback: RemotingCommand = serde_json::from_str(
        r#"{"code":0,"language":"NODE_JS","version":0,"opaque":0,"flag":1,"remark":null,"serializeTypeCurrentRPC":"JSON"}"#,
    )
    .expect("serde fallback should recognize NODE_JS");
    assert_eq!(serde_fallback.language(), LanguageCode::NODE_JS);
}

#[test]
fn ext_key_length_matches_java_signed_short_boundary() {
    let maximum = i16::MAX as usize;

    for key_length in [maximum - 1, maximum] {
        let key = "k".repeat(key_length);
        let mut command =
            RemotingCommand::create_success_response_command().set_serialize_type(SerializeType::ROCKETMQ);
        command.add_ext_field(key.clone(), "value");

        let decoded = round_trip(command);
        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get(key.as_str()))
                .map(CheetahString::as_str),
            Some("value")
        );
    }

    let oversized_key = "k".repeat(maximum + 1);
    let mut binary_command =
        RemotingCommand::create_success_response_command().set_serialize_type(SerializeType::ROCKETMQ);
    binary_command.add_ext_field(oversized_key.clone(), "value");
    let mut destination = BytesMut::from(&b"prefix"[..]);

    let error = binary_command
        .try_fast_header_encode(&mut destination)
        .expect_err("a key larger than Java's signed-short range must be rejected");
    assert_eq!(destination.as_ref(), b"prefix");
    // Canonical errors render only the stable code and detail, so the classified
    // "dynamic header key length exceeds the ROCKETMQ wire limit" text is no longer
    // observable: the rejection contract is the header-invalid descriptor.
    assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);

    let mut json_command = RemotingCommand::create_success_response_command().set_serialize_type(SerializeType::JSON);
    json_command.add_ext_field(oversized_key.clone(), "value");
    let decoded = round_trip(json_command);
    assert_eq!(
        decoded
            .ext_fields()
            .and_then(|fields| fields.get(oversized_key.as_str()))
            .map(CheetahString::as_str),
        Some("value")
    );
}

#[test]
fn non_java_remoting_client_route_query_decodes_successfully() {
    use bytes::BufMut;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;

    // Classic non-Java remoting client payload (e.g. EventHorizon.RocketMQ 0.6.2 .NET):
    // - languageCode as integer (2 for DOTNET)
    // - serializeTypeCurrentRPC omitted
    let json_header = br#"{"code":105,"languageCode":2,"version":317,"opaque":1,"flag":0,"remark":null,"extFields":{"topic":"AttributionTest"}}"#;

    let mut frame = BytesMut::with_capacity(8 + json_header.len());
    frame.put_i32(i32::try_from(4 + json_header.len()).unwrap());
    frame.put_i32(RemotingCommand::mark_serialize_type(
        i32::try_from(json_header.len()).unwrap(),
        SerializeType::JSON,
    ));
    frame.extend_from_slice(json_header);

    let decoded = RemotingCommand::decode(&mut frame)
        .expect("decoding must succeed")
        .expect("frame must be complete");

    assert_eq!(decoded.code(), 105);
    assert_eq!(decoded.language(), LanguageCode::DOTNET);
    assert_eq!(decoded.version(), 317);
    assert_eq!(decoded.opaque(), 1);
    assert_eq!(decoded.get_serialize_type(), SerializeType::JSON);

    let header = decoded
        .decode_command_custom_header::<GetRouteInfoRequestHeader>()
        .expect("GetRouteInfoRequestHeader must decode from ext_fields");
    assert_eq!(header.topic.as_str(), "AttributionTest");
}

#[test]
fn language_code_and_serialize_type_deserialize_flexible_shapes() {
    // Integer languageCode and omitted serializeType
    let cmd1: RemotingCommand = serde_json::from_str(
        r#"{"code":105,"languageCode":2,"version":317,"opaque":1,"flag":0,"remark":null,"extFields":{"topic":"TopicA"}}"#,
    )
    .expect("should deserialize integer languageCode and default serializeType");
    assert_eq!(cmd1.code(), 105);
    assert_eq!(cmd1.language(), LanguageCode::DOTNET);
    assert_eq!(cmd1.get_serialize_type(), SerializeType::JSON);

    // Case-insensitive string languageCode and serializeType
    let cmd2: RemotingCommand = serde_json::from_str(
        r#"{"code":105,"languageCode":"Dotnet","version":317,"opaque":1,"flag":0,"serializeType":"json","extFields":{"topic":"TopicA"}}"#,
    )
    .expect("should deserialize mixed-case languageCode and serializeType alias");
    assert_eq!(cmd2.code(), 105);
    assert_eq!(cmd2.language(), LanguageCode::DOTNET);
    assert_eq!(cmd2.get_serialize_type(), SerializeType::JSON);

    // Unknown language code gracefully falls back to OTHER
    let cmd3: RemotingCommand =
        serde_json::from_str(r#"{"code":105,"language":99,"version":0,"opaque":1,"flag":0,"extFields":null}"#)
            .expect("unknown language code should map to OTHER");
    assert_eq!(cmd3.language(), LanguageCode::OTHER);
}
