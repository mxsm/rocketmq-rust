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

use rocketmq_protocol::protocol::remoting_command::SERIALIZE_TYPE_ENV;
use rocketmq_protocol::protocol::remoting_command::SERIALIZE_TYPE_PROPERTY;
use rocketmq_protocol::protocol::remoting_command_facade::resolve_remoting_serialize_type;
use rocketmq_protocol::protocol::SerializeType;

#[test]
fn resolver_defaults_to_json_without_configuration() {
    assert_eq!(resolve_remoting_serialize_type(None, None), Ok(SerializeType::JSON));
}

#[test]
fn property_has_precedence_over_environment() {
    assert_eq!(
        resolve_remoting_serialize_type(Some("JSON"), Some("ROCKETMQ")),
        Ok(SerializeType::JSON)
    );
    assert_eq!(
        resolve_remoting_serialize_type(Some("ROCKETMQ"), Some("JSON")),
        Ok(SerializeType::ROCKETMQ)
    );
}

#[test]
fn environment_is_used_when_property_is_absent() {
    assert_eq!(
        resolve_remoting_serialize_type(None, Some("ROCKETMQ")),
        Ok(SerializeType::ROCKETMQ)
    );
}

#[test]
fn unsupported_values_report_the_selected_key_and_value() {
    let property_error = resolve_remoting_serialize_type(Some("CBOR"), Some("ROCKETMQ"))
        .expect_err("an invalid property must not fall back to the environment");
    assert_eq!(property_error.key(), SERIALIZE_TYPE_PROPERTY);
    assert_eq!(property_error.value(), "CBOR");

    let environment_error =
        resolve_remoting_serialize_type(None, Some("CBOR")).expect_err("an invalid environment value must fail");
    assert_eq!(environment_error.key(), SERIALIZE_TYPE_ENV);
    assert_eq!(environment_error.value(), "CBOR");
}
