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

use rocketmq_protocol::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_protocol::common::key_builder::KeyBuilder;
use rocketmq_protocol::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_protocol::common::sys_flag::topic_sys_flag;
use rocketmq_protocol::version::RocketMqVersion;

#[test]
fn java_hash_golden_covers_utf16_and_wrapping() {
    assert_eq!(JavaStringHasher::hash_str(""), 0);
    assert_eq!(JavaStringHasher::hash_str("hello"), 99_162_322);
    assert_eq!(JavaStringHasher::hash_str("😀"), 1_772_899);
    assert_eq!(JavaStringHasher::hash_str(&"a".repeat(1000)), 904_019_584);
}

#[test]
fn pop_retry_and_polling_keys_match_frozen_grammar() {
    assert_eq!(
        KeyBuilder::build_pop_retry_topic_v1("orders", "group-a"),
        "%RETRY%group-a_orders"
    );
    assert_eq!(
        KeyBuilder::build_pop_retry_topic_v2("orders", "group-a"),
        "%RETRY%group-a+orders"
    );
    assert_eq!(
        KeyBuilder::build_polling_key("orders", "group-a", -1),
        "orders@group-a@-1"
    );
    assert_eq!(
        KeyBuilder::parse_normal_topic("%RETRY%group-a+orders", "group-a"),
        "orders"
    );
}

#[test]
fn sys_flag_and_version_ordinals_remain_wire_compatible() {
    let flag = MessageSysFlag::COMPRESSED_FLAG | MessageSysFlag::TRANSACTION_COMMIT_TYPE;
    assert!(MessageSysFlag::check(flag, MessageSysFlag::COMPRESSED_FLAG));
    assert_eq!(
        MessageSysFlag::get_transaction_value(flag),
        MessageSysFlag::TRANSACTION_COMMIT_TYPE
    );
    assert!(topic_sys_flag::has_unit_flag(topic_sys_flag::build_sys_flag(
        true, false
    )));
    assert_eq!(RocketMqVersion::V3_0_0_SNAPSHOT.ordinal(), 0);
    assert_eq!(RocketMqVersion::V5_3_1_SNAPSHOT.name(), "V5_3_1_SNAPSHOT");
}

#[test]
fn protocol_version_is_an_exact_model_reexport_without_a_reverse_dependency() {
    fn protocol(value: rocketmq_protocol::version::RocketMqVersion) -> rocketmq_protocol::version::RocketMqVersion {
        value
    }
    fn model(value: rocketmq_model::version::RocketMqVersion) -> rocketmq_model::version::RocketMqVersion {
        value
    }

    let value = rocketmq_protocol::version::RocketMqVersion::V5_3_1_SNAPSHOT;
    assert_eq!(
        model(protocol(value)).ordinal(),
        rocketmq_model::version::CURRENT_VERSION.ordinal()
    );
}
