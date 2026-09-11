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

use cheetah_string::CheetahString;
use rocketmq_model::codec::message_properties_to_string;
use rocketmq_model::codec::string_to_message_properties;
use rocketmq_model::codec::NAME_VALUE_SEPARATOR;
use rocketmq_model::codec::PROPERTY_SEPARATOR;

#[test]
fn properties_round_trip_without_relying_on_serialization_order() {
    let properties = HashMap::from([
        (CheetahString::from("topic"), CheetahString::from("orders")),
        (CheetahString::from("empty"), CheetahString::from("")),
        (
            CheetahString::from("description"),
            CheetahString::from("hello world 世界"),
        ),
    ]);

    let serialized = message_properties_to_string(&properties);

    assert_eq!(string_to_message_properties(Some(&serialized)), properties);
}

#[test]
fn none_and_empty_property_strings_produce_empty_maps() {
    let empty = CheetahString::from("");

    assert!(string_to_message_properties(None).is_empty());
    assert!(string_to_message_properties(Some(&empty)).is_empty());
}

#[test]
fn malformed_entries_are_skipped_while_valid_entries_survive() {
    let input = CheetahString::from(format!(
        "malformed{PROPERTY_SEPARATOR}{NAME_VALUE_SEPARATOR}empty-name{PROPERTY_SEPARATOR}valid{NAME_VALUE_SEPARATOR}value"
    ));
    let expected = HashMap::from([(CheetahString::from("valid"), CheetahString::from("value"))]);

    assert_eq!(string_to_message_properties(Some(&input)), expected);
}

#[test]
fn empty_values_final_entries_and_empty_entries_are_preserved_as_defined() {
    let input = CheetahString::from(format!(
        "empty{NAME_VALUE_SEPARATOR}{PROPERTY_SEPARATOR}{PROPERTY_SEPARATOR}final{NAME_VALUE_SEPARATOR}value"
    ));
    let expected = HashMap::from([
        (CheetahString::from("empty"), CheetahString::from("")),
        (CheetahString::from("final"), CheetahString::from("value")),
    ]);

    assert_eq!(string_to_message_properties(Some(&input)), expected);
}

#[test]
fn repeated_names_keep_the_last_value_and_split_only_once() {
    let input = CheetahString::from(format!(
        "key{NAME_VALUE_SEPARATOR}first{PROPERTY_SEPARATOR}key{NAME_VALUE_SEPARATOR}second{NAME_VALUE_SEPARATOR}part{PROPERTY_SEPARATOR}"
    ));
    let expected = HashMap::from([(CheetahString::from("key"), CheetahString::from("second\u{0001}part"))]);

    assert_eq!(string_to_message_properties(Some(&input)), expected);
}
