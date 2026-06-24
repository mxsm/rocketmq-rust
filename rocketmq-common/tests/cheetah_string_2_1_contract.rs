/*
 * Copyright 2023 The RocketMQ Rust Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahBuilder;
use cheetah_string::CheetahBytes;
use cheetah_string::CheetahStr;
use cheetah_string::CheetahString;

#[test]
fn cheetah_str_can_be_used_for_clone_cheap_keys() {
    let topic = CheetahStr::from_static_str("topic-a");
    let cloned = topic.clone();

    assert_eq!(topic, cloned);
    assert_eq!(topic.as_str(), "topic-a");
}

#[test]
fn cheetah_string_lookup_by_str_still_works_for_hash_maps() {
    let mut map = HashMap::<CheetahString, i32>::new();
    map.insert(CheetahString::from_static_str("broker-a"), 1);

    assert_eq!(map.get("broker-a"), Some(&1));
}

#[test]
fn cheetah_builder_finishes_into_string_for_append_heavy_paths() {
    let mut builder = CheetahBuilder::with_capacity("topic".len() + 1 + "group".len());
    builder.push_str("topic");
    builder.push('@');
    builder.push_str("group");

    assert_eq!(builder.finish_string(), "topic@group");
}

#[test]
fn cheetah_builder_finish_string_preserves_owned_capacity_for_later_push() {
    let mut builder = CheetahBuilder::with_capacity(128);
    builder.push_str("topic");
    let before = builder.as_str().as_bytes().as_ptr();

    let mut value = builder.finish_string();
    value.push_str("@group");

    assert_eq!(value, "topic@group");
    assert_eq!(value.as_bytes().as_ptr(), before);
}

#[test]
fn checked_utf8_bytes_reject_invalid_protocol_text() {
    assert!(CheetahString::try_from_bytes(b"topic").is_ok());
    assert!(CheetahString::try_from_bytes(&[0xff, 0xfe]).is_err());
}

#[test]
fn cheetah_bytes_keeps_byte_semantics_and_validates_before_string_conversion() {
    let invalid = CheetahBytes::from_vec(vec![0xff, 0xfe]);
    assert!(CheetahString::try_from(invalid).is_err());

    let valid = CheetahBytes::from_static(b"topic");
    assert_eq!(CheetahString::try_from(valid).unwrap(), "topic");
}

#[test]
fn cheetah_str_hash_set_keeps_str_semantics() {
    let mut set = HashSet::<CheetahStr>::new();
    set.insert(CheetahStr::from_static_str("group-a"));

    assert!(set.contains("group-a"));
}

#[test]
fn char_split_supports_reverse_iteration_for_single_char_separators() {
    let value = CheetahString::from_static_str("a@b@c");
    let parts: Vec<&str> = value.split('@').rev().collect();

    assert_eq!(parts, vec!["c", "b", "a"]);
}
