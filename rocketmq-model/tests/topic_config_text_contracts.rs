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

use rocketmq_model::topic::TopicConfig;

#[test]
fn text_attributes_round_trip_with_spaces_and_escaped_quotes() {
    let mut original = TopicConfig::new("orders");
    original.read_queue_nums = 4;
    original.write_queue_nums = 8;
    original.perm = 6;
    original.attributes.insert("description".into(), "hello world".into());
    original.attributes.insert("label".into(), "say \"hello\" 世界".into());
    let mut decoded = TopicConfig::default();
    assert!(decoded.decode(&original.encode()));
    assert_eq!(decoded.topic_name, original.topic_name);
    assert_eq!(decoded.read_queue_nums, 4);
    assert_eq!(decoded.write_queue_nums, 8);
    assert_eq!(decoded.perm, 6);
    assert_eq!(decoded.topic_filter_type, original.topic_filter_type);
    assert_eq!(decoded.attributes, original.attributes);
}

#[test]
fn five_field_and_compact_attribute_forms_decode() {
    let mut decoded = TopicConfig::default();
    assert!(decoded.decode("orders 4 8 6 SINGLE_TAG"));
    assert_eq!(decoded.topic_name.as_deref(), Some("orders"));
    assert_eq!(
        (decoded.read_queue_nums, decoded.write_queue_nums, decoded.perm),
        (4, 8, 6)
    );
    assert!(decoded.attributes.is_empty());
    assert!(decoded.decode(r#"orders 4 8 6 SINGLE_TAG {"mode":"fast"}"#));
    assert_eq!(decoded.attributes, [("mode".into(), "fast".into())].into());
}

#[test]
fn optional_json_and_numeric_fallbacks_preserve_existing_behavior() {
    for suffix in ["", " {invalid json}"] {
        let mut decoded = TopicConfig::default();
        decoded.attributes.insert("retained".into(), "value".into());
        decoded.order = true;
        decoded.topic_sys_flag = 7;
        assert!(decoded.decode(&format!("orders invalid invalid invalid SINGLE_TAG{suffix}")));
        assert_eq!(decoded.read_queue_nums, TopicConfig::default().read_queue_nums);
        assert_eq!(decoded.write_queue_nums, TopicConfig::default().write_queue_nums);
        assert_eq!(decoded.perm, TopicConfig::default().perm);
        assert_eq!(decoded.attributes, [("retained".into(), "value".into())].into());
        assert!(decoded.order);
        assert_eq!(decoded.topic_sys_flag, 7);
    }
}
