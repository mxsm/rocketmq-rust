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

//! Locates rejected mutation arguments against the input schema a Tool publishes.
//!
//! The locator never decides whether a call is valid. The typed decoder and validator make that
//! decision, and this module only describes a rejection they already made, so a schema keyword
//! it does not understand costs a location and never a check.

use std::fmt::Write;

use rmcp::model::JsonObject;
use schemars::JsonSchema;
use serde_json::Number;
use serde_json::Value;

use crate::error::ArgumentViolation;
use crate::error::ViolationConstraint;
use crate::error::MAX_REPORTED_VIOLATIONS;
use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

/// Stands in a path for a property the schema does not declare, because its name is caller text.
const UNDECLARED_PROPERTY: &str = "*";
/// Reviewed input schemas nest two levels deep; the bound only stops a reference cycle.
const MAX_DEPTH: usize = 8;

/// Locates what is wrong with `arguments` for the Tool whose arguments decode as `T`.
///
/// Returns at most [`MAX_REPORTED_VIOLATIONS`] entries, and none when the arguments break only
/// a rule that the input schema does not declare.
pub(crate) fn locate<T: JsonSchema + 'static>(arguments: &Value) -> Vec<ArgumentViolation> {
    let Ok(schema) = rmcp::handler::server::tool::schema_for_input::<T>() else {
        return Vec::new();
    };
    let mut locator = Locator {
        definitions: schema.get("$defs").and_then(Value::as_object),
        violations: Vec::new(),
    };
    locator.visit(&schema, arguments, &mut String::new(), 0);
    locator.violations
}

struct Locator<'schema> {
    definitions: Option<&'schema JsonObject>,
    violations: Vec<ArgumentViolation>,
}

impl<'schema> Locator<'schema> {
    fn is_full(&self) -> bool {
        self.violations.len() >= MAX_REPORTED_VIOLATIONS
    }

    fn report(&mut self, path: &str, constraint: ViolationConstraint) {
        if !self.is_full() {
            self.violations.push(ArgumentViolation {
                path: path.to_owned(),
                constraint,
            });
        }
    }

    fn report_member(&mut self, path: &mut String, name: &str, constraint: ViolationConstraint) {
        let parent = path.len();
        push_segment(path, name);
        self.report(path, constraint);
        path.truncate(parent);
    }

    fn resolve(&self, reference: &str) -> Option<&'schema JsonObject> {
        self.definitions?.get(reference.strip_prefix("#/$defs/")?)?.as_object()
    }

    fn visit(&mut self, schema: &'schema JsonObject, instance: &Value, path: &mut String, depth: usize) {
        if self.is_full() || depth > MAX_DEPTH {
            return;
        }
        if let Some(target) = schema
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|reference| self.resolve(reference))
        {
            self.visit(target, instance, path, depth + 1);
        }
        if let Some(constraint) = broken_rule(schema, instance) {
            // The first broken rule is enough to correct one value, and its children no longer matter.
            return self.report(path, constraint);
        }
        match instance {
            Value::Object(members) => self.visit_members(schema, members, path, depth),
            Value::Array(items) => self.visit_items(schema, items, path, depth),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }

    fn visit_members(&mut self, schema: &'schema JsonObject, members: &JsonObject, path: &mut String, depth: usize) {
        let properties = schema.get("properties").and_then(Value::as_object);
        let required = schema.get("required").and_then(Value::as_array);
        for name in required.into_iter().flatten().filter_map(Value::as_str) {
            if !members.contains_key(name) {
                self.report_member(path, name, ViolationConstraint::Required);
            }
        }
        // Sorted, so a report does not depend on the map type `serde_json` was built with.
        let mut declared = properties.into_iter().flatten().collect::<Vec<_>>();
        declared.sort_unstable_by_key(|(name, _)| name.as_str());
        for (name, property) in declared {
            if let (Some(property), Some(member)) = (property.as_object(), members.get(name)) {
                let parent = path.len();
                push_segment(path, name);
                self.visit(property, member, path, depth + 1);
                path.truncate(parent);
            }
        }
        let undeclared = |name: &String| !properties.is_some_and(|properties| properties.contains_key(name));
        if schema.get("additionalProperties") == Some(&Value::Bool(false)) && members.keys().any(undeclared) {
            self.report_member(path, UNDECLARED_PROPERTY, ViolationConstraint::AdditionalProperty);
        }
    }

    fn visit_items(&mut self, schema: &'schema JsonObject, items: &[Value], path: &mut String, depth: usize) {
        let Some(item_schema) = schema.get("items").and_then(Value::as_object) else {
            return;
        };
        for (index, item) in items.iter().enumerate() {
            if self.is_full() {
                return;
            }
            let parent = path.len();
            // Writing to a `String` cannot fail.
            let _ = write!(path, "/{index}");
            self.visit(item_schema, item, path, depth + 1);
            path.truncate(parent);
        }
    }
}

/// Appends one JSON Pointer reference token.
fn push_segment(path: &mut String, name: &str) {
    path.push('/');
    for character in name.chars() {
        match character {
            '~' => path.push_str("~0"),
            '/' => path.push_str("~1"),
            other => path.push(other),
        }
    }
}

/// Returns the first rule of `schema` that `instance` itself breaks, leaving its children aside.
fn broken_rule(schema: &JsonObject, instance: &Value) -> Option<ViolationConstraint> {
    if schema
        .get("type")
        .is_some_and(|expected| !matches_type(expected, instance))
    {
        return Some(ViolationConstraint::Type);
    }
    let outside_enum = schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|allowed| !allowed.contains(instance));
    if outside_enum || schema.get("const").is_some_and(|constant| constant != instance) {
        return Some(ViolationConstraint::Enum);
    }
    match instance {
        Value::String(text) => {
            let length = u64::try_from(text.chars().count()).unwrap_or(u64::MAX);
            if limit(schema, "minLength").is_some_and(|minimum| length < minimum) {
                Some(ViolationConstraint::MinLength)
            } else if limit(schema, "maxLength").is_some_and(|maximum| length > maximum) {
                Some(ViolationConstraint::MaxLength)
            } else if schema
                .get("pattern")
                .and_then(Value::as_str)
                .and_then(|pattern| matches_reviewed_pattern(pattern, text))
                == Some(false)
            {
                Some(ViolationConstraint::Pattern)
            } else {
                None
            }
        }
        Value::Number(number) => {
            let value = integer(number)?;
            let (format_minimum, format_maximum) = integer_format_range(schema).unzip();
            let minimum = [bound(schema, "minimum"), format_minimum].into_iter().flatten().max();
            let maximum = [bound(schema, "maximum"), format_maximum].into_iter().flatten().min();
            if minimum.is_some_and(|minimum| value < minimum) {
                Some(ViolationConstraint::Minimum)
            } else if maximum.is_some_and(|maximum| value > maximum) {
                Some(ViolationConstraint::Maximum)
            } else {
                None
            }
        }
        Value::Array(items) => {
            let length = u64::try_from(items.len()).unwrap_or(u64::MAX);
            let too_few = limit(schema, "minItems").is_some_and(|minimum| length < minimum);
            let too_many = limit(schema, "maxItems").is_some_and(|maximum| length > maximum);
            (too_few || too_many).then_some(ViolationConstraint::Other)
        }
        Value::Null | Value::Bool(_) | Value::Object(_) => None,
    }
}

fn matches_type(expected: &Value, instance: &Value) -> bool {
    match expected {
        Value::String(name) => matches_type_name(name, instance),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .any(|name| matches_type_name(name, instance)),
        _ => true,
    }
}

fn matches_type_name(name: &str, instance: &Value) -> bool {
    match name {
        "null" => instance.is_null(),
        "boolean" => instance.is_boolean(),
        "string" => instance.is_string(),
        "array" => instance.is_array(),
        "object" => instance.is_object(),
        "number" => instance.is_number(),
        // The typed decoder rejects a float for an integer argument, even one without a fraction.
        "integer" => instance.is_i64() || instance.is_u64(),
        _ => true,
    }
}

fn limit(schema: &JsonObject, keyword: &str) -> Option<u64> {
    schema.get(keyword).and_then(Value::as_u64)
}

fn bound(schema: &JsonObject, keyword: &str) -> Option<i128> {
    schema.get(keyword).and_then(Value::as_number).and_then(integer)
}

/// Reads an integer JSON number. The reviewed input schemas bound integers only.
fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

/// The value range of the integer type that a `format` names; the typed decoder rejects the rest.
fn integer_format_range(schema: &JsonObject) -> Option<(i128, i128)> {
    Some(match schema.get("format")?.as_str()? {
        "int32" => (i32::MIN.into(), i32::MAX.into()),
        "int64" => (i64::MIN.into(), i64::MAX.into()),
        "uint32" => (0, u32::MAX.into()),
        "uint64" => (0, u64::MAX.into()),
        _ => return None,
    })
}

/// Evaluates the patterns that the reviewed input schemas declare, without a regex engine.
///
/// Returns `None` for any other pattern: an unreviewed pattern stays unlocated rather than
/// guessed at. The test `reviewed_patterns_agree_with_a_regex_engine` holds every arm to the
/// pattern it stands for.
fn matches_reviewed_pattern(pattern: &str, text: &str) -> Option<bool> {
    let only = |punctuation: &[u8]| {
        !text.is_empty()
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || punctuation.contains(&byte))
    };
    match pattern {
        "^rocketmq-mcp-control\\.arguments\\.v1$" => Some(text == MUTATION_ARGUMENTS_SCHEMA_VERSION),
        "^[a-zA-Z0-9_-]+$" => Some(only(b"_-")),
        "^[%|a-zA-Z0-9_-]+$" => Some(only(b"%|_-")),
        "^[a-zA-Z0-9._:-]+$" => Some(only(b"._:-")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde::de::DeserializeOwned;
    use serde::Deserialize;
    use serde_json::json;

    use super::*;
    use crate::tools::PatchBrokerConfigArgs;
    use crate::tools::ResetConsumerOffsetArgs;
    use crate::tools::SetConsumerRequestModeArgs;
    use crate::tools::UpsertConsumerGroupArgs;
    use crate::tools::UpsertTopicArgs;

    /// Keywords the locator evaluates.
    const LOCATED_KEYWORDS: [&str; 16] = [
        "$ref",
        "type",
        "enum",
        "const",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "format",
        "minItems",
        "maxItems",
        "items",
        "properties",
        "required",
        "additionalProperties",
    ];
    /// Keywords that describe an argument without constraining it.
    const ANNOTATION_KEYWORDS: [&str; 5] = ["$schema", "$defs", "default", "description", "title"];

    fn located(path: &str, constraint: ViolationConstraint) -> ArgumentViolation {
        ArgumentViolation {
            path: path.to_owned(),
            constraint,
        }
    }

    fn topic() -> Value {
        json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "broker_names": ["broker-a", "broker-b"],
            "read_queue_nums": 8,
            "write_queue_nums": 8,
            "perm": 6,
            "order": false,
            "message_type": "NORMAL",
            "dry_run": false,
            "confirm": true,
            "reason": "approved topic replacement",
            "request_key": "change-0001"
        })
    }

    fn consumer_group() -> Value {
        json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "consumer_group": "orders_consumers",
            "broker_names": ["broker-a"],
            "consume_enable": true,
            "consume_from_min_enable": false,
            "consume_broadcast_enable": false,
            "consume_message_orderly": false,
            "retry_queue_nums": 1,
            "retry_max_times": 16,
            "broker_id": 0,
            "which_broker_when_consume_slowly": 1,
            "notify_consumer_ids_changed_enable": true,
            "group_sys_flag": 0,
            "consume_timeout_minute": 15
        })
    }

    fn offset() -> Value {
        json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "orders_consumers",
            "timestamp": "2026-08-30T00:00:00Z",
            "reason": null
        })
    }

    fn broker_config() -> Value {
        json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "properties": {"traceTopicEnable": "true"}
        })
    }

    fn request_mode() -> Value {
        json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "orders_consumers",
            "mode": "pop",
            "pop_share_queue_num": 4,
            "timeout_millis": 12000
        })
    }

    fn with(mut arguments: Value, name: &str, value: Value) -> Value {
        arguments[name] = value;
        arguments
    }

    fn without(mut arguments: Value, name: &str) -> Value {
        arguments.as_object_mut().unwrap().remove(name);
        arguments
    }

    fn input_schemas() -> [(&'static str, std::sync::Arc<JsonObject>); 5] {
        use rmcp::handler::server::tool::schema_for_input;
        [
            ("topic", schema_for_input::<UpsertTopicArgs>().unwrap()),
            ("consumer group", schema_for_input::<UpsertConsumerGroupArgs>().unwrap()),
            ("offset", schema_for_input::<ResetConsumerOffsetArgs>().unwrap()),
            ("broker config", schema_for_input::<PatchBrokerConfigArgs>().unwrap()),
            (
                "request mode",
                schema_for_input::<SetConsumerRequestModeArgs>().unwrap(),
            ),
        ]
    }

    /// Visits every schema object, passing over the maps whose keys are names and not keywords.
    fn for_each_schema(schema: &JsonObject, visit: &mut impl FnMut(&JsonObject)) {
        visit(schema);
        for (keyword, value) in schema {
            match (keyword.as_str(), value) {
                ("properties" | "$defs", Value::Object(named)) => named
                    .values()
                    .filter_map(Value::as_object)
                    .for_each(|nested| for_each_schema(nested, visit)),
                ("items" | "additionalProperties", Value::Object(nested)) => for_each_schema(nested, visit),
                _ => {}
            }
        }
    }

    fn decodes<T: DeserializeOwned>(arguments: &Value) -> bool {
        T::deserialize(arguments).is_ok()
    }

    #[test]
    fn accepted_arguments_have_no_violations() {
        assert!(decodes::<UpsertTopicArgs>(&topic()));
        assert_eq!(locate::<UpsertTopicArgs>(&topic()), []);
        assert!(decodes::<UpsertConsumerGroupArgs>(&consumer_group()));
        assert_eq!(locate::<UpsertConsumerGroupArgs>(&consumer_group()), []);
        assert!(decodes::<ResetConsumerOffsetArgs>(&offset()));
        assert_eq!(locate::<ResetConsumerOffsetArgs>(&offset()), []);
        assert!(decodes::<PatchBrokerConfigArgs>(&broker_config()));
        assert_eq!(locate::<PatchBrokerConfigArgs>(&broker_config()), []);
        assert!(decodes::<SetConsumerRequestModeArgs>(&request_mode()));
        assert_eq!(locate::<SetConsumerRequestModeArgs>(&request_mode()), []);

        // Every bound is inclusive.
        let widest = with(
            with(topic(), "topic", json!("a".repeat(127))),
            "broker_names",
            json!((0..64).map(|index| format!("broker-{index:02}")).collect::<Vec<_>>()),
        );
        assert_eq!(locate::<UpsertTopicArgs>(&widest), []);
        let extremes = with(
            with(consumer_group(), "group_sys_flag", json!(i32::MIN)),
            "broker_id",
            json!(u64::MAX),
        );
        assert!(decodes::<UpsertConsumerGroupArgs>(&extremes));
        assert_eq!(locate::<UpsertConsumerGroupArgs>(&extremes), []);
    }

    #[test]
    fn every_decode_failure_is_located() {
        use ViolationConstraint::*;

        let cases = [
            (without(topic(), "schema_version"), located("/schema_version", Required)),
            (without(topic(), "message_type"), located("/message_type", Required)),
            (with(topic(), "topic", Value::Null), located("/topic", Type)),
            (with(topic(), "order", json!("false")), located("/order", Type)),
            (
                with(topic(), "read_queue_nums", json!("8")),
                located("/read_queue_nums", Type),
            ),
            (
                with(topic(), "read_queue_nums", json!(8.0)),
                located("/read_queue_nums", Type),
            ),
            (
                with(topic(), "read_queue_nums", json!(-1)),
                located("/read_queue_nums", Minimum),
            ),
            (
                with(topic(), "broker_names", json!("broker-a")),
                located("/broker_names", Type),
            ),
            (
                with(topic(), "broker_names", json!(["broker-a", 7])),
                located("/broker_names/1", Type),
            ),
            (
                with(topic(), "message_type", json!("normal")),
                located("/message_type", Enum),
            ),
            (with(topic(), "dry_run", json!(0)), located("/dry_run", Type)),
            (with(topic(), "reason", json!(7)), located("/reason", Type)),
            (
                with(topic(), "topic_name", json!("orders")),
                located("/*", AdditionalProperty),
            ),
        ];
        for (arguments, expected) in cases {
            assert!(!decodes::<UpsertTopicArgs>(&arguments), "decoded {expected:?}");
            assert_eq!(locate::<UpsertTopicArgs>(&arguments), [expected]);
        }

        let group_cases = [
            (
                with(consumer_group(), "group_sys_flag", json!(i64::from(i32::MAX) + 1)),
                located("/group_sys_flag", Maximum),
            ),
            (
                with(consumer_group(), "broker_id", json!(-1)),
                located("/broker_id", Minimum),
            ),
        ];
        for (arguments, expected) in group_cases {
            assert!(!decodes::<UpsertConsumerGroupArgs>(&arguments), "decoded {expected:?}");
            assert_eq!(locate::<UpsertConsumerGroupArgs>(&arguments), [expected]);
        }

        let broker_cases = [
            (
                with(broker_config(), "properties", json!({"traceTopicEnable": true})),
                located("/properties/traceTopicEnable", Type),
            ),
            (
                with(broker_config(), "properties", json!({"traceTopicEnable": null})),
                located("/properties/traceTopicEnable", Type),
            ),
            (
                with(broker_config(), "properties", json!({"flushDiskType": "ASYNC_FLUSH"})),
                located("/properties/*", AdditionalProperty),
            ),
            (
                with(broker_config(), "properties", json!(["traceTopicEnable"])),
                located("/properties", Type),
            ),
        ];
        for (arguments, expected) in broker_cases {
            assert!(!decodes::<PatchBrokerConfigArgs>(&arguments), "decoded {expected:?}");
            assert_eq!(locate::<PatchBrokerConfigArgs>(&arguments), [expected]);
        }

        let mode = with(request_mode(), "mode", json!("push"));
        assert!(!decodes::<SetConsumerRequestModeArgs>(&mode));
        assert_eq!(locate::<SetConsumerRequestModeArgs>(&mode), [located("/mode", Enum)]);

        assert!(!decodes::<UpsertTopicArgs>(&Value::Null));
        assert_eq!(locate::<UpsertTopicArgs>(&Value::Null), [located("", Type)]);
    }

    #[test]
    fn declared_bounds_of_decoded_arguments_are_located() {
        use ViolationConstraint::*;

        let cases = [
            (
                with(topic(), "schema_version", json!("v1")),
                located("/schema_version", Pattern),
            ),
            (with(topic(), "topic", json!("")), located("/topic", MinLength)),
            (
                with(topic(), "topic", json!("a".repeat(128))),
                located("/topic", MaxLength),
            ),
            (with(topic(), "topic", json!("orders.v1")), located("/topic", Pattern)),
            (
                with(topic(), "read_queue_nums", json!(0)),
                located("/read_queue_nums", Minimum),
            ),
            (with(topic(), "perm", json!(8)), located("/perm", Maximum)),
            (
                with(topic(), "broker_names", json!([])),
                located("/broker_names", Other),
            ),
            (
                with(
                    topic(),
                    "broker_names",
                    json!((0..65).map(|index| format!("broker-{index:02}")).collect::<Vec<_>>()),
                ),
                located("/broker_names", Other),
            ),
            (
                with(topic(), "broker_names", json!(["broker-a", "10.0.0.1"])),
                located("/broker_names/1", Pattern),
            ),
            (with(topic(), "reason", json!("ok")), located("/reason", MinLength)),
            (
                with(topic(), "request_key", json!("short")),
                located("/request_key", MinLength),
            ),
            (
                with(topic(), "request_key", json!("change 0001")),
                located("/request_key", Pattern),
            ),
        ];
        for (arguments, expected) in cases {
            let decoded = UpsertTopicArgs::deserialize(&arguments).expect("the case must reach the validator");
            assert!(decoded.validate(true, false).is_err(), "accepted {expected:?}");
            assert_eq!(locate::<UpsertTopicArgs>(&arguments), [expected]);
        }

        let timeout = with(request_mode(), "timeout_millis", json!(24_001));
        assert_eq!(
            locate::<SetConsumerRequestModeArgs>(&timeout),
            [located("/timeout_millis", Maximum)]
        );
        let timestamp = with(offset(), "timestamp", json!("2026-08-30"));
        assert_eq!(
            locate::<ResetConsumerOffsetArgs>(&timestamp),
            [located("/timestamp", MinLength)]
        );
    }

    #[test]
    fn rules_outside_the_schema_stay_unlocated() {
        // The validator rejects each of these for a rule the input schema cannot express.
        for arguments in [
            with(topic(), "topic", json!("TBW102")),
            with(topic(), "broker_names", json!(["broker-a", "broker-a"])),
            with(topic(), "perm", json!(1)),
            without(topic(), "reason"),
            with(topic(), "reason", json!("token=must-not-leak")),
        ] {
            let decoded = UpsertTopicArgs::deserialize(&arguments).unwrap();
            assert!(decoded.validate(true, false).is_err());
            assert_eq!(locate::<UpsertTopicArgs>(&arguments), []);
        }
        let empty_patch = with(broker_config(), "properties", json!({}));
        assert!(PatchBrokerConfigArgs::deserialize(&empty_patch)
            .unwrap()
            .validate(true, true)
            .is_err());
        assert_eq!(locate::<PatchBrokerConfigArgs>(&empty_patch), []);
    }

    #[test]
    fn violations_are_bounded_and_never_repeat_caller_text() {
        let arguments = json!({
            "schema_version": "caller-version-text",
            "cluster": "cluster-a",
            "topic": "caller topic text",
            "broker_names": ["caller broker text", "caller-broker-b", {"caller_nested_name": 1}],
            "read_queue_nums": 900_001,
            "perm": "caller-perm-text",
            "message_type": "CALLER_MESSAGE_TYPE",
            "caller_property_name": "caller-property-value",
            "~caller/escaped": true
        });
        let violations = locate::<UpsertTopicArgs>(&arguments);
        assert_eq!(violations.len(), MAX_REPORTED_VIOLATIONS);
        assert_eq!(
            violations,
            [
                located("/write_queue_nums", ViolationConstraint::Required),
                located("/order", ViolationConstraint::Required),
                located("/broker_names/0", ViolationConstraint::Pattern),
            ]
        );

        // With the required arguments present, the remaining slots still name no caller text.
        let mut complete = arguments;
        complete["write_queue_nums"] = json!(8);
        complete["order"] = json!(false);
        complete["broker_names"] = json!(["broker-a"]);
        complete["read_queue_nums"] = json!(8);
        complete["perm"] = json!(6);
        complete["message_type"] = json!("NORMAL");
        let violations = locate::<UpsertTopicArgs>(&complete);
        assert_eq!(
            violations,
            [
                located("/schema_version", ViolationConstraint::Pattern),
                located("/topic", ViolationConstraint::Pattern),
                located("/*", ViolationConstraint::AdditionalProperty),
            ]
        );
        let rendered = serde_json::to_string(&violations).unwrap();
        assert!(!rendered.contains("caller"), "{rendered}");
    }

    #[test]
    fn locator_understands_every_keyword_of_the_reviewed_schemas() {
        let mut patterns = BTreeSet::new();
        for (tool, schema) in input_schemas() {
            for_each_schema(&schema, &mut |node| {
                for (keyword, value) in node {
                    assert!(
                        LOCATED_KEYWORDS.contains(&keyword.as_str()) || ANNOTATION_KEYWORDS.contains(&keyword.as_str()),
                        "{tool}: the locator has not been reviewed for `{keyword}`"
                    );
                    match (keyword.as_str(), value.as_str()) {
                        ("pattern", Some(pattern)) => {
                            patterns.insert(pattern.to_owned());
                        }
                        ("format", _) => assert!(integer_format_range(node).is_some(), "{tool}: format {value}"),
                        ("$ref", Some(reference)) => {
                            let target = reference.strip_prefix("#/$defs/").expect("a local reference");
                            assert!(schema["$defs"].get(target).is_some(), "{tool}: {reference}");
                        }
                        _ => {}
                    }
                }
                for keyword in ["minimum", "maximum"] {
                    assert_eq!(node.contains_key(keyword), bound(node, keyword).is_some(), "{tool}");
                }
                assert_ne!(node.get("additionalProperties"), Some(&Value::Bool(true)), "{tool}");
            });
        }
        assert_eq!(patterns.len(), 4);
        for pattern in &patterns {
            assert!(matches_reviewed_pattern(pattern, "").is_some(), "unreviewed {pattern}");
        }
        assert_eq!(matches_reviewed_pattern("^[a-z]+$", "orders"), None);
    }

    #[test]
    fn reviewed_patterns_agree_with_a_regex_engine() {
        let samples = [
            "",
            "a",
            "Z9",
            "orders",
            "orders_v1",
            "orders-v1",
            "orders.v1",
            "orders:v1",
            "orders|v1",
            "%RETRY%orders",
            "orders v1",
            " orders",
            "orders\n",
            "orders/v1",
            "订单",
            "ordérs",
            "rocketmq-mcp-control.arguments.v1",
            "rocketmq-mcp-controlXargumentsXv1",
            "rocketmq-mcp-control.arguments.v1\n",
            "rocketmq-mcp-control.arguments.v2",
            "a^b",
            "a]b",
            "a\\b",
        ];
        let mut patterns = BTreeSet::new();
        for (_, schema) in input_schemas() {
            for_each_schema(&schema, &mut |node| {
                if let Some(pattern) = node.get("pattern").and_then(Value::as_str) {
                    patterns.insert(pattern.to_owned());
                }
            });
        }
        for pattern in patterns {
            let engine = regex::Regex::new(&pattern).unwrap();
            for sample in samples {
                assert_eq!(
                    matches_reviewed_pattern(&pattern, sample),
                    Some(engine.is_match(sample)),
                    "{pattern} on {sample:?}"
                );
            }
        }
    }
}
