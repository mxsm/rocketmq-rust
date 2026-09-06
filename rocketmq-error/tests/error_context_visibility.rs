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

use rocketmq_error::fields;
use rocketmq_error::CliErrorView;
use rocketmq_error::CliVerbosity;
use rocketmq_error::ContextVisibility;
use rocketmq_error::ErrorContext;
use rocketmq_error::FieldValueKind;
use rocketmq_error::FieldValueRef;
use rocketmq_error::RocketMQError;
use rocketmq_error::Sensitive;

const SENTINEL: &str = "Bearer token-secret secret_key=sk signature=sig password=pw\r\nsource-message";

#[test]
fn schemas_cover_all_value_kinds_and_visibility_classes() {
    let cases = [
        (
            fields::TOPIC.schema(),
            FieldValueKind::Text,
            ContextVisibility::Public,
            Some(127),
        ),
        (
            fields::BROKER_CODE.schema(),
            FieldValueKind::I64,
            ContextVisibility::Diagnostic,
            None,
        ),
        (
            fields::TIMEOUT_MS.schema(),
            FieldValueKind::U64,
            ContextVisibility::Public,
            None,
        ),
        (
            fields::ATTEMPTED.schema(),
            FieldValueKind::Bool,
            ContextVisibility::Diagnostic,
            None,
        ),
        (
            fields::CREDENTIALS_PRESENT.schema(),
            FieldValueKind::Presence,
            ContextVisibility::SecretPresenceOnly,
            None,
        ),
    ];

    for (schema, kind, visibility, limit) in cases {
        assert_eq!(schema.value_kind(), kind, "{}", schema.name());
        assert_eq!(schema.visibility(), visibility, "{}", schema.name());
        assert_eq!(schema.text_byte_limit(), limit, "{}", schema.name());
    }
}

#[test]
fn public_iteration_excludes_diagnostics_and_secret_presence() {
    let context = ErrorContext::new()
        .with_text(fields::TOPIC, "TopicA")
        .with_text(fields::ADDR, "10.0.0.1:10911")
        .with_secret_presence(fields::CREDENTIALS_PRESENT)
        .with_i64(fields::QUEUE_ID, -1)
        .with_u64(fields::TIMEOUT_MS, 500)
        .with_bool(fields::ATTEMPTED, true);

    let fields = context.public_fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name(), "topic");
    assert_eq!(fields[0].value(), FieldValueRef::Text("TopicA"));
    assert_eq!(fields[1].value(), FieldValueRef::I64(-1));
    assert_eq!(fields[2].value(), FieldValueRef::U64(500));

    let display = context.to_string();
    assert!(display.contains("addr=<redacted>"));
    assert!(display.contains("credentials_present=<redacted>"));
    assert!(display.contains("attempted=<redacted>"));
    assert!(!display.contains("10.0.0.1:10911"));
    assert!(!format!("{context:?}").contains("10.0.0.1:10911"));
}

#[test]
fn text_is_control_safe_bounded_and_utf8_valid() {
    let value = format!("prefix\r\n\u{0085}{}界", "x".repeat(120));
    let context = ErrorContext::new().with_text(fields::TOPIC, &value);
    let field = context.public_fields().next().expect("public topic");
    let FieldValueRef::Text(text) = field.value() else {
        panic!("topic must remain text");
    };

    assert!(text.len() <= 127);
    assert!(!text.chars().any(char::is_control));
    assert!(text.starts_with("prefix   "));
    assert!(context.is_truncated());
    assert!(std::str::from_utf8(text.as_bytes()).is_ok());
}

#[test]
fn duplicate_names_preserve_first_value_and_mark_truncation() {
    let context = ErrorContext::new()
        .with_text(fields::OPERATION, "publish")
        .with_text(fields::OPERATION_DIAGNOSTIC, "secret diagnostic operation");

    assert_eq!(context.len(), 1);
    assert!(context.is_truncated());
    let field = context.public_fields().next().expect("first public operation");
    assert_eq!(field.value(), FieldValueRef::Text("publish"));
    assert!(!context.to_string().contains("secret diagnostic operation"));
}

#[test]
fn context_capacity_is_sixteen_unique_fields() {
    let context = context_at_capacity().with_bool(fields::INSTALLED, true);

    assert_eq!(context.len(), 16);
    assert!(context.is_truncated());
    assert!(!context.to_string().contains("installed="));
}

fn context_at_capacity() -> ErrorContext {
    ErrorContext::new()
        .with_text(fields::TOPIC, "topic")
        .with_text(fields::CLUSTER, "cluster")
        .with_text(fields::GROUP, "group")
        .with_text(fields::PROPERTY, "property")
        .with_text(fields::RESOURCE, "resource")
        .with_text(fields::FIELD, "field")
        .with_text(fields::FEATURE, "feature")
        .with_text(fields::OPERATION, "operation")
        .with_u64(fields::ACTUAL_BYTES, 1)
        .with_u64(fields::LIMIT_BYTES, 2)
        .with_i64(fields::QUEUE_ID, 3)
        .with_i64(fields::MAX_QUEUE_ID, 4)
        .with_u64(fields::TIMEOUT_MS, 5)
        .with_u64(fields::DURATION_MS, 6)
        .with_i64(fields::PERMISSION_VALUE, 7)
        .with_bool(fields::ATTEMPTED, true)
}

#[test]
fn sentinel_never_enters_context_or_safe_boundary_output() {
    let secret = Sensitive::new(SENTINEL.to_owned());
    assert_eq!(secret.expose_secret(), SENTINEL);

    let context = ErrorContext::new()
        .with_secret_presence(fields::CREDENTIALS_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);
    let duplicate = ErrorContext::new()
        .with_secret_presence(fields::CREDENTIALS_PRESENT)
        .with_secret_presence(fields::CREDENTIALS_PRESENT);
    let over_capacity = context_at_capacity().with_secret_presence(fields::SOURCE_PRESENT);
    assert!(duplicate.is_truncated());
    assert!(over_capacity.is_truncated());
    for context in [&context, &duplicate, &over_capacity] {
        assert!(context.public_fields().all(|field| match field.value() {
            FieldValueRef::Text(value) => !value.contains(SENTINEL),
            _ => true,
        }));
        assert!(!context.to_string().contains(SENTINEL));
        assert!(!format!("{context:?}").contains(SENTINEL));
    }

    let error = RocketMQError::internal("sentinel operation", std::io::Error::other(SENTINEL));
    let boundary = error.boundary_view();
    let cli = CliErrorView::from_error(&error);
    assert!(!boundary.context().to_string().contains(SENTINEL));
    assert!(!format!("{boundary:?}").contains(SENTINEL));
    assert!(!cli.output(CliVerbosity::Default).stderr().contains(SENTINEL));
    assert!(!cli.output(CliVerbosity::Verbose).stderr().contains(SENTINEL));
}
