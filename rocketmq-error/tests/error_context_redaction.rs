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

use std::error::Error as _;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::FieldValueRef;
use rocketmq_error::RecoveryHint;
use rocketmq_error::Sensitive;
use rocketmq_error::AUTH_OPERATION_FAILED;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::OBSERVABILITY_INITIALIZATION_FAILED;
use rocketmq_error::OBSERVABILITY_LOG_FILTER_INVALID;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROTOCOL_HEADER_INVALID;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;

#[test]
fn sensitive_display_and_debug_are_redacted() {
    let secret = Sensitive::new("secret-value");

    assert_eq!(secret.expose_secret(), &"secret-value");
    assert_eq!(secret.to_string(), "<redacted>");
    assert!(!format!("{secret:?}").contains("secret-value"));
}

#[test]
fn error_context_redacts_sensitive_fields() {
    let context = ErrorContext::new()
        .with_text(fields::TOPIC, "TopicA")
        .with_secret_presence(fields::CREDENTIALS_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);

    let public = context.public_fields().collect::<Vec<_>>();
    assert_eq!(public.len(), 1);
    assert_eq!(public[0].value(), FieldValueRef::Text("TopicA"));
    assert_eq!(
        context.to_string(),
        "topic=TopicA, credentials_present=<redacted>, source_present=<redacted>"
    );
}

#[test]
fn canonical_error_exposes_only_catalog_message_and_safe_context() {
    let route = Error::new(&ROUTE_TOPIC_NOT_FOUND).with_context(ErrorContext::new().with_text(fields::TOPIC, "TopicA"));
    assert_eq!(
        route.public_view().expect("valid view").message(),
        "Topic route was not found"
    );

    let internal = Error::caused_by(&CORE_INTERNAL_FAILURE, std::io::Error::other("password=plain-text")).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, "run internal operation")
            .with_secret_presence(fields::SOURCE_PRESENT),
    );
    let view = internal.public_view().expect("valid view");
    assert_eq!(view.message(), "Internal error");
    assert_eq!(view.fields().count(), 0);
    assert!(!format!("{view:?}").contains("plain-text"));
    assert_eq!(internal.recovery_hint(), RecoveryHint::OperatorAction);
}

#[test]
fn observability_context_never_contains_sensitive_details() {
    let init = Error::new(&OBSERVABILITY_INITIALIZATION_FAILED).with_context(
        ErrorContext::new()
            .with_text(fields::OBSERVABILITY_SIGNAL, "metrics")
            .with_secret_presence(fields::REASON_PRESENT),
    );
    let filter = Error::new(&OBSERVABILITY_LOG_FILTER_INVALID).with_context(
        ErrorContext::new()
            .with_secret_presence(fields::FILTER_PRESENT)
            .with_secret_presence(fields::ERROR_PRESENT),
    );

    for error in [init, filter] {
        assert!(error.context().public_fields().next().is_none());
        assert!(!error.context().to_string().contains("secret"));
    }
}

#[test]
fn request_boundary_errors_preserve_typed_source_chains() {
    let cases = [
        (&PROTOCOL_BODY_INVALID, "decode checkpoint", "body-secret"),
        (&PROTOCOL_HEADER_INVALID, "decode maintenance header", "header-secret"),
        (&AUTH_OPERATION_FAILED, "authorize checkpoint", "auth-secret"),
    ];

    for (descriptor, operation, secret) in cases {
        let error = Error::caused_by(descriptor, std::io::Error::other(secret)).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, operation)
                .with_secret_presence(fields::SOURCE_PRESENT),
        );
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
        assert!(!format!("{:?}", error.public_view().expect("valid view")).contains(secret));
    }
}
