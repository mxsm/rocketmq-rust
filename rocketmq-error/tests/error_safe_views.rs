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

use std::iter::FusedIterator;

use rocketmq_error::fields;
use rocketmq_error::ContextVisibility;
use rocketmq_error::DiagnosticView;
use rocketmq_error::ErrorContext;
use rocketmq_error::FieldValueKind;
use rocketmq_error::PublicErrorView;
use rocketmq_error::ViewContextViolation;
use rocketmq_error::ViewValueRef;
use rocketmq_error::ALL_DESCRIPTORS;
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_PERMISSION_DENIED;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
use rocketmq_error::STORAGE_STATE_CORRUPTED;
use rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT;

const SENTINEL: &str = "Bearer token-secret secret_key=sk signature=sig password=pw\r\nsource-message";

#[test]
fn every_descriptor_accepts_empty_context_with_exact_identity_and_projection() {
    let context = ErrorContext::new();

    for descriptor in ALL_DESCRIPTORS {
        let public = PublicErrorView::try_new(descriptor, &context).expect("public view");
        let diagnostic = DiagnosticView::try_new(descriptor, &context).expect("diagnostic view");

        assert_eq!(public.code(), descriptor.code());
        assert_eq!(public.message(), descriptor.public_message());
        assert_eq!(public.projection(), descriptor.projection());
        assert!(!public.is_truncated());
        assert!(public.fields().next().is_none());

        assert_eq!(diagnostic.code(), descriptor.code());
        assert_eq!(diagnostic.message(), descriptor.public_message());
        assert_eq!(diagnostic.condition(), descriptor.condition());
        assert_eq!(diagnostic.severity(), descriptor.severity());
        assert_eq!(diagnostic.recovery_hint(), descriptor.recovery_hint());
        assert_eq!(diagnostic.projection(), descriptor.projection());
        assert!(!diagnostic.is_truncated());
        assert!(diagnostic.fields().next().is_none());
    }
}

#[test]
fn public_and_diagnostic_views_filter_fields_in_descriptor_order() {
    let context = ErrorContext::new()
        .with_text(fields::REMOTE_ADDR, "10.0.0.8:10911")
        .with_u64(fields::TIMEOUT_MS, 500);
    let public = PublicErrorView::try_new(&TRANSPORT_CONNECTION_TIMEOUT, &context).expect("public view");
    let diagnostic = DiagnosticView::try_new(&TRANSPORT_CONNECTION_TIMEOUT, &context).expect("diagnostic view");

    let public_fields = public.fields().collect::<Vec<_>>();
    assert_eq!(public_fields.len(), 1);
    assert_eq!(public_fields[0].name(), "timeout_ms");
    assert_eq!(public_fields[0].visibility(), ContextVisibility::Public);
    assert_eq!(public_fields[0].value(), ViewValueRef::U64(500));

    let diagnostic_fields = diagnostic.fields().collect::<Vec<_>>();
    assert_eq!(diagnostic_fields.len(), 2);
    assert_eq!(diagnostic_fields[0].name(), "timeout_ms");
    assert_eq!(diagnostic_fields[0].value(), ViewValueRef::U64(500));
    assert_eq!(diagnostic_fields[1].name(), "remote_addr");
    assert_eq!(diagnostic_fields[1].visibility(), ContextVisibility::Diagnostic);
    assert_eq!(diagnostic_fields[1].value(), ViewValueRef::Text("10.0.0.8:10911"));

    let sparse_context = ErrorContext::new().with_text(fields::REMOTE_ADDR, "10.0.0.9:10911");
    let sparse =
        DiagnosticView::try_new(&TRANSPORT_CONNECTION_TIMEOUT, &sparse_context).expect("sparse diagnostic view");
    let sparse_fields = sparse.fields().collect::<Vec<_>>();
    assert_eq!(sparse_fields.len(), 1);
    assert_eq!(sparse_fields[0].name(), "remote_addr");
    assert_eq!(sparse_fields[0].value(), ViewValueRef::Text("10.0.0.9:10911"));
}

#[test]
fn visibility_and_value_kinds_are_exposed_only_when_catalog_allows_them() {
    let route_context = ErrorContext::new().with_text(fields::TOPIC, "orders");
    let route = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &route_context).expect("route public view");
    assert_eq!(
        route.fields().next().expect("topic").value(),
        ViewValueRef::Text("orders")
    );

    let storage_context = ErrorContext::new()
        .with_text(fields::STORE_OPERATION, "read")
        .with_text(fields::STORE_COMPONENT, "commit_log")
        .with_secret_presence(fields::STORE_DETAIL_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);
    let public = PublicErrorView::try_new(&STORAGE_STATE_CORRUPTED, &storage_context).expect("storage public view");
    let storage = DiagnosticView::try_new(&STORAGE_STATE_CORRUPTED, &storage_context).expect("storage diagnostic view");
    assert!(public.fields().next().is_none());
    let fields = storage.fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].value(), ViewValueRef::Text("read"));
    assert_eq!(fields[1].value(), ViewValueRef::Text("commit_log"));
    assert_eq!(fields[2].value(), ViewValueRef::Redacted);
    assert_eq!(fields[3].value(), ViewValueRef::Redacted);

    let secret_context = ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT);
    let public = PublicErrorView::try_new(&AUTH_CREDENTIALS_INVALID, &secret_context).expect("public view");
    let diagnostic = DiagnosticView::try_new(&AUTH_CREDENTIALS_INVALID, &secret_context).expect("diagnostic view");
    assert!(public.fields().next().is_none());
    let secret = diagnostic.fields().next().expect("secret presence marker");
    assert_eq!(secret.name(), "credentials_present");
    assert_eq!(secret.visibility(), ContextVisibility::SecretPresenceOnly);
    assert_eq!(secret.value(), ViewValueRef::Redacted);

    let boolean_context = ErrorContext::new().with_bool(fields::ATTEMPTED, true);
    let violation = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &boolean_context)
        .expect_err("the catalog declares no Boolean field");
    let ViewContextViolation::UndeclaredField { actual, .. } = violation else {
        panic!("Boolean context field must be undeclared");
    };
    assert_eq!(actual.value_kind(), FieldValueKind::Bool);
}

#[test]
fn diagnostic_view_exposes_diagnostic_text_and_key_only_secret_markers() {
    let context = ErrorContext::new()
        .with_text(fields::OPERATION_DIAGNOSTIC, "compact")
        .with_secret_presence(fields::SOURCE_PRESENT);
    let public = PublicErrorView::try_new(&CORE_INTERNAL_FAILURE, &context).expect("public view");
    let diagnostic = DiagnosticView::try_new(&CORE_INTERNAL_FAILURE, &context).expect("diagnostic view");

    assert!(public.fields().next().is_none());
    let fields = diagnostic.fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "operation");
    assert_eq!(fields[0].visibility(), ContextVisibility::Diagnostic);
    assert_eq!(fields[0].value(), ViewValueRef::Text("compact"));
    assert_eq!(fields[1].name(), "source_present");
    assert_eq!(fields[1].visibility(), ContextVisibility::SecretPresenceOnly);
    assert_eq!(fields[1].value(), ViewValueRef::Redacted);
}

#[test]
fn validation_returns_the_first_full_schema_violation_for_both_views() {
    let undeclared_context = ErrorContext::new()
        .with_text(fields::ADDR, "10.0.0.8:10911")
        .with_text(fields::OPERATION_DIAGNOSTIC, "ignored-after-first");
    let public_undeclared = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &undeclared_context)
        .expect_err("address is not declared for route lookup");
    let diagnostic_undeclared = DiagnosticView::try_new(&ROUTE_TOPIC_NOT_FOUND, &undeclared_context)
        .expect_err("address is not declared for route lookup");
    assert_eq!(public_undeclared, diagnostic_undeclared);
    assert_eq!(
        public_undeclared,
        ViewContextViolation::UndeclaredField {
            descriptor: ROUTE_TOPIC_NOT_FOUND.code(),
            actual: fields::ADDR.schema(),
        }
    );

    let mismatch_context = ErrorContext::new()
        .with_text(fields::OPERATION_DIAGNOSTIC, "private operation")
        .with_text(fields::ADDR, "ignored-after-first");
    let public_mismatch = PublicErrorView::try_new(&AUTH_PERMISSION_DENIED, &mismatch_context)
        .expect_err("operation visibility must exactly match the descriptor");
    let diagnostic_mismatch = DiagnosticView::try_new(&AUTH_PERMISSION_DENIED, &mismatch_context)
        .expect_err("operation visibility must exactly match the descriptor");
    assert_eq!(public_mismatch, diagnostic_mismatch);
    assert_eq!(
        public_mismatch,
        ViewContextViolation::SchemaMismatch {
            descriptor: AUTH_PERMISSION_DENIED.code(),
            expected: fields::OPERATION.schema(),
            actual: fields::OPERATION_DIAGNOSTIC.schema(),
        }
    );
}

#[test]
fn normalized_and_duplicate_valid_contexts_remain_viewable_and_report_truncation() {
    let oversized_topic = format!("orders-{}", "x".repeat(256));
    let normalized = ErrorContext::new().with_text(fields::TOPIC, &oversized_topic);
    let public = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &normalized).expect("normalized public view");
    let diagnostic = DiagnosticView::try_new(&ROUTE_TOPIC_NOT_FOUND, &normalized).expect("normalized diagnostic view");
    assert!(public.is_truncated());
    assert!(diagnostic.is_truncated());
    let ViewValueRef::Text(topic) = public.fields().next().expect("topic").value() else {
        panic!("topic must remain text");
    };
    assert!(topic.len() <= 127);
    assert!(topic.starts_with("orders-"));

    let duplicate = ErrorContext::new()
        .with_text(fields::OPERATION, "publish")
        .with_text(fields::OPERATION_DIAGNOSTIC, "discarded diagnostic operation");
    let public = PublicErrorView::try_new(&AUTH_PERMISSION_DENIED, &duplicate).expect("duplicate public view");
    let diagnostic = DiagnosticView::try_new(&AUTH_PERMISSION_DENIED, &duplicate).expect("duplicate diagnostic view");
    assert!(public.is_truncated());
    assert!(diagnostic.is_truncated());
    assert_eq!(
        public.fields().next().expect("operation").value(),
        ViewValueRef::Text("publish")
    );
    assert_eq!(
        diagnostic.fields().next().expect("operation").value(),
        ViewValueRef::Text("publish")
    );
}

#[test]
fn field_iterators_are_fused_and_borrow_text_without_projection_allocations() {
    fn requires_fused<I: FusedIterator>(_: I) {}

    let context = ErrorContext::new()
        .with_text(fields::REMOTE_ADDR, "10.0.0.8:10911")
        .with_u64(fields::TIMEOUT_MS, 500);
    let public = PublicErrorView::try_new(&TRANSPORT_CONNECTION_TIMEOUT, &context).expect("public view");
    let diagnostic = DiagnosticView::try_new(&TRANSPORT_CONNECTION_TIMEOUT, &context).expect("diagnostic view");
    requires_fused(public.fields());
    requires_fused(diagnostic.fields());

    let public_text = context.public_fields().next().expect("timeout is public");
    assert_eq!(public_text.name(), "timeout_ms");
    let diagnostic_field = diagnostic.fields().nth(1).expect("remote address");
    let ViewValueRef::Text(remote_addr) = diagnostic_field.value() else {
        panic!("remote address must remain borrowed text");
    };
    let context_remote = context.public_fields().find(|field| field.name() == "remote_addr");
    assert!(
        context_remote.is_none(),
        "diagnostic address must not enter public context iteration"
    );
    assert_eq!(remote_addr, "10.0.0.8:10911");

    let mut fields = public.fields();
    assert_eq!(fields.next().expect("timeout").name(), "timeout_ms");
    assert!(fields.next().is_none());
    assert!(fields.next().is_none());
}

#[test]
fn secret_sentinel_never_enters_safe_views_or_violations() {
    let secret_context = ErrorContext::new().with_secret_presence(fields::CREDENTIALS_PRESENT);
    let public = PublicErrorView::try_new(&AUTH_CREDENTIALS_INVALID, &secret_context).expect("public view");
    let diagnostic = DiagnosticView::try_new(&AUTH_CREDENTIALS_INVALID, &secret_context).expect("diagnostic view");
    let safe_output = format!(
        "{public:?} {diagnostic:?} {:?}",
        diagnostic.fields().collect::<Vec<_>>()
    );
    assert!(safe_output.contains("<redacted>"));
    assert!(!safe_output.contains(SENTINEL));
    assert!(!safe_output.contains("token-secret"));

    let invalid_context = ErrorContext::new().with_text(fields::ADDR, SENTINEL);
    let violation = PublicErrorView::try_new(&ROUTE_TOPIC_NOT_FOUND, &invalid_context)
        .expect_err("undeclared address must not create a partial view");
    let violation_output = format!("{violation:?} {violation}");
    assert!(!violation_output.contains(SENTINEL));
    assert!(!violation_output.contains("token-secret"));
}

#[test]
fn safe_view_api_keeps_representation_private_and_does_not_add_text_or_serde_contracts() {
    let crate_root = include_str!("../src/lib.rs");
    let view_source = include_str!("../src/view.rs");

    assert!(crate_root.contains("mod view;"));
    assert!(!crate_root.contains("pub mod view;"));
    for reexport in [
        "pub use view::PublicErrorView;",
        "pub use view::DiagnosticView;",
        "pub use view::PublicFields;",
        "pub use view::DiagnosticFields;",
        "pub use view::ViewFieldRef;",
        "pub use view::ViewValueRef;",
        "pub use view::ViewContextViolation;",
    ] {
        assert!(crate_root.contains(reexport), "missing root re-export: {reexport}");
    }
    assert_eq!(view_source.matches("pub fn try_new").count(), 2);
    assert!(!view_source.contains("pub descriptor:"));
    assert!(!view_source.contains("pub context:"));
    assert!(!view_source.contains("pub fn context("));
    assert!(!view_source.contains("impl fmt::Display for PublicErrorView"));
    assert!(!view_source.contains("impl fmt::Display for DiagnosticView"));
    assert!(!view_source.contains("impl fmt::Display for PublicFields"));
    assert!(!view_source.contains("impl fmt::Display for DiagnosticFields"));
    assert!(!view_source.contains("serde::Serialize"));
    assert!(view_source.contains("impl fmt::Debug for PublicErrorView"));
    assert!(view_source.contains("impl fmt::Debug for DiagnosticView"));
    assert!(view_source.contains("impl FusedIterator for PublicFields"));
    assert!(view_source.contains("impl FusedIterator for DiagnosticFields"));
    assert!(!view_source.contains("Vec<"));
    assert!(!view_source.contains("Box<"));
}
