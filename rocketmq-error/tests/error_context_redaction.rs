use rocketmq_error::fields;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorKind;
use rocketmq_error::FieldValueRef;
use rocketmq_error::ObservabilityError;
use rocketmq_error::RocketMQError;
use rocketmq_error::Sensitive;

#[test]
fn sensitive_display_and_debug_are_redacted() {
    let secret = Sensitive::new("secret-value");

    assert_eq!(secret.expose_secret(), &"secret-value");
    assert_eq!(secret.to_string(), "<redacted>");

    let debug = format!("{secret:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("secret-value"));
}

#[test]
fn error_context_redacts_sensitive_fields() {
    let context = ErrorContext::new()
        .with_text(fields::TOPIC, "TopicA")
        .with_secret_presence(fields::CREDENTIALS_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);

    assert_eq!(context.len(), 3);
    let public = context.public_fields().collect::<Vec<_>>();
    assert_eq!(public.len(), 1);
    assert_eq!(public[0].name(), "topic");
    assert_eq!(public[0].value(), FieldValueRef::Text("TopicA"));

    let display = context.to_string();
    assert!(display.contains("topic=TopicA"));
    assert!(display.contains("credentials_present=<redacted>"));
    assert!(display.contains("source_present=<redacted>"));
    assert!(!display.contains("sk-123"));
    assert!(!display.contains("token-456"));

    let debug = format!("{context:?}");
    assert!(!debug.contains("sk-123"));
    assert!(!debug.contains("token-456"));
}

#[test]
fn rocketmq_error_exposes_public_message_and_redacted_context() {
    let route = RocketMQError::route_not_found("TopicA");

    assert_eq!(route.public_message(), "Route information was not found");
    assert_eq!(route.context().to_string(), "topic=TopicA");

    let internal = RocketMQError::internal("run internal operation", std::io::Error::other("password=plain-text"));
    let context = internal.context();

    assert_eq!(internal.public_message(), "Internal error");
    assert!(context.public_fields().next().is_none());
    assert_eq!(context.to_string(), "operation=<redacted>, internal_error=<redacted>");
    assert!(!context.to_string().contains("plain-text"));
}

#[test]
fn boundary_view_exposes_public_message_and_redacted_context() {
    let error = RocketMQError::internal("run internal operation", std::io::Error::other("password=plain-text"));
    let view = error.boundary_view();

    assert_eq!(view.code().as_str(), "INTERNAL");
    assert_eq!(view.message(), "Internal error");
    assert_eq!(
        view.context().to_string(),
        "operation=<redacted>, internal_error=<redacted>"
    );
    assert!(!view.context().to_string().contains("plain-text"));
    assert!(!view.is_retryable());
}

#[test]
fn observability_error_context_redacts_sensitive_details() {
    let init = RocketMQError::from(ObservabilityError::metrics_init(
        "endpoint=http://127.0.0.1:4317?token=secret",
    ));

    assert_eq!(init.kind(), ErrorKind::ObservabilityMetricsInitFailed);
    let context = init.context();
    assert!(context.public_fields().next().is_none());
    assert_eq!(context.to_string(), "reason=<redacted>");
    assert!(!context.to_string().contains("secret"));

    let filter = RocketMQError::from(ObservabilityError::invalid_log_filter(
        "rocketmq_store=trace",
        "invalid directive",
    ));
    let context = filter.context();

    assert_eq!(filter.kind(), ErrorKind::ObservabilityLogFilterInvalid);
    assert_eq!(context.len(), 2);
    assert!(context.public_fields().next().is_none());
    assert!(!context.to_string().contains("rocketmq_store=trace"));
    assert!(!context.to_string().contains("invalid directive"));
}

#[test]
fn request_boundary_errors_preserve_typed_source_chains() {
    let body = RocketMQError::request_body_source(
        "decode checkpoint",
        std::io::Error::new(std::io::ErrorKind::InvalidData, "body-secret"),
    );
    let header = RocketMQError::request_header_source(
        "decode maintenance header",
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "header-secret"),
    );
    let authentication = RocketMQError::authentication_source(
        "authorize checkpoint",
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "auth-secret"),
    );

    for (error, expected_kind, secret) in [
        (body, ErrorKind::RequestBodyInvalid, "body-secret"),
        (header, ErrorKind::RequestHeaderError, "header-secret"),
        (authentication, ErrorKind::Authentication, "auth-secret"),
    ] {
        assert_eq!(error.kind(), expected_kind);
        let source = error.source().expect("typed source must be retained");
        assert!(source.downcast_ref::<std::io::Error>().is_some());
        assert!(source.to_string().contains(secret));
        assert!(!error.boundary_view().context().to_string().contains(secret));
    }
}
use std::error::Error;
