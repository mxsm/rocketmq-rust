use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorKind;
use rocketmq_error::ObservabilityError;
use rocketmq_error::RedactionKind;
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
        .with_field("topic", "TopicA")
        .with_sensitive("secret_key", Sensitive::new("sk-123"))
        .with_sensitive("token", Sensitive::new("token-456"));

    assert_eq!(context.len(), 3);
    assert_eq!(context.fields()[0].redaction, RedactionKind::Public);
    assert_eq!(context.fields()[1].redaction, RedactionKind::Sensitive);
    assert_eq!(context.fields()[1].value, "<redacted>");

    let display = context.to_string();
    assert!(display.contains("topic=TopicA"));
    assert!(display.contains("secret_key=<redacted>"));
    assert!(display.contains("token=<redacted>"));
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

    let internal = RocketMQError::Internal("password=plain-text".to_string());
    let context = internal.context();

    assert_eq!(internal.public_message(), "Internal error");
    assert_eq!(context.fields()[0].redaction, RedactionKind::Sensitive);
    assert_eq!(context.to_string(), "internal_error=<redacted>");
    assert!(!context.to_string().contains("plain-text"));
}

#[test]
fn boundary_view_exposes_public_message_and_redacted_context() {
    let error = RocketMQError::Internal("password=plain-text".to_string());
    let view = error.boundary_view();

    assert_eq!(view.code().as_str(), "INTERNAL");
    assert_eq!(view.message(), "Internal error");
    assert_eq!(view.context().to_string(), "internal_error=<redacted>");
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
    assert_eq!(context.fields()[0].key, "reason");
    assert_eq!(context.fields()[0].redaction, RedactionKind::Sensitive);
    assert_eq!(context.to_string(), "reason=<redacted>");
    assert!(!context.to_string().contains("secret"));

    let filter = RocketMQError::from(ObservabilityError::invalid_log_filter(
        "rocketmq_store=trace",
        "invalid directive",
    ));
    let context = filter.context();

    assert_eq!(filter.kind(), ErrorKind::ObservabilityLogFilterInvalid);
    assert_eq!(context.len(), 2);
    assert_eq!(context.fields()[0].key, "filter");
    assert_eq!(context.fields()[0].redaction, RedactionKind::Sensitive);
    assert_eq!(context.fields()[1].key, "error");
    assert_eq!(context.fields()[1].redaction, RedactionKind::Sensitive);
    assert!(!context.to_string().contains("rocketmq_store=trace"));
    assert!(!context.to_string().contains("invalid directive"));
}
