use rocketmq_error::ErrorContext;
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
