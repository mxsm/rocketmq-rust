use rocketmq_error::ErrorKind;
use rocketmq_error::ErrorSeverity;
use rocketmq_error::RetryClass;
use rocketmq_error::ALL_ERROR_SPECS;

#[test]
fn selected_error_kinds_have_recovery_policy() {
    assert_eq!(ErrorKind::RouteNotFound.spec().recovery.retry, RetryClass::RefreshRoute);
    assert_eq!(
        ErrorKind::BrokerOperationFailed.spec().recovery.retry,
        RetryClass::SwitchBroker
    );
    assert_eq!(
        ErrorKind::ControllerNotLeader.spec().recovery.retry,
        RetryClass::RefreshLeader
    );
    assert_eq!(ErrorKind::AuthConfigInvalid.spec().recovery.retry, RetryClass::Never);
}

#[test]
fn selected_error_kinds_have_observability_policy() {
    let route = ErrorKind::RouteNotFound.spec();
    assert_eq!(route.observe.severity, ErrorSeverity::Warn);
    assert_eq!(route.observe.metric_label, "ROUTE_NOT_FOUND");

    let internal = ErrorKind::Internal.spec();
    assert_eq!(internal.observe.severity, ErrorSeverity::Error);
    assert_eq!(internal.observe.metric_label, "INTERNAL");

    let invalid = ErrorKind::RequestHeaderError.spec();
    assert_eq!(invalid.observe.severity, ErrorSeverity::Info);
}

#[test]
fn observability_errors_have_expected_policy() {
    assert_eq!(
        ErrorKind::ObservabilityConfigInvalid.spec().recovery.retry,
        RetryClass::Never
    );
    assert_eq!(
        ErrorKind::ObservabilityMetricsInitFailed.spec().observe.severity,
        ErrorSeverity::Error
    );
}

#[test]
fn every_error_spec_has_low_cardinality_observability_labels() {
    for spec in ALL_ERROR_SPECS {
        assert_eq!(spec.observe.metric_label, spec.code.as_str());
        assert!(!spec.observe.metric_label.is_empty());
        assert!(!spec.observe.metric_label.contains(' '));
    }
}
