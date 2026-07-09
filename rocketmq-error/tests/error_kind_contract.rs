use std::collections::HashSet;

use rocketmq_error::ErrorCategory;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorKind;
use rocketmq_error::ErrorScope;
use rocketmq_error::RocketMQError;

#[test]
fn error_code_is_a_stable_machine_code() {
    let code = ErrorCode::new("NETWORK_CONNECTION_FAILED");

    assert_eq!(code.as_str(), "NETWORK_CONNECTION_FAILED");
    assert_eq!(code.to_string(), "NETWORK_CONNECTION_FAILED");
    assert_eq!(code, ErrorKind::Network.code());
}

#[test]
fn error_kind_exposes_code_scope_and_category() {
    let cases = [
        (
            ErrorKind::BrokerOperationFailed,
            ErrorScope::Broker,
            ErrorCategory::Broker,
            "BROKER_OPERATION_FAILED",
        ),
        (
            ErrorKind::RouteNotFound,
            ErrorScope::Route,
            ErrorCategory::Route,
            "ROUTE_NOT_FOUND",
        ),
        (
            ErrorKind::StorageWriteFailed,
            ErrorScope::Storage,
            ErrorCategory::Storage,
            "STORAGE_WRITE_FAILED",
        ),
        (
            ErrorKind::AuthConfigInvalid,
            ErrorScope::Configuration,
            ErrorCategory::Configuration,
            "AUTH_CONFIG_INVALID",
        ),
        (
            ErrorKind::Internal,
            ErrorScope::System,
            ErrorCategory::System,
            "INTERNAL",
        ),
    ];

    for (kind, scope, category, code) in cases {
        assert_eq!(kind.scope(), scope);
        assert_eq!(kind.category(), category);
        assert_eq!(kind.code().as_str(), code);
    }
}

#[test]
fn observability_kinds_have_observability_scope_and_category() {
    let cases = [
        ErrorKind::ObservabilityFeatureDisabled,
        ErrorKind::ObservabilityConfigInvalid,
        ErrorKind::ObservabilityMetricsInitFailed,
        ErrorKind::ObservabilityTracesInitFailed,
        ErrorKind::ObservabilityLogsInitFailed,
        ErrorKind::ObservabilityLoggingInitFailed,
        ErrorKind::ObservabilityLogFilterInvalid,
        ErrorKind::ObservabilitySubscriberInstallFailed,
        ErrorKind::ObservabilityMetricsShutdownFailed,
        ErrorKind::ObservabilityTracesShutdownFailed,
        ErrorKind::ObservabilityLogsShutdownFailed,
    ];

    for kind in cases {
        assert_eq!(kind.scope(), ErrorScope::Observability);
        assert_eq!(kind.category(), ErrorCategory::Observability);
    }
}

#[test]
fn all_error_kinds_have_unique_codes() {
    let mut codes = HashSet::new();

    for kind in ErrorKind::ALL {
        assert!(
            codes.insert(kind.code()),
            "duplicate error code for {:?}: {}",
            kind,
            kind.code()
        );
    }

    assert!(ErrorKind::ALL.contains(&ErrorKind::Network));
    assert!(ErrorKind::ALL.contains(&ErrorKind::NotInitialized));
}

#[test]
fn rocketmq_error_reports_kind() {
    let route = RocketMQError::route_not_found("TopicA");
    assert_eq!(route.kind(), ErrorKind::RouteNotFound);

    let storage = RocketMQError::storage_write_failed("commitlog", "disk full");
    assert_eq!(storage.kind(), ErrorKind::StorageWriteFailed);

    let auth = RocketMQError::auth_config_invalid("auth.authorization", "missing provider");
    assert_eq!(auth.kind(), ErrorKind::AuthConfigInvalid);
}
