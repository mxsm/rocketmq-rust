use rocketmq_error::ObservabilityError;
use rocketmq_error::RocketMQError;

#[test]
fn observability_error_preserves_existing_variants() {
    let error = ObservabilityError::invalid_log_filter("rocketmq_store==debug", "invalid directive");

    assert!(matches!(
        error,
        ObservabilityError::InvalidLogFilter { filter, error }
            if filter == "rocketmq_store==debug" && error == "invalid directive"
    ));
}

#[test]
fn observability_subscriber_install_failure_uses_primitive_status() {
    let error = ObservabilityError::subscriber_install_failed(true, false);

    assert!(matches!(
        error,
        ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false
        }
    ));
}

#[test]
fn observability_error_converts_to_rocketmq_error() {
    let error = RocketMQError::from(ObservabilityError::metrics_init("exporter failed"));

    assert!(matches!(error, RocketMQError::Observability(_)));
}
