use std::collections::HashSet;

use rocketmq_error::error_spec;
use rocketmq_error::ErrorKind;
use rocketmq_error::ErrorSpec;
use rocketmq_error::RocketMQError;
use rocketmq_error::ALL_ERROR_SPECS;

#[test]
fn every_error_kind_has_one_spec() {
    assert_eq!(ALL_ERROR_SPECS.len(), ErrorKind::ALL.len());

    let mut kinds = HashSet::new();
    for spec in ALL_ERROR_SPECS {
        assert!(kinds.insert(spec.kind), "duplicate spec for {:?}", spec.kind);
        assert_eq!(spec.code, spec.kind.code());
        assert_eq!(spec.scope, spec.kind.scope());
        assert!(!spec.public_message.is_empty());
    }

    for kind in ErrorKind::ALL {
        assert!(kinds.contains(kind), "missing spec for {kind:?}");
    }
}

#[test]
fn error_spec_lookup_returns_static_spec() {
    let spec = error_spec(ErrorKind::RouteNotFound);

    assert_eq!(spec.kind, ErrorKind::RouteNotFound);
    assert_eq!(spec.code.as_str(), "ROUTE_NOT_FOUND");
    assert_eq!(spec.public_message, "Route information was not found");
}

#[test]
fn rocketmq_error_reports_spec() {
    let error = RocketMQError::route_not_found("TopicA");
    let spec: &'static ErrorSpec = error.spec();

    assert_eq!(spec.kind, ErrorKind::RouteNotFound);
    assert_eq!(spec.code.as_str(), "ROUTE_NOT_FOUND");
}
