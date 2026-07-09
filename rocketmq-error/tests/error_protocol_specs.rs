use rocketmq_error::CliExitCode;
use rocketmq_error::ErrorKind;
use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::ALL_ERROR_SPECS;

#[test]
fn selected_error_kinds_have_protocol_primitives() {
    let route = ErrorKind::RouteNotFound.spec();
    assert_eq!(route.remoting.code, RemotingResponseCode::TopicNotExist);
    assert_eq!(route.grpc.payload, GrpcPayloadCode::TopicNotFound);
    assert_eq!(route.grpc.status, GrpcStatusCode::NotFound);
    assert_eq!(route.http.status, HttpStatusCode::NOT_FOUND);
    assert_eq!(route.cli.exit_code, CliExitCode::NOT_FOUND);

    let denied = ErrorKind::BrokerPermissionDenied.spec();
    assert_eq!(denied.remoting.code, RemotingResponseCode::NoPermission);
    assert_eq!(denied.grpc.payload, GrpcPayloadCode::Forbidden);
    assert_eq!(denied.grpc.status, GrpcStatusCode::PermissionDenied);
    assert_eq!(denied.http.status, HttpStatusCode::FORBIDDEN);

    let invalid = ErrorKind::RequestHeaderError.spec();
    assert_eq!(invalid.remoting.code, RemotingResponseCode::InvalidParameter);
    assert_eq!(invalid.grpc.payload, GrpcPayloadCode::BadRequest);
    assert_eq!(invalid.grpc.status, GrpcStatusCode::InvalidArgument);
    assert_eq!(invalid.http.status, HttpStatusCode::BAD_REQUEST);
}

#[test]
fn observability_errors_have_conservative_boundary_primitives() {
    let config = ErrorKind::ObservabilityConfigInvalid.spec();
    assert_eq!(config.remoting.code, RemotingResponseCode::InvalidParameter);
    assert_eq!(config.grpc.payload, GrpcPayloadCode::BadRequest);
    assert_eq!(config.grpc.status, GrpcStatusCode::InvalidArgument);
    assert_eq!(config.http.status, HttpStatusCode::BAD_REQUEST);
    assert_eq!(config.cli.exit_code, CliExitCode::CONFIG);

    let init = ErrorKind::ObservabilityMetricsInitFailed.spec();
    assert_eq!(init.remoting.code, RemotingResponseCode::SystemError);
    assert_eq!(init.grpc.payload, GrpcPayloadCode::InternalError);
    assert_eq!(init.grpc.status, GrpcStatusCode::Internal);
    assert_eq!(init.http.status, HttpStatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(init.cli.exit_code, CliExitCode::SOFTWARE);
}

#[test]
fn protocol_primitives_are_complete_for_all_specs() {
    for spec in ALL_ERROR_SPECS {
        assert_ne!(spec.remoting.code.as_i32(), 0, "{:?}", spec.kind);
        assert_ne!(spec.http.status.as_u16(), 0, "{:?}", spec.kind);
        assert_ne!(spec.cli.exit_code.as_i32(), 0, "{:?}", spec.kind);
    }
}

#[test]
fn protocol_primitive_numeric_values_match_external_boundary_numbers() {
    assert_eq!(RemotingResponseCode::SystemError.as_i32(), 1);
    assert_eq!(RemotingResponseCode::NoPermission.as_i32(), 16);
    assert_eq!(RemotingResponseCode::TopicNotExist.as_i32(), 17);
    assert_eq!(RemotingResponseCode::InvalidParameter.as_i32(), 29);
    assert_eq!(HttpStatusCode::BAD_REQUEST.as_u16(), 400);
    assert_eq!(HttpStatusCode::NOT_FOUND.as_u16(), 404);
}
