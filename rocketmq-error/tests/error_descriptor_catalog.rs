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

use std::collections::HashSet;

use rocketmq_error::descriptor_by_code;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::CliExitCode;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::ErrorSeverity;
use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::RecoveryHint;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::ALL_DESCRIPTORS;
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_PERMISSION_DENIED;
use rocketmq_error::CONTROLLER_LEADERSHIP_NOT_LEADER;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::PROTOCOL_HEADER_INVALID;
use rocketmq_error::PROTOCOL_VERSION_UNSUPPORTED;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
use rocketmq_error::STORAGE_COMMIT_LOG_CORRUPT_RECORD;
use rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED;
use rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT;

#[derive(Debug)]
struct ExpectedDescriptor {
    descriptor: ErrorDescriptor,
    code: &'static str,
    condition: CanonicalCondition,
    public_message: &'static str,
    severity: ErrorSeverity,
    recovery_hint: RecoveryHint,
    remoting: RemotingResponseCode,
    grpc_payload: GrpcPayloadCode,
    grpc_status: GrpcStatusCode,
    http: HttpStatusCode,
    cli: CliExitCode,
}

const EXPECTED_DESCRIPTORS: &[ExpectedDescriptor] = &[
    ExpectedDescriptor {
        descriptor: PROTOCOL_HEADER_INVALID,
        code: "protocol.header.invalid",
        condition: CanonicalCondition::InvalidArgument,
        public_message: "Request header is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        remoting: RemotingResponseCode::InvalidParameter,
        grpc_payload: GrpcPayloadCode::BadRequest,
        grpc_status: GrpcStatusCode::InvalidArgument,
        http: HttpStatusCode::BAD_REQUEST,
        cli: CliExitCode::USAGE,
    },
    ExpectedDescriptor {
        descriptor: ROUTE_TOPIC_NOT_FOUND,
        code: "route.topic.not_found",
        condition: CanonicalCondition::NotFound,
        public_message: "Topic route was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshRoute,
        remoting: RemotingResponseCode::TopicNotExist,
        grpc_payload: GrpcPayloadCode::TopicNotFound,
        grpc_status: GrpcStatusCode::NotFound,
        http: HttpStatusCode::NOT_FOUND,
        cli: CliExitCode::NOT_FOUND,
    },
    ExpectedDescriptor {
        descriptor: AUTH_CREDENTIALS_INVALID,
        code: "auth.credentials.invalid",
        condition: CanonicalCondition::Unauthenticated,
        public_message: "Authentication credentials are invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::RefreshCredentials,
        remoting: RemotingResponseCode::NoPermission,
        grpc_payload: GrpcPayloadCode::Unauthorized,
        grpc_status: GrpcStatusCode::Unauthenticated,
        http: HttpStatusCode::UNAUTHORIZED,
        cli: CliExitCode::PERMISSION,
    },
    ExpectedDescriptor {
        descriptor: AUTH_PERMISSION_DENIED,
        code: "auth.permission.denied",
        condition: CanonicalCondition::PermissionDenied,
        public_message: "Permission was denied",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        remoting: RemotingResponseCode::NoPermission,
        grpc_payload: GrpcPayloadCode::Forbidden,
        grpc_status: GrpcStatusCode::PermissionDenied,
        http: HttpStatusCode::FORBIDDEN,
        cli: CliExitCode::PERMISSION,
    },
    ExpectedDescriptor {
        descriptor: TRANSPORT_ADMISSION_QUEUE_SATURATED,
        code: "transport.admission.queue_saturated",
        condition: CanonicalCondition::ResourceExhausted,
        public_message: "Transport admission queue is saturated",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        remoting: RemotingResponseCode::SystemBusy,
        grpc_payload: GrpcPayloadCode::TooManyRequests,
        grpc_status: GrpcStatusCode::ResourceExhausted,
        http: HttpStatusCode::TOO_MANY_REQUESTS,
        cli: CliExitCode::TEMPORARY_FAILURE,
    },
    ExpectedDescriptor {
        descriptor: CONTROLLER_LEADERSHIP_NOT_LEADER,
        code: "controller.leadership.not_leader",
        condition: CanonicalCondition::FailedPrecondition,
        public_message: "Controller is not the leader",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshLeader,
        remoting: RemotingResponseCode::ControllerNotLeader,
        grpc_payload: GrpcPayloadCode::InternalError,
        grpc_status: GrpcStatusCode::FailedPrecondition,
        http: HttpStatusCode::INTERNAL_SERVER_ERROR,
        cli: CliExitCode::SOFTWARE,
    },
    ExpectedDescriptor {
        descriptor: TRANSPORT_CONNECTION_TIMEOUT,
        code: "transport.connection.timeout",
        condition: CanonicalCondition::DeadlineExceeded,
        public_message: "Transport connection timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        remoting: RemotingResponseCode::SystemBusy,
        grpc_payload: GrpcPayloadCode::RequestTimeout,
        grpc_status: GrpcStatusCode::DeadlineExceeded,
        http: HttpStatusCode::GATEWAY_TIMEOUT,
        cli: CliExitCode::TEMPORARY_FAILURE,
    },
    ExpectedDescriptor {
        descriptor: STORAGE_COMMIT_LOG_CORRUPT_RECORD,
        code: "storage.commit_log.corrupt_record",
        condition: CanonicalCondition::DataLoss,
        public_message: "Commit log record is corrupted",
        severity: ErrorSeverity::Critical,
        recovery_hint: RecoveryHint::OperatorAction,
        remoting: RemotingResponseCode::SystemError,
        grpc_payload: GrpcPayloadCode::InternalError,
        grpc_status: GrpcStatusCode::DataLoss,
        http: HttpStatusCode::INTERNAL_SERVER_ERROR,
        cli: CliExitCode::DATA,
    },
    ExpectedDescriptor {
        descriptor: PROTOCOL_VERSION_UNSUPPORTED,
        code: "protocol.version.unsupported",
        condition: CanonicalCondition::Unimplemented,
        public_message: "Protocol version is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        remoting: RemotingResponseCode::RequestCodeNotSupported,
        grpc_payload: GrpcPayloadCode::Unsupported,
        grpc_status: GrpcStatusCode::Unimplemented,
        http: HttpStatusCode::BAD_REQUEST,
        cli: CliExitCode::USAGE,
    },
    ExpectedDescriptor {
        descriptor: CORE_INTERNAL_FAILURE,
        code: "core.internal.failure",
        condition: CanonicalCondition::Internal,
        public_message: "Internal error",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        remoting: RemotingResponseCode::SystemError,
        grpc_payload: GrpcPayloadCode::InternalError,
        grpc_status: GrpcStatusCode::Internal,
        http: HttpStatusCode::INTERNAL_SERVER_ERROR,
        cli: CliExitCode::SOFTWARE,
    },
];

#[test]
fn representative_descriptor_table_is_exact() {
    assert_eq!(EXPECTED_DESCRIPTORS.len(), 10);
    assert_eq!(ALL_DESCRIPTORS.len(), EXPECTED_DESCRIPTORS.len());

    for (actual, expected) in ALL_DESCRIPTORS.iter().zip(EXPECTED_DESCRIPTORS) {
        assert_eq!(*actual, expected.descriptor, "{}", expected.code);
        assert_eq!(actual.code().as_str(), expected.code);
        assert_eq!(actual.condition(), expected.condition, "{}", expected.code);
        assert_eq!(actual.public_message(), expected.public_message, "{}", expected.code);
        assert_eq!(actual.severity(), expected.severity, "{}", expected.code);
        assert_eq!(actual.recovery_hint(), expected.recovery_hint, "{}", expected.code);

        let projection = actual.projection();
        assert_eq!(projection.remoting().code, expected.remoting, "{}", expected.code);
        assert_eq!(projection.grpc().payload, expected.grpc_payload, "{}", expected.code);
        assert_eq!(projection.grpc().status, expected.grpc_status, "{}", expected.code);
        assert_eq!(projection.http().status, expected.http, "{}", expected.code);
        assert_eq!(projection.cli().exit_code, expected.cli, "{}", expected.code);
    }
}

#[test]
fn catalog_codes_are_unique_valid_and_lookup_is_exact() {
    let mut codes = HashSet::new();

    for descriptor in ALL_DESCRIPTORS {
        let code = descriptor.code();
        assert_eq!(ErrorCode::try_new(code.as_str()), Some(code));
        assert!(codes.insert(code.as_str()), "duplicate catalog code: {code}");
        assert_eq!(descriptor_by_code(code.as_str()), Some(descriptor));
    }

    for unknown in [
        "route.topic.missing",
        "route.topic",
        "route..topic",
        "ROUTE_NOT_FOUND",
        "NETWORK_CONNECTION_FAILED",
        "",
    ] {
        assert_eq!(descriptor_by_code(unknown), None, "unexpected lookup for {unknown:?}");
    }
}

#[test]
fn public_messages_and_protocol_values_are_boundary_safe() {
    for descriptor in ALL_DESCRIPTORS {
        let code = descriptor.code();
        let message = descriptor.public_message();
        assert!(!message.is_empty(), "{code}");
        assert_eq!(message.trim(), message, "{code}");
        assert!(!message.chars().any(char::is_control), "{code}");

        let projection = descriptor.projection();
        assert_ne!(projection.remoting().code.as_i32(), 0, "{code}");
        assert_ne!(projection.http().status.as_u16(), 0, "{code}");
        assert_ne!(projection.cli().exit_code.as_i32(), 0, "{code}");
    }
}

#[test]
fn descriptor_construction_and_catalog_macro_remain_private() {
    let descriptor_source = include_str!("../src/descriptor.rs");
    let projection_source = include_str!("../src/projection.rs");
    let catalog_source = include_str!("../src/catalog.rs");
    let crate_root = include_str!("../src/lib.rs");

    assert!(descriptor_source.contains("pub(crate) const fn new("));
    assert!(projection_source.contains("pub(crate) const fn new("));
    assert!(!descriptor_source.contains("pub code: ErrorCode"));
    assert!(!projection_source.contains("pub remoting: RemotingSpec"));
    assert!(catalog_source.contains("macro_rules! define_error_catalog"));
    assert!(!catalog_source.contains("#[macro_export]"));
    assert!(!crate_root.contains("pub mod catalog;"));
    assert!(!crate_root.contains("pub mod descriptor;"));
    assert!(!crate_root.contains("pub mod projection;"));
}
