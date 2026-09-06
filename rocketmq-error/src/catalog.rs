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

use crate::descriptor::ErrorClass;
use crate::descriptor::ErrorCode;
use crate::descriptor::ErrorDescriptor;
use crate::field::fields;
use crate::field::FieldSchema;
use crate::projection::ProjectionSpec;
use crate::BacktracePolicy;
use crate::CanonicalCondition;
use crate::CliExitCode;
use crate::CliSpec;
use crate::ComponentId;
use crate::ErrorSeverity;
use crate::Exposure;
use crate::FaultAttribution;
use crate::GrpcPayloadCode;
use crate::GrpcSpec;
use crate::GrpcStatusCode;
use crate::HttpSpec;
use crate::HttpStatusCode;
use crate::RecoveryHint;
use crate::RemotingResponseCode;
use crate::RemotingSpec;

macro_rules! define_error_catalog {
    (
        $(
            $(#[$metadata:meta])*
            $name:ident {
                code: $code:literal,
                class: $class:path,
                condition: $condition:path,
                fault: $fault:path,
                component: $component:path,
                public_message: $public_message:literal,
                severity: $severity:path,
                recovery_hint: $recovery_hint:path,
                backtrace: $backtrace:path,
                exposure: $exposure:path,
                fields: [$($field:path),* $(,)?],
                projection: {
                    remoting: $remoting:path,
                    grpc: {
                        payload: $grpc_payload:path,
                        status: $grpc_status:path,
                    },
                    http: $http:path,
                    cli: $cli:path,
                },
            }
        )+
    ) => {
        $(
            $(#[$metadata])*
            pub const $name: ErrorDescriptor = {
                const CODE: ErrorCode = match ErrorCode::try_new($code) {
                    Some(code) => code,
                    None => panic!("invalid canonical error code"),
                };
                const FIELDS: &[FieldSchema] = &[$($field.schema()),*];

                match ErrorDescriptor::try_new(
                    CODE,
                    $class,
                    $condition,
                    $fault,
                    $component,
                    $public_message,
                    $severity,
                    $recovery_hint,
                    $backtrace,
                    $exposure,
                    ProjectionSpec::new(
                        RemotingSpec::new($remoting),
                        GrpcSpec::new($grpc_payload, $grpc_status),
                        HttpSpec::new($http),
                        CliSpec::new($cli),
                    ),
                    FIELDS,
                ) {
                    Some(descriptor) => descriptor,
                    None => panic!("invalid descriptor field list"),
                }
            };
        )+

    };
}

mod auth;
mod broker;
mod client;
mod controller;
mod core;
mod observability;
mod protocol;
mod proxy;
mod route;
mod rpc;
mod tools;
mod transport;

pub use auth::*;
pub use broker::*;
pub use client::*;
pub use controller::*;
pub use core::*;
pub use observability::*;
pub use protocol::*;
pub use proxy::*;
pub use route::*;
pub use rpc::*;
pub use tools::*;
pub use transport::*;

define_error_catalog! {
    /// Invalid request-header syntax or values.
    PROTOCOL_HEADER_INVALID {
        code: "protocol.header.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Request header is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::INVALID_VALUE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Missing routing information for a topic.
    ROUTE_TOPIC_NOT_FOUND {
        code: "route.topic.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::ROUTE,
        public_message: "Topic route was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshRoute,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::TOPIC],
        projection: {
            remoting: RemotingResponseCode::TopicNotExist,
            grpc: {
                payload: GrpcPayloadCode::TopicNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Invalid authentication credentials or signature.
    AUTH_CREDENTIALS_INVALID {
        code: "auth.credentials.invalid",
        class: ErrorClass::AUTHENTICATION,
        condition: CanonicalCondition::Unauthenticated,
        fault: FaultAttribution::Caller,
        component: ComponentId::AUTH,
        public_message: "Authentication credentials are invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::RefreshCredentials,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::CREDENTIALS_PRESENT],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::Unauthorized,
                status: GrpcStatusCode::Unauthenticated,
            },
            http: HttpStatusCode::UNAUTHORIZED,
            cli: CliExitCode::PERMISSION,
        },
    }
    /// Permission denied for an authenticated principal.
    AUTH_PERMISSION_DENIED {
        code: "auth.permission.denied",
        class: ErrorClass::AUTHORIZATION,
        condition: CanonicalCondition::PermissionDenied,
        fault: FaultAttribution::Caller,
        component: ComponentId::AUTH,
        public_message: "Permission was denied",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::OPERATION],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::Forbidden,
                status: GrpcStatusCode::PermissionDenied,
            },
            http: HttpStatusCode::FORBIDDEN,
            cli: CliExitCode::PERMISSION,
        },
    }
    /// Saturated transport admission queue.
    TRANSPORT_ADMISSION_QUEUE_SATURATED {
        code: "transport.admission.queue_saturated",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::TRANSPORT,
        public_message: "Transport admission queue is saturated",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::REMOTE_ADDR],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::TooManyRequests,
                status: GrpcStatusCode::ResourceExhausted,
            },
            http: HttpStatusCode::TOO_MANY_REQUESTS,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// Operation sent to a controller that is not the leader.
    CONTROLLER_LEADERSHIP_NOT_LEADER {
        code: "controller.leadership.not_leader",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::CONTROLLER,
        public_message: "Controller is not the leader",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshLeader,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::LEADER_ID],
        projection: {
            remoting: RemotingResponseCode::ControllerNotLeader,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Timed-out transport connection attempt.
    TRANSPORT_CONNECTION_TIMEOUT {
        code: "transport.connection.timeout",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport connection timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::TIMEOUT_MS, fields::REMOTE_ADDR, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// Transport server startup failed.
    TRANSPORT_START_FAILED {
        code: "transport.start.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Unknown,
        component: ComponentId::TRANSPORT,
        public_message: "Transport server could not be started",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Transport request dispatch failed.
    TRANSPORT_DISPATCH_FAILED {
        code: "transport.dispatch.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::TRANSPORT,
        public_message: "Transport request dispatch failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Transport response delivery failed.
    TRANSPORT_RESPONSE_FAILED {
        code: "transport.response.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport response delivery failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Transport session operation failed.
    TRANSPORT_SESSION_FAILED {
        code: "transport.session.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport session operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Storage lifecycle operation attempted before startup completed.
    STORAGE_LIFECYCLE_NOT_STARTED {
        code: "storage.lifecycle.not_started",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::Caller,
        component: ComponentId::STORAGE,
        public_message: "Storage service is not started",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Configured storage backend is unavailable.
    STORAGE_BACKEND_UNAVAILABLE {
        code: "storage.backend.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::STORAGE,
        public_message: "Storage backend is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Invalid request at the storage capability boundary.
    STORAGE_REQUEST_INVALID {
        code: "storage.request.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::STORAGE,
        public_message: "Storage request is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Requested mapped file does not exist.
    STORAGE_MAPPED_FILE_NOT_FOUND {
        code: "storage.mapped_file.not_found",
        class: ErrorClass::IO,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Mapped file was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::QueryNotFound,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Storage has no remaining capacity for the operation.
    STORAGE_CAPACITY_EXHAUSTED {
        code: "storage.capacity.exhausted",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage capacity is exhausted",
        severity: ErrorSeverity::Critical,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::ResourceExhausted,
            },
            http: HttpStatusCode::INSUFFICIENT_STORAGE,
            cli: CliExitCode::DATA,
        },
    }
    /// Storage read operation failed.
    STORAGE_READ_FAILED {
        code: "storage.read.failed",
        class: ErrorClass::IO,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage read failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Storage write operation failed.
    STORAGE_WRITE_FAILED {
        code: "storage.write.failed",
        class: ErrorClass::IO,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage write failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Storage I/O operation failed.
    STORAGE_IO_FAILED {
        code: "storage.io.failed",
        class: ErrorClass::IO,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage I/O operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Storage state is corrupted.
    STORAGE_STATE_CORRUPTED {
        code: "storage.state.corrupted",
        class: ErrorClass::DATA_CORRUPTION,
        condition: CanonicalCondition::DataLoss,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage state is corrupted",
        severity: ErrorSeverity::Critical,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::DataLoss,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::DATA,
        },
    }
    /// Storage operation exceeded its deadline.
    STORAGE_OPERATION_TIMED_OUT {
        code: "storage.operation.timed_out",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::STORAGE,
        public_message: "Storage operation timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// Storage operation is not implemented by the backend.
    STORAGE_OPERATION_UNSUPPORTED {
        code: "storage.operation.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Configuration,
        component: ComponentId::STORAGE,
        public_message: "Storage operation is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::RequestCodeNotSupported,
            grpc: {
                payload: GrpcPayloadCode::Unsupported,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Internal storage failure without a more specific descriptor.
    STORAGE_INTERNAL_FAILURE {
        code: "storage.internal.failure",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Unknown,
        component: ComponentId::STORAGE,
        public_message: "Internal storage failure",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::STORE_OPERATION,
            fields::STORE_COMPONENT,
            fields::STORE_DETAIL_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Protocol version that this implementation does not support.
    PROTOCOL_VERSION_UNSUPPORTED {
        code: "protocol.version.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Protocol version is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::ORDINAL],
        projection: {
            remoting: RemotingResponseCode::RequestCodeNotSupported,
            grpc: {
                payload: GrpcPayloadCode::Unsupported,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Internal failure without a more specific catalog identity.
    CORE_INTERNAL_FAILURE {
        code: "core.internal.failure",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Unknown,
        component: ComponentId::CORE,
        public_message: "Internal error",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Runtime configuration could not be loaded or interpreted.
    RUNTIME_CONFIGURATION_FAILED {
        code: "runtime.configuration.failed",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::RUNTIME,
        public_message: "Runtime configuration is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::CONFIG,
        },
    }
    /// Runtime construction failed.
    RUNTIME_BUILD_FAILED {
        code: "runtime.build.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Configuration,
        component: ComponentId::RUNTIME,
        public_message: "Runtime could not be started",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Runtime I/O failed.
    RUNTIME_IO_FAILED {
        code: "runtime.io.failed",
        class: ErrorClass::IO,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::RUNTIME,
        public_message: "Runtime I/O operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// A required Tokio runtime context is unavailable.
    RUNTIME_CONTEXT_UNAVAILABLE {
        code: "runtime.context.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::RUNTIME,
        public_message: "Runtime context is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Runtime capacity needed by an operational path is exhausted.
    RUNTIME_CAPACITY_EXHAUSTED {
        code: "runtime.capacity.exhausted",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::RUNTIME,
        public_message: "Runtime capacity is exhausted",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::TooManyRequests,
                status: GrpcStatusCode::ResourceExhausted,
            },
            http: HttpStatusCode::TOO_MANY_REQUESTS,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// A runtime operation exceeded its deadline.
    RUNTIME_OPERATION_TIMED_OUT {
        code: "runtime.operation.timed_out",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::Dependency,
        component: ComponentId::RUNTIME,
        public_message: "Runtime operation timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// A runtime operation is unsupported.
    RUNTIME_OPERATION_UNSUPPORTED {
        code: "runtime.operation.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Caller,
        component: ComponentId::RUNTIME,
        public_message: "Runtime operation is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::OPERATION_DIAGNOSTIC],
        projection: {
            remoting: RemotingResponseCode::RequestCodeNotSupported,
            grpc: {
                payload: GrpcPayloadCode::Unsupported,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A runtime task could not be joined.
    RUNTIME_TASK_JOIN_FAILED {
        code: "runtime.task.join_failed",
        class: ErrorClass::BUG,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Bug,
        component: ComponentId::RUNTIME,
        public_message: "Runtime task failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// An internal runtime failure occurred.
    RUNTIME_INTERNAL_FAILURE {
        code: "runtime.internal.failure",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Unknown,
        component: ComponentId::RUNTIME,
        public_message: "Runtime operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
}

/// Every canonical descriptor currently declared by `rocketmq-error`.
///
/// This is the sole iterable catalog authority used by exact-code lookup.
pub const ALL_DESCRIPTORS: &[ErrorDescriptor] = &[
    PROTOCOL_HEADER_INVALID,
    ROUTE_TOPIC_NOT_FOUND,
    AUTH_CREDENTIALS_INVALID,
    AUTH_PERMISSION_DENIED,
    TRANSPORT_ADMISSION_QUEUE_SATURATED,
    CONTROLLER_LEADERSHIP_NOT_LEADER,
    TRANSPORT_CONNECTION_TIMEOUT,
    TRANSPORT_START_FAILED,
    TRANSPORT_DISPATCH_FAILED,
    TRANSPORT_RESPONSE_FAILED,
    TRANSPORT_SESSION_FAILED,
    STORAGE_LIFECYCLE_NOT_STARTED,
    STORAGE_BACKEND_UNAVAILABLE,
    STORAGE_REQUEST_INVALID,
    STORAGE_MAPPED_FILE_NOT_FOUND,
    STORAGE_CAPACITY_EXHAUSTED,
    STORAGE_READ_FAILED,
    STORAGE_WRITE_FAILED,
    STORAGE_IO_FAILED,
    STORAGE_STATE_CORRUPTED,
    STORAGE_OPERATION_TIMED_OUT,
    STORAGE_OPERATION_UNSUPPORTED,
    STORAGE_INTERNAL_FAILURE,
    PROTOCOL_VERSION_UNSUPPORTED,
    CORE_INTERNAL_FAILURE,
    RUNTIME_CONFIGURATION_FAILED,
    RUNTIME_BUILD_FAILED,
    RUNTIME_IO_FAILED,
    RUNTIME_CONTEXT_UNAVAILABLE,
    RUNTIME_CAPACITY_EXHAUSTED,
    RUNTIME_OPERATION_TIMED_OUT,
    RUNTIME_OPERATION_UNSUPPORTED,
    RUNTIME_TASK_JOIN_FAILED,
    RUNTIME_INTERNAL_FAILURE,
    TRANSPORT_ENDPOINT_INVALID,
    TRANSPORT_REMOTE_RATE_LIMITED,
    TRANSPORT_WRITE_TIMEOUT,
    TRANSPORT_RESPONSE_TIMEOUT,
    TRANSPORT_DNS_FAILED,
    TRANSPORT_CONNECTION_FAILED,
    CORE_SERIALIZATION_FAILED,
    PROTOCOL_BODY_INVALID,
    PROTOCOL_ENCODING_UNSUPPORTED,
    PROTOCOL_REQUEST_UNSUPPORTED,
    PROXY_BROKER_PERMISSION_DENIED,
    PROXY_BROKER_TOPIC_NOT_FOUND,
    PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND,
    PROXY_BROKER_RESOURCE_NOT_FOUND,
    PROXY_BROKER_OFFSET_NOT_FOUND,
    PROXY_BROKER_OFFSET_INVALID,
    PROXY_BROKER_REQUEST_UNSUPPORTED,
    PROXY_BROKER_RESPONSE_FAILED,
    PROXY_CLIENT_ID_REQUIRED,
    PROXY_CLIENT_TYPE_UNRECOGNIZED,
    PROXY_CAPABILITY_UNSUPPORTED,
    PROXY_CAPACITY_EXHAUSTED,
    PROXY_REQUEST_DRAINING,
    PROXY_METADATA_INVALID,
    PROXY_TRANSPORT_UNAVAILABLE,
    PROXY_MESSAGE_ID_INVALID,
    PROXY_TRANSACTION_ID_INVALID,
    PROXY_MESSAGE_GROUP_INVALID,
    PROXY_DELIVERY_TIME_INVALID,
    PROXY_POLLING_TIME_INVALID,
    PROXY_OFFSET_INVALID,
    PROXY_INVISIBLE_TIME_INVALID,
    PROXY_FILTER_EXPRESSION_INVALID,
    PROXY_RECEIPT_HANDLE_INVALID,
    PROXY_LITE_TOPIC_INVALID,
    PROXY_LITE_SUBSCRIPTION_QUOTA_EXCEEDED,
    PROXY_MESSAGE_PROPERTY_CONFLICT,
    PROXY_SETTINGS_UNAVAILABLE,
    PROXY_REMOTING_REQUEST_INVALID,
    PROXY_UPSTREAM_REQUEST_FAILED,
    PROXY_DRAIN_UNAVAILABLE,
    RPC_BROKER_ADDRESS_NOT_FOUND,
    RPC_REQUEST_UNSUPPORTED,
    AUTH_OPERATION_FAILED,
    CONTROLLER_INTERNAL_FAILURE,
    CONTROLLER_REQUEST_INVALID,
    CONTROLLER_CONFIGURATION_INVALID,
    CONTROLLER_LIFECYCLE_NOT_INITIALIZED,
    PROTOCOL_MESSAGE_PROPERTY_INVALID,
    BROKER_LOOKUP_NOT_FOUND,
    BROKER_REGISTRATION_FAILED,
    BROKER_OPERATION_FAILED,
    BROKER_TOPIC_NOT_FOUND,
    BROKER_QUEUE_NOT_FOUND,
    BROKER_SUBSCRIPTION_GROUP_NOT_FOUND,
    BROKER_QUEUE_ID_OUT_OF_RANGE,
    BROKER_MESSAGE_TOO_LARGE,
    BROKER_MESSAGE_INVALID,
    CLIENT_RETRY_BUDGET_EXHAUSTED,
    BROKER_TRANSACTION_REJECTED,
    BROKER_LEADERSHIP_NOT_MASTER,
    BROKER_QUERY_NOT_FOUND,
    BROKER_TASK_FAILED,
    PROTOCOL_RESPONSE_FAILED,
    ROUTE_TOPIC_INCONSISTENT,
    ROUTE_REGISTRATION_CONFLICT,
    ROUTE_CLUSTER_NOT_FOUND,
    CLIENT_LIFECYCLE_NOT_STARTED,
    CLIENT_LIFECYCLE_ALREADY_STARTED,
    CLIENT_LIFECYCLE_SHUTTING_DOWN,
    CLIENT_LIFECYCLE_INVALID_STATE,
    CLIENT_COMPONENT_UNAVAILABLE,
    RPC_REQUEST_FAILED,
    RPC_RESPONSE_FAILED,
    TOOLS_OPERATION_FAILED,
    PROTOCOL_FILTER_INVALID,
    OBSERVABILITY_FEATURE_DISABLED,
    OBSERVABILITY_CONFIGURATION_INVALID,
    OBSERVABILITY_INITIALIZATION_FAILED,
    OBSERVABILITY_LOG_FILTER_INVALID,
    OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED,
    OBSERVABILITY_SHUTDOWN_FAILED,
    CORE_CONFIGURATION_PARSE_FAILED,
    CORE_CONFIGURATION_MISSING,
    CORE_CONFIGURATION_INVALID,
    AUTH_CONFIGURATION_INVALID,
    AUTH_CONFIGURATION_RELOAD_FAILED,
    CONTROLLER_CONSENSUS_FAILED,
    CONTROLLER_CONSENSUS_TIMED_OUT,
    CORE_IO_FAILED,
    CORE_ARGUMENT_INVALID,
    CORE_OPERATION_TIMED_OUT,
    CORE_SERVICE_FAILED,
    CORE_LIFECYCLE_NOT_INITIALIZED,
];

/// Returns the registered descriptor for `code`.
///
/// Lookup accepts exact canonical dotted codes only. Unknown, malformed, and
/// transitional upper-case codes return [`None`].
#[inline]
pub fn descriptor_by_code(code: &str) -> Option<&'static ErrorDescriptor> {
    ALL_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.code().as_str() == code)
}
