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

use crate::descriptor::ErrorCode;
use crate::descriptor::ErrorDescriptor;
use crate::field::fields;
use crate::field::FieldSchema;
use crate::projection::ProjectionSpec;
use crate::CanonicalCondition;
use crate::CliExitCode;
use crate::CliSpec;
use crate::ErrorSeverity;
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
                condition: $condition:path,
                public_message: $public_message:literal,
                severity: $severity:path,
                recovery_hint: $recovery_hint:path,
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
                    $condition,
                    $public_message,
                    $severity,
                    $recovery_hint,
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

        /// Every canonical descriptor currently declared by `rocketmq-error`.
        ///
        /// The catalog declaration emits this slice together with its named
        /// constants so a declared descriptor cannot be omitted from lookup.
        pub const ALL_DESCRIPTORS: &[ErrorDescriptor] = &[$($name),+];
    };
}

define_error_catalog! {
    /// Invalid request-header syntax or values.
    PROTOCOL_HEADER_INVALID {
        code: "protocol.header.invalid",
        condition: CanonicalCondition::InvalidArgument,
        public_message: "Request header is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::INVALID_VALUE_PRESENT],
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
        condition: CanonicalCondition::NotFound,
        public_message: "Topic route was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshRoute,
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
        condition: CanonicalCondition::Unauthenticated,
        public_message: "Authentication credentials are invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::RefreshCredentials,
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
        condition: CanonicalCondition::PermissionDenied,
        public_message: "Permission was denied",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
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
        condition: CanonicalCondition::ResourceExhausted,
        public_message: "Transport admission queue is saturated",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
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
        condition: CanonicalCondition::FailedPrecondition,
        public_message: "Controller is not the leader",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshLeader,
        fields: [fields::LEADER_ID],
        projection: {
            remoting: RemotingResponseCode::ControllerNotLeader,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Timed-out transport connection attempt.
    TRANSPORT_CONNECTION_TIMEOUT {
        code: "transport.connection.timeout",
        condition: CanonicalCondition::DeadlineExceeded,
        public_message: "Transport connection timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        fields: [fields::TIMEOUT_MS, fields::REMOTE_ADDR],
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
    /// Storage lifecycle operation attempted before startup completed.
    STORAGE_LIFECYCLE_NOT_STARTED {
        code: "storage.lifecycle.not_started",
        condition: CanonicalCondition::FailedPrecondition,
        public_message: "Storage service is not started",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
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
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Configured storage backend is unavailable.
    STORAGE_BACKEND_UNAVAILABLE {
        code: "storage.backend.unavailable",
        condition: CanonicalCondition::Unavailable,
        public_message: "Storage backend is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
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
        condition: CanonicalCondition::InvalidArgument,
        public_message: "Storage request is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
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
        condition: CanonicalCondition::NotFound,
        public_message: "Mapped file was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
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
        condition: CanonicalCondition::ResourceExhausted,
        public_message: "Storage capacity is exhausted",
        severity: ErrorSeverity::Critical,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::Internal,
        public_message: "Storage read failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::Internal,
        public_message: "Storage write failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::Internal,
        public_message: "Storage I/O operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::DataLoss,
        public_message: "Storage state is corrupted",
        severity: ErrorSeverity::Critical,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::DeadlineExceeded,
        public_message: "Storage operation timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
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
        condition: CanonicalCondition::Unimplemented,
        public_message: "Storage operation is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
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
        condition: CanonicalCondition::Internal,
        public_message: "Internal storage failure",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
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
        condition: CanonicalCondition::Unimplemented,
        public_message: "Protocol version is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
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
        condition: CanonicalCondition::Internal,
        public_message: "Internal error",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
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
