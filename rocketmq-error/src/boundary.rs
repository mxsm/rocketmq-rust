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

use crate::ErrorCategory;
use crate::ErrorCode;
use crate::ErrorContext;
use crate::ErrorKind;
use crate::ErrorSeverity;
use crate::ObserveSpec;
use crate::RecoverySpec;
use crate::RetryClass;

/// Public, redaction-aware projection of a typed RocketMQ error.
///
/// Boundary adapters should use this view instead of formatting
/// [`RocketMQError`](crate::RocketMQError) directly. `Display` remains a
/// diagnostic surface and can contain internal details; this view contains only
/// stable metadata, public messages, and redacted context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryErrorView {
    kind: ErrorKind,
    code: ErrorCode,
    category: ErrorCategory,
    message: &'static str,
    context: ErrorContext,
    remoting: RemotingSpec,
    grpc: GrpcSpec,
    http: HttpSpec,
    cli: CliSpec,
    recovery: RecoverySpec,
    observe: ObserveSpec,
}

impl BoundaryErrorView {
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn new(
        kind: ErrorKind,
        code: ErrorCode,
        category: ErrorCategory,
        message: &'static str,
        context: ErrorContext,
        remoting: RemotingSpec,
        grpc: GrpcSpec,
        http: HttpSpec,
        cli: CliSpec,
        recovery: RecoverySpec,
        observe: ObserveSpec,
    ) -> Self {
        Self {
            kind,
            code,
            category,
            message,
            context,
            remoting,
            grpc,
            http,
            cli,
            recovery,
            observe,
        }
    }

    #[inline]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[inline]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[inline]
    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    #[inline]
    pub const fn message(&self) -> &'static str {
        self.message
    }

    #[inline]
    pub const fn context(&self) -> &ErrorContext {
        &self.context
    }

    #[inline]
    pub const fn remoting(&self) -> RemotingSpec {
        self.remoting
    }

    #[inline]
    pub const fn grpc(&self) -> GrpcSpec {
        self.grpc
    }

    #[inline]
    pub const fn http(&self) -> HttpSpec {
        self.http
    }

    #[inline]
    pub const fn cli(&self) -> CliSpec {
        self.cli
    }

    #[inline]
    pub const fn recovery(&self) -> RecoverySpec {
        self.recovery
    }

    #[inline]
    pub const fn retry(&self) -> RetryClass {
        self.recovery.retry
    }

    #[inline]
    pub const fn is_retryable(&self) -> bool {
        !matches!(self.retry(), RetryClass::Never)
    }

    #[inline]
    pub const fn observe(&self) -> ObserveSpec {
        self.observe
    }

    #[inline]
    pub const fn severity(&self) -> ErrorSeverity {
        self.observe.severity
    }
}

/// Remoting response-code primitive.
///
/// This mirrors stable wire numbers without depending on `rocketmq-remoting`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum RemotingResponseCode {
    SystemError = 1,
    SystemBusy = 2,
    RequestCodeNotSupported = 3,
    NoPermission = 16,
    TopicNotExist = 17,
    QueryNotFound = 22,
    SubscriptionNotExist = 24,
    SubscriptionGroupNotExist = 26,
    InvalidParameter = 29,
    MessageIllegal = 13,
    BrokerNotExist = 211,
    NotLeaderForQueue = 501,
    ControllerNotLeader = 2007,
    ControllerJraftInternalError = 2015,
}

impl RemotingResponseCode {
    #[inline]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemotingSpec {
    pub code: RemotingResponseCode,
}

impl RemotingSpec {
    #[inline]
    pub const fn new(code: RemotingResponseCode) -> Self {
        Self { code }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        Self::new(match kind {
            ErrorKind::Authentication | ErrorKind::BrokerPermissionDenied | ErrorKind::TopicSendingForbidden => {
                RemotingResponseCode::NoPermission
            }
            ErrorKind::TopicNotExist | ErrorKind::RouteNotFound => RemotingResponseCode::TopicNotExist,
            ErrorKind::SubscriptionGroupNotExist => RemotingResponseCode::SubscriptionGroupNotExist,
            ErrorKind::BrokerNotFound | ErrorKind::ClusterNotFound => RemotingResponseCode::BrokerNotExist,
            ErrorKind::QueueNotExist | ErrorKind::MessageLookupFailed | ErrorKind::QueryNotFound => {
                RemotingResponseCode::QueryNotFound
            }
            ErrorKind::MessageTooLarge | ErrorKind::MessageValidationFailed | ErrorKind::InvalidProperty => {
                RemotingResponseCode::MessageIllegal
            }
            ErrorKind::IllegalArgument
            | ErrorKind::RequestBodyInvalid
            | ErrorKind::RequestHeaderError
            | ErrorKind::ResponseProcessFailed
            | ErrorKind::ConfigParseFailed
            | ErrorKind::ConfigMissing
            | ErrorKind::ConfigInvalidValue
            | ErrorKind::AuthConfigInvalid
            | ErrorKind::ObservabilityFeatureDisabled
            | ErrorKind::ObservabilityConfigInvalid
            | ErrorKind::ObservabilityLogFilterInvalid
            | ErrorKind::MissingRequiredMessageProperty => RemotingResponseCode::InvalidParameter,
            ErrorKind::Protocol | ErrorKind::InvalidVersionOrdinal => RemotingResponseCode::RequestCodeNotSupported,
            ErrorKind::Network | ErrorKind::Timeout | ErrorKind::RetryLimitExceeded => RemotingResponseCode::SystemBusy,
            ErrorKind::NotMasterBroker => RemotingResponseCode::NotLeaderForQueue,
            ErrorKind::ControllerNotLeader => RemotingResponseCode::ControllerNotLeader,
            ErrorKind::Controller
            | ErrorKind::ControllerRaftError
            | ErrorKind::ControllerConsensusTimeout
            | ErrorKind::ControllerSnapshotFailed => RemotingResponseCode::ControllerJraftInternalError,
            _ => RemotingResponseCode::SystemError,
        })
    }
}

/// gRPC payload-code primitive.
///
/// This mirrors the proxy protobuf vocabulary without depending on generated
/// protobuf types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrpcPayloadCode {
    InternalError,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    TopicNotFound,
    ConsumerGroupNotFound,
    MessageNotFound,
    MessageBodyTooLarge,
    RequestTimeout,
    ProxyTimeout,
    TooManyRequests,
    Unsupported,
}

/// Transport-level gRPC status primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrpcStatusCode {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    DeadlineExceeded,
    ResourceExhausted,
    FailedPrecondition,
    Unimplemented,
    Unavailable,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GrpcSpec {
    pub payload: GrpcPayloadCode,
    pub status: GrpcStatusCode,
}

impl GrpcSpec {
    #[inline]
    pub const fn new(payload: GrpcPayloadCode, status: GrpcStatusCode) -> Self {
        Self { payload, status }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Authentication => Self::new(GrpcPayloadCode::Unauthorized, GrpcStatusCode::Unauthenticated),
            ErrorKind::BrokerPermissionDenied | ErrorKind::TopicSendingForbidden => {
                Self::new(GrpcPayloadCode::Forbidden, GrpcStatusCode::PermissionDenied)
            }
            ErrorKind::TopicNotExist | ErrorKind::RouteNotFound => {
                Self::new(GrpcPayloadCode::TopicNotFound, GrpcStatusCode::NotFound)
            }
            ErrorKind::SubscriptionGroupNotExist => {
                Self::new(GrpcPayloadCode::ConsumerGroupNotFound, GrpcStatusCode::NotFound)
            }
            ErrorKind::BrokerNotFound
            | ErrorKind::QueueNotExist
            | ErrorKind::ClusterNotFound
            | ErrorKind::MessageLookupFailed
            | ErrorKind::QueryNotFound => Self::new(GrpcPayloadCode::NotFound, GrpcStatusCode::NotFound),
            ErrorKind::MessageTooLarge => {
                Self::new(GrpcPayloadCode::MessageBodyTooLarge, GrpcStatusCode::ResourceExhausted)
            }
            ErrorKind::IllegalArgument
            | ErrorKind::InvalidProperty
            | ErrorKind::MessageValidationFailed
            | ErrorKind::RequestBodyInvalid
            | ErrorKind::RequestHeaderError
            | ErrorKind::ResponseProcessFailed
            | ErrorKind::ConfigParseFailed
            | ErrorKind::ConfigMissing
            | ErrorKind::ConfigInvalidValue
            | ErrorKind::AuthConfigInvalid
            | ErrorKind::ObservabilityFeatureDisabled
            | ErrorKind::ObservabilityConfigInvalid
            | ErrorKind::ObservabilityLogFilterInvalid
            | ErrorKind::MissingRequiredMessageProperty => {
                Self::new(GrpcPayloadCode::BadRequest, GrpcStatusCode::InvalidArgument)
            }
            ErrorKind::Protocol | ErrorKind::InvalidVersionOrdinal => {
                Self::new(GrpcPayloadCode::Unsupported, GrpcStatusCode::Unimplemented)
            }
            ErrorKind::Network => Self::new(GrpcPayloadCode::RequestTimeout, GrpcStatusCode::DeadlineExceeded),
            ErrorKind::Timeout => Self::new(GrpcPayloadCode::ProxyTimeout, GrpcStatusCode::DeadlineExceeded),
            ErrorKind::RetryLimitExceeded => {
                Self::new(GrpcPayloadCode::TooManyRequests, GrpcStatusCode::ResourceExhausted)
            }
            ErrorKind::NotMasterBroker | ErrorKind::ControllerNotLeader => {
                Self::new(GrpcPayloadCode::InternalError, GrpcStatusCode::FailedPrecondition)
            }
            _ => Self::new(GrpcPayloadCode::InternalError, GrpcStatusCode::Internal),
        }
    }
}

/// HTTP status-code primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HttpStatusCode(u16);

impl HttpStatusCode {
    pub const BAD_REQUEST: Self = Self(400);
    pub const UNAUTHORIZED: Self = Self(401);
    pub const FORBIDDEN: Self = Self(403);
    pub const NOT_FOUND: Self = Self(404);
    pub const CONFLICT: Self = Self(409);
    pub const REQUEST_TIMEOUT: Self = Self(408);
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    pub const TOO_MANY_REQUESTS: Self = Self(429);
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    pub const GATEWAY_TIMEOUT: Self = Self(504);
    pub const INSUFFICIENT_STORAGE: Self = Self(507);

    #[inline]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HttpSpec {
    pub status: HttpStatusCode,
}

impl HttpSpec {
    #[inline]
    pub const fn new(status: HttpStatusCode) -> Self {
        Self { status }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        Self::new(match kind {
            ErrorKind::Authentication => HttpStatusCode::UNAUTHORIZED,
            ErrorKind::BrokerPermissionDenied | ErrorKind::TopicSendingForbidden => HttpStatusCode::FORBIDDEN,
            ErrorKind::TopicNotExist
            | ErrorKind::RouteNotFound
            | ErrorKind::SubscriptionGroupNotExist
            | ErrorKind::BrokerNotFound
            | ErrorKind::QueueNotExist
            | ErrorKind::ClusterNotFound
            | ErrorKind::MessageLookupFailed
            | ErrorKind::QueryNotFound => HttpStatusCode::NOT_FOUND,
            ErrorKind::RouteRegistrationConflict
            | ErrorKind::RouteVersionConflict
            | ErrorKind::ClientAlreadyStarted
            | ErrorKind::ClientInvalidState => HttpStatusCode::CONFLICT,
            ErrorKind::MessageTooLarge => HttpStatusCode::PAYLOAD_TOO_LARGE,
            ErrorKind::IllegalArgument
            | ErrorKind::InvalidProperty
            | ErrorKind::MessageValidationFailed
            | ErrorKind::RequestBodyInvalid
            | ErrorKind::RequestHeaderError
            | ErrorKind::ResponseProcessFailed
            | ErrorKind::ConfigParseFailed
            | ErrorKind::ConfigMissing
            | ErrorKind::ConfigInvalidValue
            | ErrorKind::AuthConfigInvalid
            | ErrorKind::ObservabilityFeatureDisabled
            | ErrorKind::ObservabilityConfigInvalid
            | ErrorKind::ObservabilityLogFilterInvalid
            | ErrorKind::MissingRequiredMessageProperty
            | ErrorKind::Protocol
            | ErrorKind::InvalidVersionOrdinal => HttpStatusCode::BAD_REQUEST,
            ErrorKind::Network => HttpStatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Timeout => HttpStatusCode::GATEWAY_TIMEOUT,
            ErrorKind::RetryLimitExceeded => HttpStatusCode::TOO_MANY_REQUESTS,
            ErrorKind::StorageOutOfSpace => HttpStatusCode::INSUFFICIENT_STORAGE,
            _ => HttpStatusCode::INTERNAL_SERVER_ERROR,
        })
    }
}

/// CLI exit-code primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CliExitCode(i32);

impl CliExitCode {
    pub const DATA: Self = Self(65);
    pub const NOT_FOUND: Self = Self(66);
    pub const UNAVAILABLE: Self = Self(69);
    pub const SOFTWARE: Self = Self(70);
    pub const TEMPORARY_FAILURE: Self = Self(75);
    pub const PERMISSION: Self = Self(77);
    pub const CONFIG: Self = Self(78);
    pub const USAGE: Self = Self(64);

    #[inline]
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn as_i32(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CliSpec {
    pub exit_code: CliExitCode,
}

impl CliSpec {
    #[inline]
    pub const fn new(exit_code: CliExitCode) -> Self {
        Self { exit_code }
    }

    #[inline]
    pub const fn for_kind(kind: ErrorKind) -> Self {
        Self::new(match kind {
            ErrorKind::Authentication | ErrorKind::BrokerPermissionDenied | ErrorKind::TopicSendingForbidden => {
                CliExitCode::PERMISSION
            }
            ErrorKind::TopicNotExist
            | ErrorKind::RouteNotFound
            | ErrorKind::SubscriptionGroupNotExist
            | ErrorKind::BrokerNotFound
            | ErrorKind::QueueNotExist
            | ErrorKind::ClusterNotFound
            | ErrorKind::MessageLookupFailed
            | ErrorKind::QueryNotFound => CliExitCode::NOT_FOUND,
            ErrorKind::IllegalArgument
            | ErrorKind::InvalidProperty
            | ErrorKind::MessageValidationFailed
            | ErrorKind::RequestBodyInvalid
            | ErrorKind::RequestHeaderError
            | ErrorKind::ResponseProcessFailed
            | ErrorKind::MissingRequiredMessageProperty
            | ErrorKind::Protocol
            | ErrorKind::InvalidVersionOrdinal => CliExitCode::USAGE,
            ErrorKind::ConfigParseFailed
            | ErrorKind::ConfigMissing
            | ErrorKind::ConfigInvalidValue
            | ErrorKind::AuthConfigInvalid
            | ErrorKind::ObservabilityFeatureDisabled
            | ErrorKind::ObservabilityConfigInvalid
            | ErrorKind::ObservabilityLogFilterInvalid => CliExitCode::CONFIG,
            ErrorKind::Network | ErrorKind::Timeout | ErrorKind::RetryLimitExceeded => CliExitCode::TEMPORARY_FAILURE,
            ErrorKind::StorageCorrupted | ErrorKind::StorageOutOfSpace => CliExitCode::DATA,
            ErrorKind::Tools => CliExitCode::UNAVAILABLE,
            _ => CliExitCode::SOFTWARE,
        })
    }
}
