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
use crate::ErrorCategory;
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
    /// Returns the stable semantic error kind.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[inline]
    /// Returns the stable numeric error code.
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[inline]
    /// Returns the owning subsystem category.
    pub const fn category(&self) -> ErrorCategory {
        self.category
    }

    #[inline]
    /// Returns the redaction-safe static error message.
    pub const fn message(&self) -> &'static str {
        self.message
    }

    #[inline]
    /// Returns the redaction-safe structured context.
    pub const fn context(&self) -> &ErrorContext {
        &self.context
    }

    #[inline]
    /// Returns the RocketMQ remoting response mapping.
    pub const fn remoting(&self) -> RemotingSpec {
        self.remoting
    }

    #[inline]
    /// Returns the gRPC payload and transport-status mapping.
    pub const fn grpc(&self) -> GrpcSpec {
        self.grpc
    }

    #[inline]
    /// Returns the HTTP status mapping.
    pub const fn http(&self) -> HttpSpec {
        self.http
    }

    #[inline]
    /// Returns the CLI exit-code mapping.
    pub const fn cli(&self) -> CliSpec {
        self.cli
    }

    #[inline]
    /// Returns the retry and recovery policy.
    pub const fn recovery(&self) -> RecoverySpec {
        self.recovery
    }

    #[inline]
    /// Returns the retry classification.
    pub const fn retry(&self) -> RetryClass {
        self.recovery.retry
    }

    #[inline]
    /// Returns whether the retry classification permits another attempt.
    pub const fn is_retryable(&self) -> bool {
        !matches!(self.retry(), RetryClass::Never)
    }

    #[inline]
    /// Returns the observability classification.
    pub const fn observe(&self) -> ObserveSpec {
        self.observe
    }

    #[inline]
    /// Returns the operational severity.
    pub const fn severity(&self) -> ErrorSeverity {
        self.observe.severity
    }
}

/// Remoting response-code primitive.
///
/// This mirrors stable wire numbers without depending on the protocol or transport crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(i32)]
pub enum RemotingResponseCode {
    /// Represents the system error case.
    SystemError = 1,
    /// Represents the system busy case.
    SystemBusy = 2,
    /// Represents the request code not supported case.
    RequestCodeNotSupported = 3,
    /// Represents the no permission case.
    NoPermission = 16,
    /// Represents the topic not exist case.
    TopicNotExist = 17,
    /// Represents the query not found case.
    QueryNotFound = 22,
    /// Represents the subscription not exist case.
    SubscriptionNotExist = 24,
    /// Represents the subscription group not exist case.
    SubscriptionGroupNotExist = 26,
    /// Represents the invalid parameter case.
    InvalidParameter = 29,
    /// Represents the message illegal case.
    MessageIllegal = 13,
    /// Represents the broker not exist case.
    BrokerNotExist = 211,
    /// Represents the not leader for queue case.
    NotLeaderForQueue = 501,
    /// Represents the controller not leader case.
    ControllerNotLeader = 2007,
    /// Represents the controller jraft internal error case.
    ControllerJraftInternalError = 2015,
}

impl RemotingResponseCode {
    #[inline]
    /// Borrows this value as i32.
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Represents remoting spec.
pub struct RemotingSpec {
    /// The code value.
    pub code: RemotingResponseCode,
}

impl RemotingSpec {
    #[inline]
    /// Creates a new `RemotingSpec`.
    pub const fn new(code: RemotingResponseCode) -> Self {
        Self { code }
    }

    #[inline]
    /// Returns the mapping for the supplied error kind.
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
    /// Represents the internal error case.
    InternalError,
    /// Represents the bad request case.
    BadRequest,
    /// Represents the unauthorized case.
    Unauthorized,
    /// Represents the forbidden case.
    Forbidden,
    /// Represents the not found case.
    NotFound,
    /// Represents the topic not found case.
    TopicNotFound,
    /// Represents the consumer group not found case.
    ConsumerGroupNotFound,
    /// Represents the message not found case.
    MessageNotFound,
    /// Represents the message body too large case.
    MessageBodyTooLarge,
    /// Represents the request timeout case.
    RequestTimeout,
    /// Represents the proxy timeout case.
    ProxyTimeout,
    /// Represents the too many requests case.
    TooManyRequests,
    /// Represents the unsupported case.
    Unsupported,
}

/// Transport-level gRPC status primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrpcStatusCode {
    /// Represents the invalid argument case.
    InvalidArgument,
    /// Represents the unauthenticated case.
    Unauthenticated,
    /// Represents the permission denied case.
    PermissionDenied,
    /// Represents the not found case.
    NotFound,
    /// Represents the deadline exceeded case.
    DeadlineExceeded,
    /// Represents the resource exhausted case.
    ResourceExhausted,
    /// Represents the failed precondition case.
    FailedPrecondition,
    /// Represents the unimplemented case.
    Unimplemented,
    /// Represents the unavailable case.
    Unavailable,
    /// Represents the internal case.
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Represents grpc spec.
pub struct GrpcSpec {
    /// The payload value.
    pub payload: GrpcPayloadCode,
    /// The status value.
    pub status: GrpcStatusCode,
}

impl GrpcSpec {
    #[inline]
    /// Creates a new `GrpcSpec`.
    pub const fn new(payload: GrpcPayloadCode, status: GrpcStatusCode) -> Self {
        Self { payload, status }
    }

    #[inline]
    /// Returns the mapping for the supplied error kind.
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
    /// The bad request constant.
    pub const BAD_REQUEST: Self = Self(400);
    /// The unauthorized constant.
    pub const UNAUTHORIZED: Self = Self(401);
    /// The forbidden constant.
    pub const FORBIDDEN: Self = Self(403);
    /// The not found constant.
    pub const NOT_FOUND: Self = Self(404);
    /// The conflict constant.
    pub const CONFLICT: Self = Self(409);
    /// The request timeout constant.
    pub const REQUEST_TIMEOUT: Self = Self(408);
    /// The payload too large constant.
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    /// The too many requests constant.
    pub const TOO_MANY_REQUESTS: Self = Self(429);
    /// The internal server error constant.
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    /// The service unavailable constant.
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    /// The gateway timeout constant.
    pub const GATEWAY_TIMEOUT: Self = Self(504);
    /// The insufficient storage constant.
    pub const INSUFFICIENT_STORAGE: Self = Self(507);

    #[inline]
    /// Creates a new `HttpStatusCode`.
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    /// Borrows this value as u16.
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Represents http spec.
pub struct HttpSpec {
    /// The status value.
    pub status: HttpStatusCode,
}

impl HttpSpec {
    #[inline]
    /// Creates a new `HttpSpec`.
    pub const fn new(status: HttpStatusCode) -> Self {
        Self { status }
    }

    #[inline]
    /// Returns the mapping for the supplied error kind.
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
    /// The data constant.
    pub const DATA: Self = Self(65);
    /// The not found constant.
    pub const NOT_FOUND: Self = Self(66);
    /// The unavailable constant.
    pub const UNAVAILABLE: Self = Self(69);
    /// The software constant.
    pub const SOFTWARE: Self = Self(70);
    /// The temporary failure constant.
    pub const TEMPORARY_FAILURE: Self = Self(75);
    /// The permission constant.
    pub const PERMISSION: Self = Self(77);
    /// The config constant.
    pub const CONFIG: Self = Self(78);
    /// The usage constant.
    pub const USAGE: Self = Self(64);

    #[inline]
    /// Creates a new `CliExitCode`.
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    #[inline]
    /// Borrows this value as i32.
    pub const fn as_i32(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Represents cli spec.
pub struct CliSpec {
    /// The exit code value.
    pub exit_code: CliExitCode,
}

impl CliSpec {
    #[inline]
    /// Creates a new `CliSpec`.
    pub const fn new(exit_code: CliExitCode) -> Self {
        Self { exit_code }
    }

    #[inline]
    /// Returns the mapping for the supplied error kind.
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
