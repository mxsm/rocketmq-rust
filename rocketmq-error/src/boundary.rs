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
    /// Represents the service not available case.
    ServiceNotAvailable = 14,
    /// Represents the no permission case.
    NoPermission = 16,
    /// Represents the topic not exist case.
    TopicNotExist = 17,
    /// Represents the pull-offset-moved case.
    PullOffsetMoved = 21,
    /// Represents the query not found case.
    QueryNotFound = 22,
    /// Represents the subscription-parse-failed case.
    SubscriptionParseFailed = 23,
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
    /// Represents the offset not found case.
    OffsetNotFound,
    /// Represents the illegal offset case.
    IllegalOffset,
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
    /// Represents a missing Proxy client identifier.
    ClientIdRequired,
    /// Represents an unrecognized Proxy client type.
    UnrecognizedClientType,
    /// Represents a Proxy capability that is not implemented.
    NotImplemented,
    /// Represents an illegal message identifier.
    IllegalMessageId,
    /// Represents an invalid transaction identifier.
    InvalidTransactionId,
    /// Represents an illegal message group.
    IllegalMessageGroup,
    /// Represents an illegal delivery time.
    IllegalDeliveryTime,
    /// Represents an illegal polling time.
    IllegalPollingTime,
    /// Represents an illegal invisible time.
    IllegalInvisibleTime,
    /// Represents an illegal filter expression.
    IllegalFilterExpression,
    /// Represents an invalid receipt handle.
    InvalidReceiptHandle,
    /// Represents an illegal lite topic.
    IllegalLiteTopic,
    /// Represents an exhausted lite-subscription quota.
    LiteSubscriptionQuotaExceeded,
    /// Represents a message property that conflicts with the message type.
    MessagePropertyConflictWithType,
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
    /// Represents the already exists case.
    AlreadyExists,
    /// Represents the deadline exceeded case.
    DeadlineExceeded,
    /// Represents the resource exhausted case.
    ResourceExhausted,
    /// Represents the failed precondition case.
    FailedPrecondition,
    /// Represents the aborted case.
    Aborted,
    /// Represents the unimplemented case.
    Unimplemented,
    /// Represents the unavailable case.
    Unavailable,
    /// Represents the data loss case.
    DataLoss,
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
    /// The not implemented constant.
    pub const NOT_IMPLEMENTED: Self = Self(501);
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
}
