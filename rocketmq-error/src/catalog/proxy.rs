// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

define_error_catalog! {
    /// A Broker denied an operation issued through the Proxy.
    PROXY_BROKER_PERMISSION_DENIED {
        code: "proxy.broker_response.permission_denied",
        class: ErrorClass::AUTHORIZATION,
        condition: CanonicalCondition::PermissionDenied,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker denied the Proxy operation",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::Forbidden,
                status: GrpcStatusCode::PermissionDenied,
            },
            http: HttpStatusCode::FORBIDDEN,
            cli: CliExitCode::PERMISSION,
        },
    }
    /// A Broker reported that a topic requested through the Proxy was absent.
    PROXY_BROKER_TOPIC_NOT_FOUND {
        code: "proxy.broker_response.topic_not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker topic was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::TopicNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// A Broker reported that a consumer group requested through the Proxy was absent.
    PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND {
        code: "proxy.broker_response.consumer_group_not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker consumer group was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::ConsumerGroupNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// A Broker reported that a generic resource requested through the Proxy was absent.
    PROXY_BROKER_RESOURCE_NOT_FOUND {
        code: "proxy.broker_response.resource_not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker resource was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// A Broker reported that an offset requested through the Proxy was absent.
    PROXY_BROKER_OFFSET_NOT_FOUND {
        code: "proxy.broker_response.offset_not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker offset was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::OffsetNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// A Broker rejected an invalid offset supplied through the Proxy.
    PROXY_BROKER_OFFSET_INVALID {
        code: "proxy.broker_response.offset_invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Broker rejected an invalid offset",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::IllegalOffset,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Broker does not implement a request issued through the Proxy.
    PROXY_BROKER_REQUEST_UNSUPPORTED {
        code: "proxy.broker_response.request_unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Dependency,
        component: ComponentId::PROXY,
        public_message: "Broker does not support the Proxy request",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::Unsupported,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::NOT_IMPLEMENTED,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// A Broker returned a failure that has no more specific Proxy classification.
    PROXY_BROKER_RESPONSE_FAILED {
        code: "proxy.broker_response.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROXY,
        public_message: "Broker response failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
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
    /// A Proxy gRPC request omitted its required client identifier.
    PROXY_CLIENT_ID_REQUIRED {
        code: "proxy.client.id.required",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "gRPC client id is required",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::ClientIdRequired,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy gRPC request declared an unrecognized client type.
    PROXY_CLIENT_TYPE_UNRECOGNIZED {
        code: "proxy.client.type.unrecognized",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Proxy client type is not recognized",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::UnrecognizedClientType,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A requested Proxy capability is not implemented.
    PROXY_CAPABILITY_UNSUPPORTED {
        code: "proxy.capability.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Configuration,
        component: ComponentId::PROXY,
        public_message: "Proxy capability is not implemented",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC],
        projection: {
            remoting: RemotingResponseCode::RequestCodeNotSupported,
            grpc: {
                payload: GrpcPayloadCode::NotImplemented,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::NOT_IMPLEMENTED,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// A Proxy-local capacity limit rejected a request.
    PROXY_CAPACITY_EXHAUSTED {
        code: "proxy.capacity.exhausted",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::PROXY,
        public_message: "Proxy capacity is exhausted",
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
    /// A draining Proxy rejected a new request.
    PROXY_REQUEST_DRAINING {
        code: "proxy.request.draining",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::PROXY,
        public_message: "Proxy is draining and does not accept new requests",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [],
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
    /// A Proxy gRPC request supplied invalid metadata.
    PROXY_METADATA_INVALID {
        code: "proxy.metadata.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "gRPC metadata is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy transport dependency is unavailable.
    PROXY_TRANSPORT_UNAVAILABLE {
        code: "proxy.transport.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Unknown,
        component: ComponentId::PROXY,
        public_message: "Proxy transport is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
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
    /// A Proxy request supplied an invalid message identifier.
    PROXY_MESSAGE_ID_INVALID {
        code: "proxy.message.id.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Message id is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::IllegalMessageId,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid transaction identifier.
    PROXY_TRANSACTION_ID_INVALID {
        code: "proxy.transaction.id.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Transaction id is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InvalidTransactionId,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid message group.
    PROXY_MESSAGE_GROUP_INVALID {
        code: "proxy.message.group.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Message group is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::IllegalMessageGroup,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid delivery time.
    PROXY_DELIVERY_TIME_INVALID {
        code: "proxy.delivery.time.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Delivery time is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::IllegalDeliveryTime,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid polling time.
    PROXY_POLLING_TIME_INVALID {
        code: "proxy.polling.time.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Polling time is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::IllegalPollingTime,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid offset.
    PROXY_OFFSET_INVALID {
        code: "proxy.offset.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Offset is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::PullOffsetMoved,
            grpc: {
                payload: GrpcPayloadCode::IllegalOffset,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid invisible time.
    PROXY_INVISIBLE_TIME_INVALID {
        code: "proxy.invisible.time.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Invisible time is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::IllegalInvisibleTime,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid filter expression.
    PROXY_FILTER_EXPRESSION_INVALID {
        code: "proxy.filter.expression.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Filter expression is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SubscriptionParseFailed,
            grpc: {
                payload: GrpcPayloadCode::IllegalFilterExpression,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid receipt handle.
    PROXY_RECEIPT_HANDLE_INVALID {
        code: "proxy.receipt.handle.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Receipt handle is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InvalidReceiptHandle,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy request supplied an invalid lite topic.
    PROXY_LITE_TOPIC_INVALID {
        code: "proxy.lite_topic.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Lite topic is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::IllegalLiteTopic,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// A Proxy lite-subscription quota was exhausted.
    PROXY_LITE_SUBSCRIPTION_QUOTA_EXCEEDED {
        code: "proxy.lite_subscription.quota_exceeded",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::PROXY,
        public_message: "Lite subscription quota is exceeded",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::LiteSubscriptionQuotaExceeded,
                status: GrpcStatusCode::ResourceExhausted,
            },
            http: HttpStatusCode::TOO_MANY_REQUESTS,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// A Proxy request supplied a message property that conflicts with its message type.
    PROXY_MESSAGE_PROPERTY_CONFLICT {
        code: "proxy.message.property_conflict",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Message property conflicts with message type",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::MessagePropertyConflictWithType,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Authoritative Proxy client settings are unavailable.
    PROXY_SETTINGS_UNAVAILABLE {
        code: "proxy.settings.unavailable",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::Configuration,
        component: ComponentId::PROXY,
        public_message: "Authoritative client settings are unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MESSAGE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::CONFIG,
        },
    }
    /// A remoting request could not be decoded or validated by the Proxy.
    PROXY_REMOTING_REQUEST_INVALID {
        code: "proxy.remoting.request.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROXY,
        public_message: "Proxy remoting request is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// An upstream request issued by the Proxy failed.
    PROXY_UPSTREAM_REQUEST_FAILED {
        code: "proxy.upstream.request.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::PROXY,
        public_message: "Proxy upstream request failed",
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
    /// Proxy drain lifecycle or readiness state is unavailable.
    PROXY_DRAIN_UNAVAILABLE {
        code: "proxy.drain.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::PROXY,
        public_message: "Proxy drain service is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::ServiceNotAvailable,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
}
