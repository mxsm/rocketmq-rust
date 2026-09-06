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
