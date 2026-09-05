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

use super::*;

define_error_catalog! {
    /// Invalid request-body syntax or values.
    PROTOCOL_BODY_INVALID {
        code: "protocol.body.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Request body is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
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
    /// Unsupported protocol encoding selection.
    PROTOCOL_ENCODING_UNSUPPORTED {
        code: "protocol.encoding.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Protocol encoding is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::SERIALIZATION_TYPE],
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
    /// Unsupported RocketMQ request code.
    PROTOCOL_REQUEST_UNSUPPORTED {
        code: "protocol.request.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Protocol request is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::REQUEST_CODE],
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
    /// Invalid message property supplied to the protocol layer.
    PROTOCOL_MESSAGE_PROPERTY_INVALID {
        code: "protocol.message.property.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::PROTOCOL,
        public_message: "Message property is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::PROPERTY],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Invalid response received from a remote peer.
    PROTOCOL_RESPONSE_FAILED {
        code: "protocol.response.failed",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::PROTOCOL,
        public_message: "Response processing failed",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::REASON_PRESENT],
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
    /// Filter validation or compilation failed.
    PROTOCOL_FILTER_INVALID {
        code: "protocol.filter.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Unknown,
        component: ComponentId::PROTOCOL,
        public_message: "Filter operation failed",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [
            fields::FILTER_KIND,
            fields::FILTER_COMPILE_KIND,
            fields::FILTER_COMPILE_STAGE,
            fields::FILTER_COMPILE_POSITION,
            fields::FILTER_COMPILE_SOURCE,
            fields::POSITION,
            fields::LIMIT,
        ],
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
}
