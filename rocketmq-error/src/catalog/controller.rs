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
    /// Internal controller operation failed.
    CONTROLLER_INTERNAL_FAILURE {
        code: "controller.internal.failure",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Unknown,
        component: ComponentId::CONTROLLER,
        public_message: "Controller operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::PHASE, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Invalid controller request.
    CONTROLLER_REQUEST_INVALID {
        code: "controller.request.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::CONTROLLER,
        public_message: "Controller request is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::REASON_PRESENT, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Invalid controller configuration.
    CONTROLLER_CONFIGURATION_INVALID {
        code: "controller.configuration.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::CONTROLLER,
        public_message: "Controller configuration is invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::KEY, fields::REASON_PRESENT],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::CONFIG,
        },
    }
    /// Controller operation attempted before initialization completed.
    CONTROLLER_LIFECYCLE_NOT_INITIALIZED {
        code: "controller.lifecycle.not_initialized",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::CONTROLLER,
        public_message: "Controller is not initialized",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::COMPONENT_NAME, fields::REASON_PRESENT],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Controller consensus operation failed.
    CONTROLLER_CONSENSUS_FAILED {
        code: "controller.consensus.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::CONTROLLER,
        public_message: "Controller consensus operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::PHASE,
            fields::REASON_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Controller consensus operation exceeded its deadline.
    CONTROLLER_CONSENSUS_TIMED_OUT {
        code: "controller.consensus.timed_out",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::Dependency,
        component: ComponentId::CONTROLLER,
        public_message: "Controller consensus operation timed out",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::TIMEOUT_MS],
        projection: {
            remoting: RemotingResponseCode::ControllerJraftInternalError,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
}
