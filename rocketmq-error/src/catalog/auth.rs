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
    /// Authentication operation failed independently of credential validity.
    AUTH_OPERATION_FAILED {
        code: "auth.operation.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Authentication operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::REASON_PRESENT,
            fields::SOURCE_PRESENT,
        ],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Requested security-provider material does not exist.
    SECURITY_PROVIDER_NOT_FOUND {
        code: "auth.security_provider.not_found",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Security material was not found",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Security-provider state conflicts with the requested update.
    SECURITY_PROVIDER_CONFLICT {
        code: "auth.security_provider.conflict",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Aborted,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Security provider state conflicts with the request",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::Aborted,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// The requested security-provider capability is unsupported.
    SECURITY_PROVIDER_UNSUPPORTED {
        code: "auth.security_provider.unsupported",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::Unimplemented,
        fault: FaultAttribution::Configuration,
        component: ComponentId::AUTH,
        public_message: "Security provider operation is unsupported",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::Unsupported,
                status: GrpcStatusCode::Unimplemented,
            },
            http: HttpStatusCode::NOT_IMPLEMENTED,
            cli: CliExitCode::CONFIG,
        },
    }
    /// Security-provider data is malformed, corrupted, or cannot be authenticated.
    SECURITY_PROVIDER_INVALID_DATA {
        code: "auth.security_provider.invalid_data",
        class: ErrorClass::DATA_CORRUPTION,
        condition: CanonicalCondition::DataLoss,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Security provider data is invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::OperatorAction,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::DataLoss,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::DATA,
        },
    }
    /// A deterministic security-provider contract was violated.
    SECURITY_PROVIDER_CONTRACT_VIOLATION {
        code: "auth.security_provider.contract_violation",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::AUTH,
        public_message: "Security provider contract was violated",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
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
    /// The security provider or a required dependency is unavailable.
    SECURITY_PROVIDER_UNAVAILABLE {
        code: "auth.security_provider.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Security provider is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// A security-provider operation failed for an internal reason.
    SECURITY_PROVIDER_OPERATION_FAILED {
        code: "auth.security_provider.operation_failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::AUTH,
        public_message: "Security provider operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::OPERATION_DIAGNOSTIC, fields::SOURCE_PRESENT],
        projection: {
            remoting: RemotingResponseCode::NoPermission,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Internal,
            },
            http: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli: CliExitCode::SOFTWARE,
        },
    }
    /// Invalid authentication configuration.
    AUTH_CONFIGURATION_INVALID {
        code: "auth.configuration.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::AUTH,
        public_message: "Authentication configuration is invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::KEY, fields::REASON_PRESENT],
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
    /// Authentication configuration reload failed.
    AUTH_CONFIGURATION_RELOAD_FAILED {
        code: "auth.configuration.reload_failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::AUTH,
        public_message: "Authentication configuration reload failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::PATH_PRESENT, fields::REASON_PRESENT],
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
