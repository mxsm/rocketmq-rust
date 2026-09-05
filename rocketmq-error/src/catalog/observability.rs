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
    /// A requested observability feature is disabled.
    OBSERVABILITY_FEATURE_DISABLED {
        code: "observability.feature.disabled",
        class: ErrorClass::UNSUPPORTED,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::Configuration,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability feature is disabled",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::FEATURE],
        projection: {
            remoting: RemotingResponseCode::InvalidParameter,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::CONFIG,
        },
    }
    /// Observability configuration is invalid.
    OBSERVABILITY_CONFIGURATION_INVALID {
        code: "observability.configuration.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability configuration is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::REASON_PRESENT],
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
    /// Observability initialization failed.
    OBSERVABILITY_INITIALIZATION_FAILED {
        code: "observability.initialization.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability initialization failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::OBSERVABILITY_SIGNAL,
            fields::REASON_PRESENT,
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
    /// The configured observability log filter is invalid.
    OBSERVABILITY_LOG_FILTER_INVALID {
        code: "observability.log_filter.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Configuration,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability log filter is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::FILTER_PRESENT, fields::ERROR_PRESENT],
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
    /// Global observability subscriber installation failed.
    OBSERVABILITY_SUBSCRIBER_INSTALLATION_FAILED {
        code: "observability.subscriber.installation_failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability subscriber installation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Public,
        fields: [fields::ATTEMPTED, fields::INSTALLED],
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
    /// Observability shutdown failed.
    OBSERVABILITY_SHUTDOWN_FAILED {
        code: "observability.shutdown.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::Dependency,
        component: ComponentId::OBSERVABILITY,
        public_message: "Observability shutdown failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::OBSERVABILITY_SIGNAL,
            fields::REASON_PRESENT,
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
}
