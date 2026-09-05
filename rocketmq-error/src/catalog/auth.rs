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
