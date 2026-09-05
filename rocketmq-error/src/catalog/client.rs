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
    /// Client retry budget was exhausted.
    CLIENT_RETRY_BUDGET_EXHAUSTED {
        code: "client.retry.budget_exhausted",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::CLIENT,
        public_message: "Retry budget was exhausted",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::GROUP, fields::CURRENT, fields::MAX],
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
    /// Client operation attempted before startup completed.
    CLIENT_LIFECYCLE_NOT_STARTED {
        code: "client.lifecycle.not_started",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::Caller,
        component: ComponentId::CLIENT,
        public_message: "Client is not started",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [],
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
    /// Client startup requested after the client was already started.
    CLIENT_LIFECYCLE_ALREADY_STARTED {
        code: "client.lifecycle.already_started",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::AlreadyExists,
        fault: FaultAttribution::Caller,
        component: ComponentId::CLIENT,
        public_message: "Client is already started",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::AlreadyExists,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Client is shutting down and cannot accept the operation.
    CLIENT_LIFECYCLE_SHUTTING_DOWN {
        code: "client.lifecycle.shutting_down",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::CLIENT,
        public_message: "Client is shutting down",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
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
    /// Client lifecycle state does not permit the operation.
    CLIENT_LIFECYCLE_INVALID_STATE {
        code: "client.lifecycle.invalid_state",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::Caller,
        component: ComponentId::CLIENT,
        public_message: "Client state is invalid",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::EXPECTED_STATE, fields::ACTUAL_STATE],
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
    /// Required client component is unavailable.
    CLIENT_COMPONENT_UNAVAILABLE {
        code: "client.component.unavailable",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::CLIENT,
        public_message: "Client component is unavailable",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::CLIENT_ROLE],
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
}
