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
    /// Invalid transport endpoint syntax.
    TRANSPORT_ENDPOINT_INVALID {
        code: "transport.endpoint.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::TRANSPORT,
        public_message: "Transport endpoint is invalid",
        severity: ErrorSeverity::Info,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::REMOTE_ADDR_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::InvalidArgument,
            },
            http: HttpStatusCode::BAD_REQUEST,
            cli: CliExitCode::USAGE,
        },
    }
    /// Remote transport peer rejected work because of capacity limits.
    TRANSPORT_REMOTE_RATE_LIMITED {
        code: "transport.remote.rate_limited",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::TRANSPORT,
        public_message: "Remote transport peer rate limited the request",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::REMOTE_ADDR, fields::LIMIT],
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
    /// Transport write exceeded its deadline.
    TRANSPORT_WRITE_TIMEOUT {
        code: "transport.write.timeout",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport write timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::PHASE, fields::TIMEOUT_MS, fields::REMOTE_ADDR_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// Transport response did not arrive before its deadline.
    TRANSPORT_RESPONSE_TIMEOUT {
        code: "transport.response.timeout",
        class: ErrorClass::TIMEOUT,
        condition: CanonicalCondition::DeadlineExceeded,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport response timed out",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::PHASE, fields::TIMEOUT_MS, fields::REMOTE_ADDR_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::RequestTimeout,
                status: GrpcStatusCode::DeadlineExceeded,
            },
            http: HttpStatusCode::GATEWAY_TIMEOUT,
            cli: CliExitCode::TEMPORARY_FAILURE,
        },
    }
    /// DNS resolution for a transport endpoint failed.
    TRANSPORT_DNS_FAILED {
        code: "transport.dns.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport DNS resolution failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::HOST_PRESENT, fields::REASON_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
    /// Transport connection operation failed.
    TRANSPORT_CONNECTION_FAILED {
        code: "transport.connection.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::TRANSPORT,
        public_message: "Transport connection operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::PHASE, fields::REMOTE_ADDR_PRESENT, fields::REASON_PRESENT],
        projection: {
            remoting: RemotingResponseCode::SystemBusy,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::Unavailable,
            },
            http: HttpStatusCode::SERVICE_UNAVAILABLE,
            cli: CliExitCode::UNAVAILABLE,
        },
    }
}
