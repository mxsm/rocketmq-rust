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
    /// Topic route information is internally inconsistent.
    ROUTE_TOPIC_INCONSISTENT {
        code: "route.topic.inconsistent",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::ROUTE,
        public_message: "Topic route data is inconsistent",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::RefreshRoute,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Public,
        fields: [fields::TOPIC, fields::REASON_PRESENT],
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
    /// Route registration could not complete because of conflicting state.
    ROUTE_REGISTRATION_CONFLICT {
        code: "route.registration.conflict",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::Aborted,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::ROUTE,
        public_message: "Route registration conflict",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::RefreshRoute,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::BROKER, fields::REASON_PRESENT, fields::EXPECTED_U64, fields::ACTUAL_U64],
        projection: {
            remoting: RemotingResponseCode::SystemError,
            grpc: {
                payload: GrpcPayloadCode::BadRequest,
                status: GrpcStatusCode::Aborted,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Route lookup did not find the requested cluster.
    ROUTE_CLUSTER_NOT_FOUND {
        code: "route.cluster.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::ROUTE,
        public_message: "Cluster was not found",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::CLUSTER],
        projection: {
            remoting: RemotingResponseCode::BrokerNotExist,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
}
