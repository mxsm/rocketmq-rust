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
    /// Broker lookup did not find a matching broker.
    BROKER_LOOKUP_NOT_FOUND {
        code: "broker.lookup.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Broker was not found",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::BROKER],
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
    /// Broker registration operation failed.
    BROKER_REGISTRATION_FAILED {
        code: "broker.registration.failed",
        class: ErrorClass::UNAVAILABLE,
        condition: CanonicalCondition::Unavailable,
        fault: FaultAttribution::Dependency,
        component: ComponentId::BROKER,
        public_message: "Broker registration failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::BROKER, fields::REASON_PRESENT],
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
    /// Broker operation failed with an internal remote response.
    BROKER_OPERATION_FAILED {
        code: "broker.operation.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Broker operation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [
            fields::OPERATION_DIAGNOSTIC,
            fields::BROKER_CODE,
            fields::BROKER_ADDR,
            fields::MESSAGE_PRESENT,
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
    /// Topic does not exist at the broker.
    BROKER_TOPIC_NOT_FOUND {
        code: "broker.topic.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Topic does not exist",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::TOPIC],
        projection: {
            remoting: RemotingResponseCode::TopicNotExist,
            grpc: {
                payload: GrpcPayloadCode::TopicNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Queue does not exist at the broker.
    BROKER_QUEUE_NOT_FOUND {
        code: "broker.queue.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Queue does not exist",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::SwitchBroker,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::TOPIC, fields::QUEUE_ID],
        projection: {
            remoting: RemotingResponseCode::QueryNotFound,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Subscription group does not exist at the broker.
    BROKER_SUBSCRIPTION_GROUP_NOT_FOUND {
        code: "broker.subscription_group.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Subscription group does not exist",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::GROUP],
        projection: {
            remoting: RemotingResponseCode::SubscriptionGroupNotExist,
            grpc: {
                payload: GrpcPayloadCode::ConsumerGroupNotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Queue identifier is outside the broker's configured range.
    BROKER_QUEUE_ID_OUT_OF_RANGE {
        code: "broker.queue.id_out_of_range",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::BROKER,
        public_message: "Queue id is out of range",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::TOPIC, fields::QUEUE_ID, fields::MAX_QUEUE_ID],
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
    /// Message body exceeds the broker's configured size limit.
    BROKER_MESSAGE_TOO_LARGE {
        code: "broker.message.too_large",
        class: ErrorClass::CAPACITY,
        condition: CanonicalCondition::ResourceExhausted,
        fault: FaultAttribution::Caller,
        component: ComponentId::BROKER,
        public_message: "Message body is too large",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::ACTUAL_BYTES, fields::LIMIT_BYTES],
        projection: {
            remoting: RemotingResponseCode::MessageIllegal,
            grpc: {
                payload: GrpcPayloadCode::MessageBodyTooLarge,
                status: GrpcStatusCode::ResourceExhausted,
            },
            http: HttpStatusCode::PAYLOAD_TOO_LARGE,
            cli: CliExitCode::DATA,
        },
    }
    /// Message validation failed at the broker boundary.
    BROKER_MESSAGE_INVALID {
        code: "broker.message.invalid",
        class: ErrorClass::VALIDATION,
        condition: CanonicalCondition::InvalidArgument,
        fault: FaultAttribution::Caller,
        component: ComponentId::BROKER,
        public_message: "Message validation failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::REASON_PRESENT],
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
    /// Transaction message was rejected by the broker.
    BROKER_TRANSACTION_REJECTED {
        code: "broker.transaction.rejected",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Aborted,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Transaction message was rejected",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [],
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
    /// Broker is not the current master for the operation.
    BROKER_LEADERSHIP_NOT_MASTER {
        code: "broker.leadership.not_master",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::FailedPrecondition,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Broker is not the master",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::RefreshLeader,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Generic,
        fields: [fields::MASTER_ADDRESS],
        projection: {
            remoting: RemotingResponseCode::NotLeaderForQueue,
            grpc: {
                payload: GrpcPayloadCode::InternalError,
                status: GrpcStatusCode::FailedPrecondition,
            },
            http: HttpStatusCode::CONFLICT,
            cli: CliExitCode::DATA,
        },
    }
    /// Broker query did not find the requested result.
    BROKER_QUERY_NOT_FOUND {
        code: "broker.query.not_found",
        class: ErrorClass::ROUTING,
        condition: CanonicalCondition::NotFound,
        fault: FaultAttribution::RemotePeer,
        component: ComponentId::BROKER,
        public_message: "Broker query result was not found",
        severity: ErrorSeverity::Warn,
        recovery_hint: RecoveryHint::Backoff,
        backtrace: BacktracePolicy::Never,
        exposure: Exposure::Public,
        fields: [fields::RESOURCE, fields::OFFSET],
        projection: {
            remoting: RemotingResponseCode::QueryNotFound,
            grpc: {
                payload: GrpcPayloadCode::NotFound,
                status: GrpcStatusCode::NotFound,
            },
            http: HttpStatusCode::NOT_FOUND,
            cli: CliExitCode::NOT_FOUND,
        },
    }
    /// Broker asynchronous task failed.
    BROKER_TASK_FAILED {
        code: "broker.task.failed",
        class: ErrorClass::INTERNAL,
        condition: CanonicalCondition::Internal,
        fault: FaultAttribution::LocalResource,
        component: ComponentId::BROKER,
        public_message: "Broker asynchronous task failed",
        severity: ErrorSeverity::Error,
        recovery_hint: RecoveryHint::Never,
        backtrace: BacktracePolicy::OnDemand,
        exposure: Exposure::Generic,
        fields: [fields::TASK, fields::SOURCE_PRESENT, fields::CONTEXT_PRESENT],
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
