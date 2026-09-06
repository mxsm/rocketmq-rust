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

use std::io;

use rocketmq_error::AuthError;
use rocketmq_error::BoundaryErrorView;
use rocketmq_error::CliExitCode;
use rocketmq_error::DomainError;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::Exposure;
use rocketmq_error::FilterCompileError;
use rocketmq_error::FilterCompileErrorKind;
use rocketmq_error::FilterCompileSource;
use rocketmq_error::FilterCompileStage;
use rocketmq_error::FilterError;
use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::ObservabilityError;
use rocketmq_error::ProtocolError;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::RocketMQError;
use rocketmq_error::RpcClientError;
use rocketmq_error::SerializationError;
use rocketmq_error::ToolsError;
use rocketmq_error::UnifiedServiceError;

#[derive(Clone, Copy)]
struct Expected {
    code: &'static str,
    remoting: i32,
    grpc_payload: GrpcPayloadCode,
    grpc_status: GrpcStatusCode,
    http: HttpStatusCode,
    cli: CliExitCode,
}

impl Expected {
    const fn new(
        code: &'static str,
        remoting: i32,
        grpc_payload: GrpcPayloadCode,
        grpc_status: GrpcStatusCode,
        http: HttpStatusCode,
        cli: CliExitCode,
    ) -> Self {
        Self {
            code,
            remoting,
            grpc_payload,
            grpc_status,
            http,
            cli,
        }
    }
}

struct Case {
    label: &'static str,
    error: RocketMQError,
    expected: Expected,
}

fn assert_descriptor(label: &str, descriptor: &'static ErrorDescriptor, expected: Expected) {
    let projection = descriptor.projection();
    assert_eq!(descriptor.code().as_str(), expected.code, "descriptor code for {label}");
    assert_eq!(
        projection.remoting().code.as_i32(),
        expected.remoting,
        "literal RocketMQ remoting code for {label}"
    );
    assert_eq!(
        projection.grpc().payload,
        expected.grpc_payload,
        "gRPC payload for {label}"
    );
    assert_eq!(
        projection.grpc().status,
        expected.grpc_status,
        "gRPC status for {label}"
    );
    assert_eq!(projection.http().status, expected.http, "HTTP status for {label}");
    assert_eq!(projection.cli().exit_code, expected.cli, "CLI exit for {label}");
}

fn assert_views(
    label: &str,
    descriptor: &'static ErrorDescriptor,
    context: &ErrorContext,
    boundary: &BoundaryErrorView,
    expected: Expected,
) {
    let public = PublicErrorView::try_new(descriptor, context)
        .unwrap_or_else(|violation| panic!("schema-valid context for {label}: {violation}"));
    assert_eq!(public.code().as_str(), expected.code, "safe-view code for {label}");
    assert_eq!(
        public.message(),
        descriptor.public_message(),
        "safe-view message for {label}"
    );
    if descriptor.exposure() == Exposure::Generic {
        assert_eq!(public.fields().count(), 0, "generic public fields for {label}");
    }

    assert_eq!(boundary.code().as_str(), expected.code, "boundary code for {label}");
    assert_eq!(
        boundary.remoting().code.as_i32(),
        expected.remoting,
        "boundary remoting code for {label}"
    );
    assert_eq!(
        boundary.grpc().payload,
        expected.grpc_payload,
        "boundary payload for {label}"
    );
    assert_eq!(
        boundary.grpc().status,
        expected.grpc_status,
        "boundary status for {label}"
    );
    assert_eq!(boundary.http().status, expected.http, "boundary HTTP for {label}");
    assert_eq!(boundary.cli().exit_code, expected.cli, "boundary CLI for {label}");
}

fn assert_case(case: Case) {
    let descriptor = case.error.descriptor();
    assert_descriptor(case.label, descriptor, case.expected);
    assert_views(
        case.label,
        descriptor,
        &case.error.context(),
        &case.error.boundary_view(),
        case.expected,
    );
}

fn assert_domain_error(error: &impl DomainError, label: &str, expected: Expected) {
    let descriptor = error.descriptor();
    assert_descriptor(label, descriptor, expected);
    assert_views(label, descriptor, &error.context(), &error.boundary_view(), expected);
}

fn internal(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::InternalError,
        GrpcStatusCode::Internal,
        HttpStatusCode::INTERNAL_SERVER_ERROR,
        CliExitCode::SOFTWARE,
    )
}

fn invalid(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::BadRequest,
        GrpcStatusCode::InvalidArgument,
        HttpStatusCode::BAD_REQUEST,
        CliExitCode::USAGE,
    )
}

fn config(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::BadRequest,
        GrpcStatusCode::InvalidArgument,
        HttpStatusCode::BAD_REQUEST,
        CliExitCode::CONFIG,
    )
}

fn not_found(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::NotFound,
        GrpcStatusCode::NotFound,
        HttpStatusCode::NOT_FOUND,
        CliExitCode::NOT_FOUND,
    )
}

fn unavailable(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::InternalError,
        GrpcStatusCode::Unavailable,
        HttpStatusCode::SERVICE_UNAVAILABLE,
        CliExitCode::UNAVAILABLE,
    )
}

fn timeout(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::RequestTimeout,
        GrpcStatusCode::DeadlineExceeded,
        HttpStatusCode::GATEWAY_TIMEOUT,
        CliExitCode::TEMPORARY_FAILURE,
    )
}

fn capacity(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::TooManyRequests,
        GrpcStatusCode::ResourceExhausted,
        HttpStatusCode::TOO_MANY_REQUESTS,
        CliExitCode::TEMPORARY_FAILURE,
    )
}

fn precondition(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::InternalError,
        GrpcStatusCode::FailedPrecondition,
        HttpStatusCode::CONFLICT,
        CliExitCode::DATA,
    )
}

fn unsupported(code: &'static str, remoting: i32) -> Expected {
    Expected::new(
        code,
        remoting,
        GrpcPayloadCode::Unsupported,
        GrpcStatusCode::Unimplemented,
        HttpStatusCode::BAD_REQUEST,
        CliExitCode::USAGE,
    )
}

fn direct_cases() -> Vec<Case> {
    vec![
        Case {
            label: "InvalidProperty",
            error: RocketMQError::InvalidProperty("KEYS".into()),
            expected: invalid("protocol.message.property.invalid", 13),
        },
        Case {
            label: "BrokerNotFound",
            error: RocketMQError::BrokerNotFound {
                name: "broker-a".into(),
            },
            expected: not_found("broker.lookup.not_found", 211),
        },
        Case {
            label: "BrokerRegistrationFailed",
            error: RocketMQError::BrokerRegistrationFailed {
                name: "broker-a".into(),
                reason: "registration rejected".into(),
            },
            expected: unavailable("broker.registration.failed", 1),
        },
        Case {
            label: "BrokerOperationFailed",
            error: RocketMQError::BrokerOperationFailed {
                operation: "send",
                code: 1,
                message: "remote failure".into(),
                broker_addr: Some("127.0.0.1:10911".into()),
            },
            expected: internal("broker.operation.failed", 1),
        },
        Case {
            label: "TopicNotExist",
            error: RocketMQError::TopicNotExist { topic: "orders".into() },
            expected: Expected::new(
                "broker.topic.not_found",
                17,
                GrpcPayloadCode::TopicNotFound,
                GrpcStatusCode::NotFound,
                HttpStatusCode::NOT_FOUND,
                CliExitCode::NOT_FOUND,
            ),
        },
        Case {
            label: "QueueNotExist",
            error: RocketMQError::QueueNotExist {
                topic: "orders".into(),
                queue_id: 4,
            },
            expected: not_found("broker.queue.not_found", 22),
        },
        Case {
            label: "SubscriptionGroupNotExist",
            error: RocketMQError::SubscriptionGroupNotExist {
                group: "workers".into(),
            },
            expected: Expected::new(
                "broker.subscription_group.not_found",
                26,
                GrpcPayloadCode::ConsumerGroupNotFound,
                GrpcStatusCode::NotFound,
                HttpStatusCode::NOT_FOUND,
                CliExitCode::NOT_FOUND,
            ),
        },
        Case {
            label: "QueueIdOutOfRange",
            error: RocketMQError::QueueIdOutOfRange {
                topic: "orders".into(),
                queue_id: 8,
                max: 4,
            },
            expected: invalid("broker.queue.id_out_of_range", 1),
        },
        Case {
            label: "MessageTooLarge",
            error: RocketMQError::MessageTooLarge {
                actual: 2048,
                limit: 1024,
            },
            expected: Expected::new(
                "broker.message.too_large",
                13,
                GrpcPayloadCode::MessageBodyTooLarge,
                GrpcStatusCode::ResourceExhausted,
                HttpStatusCode::PAYLOAD_TOO_LARGE,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "MessageValidationFailed",
            error: RocketMQError::MessageValidationFailed {
                reason: "empty body".into(),
            },
            expected: invalid("broker.message.invalid", 13),
        },
        Case {
            label: "RetryLimitExceeded",
            error: RocketMQError::RetryLimitExceeded {
                group: "workers".into(),
                current: 4,
                max: 3,
            },
            expected: capacity("client.retry.budget_exhausted", 2),
        },
        Case {
            label: "TransactionRejected",
            error: RocketMQError::TransactionRejected,
            expected: Expected::new(
                "broker.transaction.rejected",
                1,
                GrpcPayloadCode::BadRequest,
                GrpcStatusCode::Aborted,
                HttpStatusCode::CONFLICT,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "BrokerPermissionDenied",
            error: RocketMQError::BrokerPermissionDenied {
                operation: "send".into(),
            },
            expected: Expected::new(
                "auth.permission.denied",
                16,
                GrpcPayloadCode::Forbidden,
                GrpcStatusCode::PermissionDenied,
                HttpStatusCode::FORBIDDEN,
                CliExitCode::PERMISSION,
            ),
        },
        Case {
            label: "NotMasterBroker",
            error: RocketMQError::NotMasterBroker {
                master_address: "127.0.0.1:10911".into(),
            },
            expected: precondition("broker.leadership.not_master", 501),
        },
        Case {
            label: "MessageLookupFailed",
            error: RocketMQError::MessageLookupFailed { offset: 42 },
            expected: not_found("broker.query.not_found", 22),
        },
        Case {
            label: "QueryNotFound",
            error: RocketMQError::QueryNotFound {
                resource: "message:42".into(),
            },
            expected: not_found("broker.query.not_found", 22),
        },
        Case {
            label: "TopicSendingForbidden",
            error: RocketMQError::TopicSendingForbidden { topic: "orders".into() },
            expected: Expected::new(
                "auth.permission.denied",
                16,
                GrpcPayloadCode::Forbidden,
                GrpcStatusCode::PermissionDenied,
                HttpStatusCode::FORBIDDEN,
                CliExitCode::PERMISSION,
            ),
        },
        Case {
            label: "BrokerAsyncTaskFailed",
            error: RocketMQError::BrokerAsyncTaskFailed {
                task: "pre-online",
                context: "join failed".into(),
                source: Box::new(io::Error::other("worker stopped")),
            },
            expected: internal("broker.task.failed", 1),
        },
        Case {
            label: "RequestBodyInvalid",
            error: RocketMQError::RequestBodyInvalid {
                operation: "decode",
                reason: "missing body".into(),
            },
            expected: invalid("protocol.body.invalid", 29),
        },
        Case {
            label: "RequestHeaderError",
            error: RocketMQError::RequestHeaderError("missing topic".into()),
            expected: invalid("protocol.header.invalid", 29),
        },
        Case {
            label: "ResponseProcessFailed",
            error: RocketMQError::ResponseProcessFailed {
                operation: "decode",
                reason: "bad response".into(),
            },
            expected: invalid("protocol.response.failed", 29),
        },
        Case {
            label: "RouteNotFound",
            error: RocketMQError::RouteNotFound { topic: "orders".into() },
            expected: Expected::new(
                "route.topic.not_found",
                17,
                GrpcPayloadCode::TopicNotFound,
                GrpcStatusCode::NotFound,
                HttpStatusCode::NOT_FOUND,
                CliExitCode::NOT_FOUND,
            ),
        },
        Case {
            label: "RouteInconsistent",
            error: RocketMQError::RouteInconsistent {
                topic: "orders".into(),
                reason: "epoch mismatch".into(),
            },
            expected: internal("route.topic.inconsistent", 1),
        },
        Case {
            label: "RouteRegistrationConflict",
            error: RocketMQError::RouteRegistrationConflict {
                broker_name: "broker-a".into(),
                reason: "epoch conflict".into(),
            },
            expected: Expected::new(
                "route.registration.conflict",
                1,
                GrpcPayloadCode::BadRequest,
                GrpcStatusCode::Aborted,
                HttpStatusCode::CONFLICT,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "RouteVersionConflict",
            error: RocketMQError::RouteVersionConflict { expected: 4, actual: 5 },
            expected: Expected::new(
                "route.registration.conflict",
                1,
                GrpcPayloadCode::BadRequest,
                GrpcStatusCode::Aborted,
                HttpStatusCode::CONFLICT,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "ClusterNotFound",
            error: RocketMQError::ClusterNotFound { cluster: "east".into() },
            expected: not_found("route.cluster.not_found", 211),
        },
        Case {
            label: "ClientNotStarted",
            error: RocketMQError::ClientNotStarted,
            expected: precondition("client.lifecycle.not_started", 1),
        },
        Case {
            label: "ClientAlreadyStarted",
            error: RocketMQError::ClientAlreadyStarted,
            expected: Expected::new(
                "client.lifecycle.already_started",
                1,
                GrpcPayloadCode::BadRequest,
                GrpcStatusCode::AlreadyExists,
                HttpStatusCode::CONFLICT,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "ClientShuttingDown",
            error: RocketMQError::ClientShuttingDown,
            expected: unavailable("client.lifecycle.shutting_down", 1),
        },
        Case {
            label: "ClientInvalidState",
            error: RocketMQError::ClientInvalidState {
                expected: "running",
                actual: "stopped".into(),
            },
            expected: precondition("client.lifecycle.invalid_state", 1),
        },
        Case {
            label: "ProducerNotAvailable",
            error: RocketMQError::ProducerNotAvailable,
            expected: unavailable("client.component.unavailable", 1),
        },
        Case {
            label: "ConsumerNotAvailable",
            error: RocketMQError::ConsumerNotAvailable,
            expected: unavailable("client.component.unavailable", 1),
        },
        Case {
            label: "Tools",
            error: RocketMQError::Tools(ToolsError::TopicAlreadyExists { topic: "orders".into() }),
            expected: internal("tools.operation.failed", 1),
        },
        Case {
            label: "ObservabilityFeatureDisabled",
            error: RocketMQError::Observability(ObservabilityError::FeatureDisabled("otlp")),
            expected: Expected::new(
                "observability.feature.disabled",
                29,
                GrpcPayloadCode::InternalError,
                GrpcStatusCode::FailedPrecondition,
                HttpStatusCode::CONFLICT,
                CliExitCode::CONFIG,
            ),
        },
        Case {
            label: "ObservabilityConfigInvalid",
            error: RocketMQError::Observability(ObservabilityError::InvalidConfig("bad endpoint".into())),
            expected: config("observability.configuration.invalid", 29),
        },
        Case {
            label: "ObservabilityMetricsInitFailed",
            error: RocketMQError::Observability(ObservabilityError::MetricsInit("provider failed".into())),
            expected: internal("observability.initialization.failed", 1),
        },
        Case {
            label: "ObservabilityTracesInitFailed",
            error: RocketMQError::Observability(ObservabilityError::TracesInit("provider failed".into())),
            expected: internal("observability.initialization.failed", 1),
        },
        Case {
            label: "ObservabilityLogsInitFailed",
            error: RocketMQError::Observability(ObservabilityError::LogsInit("provider failed".into())),
            expected: internal("observability.initialization.failed", 1),
        },
        Case {
            label: "ObservabilityLoggingInitFailed",
            error: RocketMQError::Observability(ObservabilityError::LoggingInit("subscriber failed".into())),
            expected: internal("observability.initialization.failed", 1),
        },
        Case {
            label: "ObservabilityLogFilterInvalid",
            error: RocketMQError::Observability(ObservabilityError::InvalidLogFilter {
                filter: "invalid".into(),
                error: "parse failed".into(),
            }),
            expected: config("observability.log_filter.invalid", 29),
        },
        Case {
            label: "ObservabilitySubscriberInstallFailed",
            error: RocketMQError::Observability(ObservabilityError::SubscriberInstallFailed {
                attempted: true,
                installed: false,
            }),
            expected: precondition("observability.subscriber.installation_failed", 1),
        },
        Case {
            label: "ObservabilityMetricsShutdownFailed",
            error: RocketMQError::Observability(ObservabilityError::MetricsShutdown("flush failed".into())),
            expected: internal("observability.shutdown.failed", 1),
        },
        Case {
            label: "ObservabilityTracesShutdownFailed",
            error: RocketMQError::Observability(ObservabilityError::TracesShutdown("flush failed".into())),
            expected: internal("observability.shutdown.failed", 1),
        },
        Case {
            label: "ObservabilityLogsShutdownFailed",
            error: RocketMQError::Observability(ObservabilityError::LogsShutdown("flush failed".into())),
            expected: internal("observability.shutdown.failed", 1),
        },
        Case {
            label: "StorageReadFailed",
            error: RocketMQError::StorageReadFailed {
                path: "commitlog".into(),
                reason: "read failed".into(),
            },
            expected: internal("storage.read.failed", 1),
        },
        Case {
            label: "StorageWriteFailed",
            error: RocketMQError::StorageWriteFailed {
                path: "commitlog".into(),
                reason: "write failed".into(),
            },
            expected: internal("storage.write.failed", 1),
        },
        Case {
            label: "StorageCorrupted",
            error: RocketMQError::StorageCorrupted {
                path: "commitlog".into(),
            },
            expected: Expected::new(
                "storage.state.corrupted",
                1,
                GrpcPayloadCode::InternalError,
                GrpcStatusCode::DataLoss,
                HttpStatusCode::INTERNAL_SERVER_ERROR,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "StorageOutOfSpace",
            error: RocketMQError::StorageOutOfSpace {
                path: "commitlog".into(),
            },
            expected: Expected::new(
                "storage.capacity.exhausted",
                1,
                GrpcPayloadCode::InternalError,
                GrpcStatusCode::ResourceExhausted,
                HttpStatusCode::INSUFFICIENT_STORAGE,
                CliExitCode::DATA,
            ),
        },
        Case {
            label: "StorageLockFailed",
            error: RocketMQError::StorageLockFailed {
                path: "commitlog".into(),
            },
            expected: unavailable("storage.backend.unavailable", 1),
        },
        Case {
            label: "ConfigParseFailed",
            error: RocketMQError::ConfigParseFailed {
                key: "listen_port",
                reason: "not a number".into(),
            },
            expected: config("core.configuration.parse_failed", 29),
        },
        Case {
            label: "ConfigMissing",
            error: RocketMQError::ConfigMissing { key: "listen_port" },
            expected: config("core.configuration.missing", 29),
        },
        Case {
            label: "ConfigInvalidValue",
            error: RocketMQError::ConfigInvalidValue {
                key: "listen_port",
                value: "zero".into(),
                reason: "must be positive".into(),
            },
            expected: config("core.configuration.invalid", 29),
        },
        Case {
            label: "AuthConfigInvalid",
            error: RocketMQError::AuthConfigInvalid {
                key: "auth.provider",
                reason: "unknown provider".into(),
            },
            expected: config("auth.configuration.invalid", 29),
        },
        Case {
            label: "AuthHotReloadFailed",
            error: RocketMQError::AuthHotReloadFailed {
                path: "acl.yml".into(),
                reason: "parse failed".into(),
            },
            expected: internal("auth.configuration.reload_failed", 1),
        },
        Case {
            label: "Io",
            error: RocketMQError::IO(io::Error::other("read failed")),
            expected: internal("core.io.failed", 1),
        },
        Case {
            label: "IllegalArgument",
            error: RocketMQError::IllegalArgument("queue id".into()),
            expected: invalid("core.argument.invalid", 29),
        },
        Case {
            label: "Timeout",
            error: RocketMQError::Timeout {
                operation: "request",
                timeout_ms: 100,
            },
            expected: timeout("core.operation.timed_out", 2),
        },
        Case {
            label: "Internal",
            error: RocketMQError::Internal {
                operation: "load metadata",
                source: Box::new(io::Error::other("unavailable")),
            },
            expected: internal("core.internal.failure", 1),
        },
        Case {
            label: "Service",
            error: RocketMQError::Service(UnifiedServiceError::StartupFailed("bind failed".into())),
            expected: internal("core.service.failed", 1),
        },
        Case {
            label: "InvalidVersionOrdinal",
            error: RocketMQError::InvalidVersionOrdinal(999),
            expected: unsupported("protocol.version.unsupported", 3),
        },
        Case {
            label: "NotInitialized",
            error: RocketMQError::NotInitialized("client".into()),
            expected: precondition("core.lifecycle.not_initialized", 1),
        },
    ]
}

#[test]
fn retained_direct_legacy_rows_have_final_descriptor_projections() {
    let cases = direct_cases();
    assert_eq!(
        cases.len(),
        65,
        "rows 7-72 except Filter, which is covered by leaf cases"
    );
    for case in cases {
        assert_case(case);
    }
}

fn assert_filter(label: &'static str, error: FilterError, expected: Expected) {
    assert_domain_error(&error, label, expected);
    assert_case(Case {
        label,
        error: RocketMQError::Filter(error),
        expected,
    });
}

#[test]
fn filter_compile_and_bits_array_leaves_use_their_final_conditions() {
    let deterministic_compile_cases = [
        (
            "EmptyExpression",
            FilterCompileErrorKind::EmptyExpression,
            FilterCompileStage::Lex,
            None,
        ),
        (
            "ExpressionTooLarge",
            FilterCompileErrorKind::ExpressionTooLarge,
            FilterCompileStage::Lex,
            None,
        ),
        (
            "TooManyTokens",
            FilterCompileErrorKind::TooManyTokens,
            FilterCompileStage::Lex,
            None,
        ),
        (
            "NestingLimitExceeded",
            FilterCompileErrorKind::NestingLimitExceeded,
            FilterCompileStage::Parse,
            Some(12),
        ),
        (
            "UnexpectedToken",
            FilterCompileErrorKind::UnexpectedToken,
            FilterCompileStage::Parse,
            Some(7),
        ),
        (
            "InvalidNumber",
            FilterCompileErrorKind::InvalidNumber,
            FilterCompileStage::Parse,
            Some(3),
        ),
        (
            "InvalidBetweenBounds",
            FilterCompileErrorKind::InvalidBetweenBounds,
            FilterCompileStage::Semantic,
            Some(5),
        ),
        (
            "UnsupportedOperand",
            FilterCompileErrorKind::UnsupportedOperand,
            FilterCompileStage::Semantic,
            Some(9),
        ),
    ];
    assert_eq!(deterministic_compile_cases.len(), 8);
    for (label, kind, stage, position) in deterministic_compile_cases {
        let compile = FilterCompileError::new_with_source(kind, stage, position, FilterCompileSource::Sql92);
        let expected = invalid("protocol.filter.invalid", 1);
        assert_domain_error(&compile, label, expected);
        assert_filter(label, FilterError::Compile(compile), expected);
    }

    let bits_array_cases = [
        ("EmptyBytes", FilterError::EmptyBytes),
        ("InvalidBitLength", FilterError::InvalidBitLength),
        ("BitLengthTooSmall", FilterError::BitLengthTooSmall),
        ("BitPositionOutOfBounds", FilterError::BitPositionOutOfBounds(8, 4)),
        ("BytePositionOutOfBounds", FilterError::BytePositionOutOfBounds(8, 4)),
    ];
    assert_eq!(bits_array_cases.len(), 5);
    for (label, error) in bits_array_cases {
        assert_filter(label, error, invalid("protocol.filter.invalid", 1));
    }

    let legacy = FilterCompileError::new(
        FilterCompileErrorKind::LegacyAdapter,
        FilterCompileStage::Compatibility,
        None,
    );
    let legacy_expected = internal("core.internal.failure", 1);
    assert_descriptor("LegacyAdapter", legacy.descriptor(), legacy_expected);
    assert_filter("LegacyAdapter", FilterError::Compile(legacy), legacy_expected);

    assert_filter(
        "Uninitialized",
        FilterError::Uninitialized,
        precondition("core.lifecycle.not_initialized", 1),
    );
}

#[test]
fn shared_filter_view_keeps_the_same_descriptor_and_projection() {
    let compile = FilterCompileError::new_with_source(
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
        Some(7),
        FilterCompileSource::Sql92,
    );
    let legacy = RocketMQError::Filter(FilterError::Compile(compile));
    let context = legacy.context();
    let shared = RocketMQError::Shared(std::sync::Arc::new(
        Error::caused_by(legacy.descriptor(), legacy).with_context(context),
    ));
    assert_domain_error(
        &shared,
        "shared Filter UnexpectedToken",
        invalid("protocol.filter.invalid", 1),
    );
}

#[test]
fn the_two_retained_corrected_legacy_backoff_associations_publish_never() {
    let cases = [
        (
            "retry budget",
            RocketMQError::RetryLimitExceeded {
                group: "workers".into(),
                current: 4,
                max: 3,
            },
            "client.retry.budget_exhausted",
        ),
        (
            "unsupported RPC request",
            RocketMQError::Rpc(RpcClientError::UnsupportedRequestCode { code: 999 }),
            "rpc.request.unsupported",
        ),
    ];

    for (label, error, code) in cases {
        assert_eq!(error.descriptor().code().as_str(), code, "descriptor for {label}");
        assert_eq!(
            error.descriptor().recovery_hint(),
            RecoveryHint::Never,
            "hint for {label}"
        );
        let context = error.context();
        let shared = RocketMQError::Shared(std::sync::Arc::new(
            Error::caused_by(error.descriptor(), error).with_context(context),
        ));
        assert_eq!(
            shared.descriptor().code().as_str(),
            code,
            "shared descriptor for {label}"
        );
        assert_eq!(shared.recovery_hint(), RecoveryHint::Never, "shared hint for {label}");
    }
}

fn assert_protocol(label: &'static str, error: ProtocolError, expected: Expected) {
    assert_domain_error(&error, label, expected);
    assert_case(Case {
        label,
        error: RocketMQError::Protocol(error),
        expected,
    });
}

#[test]
fn retained_protocol_leaves_keep_remoting_three() {
    assert_protocol(
        "InvalidCommand",
        ProtocolError::InvalidCommand { code: 999 },
        unsupported("protocol.request.unsupported", 3),
    );
    assert_protocol(
        "UnsupportedSerializationType",
        ProtocolError::UnsupportedSerializationType { serialize_type: 7 },
        unsupported("protocol.encoding.unsupported", 3),
    );
}

fn assert_rpc(label: &'static str, error: RpcClientError, expected: Expected) {
    assert_domain_error(&error, label, expected);
    assert_case(Case {
        label,
        error: RocketMQError::Rpc(error),
        expected,
    });
}

#[test]
fn rpc_leaves_keep_remoting_one_with_final_non_remoting_semantics() {
    let cases = [
        (
            "BrokerNotFound",
            RpcClientError::BrokerNotFound {
                broker_name: "broker-a".into(),
            },
            not_found("rpc.broker_address.not_found", 1),
        ),
        (
            "RequestFailed",
            RpcClientError::RequestFailed {
                addr: "127.0.0.1:10911".into(),
                request_code: 10,
                timeout_ms: 100,
                source: Box::new(io::Error::other("closed")),
            },
            unavailable("rpc.request.failed", 1),
        ),
        (
            "UnexpectedResponseCode",
            RpcClientError::UnexpectedResponseCode {
                code: 1,
                code_name: "SYSTEM_ERROR".into(),
            },
            internal("rpc.response.failed", 1),
        ),
        (
            "UnsupportedRequestCode",
            RpcClientError::UnsupportedRequestCode { code: 999 },
            unsupported("rpc.request.unsupported", 1),
        ),
        (
            "RemoteError",
            RpcClientError::RemoteError(1, "remote failure".into()),
            internal("rpc.response.failed", 1),
        ),
    ];
    assert_eq!(cases.len(), 5);
    for (label, error, expected) in cases {
        assert_rpc(label, error, expected);
    }
}

fn credential() -> Expected {
    Expected::new(
        "auth.credentials.invalid",
        16,
        GrpcPayloadCode::Unauthorized,
        GrpcStatusCode::Unauthenticated,
        HttpStatusCode::UNAUTHORIZED,
        CliExitCode::PERMISSION,
    )
}

fn assert_auth(label: &'static str, error: AuthError, expected: Expected) {
    assert_domain_error(&error, label, expected);
    assert_case(Case {
        label,
        error: RocketMQError::Authentication(error),
        expected,
    });
}

#[test]
fn auth_leaves_keep_remoting_sixteen_and_split_failure_semantics() {
    let cases = [
        (
            "MissingDateTime",
            AuthError::MissingDateTime("missing".into()),
            credential(),
        ),
        (
            "InvalidAuthorizationHeader",
            AuthError::InvalidAuthorizationHeader("bad scheme".into()),
            credential(),
        ),
        (
            "InvalidCredential",
            AuthError::InvalidCredential("bad key".into()),
            credential(),
        ),
        (
            "InvalidHexSignature",
            AuthError::InvalidHexSignature("bad hex".into()),
            credential(),
        ),
        (
            "ContextCreationError",
            AuthError::ContextCreationError("provider failed".into()),
            internal("auth.operation.failed", 16),
        ),
        (
            "AuthenticationFailed",
            AuthError::AuthenticationFailed("signature mismatch".into()),
            credential(),
        ),
        (
            "AuthorizationFailed",
            AuthError::AuthorizationFailed("policy denied".into()),
            Expected::new(
                "auth.permission.denied",
                16,
                GrpcPayloadCode::Forbidden,
                GrpcStatusCode::PermissionDenied,
                HttpStatusCode::FORBIDDEN,
                CliExitCode::PERMISSION,
            ),
        ),
        ("UserNotFound", AuthError::UserNotFound("alice".into()), credential()),
        (
            "InvalidSignature",
            AuthError::InvalidSignature("mismatch".into()),
            credential(),
        ),
        (
            "RequestTimestampExpired",
            AuthError::RequestTimestampExpired {
                request_timestamp_millis: 1,
                now_millis: 2,
                allowed_skew_millis: 3,
            },
            credential(),
        ),
        (
            "InvalidUserStatus",
            AuthError::InvalidUserStatus("disabled".into()),
            credential(),
        ),
        (
            "Operation",
            AuthError::operation("load user", io::Error::other("provider unavailable")),
            internal("auth.operation.failed", 16),
        ),
    ];
    assert_eq!(cases.len(), 12);
    for (label, error, expected) in cases {
        assert_auth(label, error, expected);
    }
}

#[test]
fn serialization_row_associates_its_concrete_facade() {
    let error = SerializationError::encode_failed("json", "invalid value");
    let expected = internal("core.serialization.failed", 1);
    assert_domain_error(&error, "SerializationError", expected);
    assert_case(Case {
        label: "SerializationError",
        error: RocketMQError::Serialization(error),
        expected,
    });
}

#[test]
fn representative_source_contexts_validate_without_exposing_source_text() {
    const SENTINEL: &str = "do-not-expose-source";
    let cases = [
        Case {
            label: "RequestBodySource",
            error: RocketMQError::RequestBodySource {
                operation: "decode body",
                source: Box::new(io::Error::other(SENTINEL)),
            },
            expected: invalid("protocol.body.invalid", 29),
        },
        Case {
            label: "RequestHeaderSource",
            error: RocketMQError::RequestHeaderSource {
                operation: "decode header",
                source: Box::new(io::Error::other(SENTINEL)),
            },
            expected: invalid("protocol.header.invalid", 29),
        },
        Case {
            label: "AuthenticationSource",
            error: RocketMQError::AuthenticationSource {
                operation: "authenticate",
                source: Box::new(io::Error::other(SENTINEL)),
            },
            expected: internal("auth.operation.failed", 16),
        },
    ];

    for case in cases {
        let descriptor = case.error.descriptor();
        let context = case.error.context();
        let public = PublicErrorView::try_new(descriptor, &context)
            .unwrap_or_else(|violation| panic!("schema-valid context for {}: {violation}", case.label));
        assert!(!format!("{public:?}").contains(SENTINEL));
        assert_case(case);
    }
}
