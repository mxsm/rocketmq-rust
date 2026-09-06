// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_error::fields;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::CliExitCode;
use rocketmq_error::ComponentId;
use rocketmq_error::ContextVisibility;
use rocketmq_error::DiagnosticView;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::Exposure;
use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::ViewValueRef;
use rocketmq_error::PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_OFFSET_INVALID;
use rocketmq_error::PROXY_BROKER_OFFSET_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_PERMISSION_DENIED;
use rocketmq_error::PROXY_BROKER_REQUEST_UNSUPPORTED;
use rocketmq_error::PROXY_BROKER_RESOURCE_NOT_FOUND;
use rocketmq_error::PROXY_BROKER_RESPONSE_FAILED;
use rocketmq_error::PROXY_BROKER_TOPIC_NOT_FOUND;

struct ExpectedProjection {
    descriptor: &'static ErrorDescriptor,
    code: &'static str,
    message: &'static str,
    condition: CanonicalCondition,
    grpc_payload: GrpcPayloadCode,
    grpc_status: GrpcStatusCode,
    http_status: HttpStatusCode,
    cli_exit: CliExitCode,
}

#[test]
fn broker_response_descriptors_have_exact_proxy_owned_projections() {
    let cases = [
        ExpectedProjection {
            descriptor: &PROXY_BROKER_PERMISSION_DENIED,
            code: "proxy.broker_response.permission_denied",
            message: "Broker denied the Proxy operation",
            condition: CanonicalCondition::PermissionDenied,
            grpc_payload: GrpcPayloadCode::Forbidden,
            grpc_status: GrpcStatusCode::PermissionDenied,
            http_status: HttpStatusCode::FORBIDDEN,
            cli_exit: CliExitCode::PERMISSION,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_TOPIC_NOT_FOUND,
            code: "proxy.broker_response.topic_not_found",
            message: "Broker topic was not found",
            condition: CanonicalCondition::NotFound,
            grpc_payload: GrpcPayloadCode::TopicNotFound,
            grpc_status: GrpcStatusCode::NotFound,
            http_status: HttpStatusCode::NOT_FOUND,
            cli_exit: CliExitCode::NOT_FOUND,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_CONSUMER_GROUP_NOT_FOUND,
            code: "proxy.broker_response.consumer_group_not_found",
            message: "Broker consumer group was not found",
            condition: CanonicalCondition::NotFound,
            grpc_payload: GrpcPayloadCode::ConsumerGroupNotFound,
            grpc_status: GrpcStatusCode::NotFound,
            http_status: HttpStatusCode::NOT_FOUND,
            cli_exit: CliExitCode::NOT_FOUND,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_RESOURCE_NOT_FOUND,
            code: "proxy.broker_response.resource_not_found",
            message: "Broker resource was not found",
            condition: CanonicalCondition::NotFound,
            grpc_payload: GrpcPayloadCode::NotFound,
            grpc_status: GrpcStatusCode::NotFound,
            http_status: HttpStatusCode::NOT_FOUND,
            cli_exit: CliExitCode::NOT_FOUND,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_OFFSET_NOT_FOUND,
            code: "proxy.broker_response.offset_not_found",
            message: "Broker offset was not found",
            condition: CanonicalCondition::NotFound,
            grpc_payload: GrpcPayloadCode::OffsetNotFound,
            grpc_status: GrpcStatusCode::NotFound,
            http_status: HttpStatusCode::NOT_FOUND,
            cli_exit: CliExitCode::NOT_FOUND,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_OFFSET_INVALID,
            code: "proxy.broker_response.offset_invalid",
            message: "Broker rejected an invalid offset",
            condition: CanonicalCondition::InvalidArgument,
            grpc_payload: GrpcPayloadCode::IllegalOffset,
            grpc_status: GrpcStatusCode::InvalidArgument,
            http_status: HttpStatusCode::BAD_REQUEST,
            cli_exit: CliExitCode::USAGE,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_REQUEST_UNSUPPORTED,
            code: "proxy.broker_response.request_unsupported",
            message: "Broker does not support the Proxy request",
            condition: CanonicalCondition::Unimplemented,
            grpc_payload: GrpcPayloadCode::Unsupported,
            grpc_status: GrpcStatusCode::Unimplemented,
            http_status: HttpStatusCode::NOT_IMPLEMENTED,
            cli_exit: CliExitCode::SOFTWARE,
        },
        ExpectedProjection {
            descriptor: &PROXY_BROKER_RESPONSE_FAILED,
            code: "proxy.broker_response.failed",
            message: "Broker response failed",
            condition: CanonicalCondition::Internal,
            grpc_payload: GrpcPayloadCode::InternalError,
            grpc_status: GrpcStatusCode::Internal,
            http_status: HttpStatusCode::INTERNAL_SERVER_ERROR,
            cli_exit: CliExitCode::SOFTWARE,
        },
    ];

    for expected in cases {
        let descriptor = expected.descriptor;
        let projection = descriptor.projection();
        assert_eq!(descriptor.code().as_str(), expected.code);
        assert_eq!(descriptor.public_message(), expected.message);
        assert_eq!(descriptor.component(), ComponentId::PROXY);
        assert_eq!(descriptor.condition(), expected.condition);
        assert_eq!(descriptor.exposure(), Exposure::Generic);
        assert_eq!(projection.remoting().code, RemotingResponseCode::SystemError);
        assert_eq!(projection.grpc().payload, expected.grpc_payload);
        assert_eq!(projection.grpc().status, expected.grpc_status);
        assert_eq!(projection.http().status, expected.http_status);
        assert_eq!(projection.cli().exit_code, expected.cli_exit);
        assert_eq!(
            descriptor.fields(),
            [
                fields::OPERATION_DIAGNOSTIC.schema(),
                fields::BROKER_CODE.schema(),
                fields::BROKER_ADDR.schema(),
                fields::MESSAGE_PRESENT.schema(),
                fields::SOURCE_PRESENT.schema(),
            ]
        );
    }
}

#[test]
fn broker_response_views_keep_origin_detail_out_of_public_output() {
    const RAW_REMARK: &str = "Bearer token-secret C:/private/path\r\nnext-header";
    let context = ErrorContext::new()
        .with_text(fields::OPERATION_DIAGNOSTIC, "query_offset")
        .with_i64(fields::BROKER_CODE, 22)
        .with_text(fields::BROKER_ADDR, "10.0.0.8:10911")
        .with_secret_presence(fields::MESSAGE_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);

    let public = PublicErrorView::try_new(&PROXY_BROKER_OFFSET_NOT_FOUND, &context).expect("public view");
    assert_eq!(public.message(), "Broker offset was not found");
    assert!(public.fields().next().is_none());

    let diagnostic = DiagnosticView::try_new(&PROXY_BROKER_OFFSET_NOT_FOUND, &context).expect("diagnostic view");
    let fields = diagnostic.fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 5);
    assert_eq!(fields[0].name(), "operation");
    assert_eq!(fields[0].visibility(), ContextVisibility::Diagnostic);
    assert_eq!(fields[0].value(), ViewValueRef::Text("query_offset"));
    assert_eq!(fields[1].name(), "broker_code");
    assert_eq!(fields[1].value(), ViewValueRef::I64(22));
    assert_eq!(fields[2].name(), "broker_addr");
    assert_eq!(fields[2].value(), ViewValueRef::Text("10.0.0.8:10911"));
    assert_eq!(fields[3].name(), "message");
    assert_eq!(fields[3].value(), ViewValueRef::Redacted);
    assert_eq!(fields[4].name(), "source_present");
    assert_eq!(fields[4].value(), ViewValueRef::Redacted);

    let rendered = format!("{public:?} {diagnostic:?}");
    assert!(!rendered.contains(RAW_REMARK));
    assert!(!rendered.contains("token-secret"));
    assert!(!rendered.contains("private/path"));
}
