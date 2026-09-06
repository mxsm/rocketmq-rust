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

use rocketmq_error::CliExitCode;
use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::GrpcStatusCode;
use rocketmq_error::HttpStatusCode;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::RocketMQError;

#[test]
fn four_boundary_mappings_match_the_golden_contract() {
    let cases = [
        (
            RocketMQError::authentication_failed("invalid signature"),
            (
                "auth.credentials.invalid",
                RemotingResponseCode::NoPermission,
                GrpcPayloadCode::Unauthorized,
                GrpcStatusCode::Unauthenticated,
                HttpStatusCode::UNAUTHORIZED,
                CliExitCode::PERMISSION,
            ),
        ),
        (
            RocketMQError::route_not_found("orders"),
            (
                "route.topic.not_found",
                RemotingResponseCode::TopicNotExist,
                GrpcPayloadCode::TopicNotFound,
                GrpcStatusCode::NotFound,
                HttpStatusCode::NOT_FOUND,
                CliExitCode::NOT_FOUND,
            ),
        ),
        (
            RocketMQError::illegal_argument("queue id"),
            (
                "core.argument.invalid",
                RemotingResponseCode::InvalidParameter,
                GrpcPayloadCode::BadRequest,
                GrpcStatusCode::InvalidArgument,
                HttpStatusCode::BAD_REQUEST,
                CliExitCode::USAGE,
            ),
        ),
        (
            RocketMQError::StorageOutOfSpace {
                path: "commitlog".to_owned(),
            },
            (
                "storage.capacity.exhausted",
                RemotingResponseCode::SystemError,
                GrpcPayloadCode::InternalError,
                GrpcStatusCode::ResourceExhausted,
                HttpStatusCode::INSUFFICIENT_STORAGE,
                CliExitCode::DATA,
            ),
        ),
    ];

    for (error, expected) in cases {
        let view = error.boundary_view();
        let actual = (
            view.code().as_str(),
            view.remoting().code,
            view.grpc().payload,
            view.grpc().status,
            view.http().status,
            view.cli().exit_code,
        );
        assert_eq!(
            expected,
            actual,
            "boundary mapping changed for {}",
            error.descriptor().code()
        );
    }
}
