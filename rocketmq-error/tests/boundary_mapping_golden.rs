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
use rocketmq_error::AUTH_CREDENTIALS_INVALID;
use rocketmq_error::AUTH_USER_NOT_FOUND;
use rocketmq_error::CORE_ARGUMENT_INVALID;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
use rocketmq_error::STORAGE_CAPACITY_EXHAUSTED;

#[test]
fn four_boundary_mappings_match_the_golden_contract() {
    let cases = [
        (
            &AUTH_CREDENTIALS_INVALID,
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
            &AUTH_USER_NOT_FOUND,
            (
                "auth.user.not_found",
                RemotingResponseCode::UserNotExist,
                GrpcPayloadCode::NotFound,
                GrpcStatusCode::NotFound,
                HttpStatusCode::NOT_FOUND,
                CliExitCode::NOT_FOUND,
            ),
        ),
        (
            &ROUTE_TOPIC_NOT_FOUND,
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
            &CORE_ARGUMENT_INVALID,
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
            &STORAGE_CAPACITY_EXHAUSTED,
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

    for (descriptor, expected) in cases {
        let projection = descriptor.projection();
        let actual = (
            descriptor.code().as_str(),
            projection.remoting().code,
            projection.grpc().payload,
            projection.grpc().status,
            projection.http().status,
            projection.cli().exit_code,
        );
        assert_eq!(expected, actual, "boundary mapping changed for {}", descriptor.code());
    }
}
