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

//! Compatibility facade for the Core-owned gRPC protocol adapter.

pub use rocketmq_proxy_core::grpc::adapter::*;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryRouteRequest;
use crate::processor::SendMessageRequest;
use crate::proto::v2;
use crate::ProxyResult;

/// Adapts the legacy aggregate configuration to the Core gRPC configuration.
pub fn build_query_route_request(
    config: &ProxyConfig,
    request: &v2::QueryRouteRequest,
) -> ProxyResult<QueryRouteRequest> {
    rocketmq_proxy_core::grpc::adapter::build_query_route_request(&config.grpc, request)
}

/// Adapts the legacy aggregate configuration to the Core gRPC configuration.
pub fn build_query_assignment_request(
    config: &ProxyConfig,
    request: &v2::QueryAssignmentRequest,
) -> ProxyResult<QueryAssignmentRequest> {
    rocketmq_proxy_core::grpc::adapter::build_query_assignment_request(&config.grpc, request)
}

/// Removes the facade-private principal proof before building a neutral Core request.
pub fn build_send_message_request(
    context: &ProxyContext,
    request: &v2::SendMessageRequest,
) -> ProxyResult<SendMessageRequest> {
    let context = context.without_principal();
    rocketmq_proxy_core::grpc::adapter::build_send_message_request(&context, request)
}
