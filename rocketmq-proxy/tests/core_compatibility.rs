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

use rocketmq_proxy::proto as legacy_proto;
use rocketmq_proxy_core::proto as canonical_proto;

#[test]
fn legacy_paths_reexport_canonical_core_types() {
    let canonical_resource = canonical_proto::v2::Resource {
        resource_namespace: "tenant-a".to_owned(),
        name: "TopicA".to_owned(),
    };
    let legacy_resource: legacy_proto::Resource = canonical_resource;
    let _: canonical_proto::v2::Resource = legacy_resource;

    let canonical_error = rocketmq_proxy_core::ProxyError::ClientIdRequired;
    let legacy_error: rocketmq_proxy::ProxyError = canonical_error;
    assert_eq!(
        rocketmq_proxy::status::ProxyStatusMapper::from_error(&legacy_error),
        rocketmq_proxy_core::status::ProxyStatusMapper::from_error(&legacy_error),
    );

    let canonical_endpoint = rocketmq_proxy_core::ResolvedEndpoint {
        scheme: rocketmq_proxy_core::ResolvedAddressScheme::Ipv4,
        host: "127.0.0.1".to_owned(),
        port: 8081,
    };
    let _: rocketmq_proxy::context::ResolvedEndpoint = canonical_endpoint;

    let canonical_config = rocketmq_proxy_core::GrpcConfig::default();
    let _: rocketmq_proxy::GrpcConfig = canonical_config;
    let canonical_resource = rocketmq_proxy_core::ResourceIdentity::new("tenant-a", "TopicA");
    let _: rocketmq_proxy::ResourceIdentity = canonical_resource;

    let canonical_message = rocketmq_proxy_core::ProxyMessage::new("TopicA", b"hello".to_vec());
    let _: rocketmq_proxy::ProxyMessage = canonical_message;
    let canonical_request = rocketmq_proxy_core::QueryRouteRequest {
        topic: rocketmq_proxy_core::ResourceIdentity::new("tenant-a", "TopicA"),
        endpoints: Vec::new(),
    };
    let _: rocketmq_proxy::QueryRouteRequest = canonical_request;

    fn accept_legacy_route_service(_: std::sync::Arc<dyn rocketmq_proxy::RouteService>) {}
    let canonical_service: std::sync::Arc<dyn rocketmq_proxy_core::RouteService> =
        std::sync::Arc::new(rocketmq_proxy_core::StaticRouteService::default());
    accept_legacy_route_service(canonical_service);
}

#[test]
fn legacy_generated_client_and_session_registry_keep_type_identity() {
    fn accept_legacy_client(
        _: Option<legacy_proto::v2::messaging_service_client::MessagingServiceClient<tonic::transport::Channel>>,
    ) {
    }
    let canonical_client: Option<
        canonical_proto::v2::messaging_service_client::MessagingServiceClient<tonic::transport::Channel>,
    > = None;
    accept_legacy_client(canonical_client);

    fn accept_legacy_registry(_: rocketmq_proxy::ClientSessionRegistry) {}
    let canonical_registry: rocketmq_proxy_core::ClientSessionRegistry<rocketmq_remoting::net::channel::Channel> =
        Default::default();
    accept_legacy_registry(canonical_registry);
}

#[test]
fn legacy_and_canonical_ingress_error_mappings_are_identical() {
    let errors = [
        rocketmq_proxy::ProxyError::ClientIdRequired,
        rocketmq_proxy::ProxyError::invalid_metadata("invalid metadata"),
        rocketmq_proxy::ProxyError::Transport {
            message: "transport unavailable".to_owned(),
        },
        rocketmq_proxy::ProxyError::not_implemented("feature-a"),
        rocketmq_proxy::ProxyError::too_many_requests("route"),
        rocketmq_error::RocketMQError::authentication_failed("bad signature").into(),
        rocketmq_error::RocketMQError::topic_not_found("TopicA").into(),
    ];

    for error in errors {
        assert_eq!(
            rocketmq_proxy::status::ProxyStatusMapper::from_error(&error),
            rocketmq_proxy_core::status::ProxyStatusMapper::from_error(&error),
        );
        let legacy = rocketmq_proxy::status::ProxyStatusMapper::to_tonic_status(&error);
        let canonical = rocketmq_proxy_core::status::ProxyStatusMapper::to_tonic_status(&error);
        assert_eq!(legacy.code(), canonical.code());
        assert_eq!(legacy.message(), canonical.message());
    }
}
