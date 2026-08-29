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

use cheetah_string::CheetahString;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use tracing::debug;
use tracing::info;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::processor::client_request_processor::encode_topic_route_response_for_zone;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;
use crate::route::zone_filter::filter_route_by_zone;
use crate::route::zone_filter::ZoneRequest;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_ENABLED;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_MARKER;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_SHADOW;

mod lookup_cache;
mod route_lookup;

pub(crate) use route_lookup::ClusterTestRouteLookup;
pub(crate) use route_lookup::TransportClusterTestRouteLookup;

pub struct ClusterTestRequestProcessor {
    name_server_runtime_inner: NameServerRuntimeHandle,
    command_factory: RemotingCommandFactory,
}

impl ClusterTestRequestProcessor {
    pub(crate) fn new(name_server_runtime_inner: NameServerRuntimeHandle) -> Self {
        let command_factory = name_server_runtime_inner.remoting_command_factory();
        Self {
            name_server_runtime_inner,
            command_factory,
        }
    }

    async fn get_route_info_by_topic(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetRouteInfoRequestHeader>()?;
        let route_config = self.name_server_runtime_inner.name_server_config();

        let mut topic_route_data = match self
            .name_server_runtime_inner
            .route_info_manager()
            .pickup_topic_route_data(request_header.topic.as_ref())
        {
            Ok(route_data) => Some(route_data),
            Err(
                rocketmq_error::RocketMQError::TopicNotExist { .. }
                | rocketmq_error::RocketMQError::RouteNotFound { .. },
            ) => None,
            Err(error) => return Err(error),
        };

        if let Some(route_data) = topic_route_data.as_mut() {
            route_data.order_topic_conf = self.name_server_runtime_inner.kvconfig_manager().get_kvconfig(
                &CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                &request_header.topic,
            );
        } else if let Some(cluster_test_route_lookup) = self.name_server_runtime_inner.cluster_test_route_lookup() {
            match cluster_test_route_lookup
                .lookup_topic_route(&request_header.topic)
                .await
            {
                Ok(Some(route_data)) => {
                    topic_route_data = Some(route_data);
                }
                Ok(None) => {}
                Err(error) => {
                    info!(
                        "get route info by topic from product environment failed. envName={}, error={}",
                        route_config.product_env_name, error
                    );
                }
            }
        }

        if let Some(topic_route_data) = topic_route_data {
            let zone_request = ZoneRequest::from_command(request);
            let zone_filtered = zone_request.is_enabled();
            let force_java_zone_legacy_json = route_config.namesrv_typed_zone_route_enable && zone_filtered;
            let filtered_route;
            let topic_route_data = if zone_filtered {
                request.add_ext_field(TYPED_ZONE_ROUTE_MARKER, TYPED_ZONE_ROUTE_ENABLED);
                filtered_route = filter_route_by_zone(&topic_route_data, &zone_request);
                filtered_route.as_ref()
            } else {
                if route_config.namesrv_typed_zone_route_shadow {
                    request.add_ext_field(TYPED_ZONE_ROUTE_MARKER, TYPED_ZONE_ROUTE_SHADOW);
                }
                &topic_route_data
            };
            let content = encode_topic_route_response_for_zone(
                topic_route_data,
                request.version(),
                request_header.accept_standard_json_only,
                force_java_zone_legacy_json,
            )?;
            return Ok(Some(
                self.command_factory
                    .create_response_command_with_code(ResponseCode::Success)
                    .set_body(content),
            ));
        }

        Ok(Some(
            self.command_factory
                .create_response_command_with_code(ResponseCode::TopicNotExist)
                .set_remark(format!(
                    "No topic route info in name server for the topic: {}{}",
                    request_header.topic,
                    FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                )),
        ))
    }

    pub(crate) async fn handle_request(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let _runtime_guard = self.name_server_runtime_inner.upgrade().ok_or_else(|| {
            rocketmq_error::RocketMQError::not_initialized("NameServer runtime is no longer available")
        })?;
        let request_code = RequestCode::from(request.code());
        debug!(
            "Name server ClusterTestRequestProcessor received request code: {:?}",
            request_code
        );

        self.get_route_info_by_topic(request).await
    }
}

impl RequestProcessorV2 for ClusterTestRequestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let response = self.handle_request(request.command_mut()).await?;
        crate::processor::response_outcome(response)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::bootstrap::Builder;
    use crate::bootstrap::NameServerRuntimeHandle;
    use crate::NamesrvConfig;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::RemotingSerializable;

    use super::route_lookup::ClusterTestLookupFuture;

    struct TestClusterTestRouteLookup {
        route: Option<TopicRouteData>,
    }

    impl ClusterTestRouteLookup for TestClusterTestRouteLookup {
        fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn lookup_topic_route(&self, _topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
            let route = self.route.clone();
            Box::pin(async move { Ok(route) })
        }

        fn shutdown(&self) -> ClusterTestLookupFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn sample_topic_route_data() -> TopicRouteData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from("10.0.0.10:10911"));

        TopicRouteData {
            order_topic_conf: Some(CheetahString::from("broker-a:2")),
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                Some(CheetahString::from("zone-a")),
            )],
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    #[tokio::test]
    async fn cluster_test_processor_falls_back_to_product_env_lookup() {
        let namesrv_config = NamesrvConfig {
            cluster_test: true,
            ..NamesrvConfig::default()
        };
        let mock_lookup = Arc::new(TestClusterTestRouteLookup {
            route: Some(sample_topic_route_data()),
        });

        let runtime = rocketmq_runtime::RuntimeContext::from_current("namesrv-cluster-test-processor");
        let command_factory = rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory::new(
            rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults::new(
                657,
                rocketmq_protocol::protocol::SerializeType::ROCKETMQ,
            ),
        );
        let bootstrap = Builder::new(
            runtime.service_context("namesrv"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .set_name_server_config(namesrv_config)
        .set_remoting_command_factory(command_factory)
        .set_cluster_test_route_lookup(mock_lookup)
        .build();

        let runtime = bootstrap.runtime_inner();
        let processor = ClusterTestRequestProcessor::new(NameServerRuntimeHandle::new(&runtime));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(CheetahString::from("missing-topic"), Some(true)),
        );
        request.make_custom_header_to_net();

        let response = processor
            .handle_request(&mut request)
            .await
            .expect("request should succeed")
            .expect("processor should always return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(response.version(), 657);
        assert_eq!(
            response.serialize_type(),
            rocketmq_protocol::protocol::SerializeType::ROCKETMQ
        );
        let body = response.body().expect("cluster test response should include a body");
        let route_data = TopicRouteData::decode(body).expect("route body should decode");
        assert_eq!(
            body.as_ref(),
            route_data.encode().expect("legacy encoding should succeed").as_slice()
        );
    }
}
