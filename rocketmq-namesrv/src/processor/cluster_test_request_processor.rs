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
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use tracing::debug;
use tracing::info;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;

mod route_lookup;

pub(crate) use route_lookup::ClusterTestRouteLookup;
pub(crate) use route_lookup::TransportClusterTestRouteLookup;

pub struct ClusterTestRequestProcessor {
    name_server_runtime_inner: NameServerRuntimeHandle,
}

impl ClusterTestRequestProcessor {
    pub(crate) fn new(name_server_runtime_inner: NameServerRuntimeHandle) -> Self {
        Self {
            name_server_runtime_inner,
        }
    }

    async fn get_route_info_by_topic(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetRouteInfoRequestHeader>()?;

        let mut topic_route_data = self
            .name_server_runtime_inner
            .route_info_manager()
            .pickup_topic_route_data(request_header.topic.as_ref());

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
                        self.name_server_runtime_inner.name_server_config().product_env_name,
                        error
                    );
                }
            }
        }

        if let Some(topic_route_data) = topic_route_data {
            let content = topic_route_data.encode()?;
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_body(content),
            ));
        }

        Ok(Some(
            RemotingCommand::create_response_command_with_code(ResponseCode::TopicNotExist).set_remark(format!(
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

impl RequestProcessor for ClusterTestRequestProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.handle_request(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::bootstrap::Builder;
    use crate::bootstrap::NameServerRuntimeHandle;
    use crate::route::route_info_manager_wrapper::RouteInfoManagerWrapper;
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_remoting::local::LocalRequestHarness;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_remoting::runtime::processor::RequestProcessor;

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
            use_route_info_manager_v2: true,
            ..NamesrvConfig::default()
        };
        let mock_lookup = Arc::new(TestClusterTestRouteLookup {
            route: Some(sample_topic_route_data()),
        });

        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_cluster_test_route_lookup(mock_lookup)
            .build();

        assert!(matches!(
            bootstrap.runtime_inner().route_info_manager().as_ref(),
            RouteInfoManagerWrapper::V2(_)
        ));

        let harness = LocalRequestHarness::new().await.unwrap();
        let runtime = bootstrap.runtime_inner();
        let mut processor = ClusterTestRequestProcessor::new(NameServerRuntimeHandle::new(&runtime));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(CheetahString::from("missing-topic"), Some(true)),
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("request should succeed")
            .expect("processor should always return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("cluster test response should include a body");
        let route_data = TopicRouteData::decode(body).expect("route body should decode");
        assert_eq!(
            body.as_ref(),
            route_data.encode().expect("legacy encoding should succeed").as_slice()
        );
    }
}
