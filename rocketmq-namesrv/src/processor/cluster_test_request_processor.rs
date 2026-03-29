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

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client::admin::default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
use rocketmq_client::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client::base::client_config::ClientConfig;
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::info;

use crate::bootstrap::NameServerRuntimeInner;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;

const CLUSTER_TEST_INSTANCE_PREFIX: &str = "CLUSTER_TEST_NS_INS_";
type ClusterTestLookupFuture<'a, T> = Pin<Box<dyn Future<Output = rocketmq_error::RocketMQResult<T>> + Send + 'a>>;
type ClusterTestShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub(crate) enum ClusterTestRouteLookup {
    Default(DefaultClusterTestRouteLookup),
    #[cfg(test)]
    Test(TestClusterTestRouteLookup),
}

impl ClusterTestRouteLookup {
    pub(crate) fn new(product_env_name: &str) -> Self {
        Self::Default(DefaultClusterTestRouteLookup::new(product_env_name))
    }

    #[cfg(test)]
    pub(crate) fn for_test(route: Option<TopicRouteData>) -> Self {
        Self::Test(TestClusterTestRouteLookup::new(route))
    }

    pub(crate) fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
        match self {
            ClusterTestRouteLookup::Default(lookup) => lookup.start(),
            #[cfg(test)]
            ClusterTestRouteLookup::Test(lookup) => lookup.start(),
        }
    }

    pub(crate) fn lookup_topic_route(
        &self,
        topic: &CheetahString,
    ) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
        match self {
            ClusterTestRouteLookup::Default(lookup) => lookup.lookup_topic_route(topic),
            #[cfg(test)]
            ClusterTestRouteLookup::Test(lookup) => lookup.lookup_topic_route(topic),
        }
    }

    pub(crate) fn shutdown(&self) -> ClusterTestShutdownFuture<'_> {
        match self {
            ClusterTestRouteLookup::Default(lookup) => lookup.shutdown(),
            #[cfg(test)]
            ClusterTestRouteLookup::Test(lookup) => lookup.shutdown(),
        }
    }
}

pub(crate) struct DefaultClusterTestRouteLookup {
    admin_ext: ArcMut<DefaultMQAdminExtImpl>,
}

impl DefaultClusterTestRouteLookup {
    pub(crate) fn new(product_env_name: &str) -> Self {
        let mut client_config = ClientConfig::default();
        client_config.set_unit_name(CheetahString::from(product_env_name));
        client_config.set_instance_name(CheetahString::from_string(format!(
            "{}{}",
            CLUSTER_TEST_INSTANCE_PREFIX, product_env_name
        )));
        let admin_ext_group =
            CheetahString::from_string(format!("{}{}_GROUP", CLUSTER_TEST_INSTANCE_PREFIX, product_env_name));
        let admin_ext = ArcMut::new(DefaultMQAdminExtImpl::new(
            None,
            Duration::from_secs(3),
            ArcMut::new(client_config),
            admin_ext_group,
        ));
        admin_ext.mut_from_ref().set_inner(admin_ext.clone());

        Self { admin_ext }
    }

    fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async move { self.admin_ext.mut_from_ref().start().await })
    }

    fn lookup_topic_route(&self, topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
        let topic = topic.clone();
        Box::pin(async move { self.admin_ext.examine_topic_route_info(topic).await })
    }

    fn shutdown(&self) -> ClusterTestShutdownFuture<'_> {
        Box::pin(async move {
            self.admin_ext.mut_from_ref().shutdown().await;
        })
    }
}

#[cfg(test)]
pub(crate) struct TestClusterTestRouteLookup {
    route: Option<TopicRouteData>,
}

#[cfg(test)]
impl TestClusterTestRouteLookup {
    fn new(route: Option<TopicRouteData>) -> Self {
        Self { route }
    }

    fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn lookup_topic_route(&self, _topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
        let route = self.route.clone();
        Box::pin(async move { Ok(route) })
    }

    fn shutdown(&self) -> ClusterTestShutdownFuture<'_> {
        Box::pin(async {})
    }
}

pub struct ClusterTestRequestProcessor {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
}

impl ClusterTestRequestProcessor {
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
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
}

impl RequestProcessor for ClusterTestRequestProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        debug!(
            "Name server ClusterTestRequestProcessor received request code: {:?}",
            request_code
        );

        self.get_route_info_by_topic(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::bootstrap::Builder;
    use crate::route::route_info_manager_wrapper::RouteInfoManagerWrapper;
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_remoting::local::LocalRequestHarness;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::runtime::processor::RequestProcessor;

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
        let mock_lookup = Arc::new(ClusterTestRouteLookup::for_test(Some(sample_topic_route_data())));

        let bootstrap = Builder::new()
            .set_name_server_config(namesrv_config)
            .set_cluster_test_route_lookup(mock_lookup)
            .build();

        assert!(matches!(
            bootstrap.runtime_inner().route_info_manager(),
            RouteInfoManagerWrapper::V2(_)
        ));

        let harness = LocalRequestHarness::new().await.unwrap();
        let mut processor = ClusterTestRequestProcessor::new(bootstrap.runtime_inner());
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
