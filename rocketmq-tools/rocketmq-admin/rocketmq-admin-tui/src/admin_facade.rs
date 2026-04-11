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

use rocketmq_admin_core::core::admin::AdminBuilder;
use rocketmq_admin_core::core::topic::AllocateMqQueryRequest;
use rocketmq_admin_core::core::topic::DeleteTopicRequest;
use rocketmq_admin_core::core::topic::OrderConfRequest;
use rocketmq_admin_core::core::topic::TopicClusterList;
use rocketmq_admin_core::core::topic::TopicClusterQueryRequest;
use rocketmq_admin_core::core::topic::TopicListQueryRequest;
use rocketmq_admin_core::core::topic::TopicRouteData;
use rocketmq_admin_core::core::topic::TopicRouteQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicStatusQueryRequest;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicPermRequest;
use rocketmq_admin_core::core::topic::UpdateTopicRequest;
use rocketmq_admin_core::core::RocketMQResult;

#[derive(Debug, Clone, Default)]
pub struct TuiAdminFacade {
    namesrv_addr: Option<String>,
}

impl TuiAdminFacade {
    pub fn with_namesrv_addr(addr: impl Into<String>) -> Self {
        Self {
            namesrv_addr: Some(addr.into()),
        }
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match &self.namesrv_addr {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }

    pub fn topic_cluster_request(&self, topic: impl Into<String>) -> RocketMQResult<TopicClusterQueryRequest> {
        Ok(TopicClusterQueryRequest::try_new(topic)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn topic_route_request(&self, topic: impl Into<String>) -> RocketMQResult<TopicRouteQueryRequest> {
        Ok(TopicRouteQueryRequest::try_new(topic)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn topic_status_request(
        &self,
        topic: impl Into<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<TopicStatusQueryRequest> {
        Ok(TopicStatusQueryRequest::try_new(topic)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone())
            .with_optional_cluster_name(cluster_name))
    }

    pub fn topic_list_request(&self, cluster_name: Option<String>) -> TopicListQueryRequest {
        TopicListQueryRequest::new()
            .with_optional_namesrv_addr(self.namesrv_addr.clone())
            .with_optional_cluster_name(cluster_name)
    }

    pub fn delete_topic_request(
        &self,
        topic: impl Into<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<DeleteTopicRequest> {
        Ok(DeleteTopicRequest::try_new(topic, cluster_name)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn order_conf_request(
        &self,
        topic: impl Into<String>,
        method: impl AsRef<str>,
        order_conf: Option<String>,
    ) -> RocketMQResult<OrderConfRequest> {
        Ok(OrderConfRequest::try_new(topic, method, order_conf)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn allocate_mq_request(
        &self,
        topic: impl Into<String>,
        ip_list: impl Into<String>,
    ) -> RocketMQResult<AllocateMqQueryRequest> {
        Ok(AllocateMqQueryRequest::try_new(topic, ip_list)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic_request(
        &self,
        topic: impl Into<String>,
        target: TopicTarget,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: Option<u32>,
        order: Option<bool>,
        unit: Option<bool>,
        has_unit_sub: Option<bool>,
    ) -> RocketMQResult<UpdateTopicRequest> {
        Ok(UpdateTopicRequest::try_new(
            topic,
            target,
            read_queue_nums,
            write_queue_nums,
            perm,
            order,
            unit,
            has_unit_sub,
        )?
        .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn update_topic_perm_request(
        &self,
        topic: impl Into<String>,
        target: TopicTarget,
        perm: i32,
    ) -> RocketMQResult<UpdateTopicPermRequest> {
        Ok(UpdateTopicPermRequest::try_new(topic, target, perm)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub async fn query_topic_clusters(&self, topic: impl Into<String>) -> RocketMQResult<TopicClusterList> {
        TopicService::query_topic_clusters(self.topic_cluster_request(topic)?).await
    }

    pub async fn query_topic_route(&self, topic: impl Into<String>) -> RocketMQResult<Option<TopicRouteData>> {
        TopicService::query_topic_route(self.topic_route_request(topic)?).await
    }
}

#[cfg(test)]
mod tests {
    use super::TuiAdminFacade;

    #[test]
    fn facade_builds_topic_cluster_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
        let request = facade.topic_cluster_request(" TestTopic ").unwrap();

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn facade_builds_topic_route_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");
        let request = facade.topic_route_request(" RouteTopic ").unwrap();

        assert_eq!(request.topic().as_str(), "RouteTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn facade_builds_topic_status_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");
        let request = facade
            .topic_status_request(" StatusTopic ", Some(" DefaultCluster ".to_string()))
            .unwrap();

        assert_eq!(request.topic().as_str(), "StatusTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(
            request.cluster_name().map(|value| value.as_str()),
            Some("DefaultCluster")
        );
    }

    #[test]
    fn facade_builds_additional_topic_requests_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        assert_eq!(
            facade
                .topic_list_request(Some(" DefaultCluster ".to_string()))
                .cluster_name()
                .map(|value| value.as_str()),
            Some("DefaultCluster")
        );
        assert_eq!(
            facade
                .delete_topic_request(" TestTopic ", Some(" DefaultCluster ".to_string()))
                .unwrap()
                .cluster_name()
                .as_str(),
            "DefaultCluster"
        );
        assert_eq!(
            facade
                .order_conf_request(" TestTopic ", "put", Some(" broker-a:4 ".to_string()))
                .unwrap()
                .order_conf(),
            Some("broker-a:4")
        );
        assert_eq!(
            facade
                .allocate_mq_request(" TestTopic ", " 192.168.1.1 ")
                .unwrap()
                .ip_list()
                .as_str(),
            "192.168.1.1"
        );
    }

    #[test]
    fn facade_builds_update_topic_requests_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        let update_topic = facade
            .update_topic_request(
                " TestTopic ",
                rocketmq_admin_core::core::topic::TopicTarget::Broker("127.0.0.1:10911".into()),
                8,
                8,
                Some(6),
                Some(false),
                Some(false),
                Some(false),
            )
            .unwrap();
        assert_eq!(update_topic.config().topic_name.as_str(), "TestTopic");

        let update_perm = facade
            .update_topic_perm_request(
                " TestTopic ",
                rocketmq_admin_core::core::topic::TopicTarget::Cluster("DefaultCluster".into()),
                6,
            )
            .unwrap();
        assert_eq!(update_perm.perm(), 6);
    }
}
