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
use rocketmq_admin_core::core::broker::BrokerConfigQueryRequest;
use rocketmq_admin_core::core::broker::BrokerConfigQueryResult;
use rocketmq_admin_core::core::broker::BrokerConfigUpdateApplyResult;
use rocketmq_admin_core::core::broker::BrokerConfigUpdatePlanResult;
use rocketmq_admin_core::core::broker::BrokerConfigUpdateRequest;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerConsumeStatsResult;
use rocketmq_admin_core::core::broker::BrokerRuntimeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerRuntimeStatsResult;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::cluster::ClusterBrokerNameQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterBrokerNameQueryResult;
use rocketmq_admin_core::core::cluster::ClusterListQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtRequest;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
use rocketmq_admin_core::core::cluster::ClusterService;
use rocketmq_admin_core::core::connection::ConnectionService;
use rocketmq_admin_core::core::connection::ConsumerConnectionQueryRequest;
use rocketmq_admin_core::core::connection::ConsumerConnectionQueryResult;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryRequest;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryResult;
use rocketmq_admin_core::core::consumer::ConsumerConfigQueryRequest;
use rocketmq_admin_core::core::consumer::ConsumerConfigQueryResult;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerProgressRequest;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoRequest;
use rocketmq_admin_core::core::consumer::ConsumerRunningInfoResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::consumer::DeleteSubscriptionGroupRequest;
use rocketmq_admin_core::core::consumer::SetConsumeModeRequest;
use rocketmq_admin_core::core::ha::HaService;
use rocketmq_admin_core::core::ha::HaStatusQueryRequest;
use rocketmq_admin_core::core::ha::HaStatusQueryResult;
use rocketmq_admin_core::core::ha::SyncStateSetQueryRequest;
use rocketmq_admin_core::core::ha::SyncStateSetQueryResult;
use rocketmq_admin_core::core::namesrv::KvConfigDeleteRequest;
use rocketmq_admin_core::core::namesrv::KvConfigUpdateRequest;
use rocketmq_admin_core::core::namesrv::KvConfigUpdateResult;
use rocketmq_admin_core::core::namesrv::NameServerService;
use rocketmq_admin_core::core::namesrv::NamesrvConfigQueryRequest;
use rocketmq_admin_core::core::namesrv::NamesrvConfigQueryResult;
use rocketmq_admin_core::core::namesrv::NamesrvConfigUpdateRequest;
use rocketmq_admin_core::core::namesrv::NamesrvConfigUpdateResult;
use rocketmq_admin_core::core::namesrv::WritePermRequest;
use rocketmq_admin_core::core::namesrv::WritePermResult;
use rocketmq_admin_core::core::offset::CloneGroupOffsetRequest;
use rocketmq_admin_core::core::offset::ConsumerStatusQueryRequest;
use rocketmq_admin_core::core::offset::ConsumerStatusResult;
use rocketmq_admin_core::core::offset::OffsetService;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeOldRequest;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeRequest;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeResult;
use rocketmq_admin_core::core::offset::SkipAccumulatedMessageRequest;
use rocketmq_admin_core::core::offset::SkipAccumulatedMessageResult;
use rocketmq_admin_core::core::producer::ProducerInfoQueryRequest;
use rocketmq_admin_core::core::producer::ProducerInfoQueryResult;
use rocketmq_admin_core::core::producer::ProducerService;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressRequest;
use rocketmq_admin_core::core::queue::CheckRocksdbCqWriteProgressResult;
use rocketmq_admin_core::core::queue::QueryConsumeQueueRequest;
use rocketmq_admin_core::core::queue::QueryConsumeQueueResult;
use rocketmq_admin_core::core::queue::QueueService;
use rocketmq_admin_core::core::stats::StatsAllQueryRequest;
use rocketmq_admin_core::core::stats::StatsAllQueryResult;
use rocketmq_admin_core::core::stats::StatsService;
use rocketmq_admin_core::core::topic::AllocateMqQueryRequest;
use rocketmq_admin_core::core::topic::AllocatedMqQueryResult;
use rocketmq_admin_core::core::topic::DeleteTopicRequest;
use rocketmq_admin_core::core::topic::DeleteTopicResult;
use rocketmq_admin_core::core::topic::OrderConfRequest;
use rocketmq_admin_core::core::topic::OrderConfResult;
use rocketmq_admin_core::core::topic::TopicClusterList;
use rocketmq_admin_core::core::topic::TopicClusterQueryRequest;
use rocketmq_admin_core::core::topic::TopicListQueryRequest;
use rocketmq_admin_core::core::topic::TopicListResult;
use rocketmq_admin_core::core::topic::TopicRouteData;
use rocketmq_admin_core::core::topic::TopicRouteQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicStatsTable;
use rocketmq_admin_core::core::topic::TopicStatusQueryRequest;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicPermRequest;
use rocketmq_admin_core::core::topic::UpdateTopicPermResult;
use rocketmq_admin_core::core::topic::UpdateTopicRequest;
use rocketmq_admin_core::core::topic::UpdateTopicResult;
use rocketmq_admin_core::core::RocketMQResult;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;

#[derive(Debug, Clone, Default)]
pub struct TuiAdminFacade {
    namesrv_addr: Option<String>,
}

impl TuiAdminFacade {
    #[allow(dead_code)]
    pub fn with_namesrv_addr(addr: impl Into<String>) -> Self {
        Self {
            namesrv_addr: Some(addr.into()),
        }
    }

    pub fn set_namesrv_addr(&mut self, addr: Option<String>) {
        self.namesrv_addr = addr.map(|addr| addr.trim().to_string()).filter(|addr| !addr.is_empty());
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    #[allow(dead_code)]
    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match &self.namesrv_addr {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }

    pub fn namesrv_config_query_request(&self) -> RocketMQResult<NamesrvConfigQueryRequest> {
        NamesrvConfigQueryRequest::try_new(self.namesrv_addr.clone())
    }

    pub fn namesrv_config_update_request(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<NamesrvConfigUpdateRequest> {
        NamesrvConfigUpdateRequest::try_new(key, value, self.namesrv_addr.clone())
    }

    pub fn kv_config_update_request(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<KvConfigUpdateRequest> {
        Ok(
            KvConfigUpdateRequest::try_new(namespace, key, value)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn kv_config_delete_request(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> RocketMQResult<KvConfigDeleteRequest> {
        Ok(KvConfigDeleteRequest::try_new(namespace, key)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn write_perm_request(&self, broker_name: impl Into<String>) -> RocketMQResult<WritePermRequest> {
        Ok(WritePermRequest::try_new(broker_name)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn broker_config_query_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        key_pattern: Option<String>,
    ) -> RocketMQResult<BrokerConfigQueryRequest> {
        Ok(
            BrokerConfigQueryRequest::try_new(broker_addr, cluster_name, key_pattern)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn broker_config_update_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        entries: std::collections::BTreeMap<String, String>,
        rollback_enabled: bool,
    ) -> RocketMQResult<BrokerConfigUpdateRequest> {
        Ok(BrokerConfigUpdateRequest::try_new(broker_addr, cluster_name, entries)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone())
            .with_rollback_enabled(rollback_enabled))
    }

    pub fn broker_runtime_stats_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<BrokerRuntimeStatsQueryRequest> {
        Ok(BrokerRuntimeStatsQueryRequest::try_new(broker_addr, cluster_name)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn broker_consume_stats_request(
        &self,
        broker_addr: impl Into<String>,
        timeout_millis: u64,
        diff_level: i64,
        is_order: bool,
    ) -> RocketMQResult<BrokerConsumeStatsQueryRequest> {
        Ok(
            BrokerConsumeStatsQueryRequest::try_new(broker_addr, timeout_millis, diff_level, is_order)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn cluster_list_request(&self, more_stats: bool, cluster_name: Option<String>) -> ClusterListQueryRequest {
        ClusterListQueryRequest::new(more_stats, cluster_name).with_optional_namesrv_addr(self.namesrv_addr.clone())
    }

    pub fn cluster_broker_names_request(&self, cluster_name: Option<String>) -> ClusterBrokerNameQueryRequest {
        ClusterBrokerNameQueryRequest::new(cluster_name).with_optional_namesrv_addr(self.namesrv_addr.clone())
    }

    pub fn cluster_send_message_rt_request(
        &self,
        amount: u64,
        size: u64,
        cluster_name: Option<String>,
    ) -> RocketMQResult<ClusterSendMessageRtRequest> {
        Ok(ClusterSendMessageRtRequest::try_new(amount, size, cluster_name)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn consumer_connection_request(
        &self,
        consumer_group: impl Into<String>,
        broker_addr: Option<String>,
    ) -> RocketMQResult<ConsumerConnectionQueryRequest> {
        Ok(ConsumerConnectionQueryRequest::try_new(consumer_group, broker_addr)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn producer_connection_request(
        &self,
        producer_group: impl Into<String>,
        topic: impl Into<String>,
    ) -> RocketMQResult<ProducerConnectionQueryRequest> {
        Ok(ProducerConnectionQueryRequest::try_new(producer_group, topic)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn consumer_config_request(&self, group_name: impl Into<String>) -> RocketMQResult<ConsumerConfigQueryRequest> {
        Ok(ConsumerConfigQueryRequest::try_new(group_name)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn delete_subscription_group_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        group_name: impl Into<String>,
        remove_offset: bool,
    ) -> RocketMQResult<DeleteSubscriptionGroupRequest> {
        Ok(
            DeleteSubscriptionGroupRequest::try_new(broker_addr, cluster_name, group_name, remove_offset)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn set_consume_mode_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        topic_name: impl Into<String>,
        group_name: impl Into<String>,
        mode: MessageRequestMode,
        pop_share_queue_num: Option<i32>,
    ) -> RocketMQResult<SetConsumeModeRequest> {
        Ok(SetConsumeModeRequest::try_new(
            broker_addr,
            cluster_name,
            topic_name,
            group_name,
            mode,
            pop_share_queue_num,
        )?
        .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn consumer_running_info_request(
        &self,
        group_name: impl Into<String>,
        client_id: Option<String>,
        broker_addr: Option<String>,
        jstack: bool,
    ) -> RocketMQResult<ConsumerRunningInfoRequest> {
        ConsumerRunningInfoRequest::try_new(group_name, client_id, broker_addr, jstack, self.namesrv_addr.clone())
    }

    pub fn consumer_progress_request(
        &self,
        consumer_group: Option<String>,
        topic_name: Option<String>,
        show_client_ip: bool,
        cluster: Option<String>,
    ) -> RocketMQResult<ConsumerProgressRequest> {
        ConsumerProgressRequest::try_new(
            consumer_group,
            topic_name,
            show_client_ip,
            cluster,
            self.namesrv_addr.clone(),
        )
    }

    pub fn clone_group_offset_request(
        &self,
        src_group: impl Into<String>,
        dest_group: impl Into<String>,
        topic: impl Into<String>,
        offline: bool,
    ) -> RocketMQResult<CloneGroupOffsetRequest> {
        Ok(CloneGroupOffsetRequest::try_new(src_group, dest_group, topic, offline)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn consumer_status_request(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        origin_client_id: Option<String>,
    ) -> RocketMQResult<ConsumerStatusQueryRequest> {
        Ok(ConsumerStatusQueryRequest::try_new(group, topic, origin_client_id)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn skip_accumulated_message_request(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        cluster: Option<String>,
        force: Option<bool>,
    ) -> RocketMQResult<SkipAccumulatedMessageRequest> {
        SkipAccumulatedMessageRequest::try_new(group, topic, cluster, force, self.namesrv_addr.clone())
    }

    pub fn reset_offset_by_time_request(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        timestamp: u64,
    ) -> RocketMQResult<ResetOffsetByTimeRequest> {
        Ok(ResetOffsetByTimeRequest::try_new(group, topic, timestamp)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn reset_offset_by_time_old_request(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        timestamp: u64,
        force: Option<bool>,
        cluster: Option<String>,
    ) -> RocketMQResult<ResetOffsetByTimeOldRequest> {
        ResetOffsetByTimeOldRequest::try_new(group, topic, timestamp, force, cluster, self.namesrv_addr.clone())
    }

    pub fn query_consume_queue_request(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        index: u64,
        count: i32,
        broker_addr: Option<String>,
        consumer_group: Option<String>,
    ) -> RocketMQResult<QueryConsumeQueueRequest> {
        Ok(
            QueryConsumeQueueRequest::try_new(topic, queue_id, index, count, broker_addr, consumer_group)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn check_rocksdb_cq_write_progress_request(
        &self,
        cluster_name: impl Into<String>,
        topic: Option<String>,
        check_from: Option<i64>,
    ) -> RocketMQResult<CheckRocksdbCqWriteProgressRequest> {
        CheckRocksdbCqWriteProgressRequest::try_new(
            cluster_name,
            self.namesrv_addr.clone().unwrap_or_default(),
            topic,
            check_from,
        )
    }

    pub fn ha_status_request(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<HaStatusQueryRequest> {
        Ok(HaStatusQueryRequest::try_new(broker_addr, cluster_name)?
            .with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }

    pub fn sync_state_set_request(
        &self,
        controller_address: impl Into<String>,
        broker_name: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<SyncStateSetQueryRequest> {
        Ok(
            SyncStateSetQueryRequest::try_new(controller_address, broker_name, cluster_name)?
                .with_optional_namesrv_addr(self.namesrv_addr.clone()),
        )
    }

    pub fn stats_all_request(&self, active_topic: bool, topic: Option<String>) -> StatsAllQueryRequest {
        StatsAllQueryRequest::new(active_topic, topic).with_optional_namesrv_addr(self.namesrv_addr.clone())
    }

    pub fn producer_info_request(&self, broker_addr: impl Into<String>) -> RocketMQResult<ProducerInfoQueryRequest> {
        Ok(ProducerInfoQueryRequest::try_new(broker_addr)?.with_optional_namesrv_addr(self.namesrv_addr.clone()))
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

    pub async fn query_topic_status(
        &self,
        topic: impl Into<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<TopicStatsTable> {
        TopicService::query_topic_status(self.topic_status_request(topic, cluster_name)?).await
    }

    pub async fn query_topic_list(&self, cluster_name: Option<String>) -> RocketMQResult<TopicListResult> {
        TopicService::query_topic_list(self.topic_list_request(cluster_name)).await
    }

    pub async fn delete_topic(
        &self,
        topic: impl Into<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<DeleteTopicResult> {
        TopicService::delete_topic_by_request(self.delete_topic_request(topic, cluster_name)?).await
    }

    pub async fn apply_order_conf(
        &self,
        topic: impl Into<String>,
        method: impl AsRef<str>,
        order_conf: Option<String>,
    ) -> RocketMQResult<OrderConfResult> {
        TopicService::apply_order_conf(self.order_conf_request(topic, method, order_conf)?).await
    }

    pub async fn query_allocated_mq(
        &self,
        topic: impl Into<String>,
        ip_list: impl Into<String>,
    ) -> RocketMQResult<AllocatedMqQueryResult> {
        TopicService::query_allocated_mq_by_request(self.allocate_mq_request(topic, ip_list)?).await
    }

    pub async fn create_or_update_topic(&self, request: UpdateTopicRequest) -> RocketMQResult<UpdateTopicResult> {
        TopicService::create_or_update_topic_by_request(request).await
    }

    pub async fn update_topic_perm(&self, request: UpdateTopicPermRequest) -> RocketMQResult<UpdateTopicPermResult> {
        TopicService::update_topic_perm_by_request(request).await
    }

    pub async fn query_namesrv_config(&self) -> RocketMQResult<NamesrvConfigQueryResult> {
        NameServerService::query_namesrv_config(self.namesrv_config_query_request()?).await
    }

    pub async fn update_namesrv_config(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<NamesrvConfigUpdateResult> {
        NameServerService::update_namesrv_config_by_request(self.namesrv_config_update_request(key, value)?).await
    }

    pub async fn update_kv_config(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<KvConfigUpdateResult> {
        NameServerService::update_kv_config_by_request(self.kv_config_update_request(namespace, key, value)?).await
    }

    pub async fn delete_kv_config(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> RocketMQResult<KvConfigUpdateResult> {
        NameServerService::delete_kv_config_by_request(self.kv_config_delete_request(namespace, key)?).await
    }

    pub async fn add_write_perm(&self, broker_name: impl Into<String>) -> RocketMQResult<WritePermResult> {
        NameServerService::add_write_perm_by_request(self.write_perm_request(broker_name)?).await
    }

    pub async fn wipe_write_perm(&self, broker_name: impl Into<String>) -> RocketMQResult<WritePermResult> {
        NameServerService::wipe_write_perm_by_request(self.write_perm_request(broker_name)?).await
    }

    pub async fn query_broker_config(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        key_pattern: Option<String>,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        BrokerService::query_broker_config_by_request(self.broker_config_query_request(
            broker_addr,
            cluster_name,
            key_pattern,
        )?)
        .await
    }

    pub async fn build_broker_config_update_plan(
        &self,
        request: BrokerConfigUpdateRequest,
    ) -> RocketMQResult<BrokerConfigUpdatePlanResult> {
        BrokerService::build_broker_config_update_plan_by_request(request).await
    }

    pub async fn apply_broker_config_update(
        &self,
        request: BrokerConfigUpdateRequest,
    ) -> RocketMQResult<BrokerConfigUpdateApplyResult> {
        BrokerService::apply_broker_config_update_by_request(request).await
    }

    pub async fn query_broker_runtime_stats(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<BrokerRuntimeStatsResult> {
        BrokerService::query_broker_runtime_stats_by_request(
            self.broker_runtime_stats_request(broker_addr, cluster_name)?,
        )
        .await
    }

    pub async fn query_broker_consume_stats(
        &self,
        broker_addr: impl Into<String>,
        timeout_millis: u64,
        diff_level: i64,
        is_order: bool,
    ) -> RocketMQResult<BrokerConsumeStatsResult> {
        BrokerService::query_broker_consume_stats_by_request(self.broker_consume_stats_request(
            broker_addr,
            timeout_millis,
            diff_level,
            is_order,
        )?)
        .await
    }

    pub async fn query_cluster_list(
        &self,
        more_stats: bool,
        cluster_name: Option<String>,
    ) -> RocketMQResult<ClusterListQueryResult> {
        ClusterService::query_cluster_list_by_request_with_rpc_hook(
            self.cluster_list_request(more_stats, cluster_name),
            None,
        )
        .await
    }

    pub async fn query_cluster_broker_names(
        &self,
        cluster_name: Option<String>,
    ) -> RocketMQResult<ClusterBrokerNameQueryResult> {
        ClusterService::query_cluster_broker_names_by_request_with_rpc_hook(
            self.cluster_broker_names_request(cluster_name),
            None,
        )
        .await
    }

    pub async fn check_cluster_send_message_rt(
        &self,
        amount: u64,
        size: u64,
        cluster_name: Option<String>,
    ) -> RocketMQResult<ClusterSendMessageRtResult> {
        ClusterService::send_message_rt_by_request_with_rpc_hook(
            self.cluster_send_message_rt_request(amount, size, cluster_name)?,
            None,
        )
        .await
    }

    pub async fn query_consumer_connection(
        &self,
        consumer_group: impl Into<String>,
        broker_addr: Option<String>,
    ) -> RocketMQResult<ConsumerConnectionQueryResult> {
        ConnectionService::query_consumer_connection_by_request_with_rpc_hook(
            self.consumer_connection_request(consumer_group, broker_addr)?,
            None,
        )
        .await
    }

    pub async fn query_producer_connection(
        &self,
        producer_group: impl Into<String>,
        topic: impl Into<String>,
    ) -> RocketMQResult<ProducerConnectionQueryResult> {
        ConnectionService::query_producer_connection_by_request_with_rpc_hook(
            self.producer_connection_request(producer_group, topic)?,
            None,
        )
        .await
    }

    pub async fn query_consumer_config(
        &self,
        group_name: impl Into<String>,
    ) -> RocketMQResult<ConsumerConfigQueryResult> {
        ConsumerService::query_consumer_config_by_request_with_rpc_hook(self.consumer_config_request(group_name)?, None)
            .await
    }

    pub async fn delete_subscription_group(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        group_name: impl Into<String>,
        remove_offset: bool,
    ) -> RocketMQResult<ConsumerOperationResult> {
        ConsumerService::delete_subscription_group_by_request_with_rpc_hook(
            self.delete_subscription_group_request(broker_addr, cluster_name, group_name, remove_offset)?,
            None,
        )
        .await
    }

    pub async fn set_consume_mode(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        topic_name: impl Into<String>,
        group_name: impl Into<String>,
        mode: MessageRequestMode,
        pop_share_queue_num: Option<i32>,
    ) -> RocketMQResult<ConsumerOperationResult> {
        ConsumerService::set_consume_mode_by_request_with_rpc_hook(
            self.set_consume_mode_request(
                broker_addr,
                cluster_name,
                topic_name,
                group_name,
                mode,
                pop_share_queue_num,
            )?,
            None,
        )
        .await
    }

    pub async fn query_consumer_running_info(
        &self,
        group_name: impl Into<String>,
        client_id: Option<String>,
        broker_addr: Option<String>,
        jstack: bool,
    ) -> RocketMQResult<ConsumerRunningInfoResult> {
        ConsumerService::query_consumer_running_info_by_request_with_rpc_hook(
            self.consumer_running_info_request(group_name, client_id, broker_addr, jstack)?,
            None,
        )
        .await
    }

    pub async fn query_consumer_progress(
        &self,
        consumer_group: Option<String>,
        topic_name: Option<String>,
        show_client_ip: bool,
        cluster: Option<String>,
    ) -> RocketMQResult<ConsumerProgressResult> {
        ConsumerService::query_consumer_progress_by_request_with_rpc_hook(
            self.consumer_progress_request(consumer_group, topic_name, show_client_ip, cluster)?,
            None,
        )
        .await
    }

    pub async fn clone_group_offset(
        &self,
        src_group: impl Into<String>,
        dest_group: impl Into<String>,
        topic: impl Into<String>,
        offline: bool,
    ) -> RocketMQResult<()> {
        OffsetService::clone_group_offset_by_request_with_rpc_hook(
            self.clone_group_offset_request(src_group, dest_group, topic, offline)?,
            None,
        )
        .await
    }

    pub async fn query_consumer_status(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        origin_client_id: Option<String>,
    ) -> RocketMQResult<ConsumerStatusResult> {
        OffsetService::query_consumer_status_by_request_with_rpc_hook(
            self.consumer_status_request(group, topic, origin_client_id)?,
            None,
        )
        .await
    }

    pub async fn skip_accumulated_message(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        cluster: Option<String>,
        force: Option<bool>,
    ) -> RocketMQResult<SkipAccumulatedMessageResult> {
        OffsetService::skip_accumulated_message_by_request_with_rpc_hook(
            self.skip_accumulated_message_request(group, topic, cluster, force)?,
            None,
        )
        .await
    }

    pub async fn reset_offset_by_time(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        timestamp: u64,
    ) -> RocketMQResult<ResetOffsetByTimeResult> {
        OffsetService::reset_offset_by_time_by_request_with_rpc_hook(
            self.reset_offset_by_time_request(group, topic, timestamp)?,
            None,
        )
        .await
    }

    pub async fn reset_offset_by_time_old(
        &self,
        group: impl Into<String>,
        topic: impl Into<String>,
        timestamp: u64,
        force: Option<bool>,
        cluster: Option<String>,
    ) -> RocketMQResult<Vec<RollbackStats>> {
        OffsetService::reset_offset_by_time_old_by_request_with_rpc_hook(
            self.reset_offset_by_time_old_request(group, topic, timestamp, force, cluster)?,
            None,
        )
        .await
    }

    pub async fn query_consume_queue(
        &self,
        topic: impl Into<String>,
        queue_id: i32,
        index: u64,
        count: i32,
        broker_addr: Option<String>,
        consumer_group: Option<String>,
    ) -> RocketMQResult<QueryConsumeQueueResult> {
        QueueService::query_consume_queue_by_request_with_rpc_hook(
            self.query_consume_queue_request(topic, queue_id, index, count, broker_addr, consumer_group)?,
            None,
        )
        .await
    }

    pub async fn check_rocksdb_cq_write_progress(
        &self,
        cluster_name: impl Into<String>,
        topic: Option<String>,
        check_from: Option<i64>,
    ) -> RocketMQResult<CheckRocksdbCqWriteProgressResult> {
        QueueService::check_rocksdb_cq_write_progress_by_request_with_rpc_hook(
            self.check_rocksdb_cq_write_progress_request(cluster_name, topic, check_from)?,
            None,
        )
        .await
    }

    pub async fn query_ha_status(
        &self,
        broker_addr: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<HaStatusQueryResult> {
        HaService::query_ha_status_by_request_with_rpc_hook(self.ha_status_request(broker_addr, cluster_name)?, None)
            .await
    }

    pub async fn query_sync_state_set(
        &self,
        controller_address: impl Into<String>,
        broker_name: Option<String>,
        cluster_name: Option<String>,
    ) -> RocketMQResult<SyncStateSetQueryResult> {
        HaService::query_sync_state_set_by_request_with_rpc_hook(
            self.sync_state_set_request(controller_address, broker_name, cluster_name)?,
            None,
        )
        .await
    }

    pub async fn query_stats_all(
        &self,
        active_topic: bool,
        topic: Option<String>,
    ) -> RocketMQResult<StatsAllQueryResult> {
        StatsService::query_stats_all_by_request_with_rpc_hook(self.stats_all_request(active_topic, topic), None).await
    }

    pub async fn query_producer_info(&self, broker_addr: impl Into<String>) -> RocketMQResult<ProducerInfoQueryResult> {
        ProducerService::query_producer_info_by_request_with_rpc_hook(self.producer_info_request(broker_addr)?, None)
            .await
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

    #[test]
    fn facade_builds_namesrv_requests_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876;127.0.0.2:9876 ");

        assert_eq!(facade.namesrv_config_query_request().unwrap().namesrv_addrs().len(), 2);
        let update_config = facade.namesrv_config_update_request(" deleteWhen ", " 04 ").unwrap();
        assert!(update_config
            .properties()
            .iter()
            .any(|(key, value)| key.as_str() == "deleteWhen" && value.as_str() == "04"));
        assert_eq!(
            facade
                .kv_config_update_request(" ns ", " key ", " value ")
                .unwrap()
                .namespace()
                .as_str(),
            "ns"
        );
        assert_eq!(
            facade.kv_config_delete_request(" ns ", " key ").unwrap().key().as_str(),
            "key"
        );
        assert_eq!(
            facade.write_perm_request(" broker-a ").unwrap().broker_name().as_str(),
            "broker-a"
        );
    }

    #[test]
    fn facade_builds_broker_config_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
        let request = facade
            .broker_config_query_request(
                None,
                Some(" DefaultCluster ".to_string()),
                Some(" ^flush.* ".to_string()),
            )
            .unwrap();

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(request.key_pattern(), Some("^flush.*"));
        assert!(matches!(
            request.target(),
            rocketmq_admin_core::core::broker::BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
        ));

        let mut entries = std::collections::BTreeMap::new();
        entries.insert(" flushDiskType ".to_string(), " ASYNC_FLUSH ".to_string());
        let update_request = facade
            .broker_config_update_request(Some(" 127.0.0.1:10911 ".to_string()), None, entries, false)
            .unwrap();
        assert_eq!(update_request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(!update_request.rollback_enabled());
        assert!(matches!(
            update_request.target(),
            rocketmq_admin_core::core::broker::BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
        ));
    }

    #[test]
    fn facade_builds_broker_runtime_stats_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
        let request = facade
            .broker_runtime_stats_request(None, Some(" DefaultCluster ".to_string()))
            .unwrap();

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(matches!(
            request.target(),
            rocketmq_admin_core::core::broker::BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
        ));
    }

    #[test]
    fn facade_builds_broker_consume_stats_request_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
        let request = facade
            .broker_consume_stats_request(" 127.0.0.1:10911 ", 3_000, 42, true)
            .unwrap();

        assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.timeout_millis(), 3_000);
        assert_eq!(request.diff_level(), 42);
        assert!(request.is_order());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn facade_exposes_topic_service_futures_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        std::mem::drop(facade.query_topic_clusters("TestTopic"));
        std::mem::drop(facade.query_topic_route("TestTopic"));
        std::mem::drop(facade.query_topic_status("TestTopic", Some("DefaultCluster".to_string())));
        std::mem::drop(facade.query_topic_list(Some("DefaultCluster".to_string())));
        std::mem::drop(facade.delete_topic("TestTopic", Some("DefaultCluster".to_string())));
        std::mem::drop(facade.apply_order_conf("TestTopic", "get", None));
        std::mem::drop(facade.query_allocated_mq("TestTopic", "192.168.1.1"));

        let update_topic = facade
            .update_topic_request(
                "TestTopic",
                rocketmq_admin_core::core::topic::TopicTarget::Broker("127.0.0.1:10911".into()),
                8,
                8,
                Some(6),
                Some(false),
                Some(false),
                Some(false),
            )
            .unwrap();
        std::mem::drop(facade.create_or_update_topic(update_topic));

        let update_perm = facade
            .update_topic_perm_request(
                "TestTopic",
                rocketmq_admin_core::core::topic::TopicTarget::Cluster("DefaultCluster".into()),
                6,
            )
            .unwrap();
        std::mem::drop(facade.update_topic_perm(update_perm));
    }

    #[test]
    fn facade_exposes_namesrv_service_futures_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        std::mem::drop(facade.query_namesrv_config());
        std::mem::drop(facade.update_namesrv_config("deleteWhen", "04"));
        std::mem::drop(facade.update_kv_config("ns", "key", "value"));
        std::mem::drop(facade.delete_kv_config("ns", "key"));
        std::mem::drop(facade.add_write_perm("broker-a"));
        std::mem::drop(facade.wipe_write_perm("broker-a"));
    }

    #[test]
    fn facade_exposes_broker_service_futures_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        std::mem::drop(facade.query_broker_config(
            None,
            Some("DefaultCluster".to_string()),
            Some("^flush.*".to_string()),
        ));
        std::mem::drop(facade.query_broker_runtime_stats(None, Some("DefaultCluster".to_string())));
        std::mem::drop(facade.query_broker_consume_stats("127.0.0.1:10911", 3_000, 0, false));

        let mut entries = std::collections::BTreeMap::new();
        entries.insert("flushDiskType".to_string(), "ASYNC_FLUSH".to_string());
        let update_request = facade
            .broker_config_update_request(Some("127.0.0.1:10911".to_string()), None, entries, true)
            .unwrap();
        std::mem::drop(facade.build_broker_config_update_plan(update_request.clone()));
        std::mem::drop(facade.apply_broker_config_update(update_request));
    }

    #[test]
    fn facade_builds_operational_requests_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");

        let cluster_list = facade.cluster_list_request(true, Some(" DefaultCluster ".to_string()));
        assert_eq!(cluster_list.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(
            cluster_list.cluster_name().map(|value| value.as_str()),
            Some("DefaultCluster")
        );

        let consumer_connection = facade
            .consumer_connection_request(" GroupA ", Some(" 127.0.0.1:10911 ".to_string()))
            .unwrap();
        assert_eq!(consumer_connection.consumer_group().as_str(), "GroupA");
        assert_eq!(consumer_connection.broker_addr(), Some("127.0.0.1:10911"));

        let progress = facade
            .consumer_progress_request(
                Some(" GroupA ".to_string()),
                Some(" TopicA ".to_string()),
                true,
                Some(" DefaultCluster ".to_string()),
            )
            .unwrap();
        assert_eq!(progress.consumer_group().unwrap().as_str(), "GroupA");
        assert!(progress.show_client_ip());

        let reset = facade
            .reset_offset_by_time_request(" GroupA ", " TopicA ", 1234)
            .unwrap();
        assert_eq!(reset.group().as_str(), "GroupA");
        assert_eq!(reset.namesrv_addr(), Some("127.0.0.1:9876"));

        let queue = facade
            .query_consume_queue_request(
                " TopicA ",
                1,
                10,
                20,
                Some(" 127.0.0.1:10911 ".to_string()),
                Some(" GroupA ".to_string()),
            )
            .unwrap();
        assert_eq!(queue.topic().as_str(), "TopicA");
        assert_eq!(queue.namesrv_addr(), Some("127.0.0.1:9876"));

        let ha = facade
            .ha_status_request(Some(" 127.0.0.1:10911 ".to_string()), None)
            .unwrap();
        assert_eq!(ha.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn facade_exposes_operational_service_futures_without_cli_types() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        std::mem::drop(facade.query_cluster_list(false, Some("DefaultCluster".to_string())));
        std::mem::drop(facade.query_cluster_broker_names(Some("DefaultCluster".to_string())));
        std::mem::drop(facade.check_cluster_send_message_rt(2, 128, Some("DefaultCluster".to_string())));
        std::mem::drop(facade.query_consumer_connection("GroupA", Some("127.0.0.1:10911".to_string())));
        std::mem::drop(facade.query_producer_connection("ProducerGroupA", "TopicA"));
        std::mem::drop(facade.query_consumer_config("GroupA"));
        std::mem::drop(facade.query_consumer_running_info(
            "GroupA",
            Some("client-a".to_string()),
            Some("127.0.0.1:10911".to_string()),
            false,
        ));
        std::mem::drop(facade.query_consumer_progress(
            Some("GroupA".to_string()),
            Some("TopicA".to_string()),
            false,
            Some("DefaultCluster".to_string()),
        ));
        std::mem::drop(facade.clone_group_offset("SourceGroup", "DestGroup", "TopicA", false));
        std::mem::drop(facade.query_consumer_status("GroupA", "TopicA", Some("client-a".to_string())));
        std::mem::drop(facade.skip_accumulated_message(
            "GroupA",
            "TopicA",
            Some("DefaultCluster".to_string()),
            Some(true),
        ));
        std::mem::drop(facade.reset_offset_by_time("GroupA", "TopicA", 1234));
        std::mem::drop(facade.reset_offset_by_time_old(
            "GroupA",
            "TopicA",
            1234,
            Some(true),
            Some("DefaultCluster".to_string()),
        ));
        std::mem::drop(facade.query_consume_queue(
            "TopicA",
            1,
            10,
            20,
            Some("127.0.0.1:10911".to_string()),
            Some("GroupA".to_string()),
        ));
        std::mem::drop(facade.check_rocksdb_cq_write_progress(
            "DefaultCluster",
            Some("TopicA".to_string()),
            Some(1000),
        ));
        std::mem::drop(facade.query_ha_status(Some("127.0.0.1:10911".to_string()), None));
        std::mem::drop(facade.query_sync_state_set("127.0.0.1:9878", Some("broker-a".to_string()), None));
        std::mem::drop(facade.query_stats_all(false, Some("TopicA".to_string())));
        std::mem::drop(facade.query_producer_info("127.0.0.1:10911"));
    }
}
