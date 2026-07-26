// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Narrow administration mutation surface.
//!
//! Method names are intentionally distinct from the legacy mixed
//! [`super::mq_admin_ext_async::MQAdminExt`] surface. This keeps wildcard
//! imports source-compatible when `admin-full` is enabled while allowing
//! mutation-only consumers to depend on an explicit capability trait.

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Explicit RocketMQ mutation capability.
///
/// The legacy mixed administration API remains available only through
/// `admin-full`. New integrations should request this trait deliberately and
/// keep it out of read-only process dependency graphs.
#[allow(async_fn_in_trait)]
pub trait MQAdminMutationExt: Send {
    async fn upsert_topic_config(
        &self,
        broker_addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn remove_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn reset_consumer_offset(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>>;

    async fn upsert_subscription_group(
        &self,
        broker_addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn remove_subscription_group(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn configure_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()>;

    async fn consume_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult>;

    async fn clone_consumer_group_offset(
        &self,
        source_group: CheetahString,
        destination_group: CheetahString,
        topic: CheetahString,
        offline: bool,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Returns the cluster topology needed to resolve mutation targets.
    async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo>;

    /// Returns route data used to scope a mutation to the topic's brokers.
    async fn mutation_topic_route(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>>;

    /// Reads a broker's current topic configuration before a bounded test send.
    async fn mutation_topic_config(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig>;

    /// Removes a topic from an explicitly resolved broker set.
    async fn remove_topic_from_brokers(
        &self,
        broker_addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Removes a topic from an explicitly resolved NameServer set.
    async fn remove_topic_from_name_servers(
        &self,
        namesrv_addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Returns the NameServer targets owned by this started admin session.
    async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>>;

    /// Updates the order-topic configuration associated with a topic mutation.
    async fn upsert_order_topic_config(
        &self,
        topic: CheetahString,
        value: CheetahString,
        cluster_wide: bool,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Performs the broker-by-broker offset reset fallback used for offline consumers.
    async fn reset_consumer_offset_legacy(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>>;

    /// Views the message metadata required to validate a DLQ resend.
    async fn view_message_for_mutation(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt>;
}

impl MQAdminMutationExt for DefaultMQAdminExt {
    async fn upsert_topic_config(
        &self,
        broker_addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_topic_config(self.inner(), broker_addr, config).await
    }

    async fn remove_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic(self.inner(), topic_name, cluster_name).await
    }

    async fn reset_consumer_offset(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        MQAdminMutationExt::reset_consumer_offset(self.inner(), cluster_name, topic, consumer_group, timestamp, force)
            .await
    }

    async fn upsert_subscription_group(
        &self,
        broker_addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_subscription_group(self.inner(), broker_addr, config).await
    }

    async fn remove_subscription_group(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_subscription_group(self.inner(), broker_addr, group_name, remove_offset).await
    }

    async fn configure_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::configure_message_request_mode(
            self.inner(),
            broker_addr,
            topic,
            consumer_group,
            mode,
            pop_work_group_size,
            timeout_millis,
        )
        .await
    }

    async fn consume_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        MQAdminMutationExt::consume_directly(self.inner(), consumer_group, client_id, topic, message_id).await
    }

    async fn clone_consumer_group_offset(
        &self,
        source_group: CheetahString,
        destination_group: CheetahString,
        topic: CheetahString,
        offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::clone_consumer_group_offset(self.inner(), source_group, destination_group, topic, offline)
            .await
    }

    async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        MQAdminMutationExt::mutation_cluster_info(self.inner()).await
    }

    async fn mutation_topic_route(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        MQAdminMutationExt::mutation_topic_route(self.inner(), topic).await
    }

    async fn mutation_topic_config(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        MQAdminMutationExt::mutation_topic_config(self.inner(), broker_addr, topic).await
    }

    async fn remove_topic_from_brokers(
        &self,
        broker_addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic_from_brokers(self.inner(), broker_addrs, topic).await
    }

    async fn remove_topic_from_name_servers(
        &self,
        namesrv_addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::remove_topic_from_name_servers(self.inner(), namesrv_addrs, cluster_name, topic).await
    }

    async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
        MQAdminMutationExt::mutation_name_server_addresses(self.inner()).await
    }

    async fn upsert_order_topic_config(
        &self,
        topic: CheetahString,
        value: CheetahString,
        cluster_wide: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        MQAdminMutationExt::upsert_order_topic_config(self.inner(), topic, value, cluster_wide).await
    }

    async fn reset_consumer_offset_legacy(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        MQAdminMutationExt::reset_consumer_offset_legacy(
            self.inner(),
            cluster_name,
            consumer_group,
            topic,
            timestamp,
            force,
        )
        .await
    }

    async fn view_message_for_mutation(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        MQAdminMutationExt::view_message_for_mutation(self.inner(), topic, message_id).await
    }
}
