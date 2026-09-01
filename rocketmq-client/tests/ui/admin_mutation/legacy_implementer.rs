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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::BrokerConfigPatchOutcome;
use rocketmq_client_rust::MQAdminMutationExt;
use rocketmq_client_rust::SubscriptionGroupConfigPatch;
use rocketmq_client_rust::SubscriptionGroupConfigPatchOutcome;
use rocketmq_client_rust::TopicConfigPatch;
use rocketmq_client_rust::TopicConfigPatchOutcome;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

struct LegacyAdmin;

fn unsupported<T>() -> RocketMQResult<T> {
    Err(rocketmq_error::RocketMQError::illegal_argument("fixture"))
}

impl MQAdminMutationExt for LegacyAdmin {
    async fn begin_proxy_drain(
        &self,
        _proxy_addr: CheetahString,
        _operation_id: CheetahString,
    ) -> RocketMQResult<ProxyDrainStateResponseBody> {
        unsupported()
    }

    async fn cancel_proxy_drain(
        &self,
        _proxy_addr: CheetahString,
        _operation_id: CheetahString,
    ) -> RocketMQResult<ProxyDrainStateResponseBody> {
        unsupported()
    }

    async fn broker_config_generation(&self, _broker_addr: CheetahString) -> RocketMQResult<u64> {
        unsupported()
    }

    async fn patch_broker_config_if_generation(
        &self,
        _broker_addr: CheetahString,
        _expected_generation: u64,
        _properties: HashMap<CheetahString, CheetahString>,
    ) -> RocketMQResult<BrokerConfigPatchOutcome> {
        unsupported()
    }

    async fn patch_topic_config_if_version(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
        _expected_version: u64,
        _patch: TopicConfigPatch,
    ) -> RocketMQResult<TopicConfigPatchOutcome> {
        unsupported()
    }

    async fn patch_subscription_group_config_if_version(
        &self,
        _broker_addr: CheetahString,
        _group: CheetahString,
        _expected_version: u64,
        _patch: SubscriptionGroupConfigPatch,
    ) -> RocketMQResult<SubscriptionGroupConfigPatchOutcome> {
        unsupported()
    }

    async fn upsert_topic_config(&self, _broker_addr: CheetahString, _config: TopicConfig) -> RocketMQResult<()> {
        unsupported()
    }

    async fn remove_topic(&self, _topic_name: CheetahString, _cluster_name: CheetahString) -> RocketMQResult<()> {
        unsupported()
    }

    async fn reset_consumer_offset(
        &self,
        _cluster_name: Option<CheetahString>,
        _topic: CheetahString,
        _consumer_group: CheetahString,
        _timestamp: u64,
        _force: bool,
    ) -> RocketMQResult<HashMap<MessageQueue, u64>> {
        unsupported()
    }

    async fn upsert_subscription_group(
        &self,
        _broker_addr: CheetahString,
        _config: SubscriptionGroupConfig,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn remove_subscription_group(
        &self,
        _broker_addr: CheetahString,
        _group_name: CheetahString,
        _remove_offset: Option<bool>,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn remove_subscription_groups(
        &self,
        _broker_addr: CheetahString,
        _group_names: Vec<CheetahString>,
        _clean_offset: bool,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn configure_message_request_mode(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
        _consumer_group: CheetahString,
        _mode: MessageRequestMode,
        _pop_work_group_size: i32,
        _timeout_millis: u64,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn consume_directly(
        &self,
        _consumer_group: CheetahString,
        _client_id: CheetahString,
        _topic: CheetahString,
        _message_id: CheetahString,
    ) -> RocketMQResult<ConsumeMessageDirectlyResult> {
        unsupported()
    }

    async fn clone_consumer_group_offset(
        &self,
        _source_group: CheetahString,
        _destination_group: CheetahString,
        _topic: CheetahString,
        _offline: bool,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn mutation_cluster_info(&self) -> RocketMQResult<ClusterInfo> {
        unsupported()
    }

    async fn mutation_topic_route(&self, _topic: CheetahString) -> RocketMQResult<Option<TopicRouteData>> {
        unsupported()
    }

    async fn mutation_topic_config(
        &self,
        _broker_addr: CheetahString,
        _topic: CheetahString,
    ) -> RocketMQResult<TopicConfig> {
        unsupported()
    }

    async fn remove_topic_from_brokers(
        &self,
        _broker_addrs: HashSet<CheetahString>,
        _topic: CheetahString,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn remove_topics_from_broker(
        &self,
        _broker_addr: CheetahString,
        _topics: Vec<CheetahString>,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn remove_topic_from_name_servers(
        &self,
        _namesrv_addrs: HashSet<CheetahString>,
        _cluster_name: Option<CheetahString>,
        _topic: CheetahString,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn mutation_name_server_addresses(&self) -> RocketMQResult<Vec<CheetahString>> {
        unsupported()
    }

    async fn upsert_order_topic_config(
        &self,
        _topic: CheetahString,
        _value: CheetahString,
        _cluster_wide: bool,
    ) -> RocketMQResult<()> {
        unsupported()
    }

    async fn reset_consumer_offset_legacy(
        &self,
        _cluster_name: Option<CheetahString>,
        _consumer_group: CheetahString,
        _topic: CheetahString,
        _timestamp: u64,
        _force: bool,
    ) -> RocketMQResult<Vec<RollbackStats>> {
        unsupported()
    }

    async fn view_message_for_mutation(
        &self,
        _topic: CheetahString,
        _message_id: CheetahString,
    ) -> RocketMQResult<MessageExt> {
        unsupported()
    }
}

fn main() {
    fn accepts_legacy<T: MQAdminMutationExt>(_value: T) {}
    accepts_legacy(LegacyAdmin);
}
