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

//! Generation-coherent management reads.
//!
//! These queries read the cluster, broker, topic-queue, and live source tables.
//! They never mutate a table or publish topic snapshots. Each method holds the
//! route mutation coordinator only while cloning its response DTO; encoding and
//! network I/O must remain outside the guard.

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::TopicSysFlag;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::topic::topic_list::TopicList;

use super::RouteInfoManager;
use crate::route::error::RocketMQError;
use crate::route::error::RouteResult;
use crate::route::types::TopicName;

impl RouteInfoManager {
    /// Gets all topics registered in this NameServer generation.
    pub fn get_all_topics(&self) -> Vec<TopicName> {
        let _management_view = self.route_mutations.begin_management_read();
        self.topic_queue_table.get_all_topics()
    }

    /// Gets topics associated with brokers in `cluster_name`.
    pub fn get_topics_by_cluster(&self, cluster_name: &str) -> RouteResult<Vec<TopicName>> {
        let _management_view = self.route_mutations.begin_management_read();
        let broker_names = self.cluster_addr_table.get_brokers(cluster_name);

        if broker_names.is_empty() {
            return Err(RocketMQError::cluster_not_found(cluster_name));
        }

        let broker_names = broker_names.into_iter().collect::<HashSet<_>>();
        Ok(self.topic_queue_table.topics_for_brokers_with_duplicates(&broker_names))
    }

    /// Gets the member addresses for a broker group.
    pub fn get_broker_member_group(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> Option<BrokerMemberGroup> {
        let _management_view = self.route_mutations.begin_management_read();
        let mut group_member = BrokerMemberGroup::new(cluster_name, broker_name.clone());

        if let Some(broker_data) = self.broker_addr_table.get(&broker_name) {
            group_member.broker_addrs = broker_data.broker_addrs().clone();
        }

        Some(group_member)
    }

    /// Gets cluster and broker tables from the same route generation.
    pub fn get_all_cluster_info(&self) -> ClusterInfo {
        let _management_view = self.route_mutations.begin_management_read();
        ClusterInfo {
            broker_addr_table: Some(self.broker_addr_table.snapshot()),
            cluster_addr_table: Some(self.cluster_addr_table.snapshot()),
        }
    }

    /// Gets the Java-compatible system topic list.
    pub fn get_system_topic_list(&self) -> TopicList {
        let _management_view = self.route_mutations.begin_management_read();
        let mut topic_list =
            Vec::with_capacity(self.cluster_addr_table.cluster_count() + self.cluster_addr_table.total_broker_count());
        self.cluster_addr_table.append_cluster_and_broker_names(&mut topic_list);

        TopicList {
            topic_list,
            broker_addr: self.broker_addr_table.first_broker_addr(),
        }
    }

    /// Gets topics marked with the unit flag.
    pub fn get_unit_topics(&self) -> TopicList {
        let _management_view = self.route_mutations.begin_management_read();
        let topics = self
            .topic_queue_table
            .filter_topics_by_first_queue(|queue_data| TopicSysFlag::has_unit_flag(queue_data.topic_sys_flag()));

        TopicList {
            topic_list: topics,
            broker_addr: None,
        }
    }

    /// Gets topics marked with unit-subscription semantics.
    pub fn get_has_unit_sub_topic_list(&self) -> TopicList {
        let _management_view = self.route_mutations.begin_management_read();
        let topics = self
            .topic_queue_table
            .filter_topics_by_first_queue(|queue_data| TopicSysFlag::has_unit_sub_flag(queue_data.topic_sys_flag()));

        TopicList {
            topic_list: topics,
            broker_addr: None,
        }
    }

    /// Gets non-unit topics that still have unit-subscription semantics.
    pub fn get_has_unit_sub_ununit_topic_list(&self) -> TopicList {
        let _management_view = self.route_mutations.begin_management_read();
        let topics = self.topic_queue_table.filter_topics_by_first_queue(|queue_data| {
            let sys_flag = queue_data.topic_sys_flag();
            !TopicSysFlag::has_unit_flag(sys_flag) && TopicSysFlag::has_unit_sub_flag(sys_flag)
        });

        TopicList {
            topic_list: topics,
            broker_addr: None,
        }
    }
}
