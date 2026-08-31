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

//! Exact-Broker, read-only Topic statistics capability.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Additive capability for reading Topic statistics from one already resolved
/// Broker target. Address resolution remains the responsibility of a trusted
/// adapter and addresses never cross this trait's result boundary.
#[allow(async_fn_in_trait)]
pub trait MQAdminTopicStatsReadExt: Send {
    /// Reads the Topic statistics exposed by one exact Broker.
    async fn topic_stats_at(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable>;
}

impl MQAdminTopicStatsReadExt for DefaultMQAdminExt {
    async fn topic_stats_at(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        self.inner()
            .mq_client_api()?
            .get_topic_stats_info(
                &broker_addr,
                GetTopicStatsInfoRequestHeader {
                    topic,
                    topic_request_header: None,
                },
                self.inner().remoting_timeout_millis()?,
            )
            .await
    }
}
