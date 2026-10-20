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

//! Exact-Broker, read-only Topic subscriber capability.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::group_list::GroupList;
use rocketmq_protocol::protocol::header::query_topic_consume_by_who_request_header::QueryTopicConsumeByWhoRequestHeader;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// Additive capability for reading which Consumer Groups one already resolved
/// Broker records as consumers of a Topic. Address resolution remains the
/// responsibility of a trusted adapter and addresses never cross this trait's
/// result boundary.
///
/// `MQAdminReadExt::query_topic_consume_by_who` asks the first master of the
/// Topic route only. This capability lets an adapter ask every Broker it
/// selected and keep the failure of each one apart.
#[allow(async_fn_in_trait)]
pub trait MQAdminTopicSubscriberReadExt: Send {
    /// Reads the Consumer Groups that one exact Broker records for a Topic.
    async fn topic_consumer_groups_at(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GroupList>;
}

impl MQAdminTopicSubscriberReadExt for DefaultMQAdminExt {
    async fn topic_consumer_groups_at(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> crate::ClientResult<GroupList> {
        self.inner()
            .mq_client_api()?
            .query_topic_consume_by_who(
                &broker_addr,
                QueryTopicConsumeByWhoRequestHeader {
                    topic,
                    topic_request_header: None,
                },
                self.inner().remoting_timeout_millis()?,
            )
            .await
    }
}
