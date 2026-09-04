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

use std::net::SocketAddr;

use rand::RngExt;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::BrokerReadWriteStore;
use tracing::error;
use tracing::warn;

use super::build_notification_filter_contract;
use super::NotificationFilterContract;
use super::NotificationProcessor;

pub(super) struct NotificationCoreReady {
    pub(super) has_msg: bool,
    pub(super) filter_contract: Option<NotificationFilterContract>,
}

pub(super) enum NotificationCoreOutcome {
    Reply(RemotingCommand),
    Ready(NotificationCoreReady),
}

impl<MS> NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    /// Executes validation and the one real store read without a Channel/context.
    pub(super) async fn execute_notification_core(
        &self,
        request_header: &NotificationRequestHeader,
        effective_peer: SocketAddr,
        opaque: i32,
        frozen_filter: Option<NotificationFilterContract>,
    ) -> NotificationCoreOutcome {
        let mut response = self
            .context
            .command_factory
            .create_java_default_error_response_command();
        response.set_opaque_mut(opaque);

        if !PermName::is_readable(self.context.policy.broker_permission.get()) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "the broker[{}] peeking message is forbidden",
                self.context.policy.broker_ip1
            ));
            return NotificationCoreOutcome::Reply(response);
        }

        let Some(topic_config) = self
            .context
            .topic_config_manager
            .select_topic_config(&request_header.topic)
        else {
            error!(
                "The topic {} not exist, consumer: {}",
                request_header.topic, effective_peer
            );
            response.set_code_ref(ResponseCode::TopicNotExist);
            response.set_remark_mut(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            ));
            return NotificationCoreOutcome::Reply(response);
        };

        if !PermName::is_readable(topic_config.perm) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "the topic[{}] peeking message is forbidden",
                request_header.topic
            ));
            return NotificationCoreOutcome::Reply(response);
        }

        if request_header.queue_id >= topic_config.get_read_queue_nums() as i32 {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.get_read_queue_nums(),
                effective_peer
            );
            warn!("{}", error_info);
            response.set_code_ref(ResponseCode::InvalidParameter);
            response.set_remark_mut(&error_info);
            return NotificationCoreOutcome::Reply(response);
        }

        let Some(subscription_group_config) = self
            .context
            .subscription_group_lookup
            .find_subscription_group_config(&request_header.consumer_group)
        else {
            response.set_code_ref(ResponseCode::SubscriptionGroupNotExist);
            response.set_remark_mut(format!(
                "subscription group [{}] does not exist, {}",
                request_header.consumer_group,
                FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
            ));
            return NotificationCoreOutcome::Reply(response);
        };

        if !subscription_group_config.consume_enable() {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "subscription group no permission, {}",
                request_header.consumer_group
            ));
            return NotificationCoreOutcome::Reply(response);
        }

        let filter_contract = match frozen_filter {
            Some(contract) => Some(contract),
            None => match build_notification_filter_contract(
                self.context.policy.use_message_filter_for_notification,
                &self.context.consumer_filter_manager,
                request_header,
            ) {
                Ok(contract) => contract,
                Err(()) => {
                    warn!(
                        "Parse the consumer's subscription[{:?}] failed, group: {}",
                        request_header.exp, request_header.consumer_group
                    );
                    response.set_code_ref(ResponseCode::SubscriptionParseFailed);
                    response.set_remark_mut("parse the consumer's subscription failed");
                    return NotificationCoreOutcome::Reply(response);
                }
            },
        };

        let random_q: i32 = rand::rng().random_range(0..100);
        let need_retry = random_q % 5 == 0;
        let retry_policy = self.context.retry_policies.retry_policy(&request_header.consumer_group);
        let mut has_msg = false;
        if need_retry {
            for retry_topic in
                retry_policy.read_topics(request_header.topic.as_str(), request_header.consumer_group.as_str())
            {
                has_msg = self
                    .has_msg_from_topic_name(&retry_topic.into(), random_q, request_header, None)
                    .await;
                if has_msg {
                    break;
                }
            }
        }
        if !has_msg {
            if request_header.queue_id < 0 {
                has_msg = self
                    .has_msg_from_topic(Some(&topic_config), random_q, request_header, filter_contract.as_ref())
                    .await;
            } else if let Some(topic_name) = topic_config.topic_name.as_ref() {
                has_msg = self
                    .has_msg_from_queue(
                        topic_name,
                        request_header,
                        request_header.queue_id,
                        filter_contract.as_ref(),
                    )
                    .await;
            }
            if !need_retry && !has_msg {
                for retry_topic in
                    retry_policy.read_topics(request_header.topic.as_str(), request_header.consumer_group.as_str())
                {
                    has_msg = self
                        .has_msg_from_topic_name(&retry_topic.into(), random_q, request_header, None)
                        .await;
                    if has_msg {
                        break;
                    }
                }
            }
        }

        NotificationCoreOutcome::Ready(NotificationCoreReady {
            has_msg,
            filter_contract,
        })
    }
}
