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

use std::any::Any;

use cheetah_string::CheetahString;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::net::broker_to_client::Broker2Client;
use crate::transaction::transactional_message_check_listener::TransactionalMessageCheckListener;

const TCMT_QUEUE_NUMS: i32 = 1;

pub struct DefaultTransactionalMessageCheckListener<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    broker_client: ArcMut<Broker2Client>,
}

impl<MS: MessageStore> Clone for DefaultTransactionalMessageCheckListener<MS> {
    fn clone(&self) -> Self {
        Self {
            broker_client: self.broker_client.clone(),
            broker_runtime_inner: self.broker_runtime_inner.clone(),
        }
    }
}

impl<MS: MessageStore> DefaultTransactionalMessageCheckListener<MS> {
    pub fn new(broker_client: Broker2Client, broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            broker_client: ArcMut::new(broker_client),
        }
    }
}

impl<MS> TransactionalMessageCheckListener for DefaultTransactionalMessageCheckListener<MS>
where
    MS: MessageStore,
{
    async fn resolve_discard_msg(&mut self, msg_ext: MessageExt) {
        error!(
            "MsgExt:{} has been checked too many times, so discard it by moving it to system topic \
             TRANS_CHECK_MAXTIME_TOPIC",
            msg_ext
        );

        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager_mut()
            .create_topic_of_tran_check_max_time(TCMT_QUEUE_NUMS, PermName::PERM_READ | PermName::PERM_WRITE)
            .await
            .expect("Create topic of tran check max time failed");
        let broker_inner = to_message_ext_broker_inner(&topic_config, &msg_ext);
        let put_message_result = self
            .broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .unwrap()
            .put_message(broker_inner)
            .await;

        if put_message_result.put_message_status() == PutMessageStatus::PutOk {
            info!(
                "Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, \
                 commitLogOffset={}, real topic={:?}",
                msg_ext.queue_offset,
                msg_ext.commit_log_offset,
                msg_ext.get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC,)),
            );
        } else {
            error!(
                "Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}",
                msg_ext.get_topic(),
                msg_ext.msg_id(),
            );
        }
    }

    async fn send_check_message(&self, mut msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let msg_id = msg_ext.get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ));
        let header = CheckTransactionStateRequestHeader {
            topic: Some(msg_ext.message.topic.clone()),
            commit_log_offset: msg_ext.commit_log_offset,
            offset_msg_id: Some(msg_ext.msg_id().clone()),
            msg_id: msg_id.clone(),
            transaction_id: msg_id,
            tran_state_table_offset: msg_ext.queue_offset,
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(self.broker_runtime_inner.broker_config().broker_name().clone()),
                ..Default::default()
            }),
        };
        let topic = msg_ext.get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
        if let Some(topic) = topic {
            msg_ext.set_topic(topic);
        }
        let queue_id = msg_ext.get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID));
        if let Some(queue_id) = queue_id {
            msg_ext.set_queue_id(queue_id.as_str().parse::<i32>().unwrap_or_default());
        }
        msg_ext.store_size = 0;
        let group_id =
            msg_ext.get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
        let channel = self
            .broker_runtime_inner
            .producer_manager()
            .get_available_channel(group_id.as_ref());
        if let Some(mut channel) = channel {
            self.broker_client
                .check_producer_transaction_state(group_id.as_ref().unwrap(), &mut channel, header, msg_ext)
                .await;
        } else {
            warn!("Check transaction failed, channel is null. groupId={:?}", group_id);
        }
        Ok(())
    }

    async fn resolve_half_msg(&self, msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let this = self.clone();
        tokio::spawn(async move {
            this.send_check_message(msg_ext).await.unwrap_or_else(|e| {
                error!("Failed to send check message: {}", e);
            });
        });
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

fn to_message_ext_broker_inner(topic_config: &TopicConfig, msg_ext: &MessageExt) -> MessageExtBrokerInner {
    let mut inner = MessageExtBrokerInner::default();
    if let Some(topic_name) = &topic_config.topic_name {
        inner.set_topic(topic_name.clone());
    }
    if let Some(body) = msg_ext.get_body() {
        inner.set_body(body.clone());
    }
    inner.set_flag(msg_ext.get_flag());
    MessageAccessor::set_properties(&mut inner, msg_ext.get_properties().clone());
    inner.properties_string = MessageDecoder::message_properties_to_string(msg_ext.get_properties());
    inner.tags_code = MessageExtBrokerInner::tags_string_to_tags_code(msg_ext.get_tags().unwrap_or_default().as_str());
    inner.message_ext_inner.queue_id = 0;
    inner.message_ext_inner.sys_flag = msg_ext.sys_flag();
    inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
    inner.message_ext_inner.born_host = msg_ext.born_host;
    inner.message_ext_inner.store_timestamp = msg_ext.store_timestamp;
    inner.message_ext_inner.store_host = msg_ext.store_host;
    inner.message_ext_inner.msg_id = msg_ext.msg_id().clone();
    inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times();
    inner.set_wait_store_msg_ok(false);

    inner
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_message_ext_broker_inner_with_valid_message() {
        let topic_config = TopicConfig {
            topic_name: Some("test_topic".into()),
            ..Default::default()
        };
        let msg_ext = MessageExt::default();
        let result = to_message_ext_broker_inner(&topic_config, &msg_ext);
        assert_eq!(result.get_topic(), "test_topic");
        assert_eq!(result.get_body(), msg_ext.get_body());
        assert_eq!(result.get_flag(), msg_ext.get_flag());
        assert_eq!(
            result.properties_string,
            MessageDecoder::message_properties_to_string(msg_ext.get_properties())
        );
        assert_eq!(
            result.tags_code,
            MessageExtBrokerInner::tags_string_to_tags_code(msg_ext.get_tags().unwrap_or_default().as_str())
        );
        assert_eq!(result.message_ext_inner.queue_id, result.message_ext_inner.queue_id);
        assert_eq!(result.message_ext_inner.sys_flag, msg_ext.sys_flag());
        assert_eq!(result.message_ext_inner.born_timestamp, msg_ext.born_timestamp);
        assert_eq!(result.message_ext_inner.born_host, msg_ext.born_host);
        assert_eq!(result.message_ext_inner.store_timestamp, msg_ext.store_timestamp);
        assert_eq!(result.message_ext_inner.store_host, msg_ext.store_host);
        assert_eq!(result.message_ext_inner.msg_id, msg_ext.msg_id().clone());
        assert_eq!(result.message_ext_inner.reconsume_times, msg_ext.reconsume_times());
        assert!(!result.is_wait_store_msg_ok());
    }

    #[test]
    fn to_message_ext_broker_inner_with_empty_body() {
        let topic_config = TopicConfig {
            topic_name: Some("test_topic".into()),
            ..Default::default()
        };
        let msg_ext = MessageExt::default();
        let result = to_message_ext_broker_inner(&topic_config, &msg_ext);
        assert!(result.get_body().is_none());
    }

    #[test]
    fn to_message_ext_broker_inner_with_missing_topic_name() {
        let topic_config = TopicConfig {
            topic_name: None,
            ..Default::default()
        };
        let msg_ext = MessageExt::default();
        let result = to_message_ext_broker_inner(&topic_config, &msg_ext);
        assert!(result.get_topic().is_empty());
    }
}
