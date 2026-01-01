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

use std::ops::Deref;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::future::BoxFuture;
use futures::FutureExt;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;

const SEND_TIMEOUT: u64 = 3_000;
const DEFAULT_PULL_TIMEOUT_MILLIS: u64 = 10_000;

///### RocketMQ's EscapeBridge for Dead Letter Queue (DLQ) Mechanism
///
/// In the context of message passing within RocketMQ, the `EscapeBridge` primarily handles the Dead
/// Letter Queue (DLQ) mechanism. When messages fail to be successfully consumed by a consumer after
/// multiple attempts, these messages are designated as "dead letters"—messages that cannot be
/// processed normally. To prevent such messages from indefinitely blocking the consumer's
/// processing flow, RocketMQ provides functionality to transfer these messages to a special queue
/// known as the dead letter queue.
///
/// The `EscapeBridge` acts as a bridge in this process, responsible for moving messages that have
/// failed consumption from their original queue into the DLQ. This action helps maintain system
/// health and prevents the entire consumption process from being obstructed by a few problematic
/// messages. Additionally, it provides developers with an opportunity to analyze and address these
/// abnormal messages at a later time.
///
/// #### Functions of EscapeBridge
///
/// - **Isolation of Problematic Messages:** Moves messages that cannot be consumed into the DLQ to
///   ensure they do not continue to affect normal consumption processes.
/// - **Preservation of Message Data:** Even if a message is considered unconsumable, its content is
///   preserved, allowing for subsequent diagnosis or specialized handling.
/// - **Support for Retry Logic:** For messages that may have failed due to transient issues, retry
///   logic can be applied by requeueing or specially processing messages in the DLQ, enabling
///   another attempt at consumption.
///
/// Through this approach, RocketMQ enhances the reliability and stability of the messaging system.
/// It also equips developers with better tools for managing and troubleshooting issues in message
/// transmission.
///
/// #### Conclusion
///
/// RocketMQ's `EscapeBridge` plays a critical role in maintaining the robustness of the messaging
/// system by effectively handling messages that cannot be processed. By isolating problematic
/// messages, preserving their data, and supporting retry mechanisms, it ensures that the overall
/// consumption process remains healthy and unobstructed. Developers gain valuable insights into
/// message failures, aiding in the diagnosis and resolution of potential issues.
///
/// **Note:** The specific configuration and usage methods may vary depending on the version of
/// RocketMQ. Please refer to the official documentation for the most accurate information.
pub(crate) struct EscapeBridge<MS: MessageStore> {
    inner_producer_group_name: CheetahString,
    inner_consumer_group_name: CheetahString,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> EscapeBridge<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let inner_producer_group_name = CheetahString::from_string(format!(
            "InnerProducerGroup_{}_{}",
            broker_runtime_inner.broker_config().broker_name(),
            broker_runtime_inner.broker_config().broker_identity.broker_id
        ));
        let inner_consumer_group_name = CheetahString::from_string(format!(
            "InnerConsumerGroup_{}_{}",
            broker_runtime_inner.broker_config().broker_name(),
            broker_runtime_inner.broker_config().broker_identity.broker_id
        ));

        Self {
            inner_producer_group_name,
            inner_consumer_group_name,
            broker_runtime_inner,
        }
    }

    pub fn start(&mut self /* message_store: Option<ArcMut<MS>> */) {
        if self.broker_runtime_inner.broker_config().enable_slave_acting_master
            && self.broker_runtime_inner.broker_config().enable_remote_escape
        {

            //self.message_store = message_store;
        }
    }

    pub fn shutdown(&mut self) {
        warn!("EscapeBridge shutdown not implemented");
    }
}

impl<MS> EscapeBridge<MS>
where
    MS: MessageStore,
{
    pub async fn put_message(&mut self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        if self.broker_runtime_inner.broker_config().broker_identity.broker_id == mix_all::MASTER_ID {
            self.broker_runtime_inner
                .message_store_mut()
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_runtime_inner.broker_config().enable_slave_acting_master
            && self.broker_runtime_inner.broker_config().enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            match self.put_message_to_remote_broker(message_ext, None).await {
                Ok(send_result) => transform_send_result2put_result(send_result),
                Err(e) => {
                    error!("sendMessageInFailover to remote failed, {}", e);
                    PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true)
                }
            }
        } else {
            warn!(
                "Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                self.broker_runtime_inner.broker_config().enable_slave_acting_master,
                self.broker_runtime_inner.broker_config().enable_remote_escape
            );
            PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable)
        }
    }

    pub async fn put_message_to_remote_broker(
        &mut self,
        message_ext: MessageExtBrokerInner,
        mut broker_name_to_send: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
        let broker_name = self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_name
            .as_str();
        if broker_name.is_empty() || broker_name == broker_name_to_send.as_ref().map_or("", |value| value.as_str()) {
            // not remote broker
            return Ok(None);
        }
        let is_trans_half_message = TransactionalMessageUtil::build_half_topic() == message_ext.get_topic();
        let mut message_to_put = if is_trans_half_message {
            TransactionalMessageUtil::build_transactional_message_from_half_message(&message_ext.message_ext_inner)
        } else {
            message_ext
        };
        let topic_publish_info = self
            .broker_runtime_inner
            .topic_route_info_manager()
            .try_to_find_topic_publish_info(message_to_put.get_topic())
            .await;
        if !topic_publish_info.as_ref().is_some_and(|value| value.ok()) {
            warn!(
                "putMessageToRemoteBroker: no route info of topic {} when escaping message, msgId={}",
                message_to_put.get_topic(),
                message_to_put.message_ext_inner.msg_id
            );
            return Ok(None);
        }
        let topic_publish_info = topic_publish_info.unwrap();
        let _mq_selected = if broker_name_to_send.as_ref().is_none_or(|value| !value.is_empty()) {
            let mq = topic_publish_info
                .select_one_message_queue_by_broker(broker_name_to_send.as_ref())
                .unwrap();
            message_to_put.message_ext_inner.queue_id = mq.get_queue_id();
            broker_name_to_send = Some(mq.get_broker_name().clone());
            if self
                .broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_name
                .as_str()
                == mq.get_broker_name().as_str()
            {
                warn!(
                    "putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId: {}, Broker: {}",
                    message_to_put.get_topic(),
                    message_to_put.message_ext_inner.msg_id,
                    mq.get_broker_name()
                );
                return Ok(None);
            }
            mq
        } else {
            MessageQueue::from_parts(
                message_to_put.get_topic(),
                broker_name_to_send.clone().unwrap(),
                message_to_put.queue_id(),
            )
        };
        let broker_addr_to_send = self
            .broker_runtime_inner
            .topic_route_info_manager()
            .find_broker_address_in_publish(broker_name_to_send.as_ref());
        if broker_addr_to_send.is_none() {
            warn!(
                "putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId:  {}, Broker: {}",
                message_to_put.get_topic(),
                message_to_put.message_ext_inner.msg_id,
                broker_name_to_send.as_ref().unwrap()
            );
            return Ok(None);
        }
        let producer_group = self.get_producer_group(&message_to_put);
        let result = self
            .broker_runtime_inner
            .broker_outer_api()
            .send_message_to_specific_broker(
                broker_addr_to_send.as_ref().unwrap(),
                broker_name_to_send.as_ref().unwrap(),
                message_to_put.message_ext_inner,
                producer_group,
                SEND_TIMEOUT,
            )
            .await?;
        if result.send_status == SendStatus::SendOk {
            return Ok(Some(result));
        }
        Ok(None)
    }

    fn get_producer_group(&self, message_ext: &MessageExtBrokerInner) -> CheetahString {
        let producer_group =
            message_ext.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
        match producer_group {
            None => self.inner_producer_group_name.clone(),
            Some(value) => value,
        }
    }

    pub async fn async_put_message(&mut self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        if self.broker_runtime_inner.broker_config().broker_identity.broker_id == mix_all::MASTER_ID {
            self.broker_runtime_inner
                .message_store_mut()
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_runtime_inner.broker_config().enable_slave_acting_master
            && self.broker_runtime_inner.broker_config().enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .broker_runtime_inner
                .topic_route_info_manager()
                .try_to_find_topic_publish_info(message_ext.get_topic())
                .await;
            if topic_publish_info.is_none() {
                return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
            }
            let topic_publish_info = topic_publish_info.unwrap();
            let mq_selected = topic_publish_info.select_one_message_queue();
            if mq_selected.is_none() {
                return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
            }
            let message_queue = mq_selected.unwrap();
            message_ext.message_ext_inner.queue_id = message_queue.get_queue_id();
            let broker_name_to_send = message_queue.get_broker_name();
            let broker_addr_to_send = self
                .broker_runtime_inner
                .topic_route_info_manager()
                .find_broker_address_in_publish(Some(broker_name_to_send));
            let producer_group = self.get_producer_group(&message_ext);
            let result = self
                .broker_runtime_inner
                .broker_outer_api()
                .send_message_to_specific_broker(
                    broker_addr_to_send.as_ref().unwrap(),
                    broker_name_to_send,
                    message_ext.message_ext_inner,
                    producer_group,
                    SEND_TIMEOUT,
                )
                .await;
            transform_send_result2put_result(result.ok())
        } else {
            PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable)
        }
    }

    pub async fn put_message_to_specific_queue(&mut self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        if self.broker_runtime_inner.broker_config().broker_identity.broker_id == mix_all::MASTER_ID {
            self.broker_runtime_inner
                .message_store_mut()
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_runtime_inner.broker_config().enable_slave_acting_master
            && self.broker_runtime_inner.broker_config().enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .broker_runtime_inner
                .topic_route_info_manager()
                .try_to_find_topic_publish_info(message_ext.get_topic())
                .await;
            if topic_publish_info.is_none() {
                return PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true);
            }
            let topic_publish_info = topic_publish_info.unwrap();

            if topic_publish_info.message_queue_list.is_empty() {
                return PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true);
            }
            //let message_queue = mq_selected.unwrap();
            let id = format!(
                "{}{}",
                message_ext.get_topic(),
                message_ext.message_ext_inner.store_host
            );
            let code = JavaStringHasher::hash_str(id.as_str());
            let index = code as usize % topic_publish_info.message_queue_list.len();
            let message_queue = topic_publish_info.message_queue_list[index].clone();
            message_ext.message_ext_inner.queue_id = message_queue.get_queue_id();
            let broker_name_to_send = message_queue.get_broker_name();
            let broker_addr_to_send = self
                .broker_runtime_inner
                .topic_route_info_manager()
                .find_broker_address_in_publish(Some(broker_name_to_send));
            let producer_group = self.get_producer_group(&message_ext);
            match self
                .broker_runtime_inner
                .broker_outer_api()
                .send_message_to_specific_broker(
                    broker_addr_to_send.as_ref().unwrap(),
                    broker_name_to_send,
                    message_ext.message_ext_inner,
                    producer_group,
                    SEND_TIMEOUT,
                )
                .await
            {
                Ok(result) => transform_send_result2put_result(Some(result)),
                Err(e) => {
                    error!("sendMessageInFailover to remote failed, {}", e);
                    PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true)
                }
            }
        } else {
            warn!(
                "Put message failed, enableSlaveActingMaster={}, enableRemoteEscape={}.",
                self.broker_runtime_inner.broker_config().enable_slave_acting_master,
                self.broker_runtime_inner.broker_config().enable_remote_escape
            );
            PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable)
        }
    }

    pub fn get_message_async(
        &self,
        topic: &CheetahString,
        offset: i64,
        queue_id: i32,
        broker_name: &CheetahString,
        de_compress_body: bool,
    ) -> BoxFuture<'_, (Option<MessageExt>, String, bool)> {
        let message_store = self.broker_runtime_inner.message_store().unwrap();
        let inner_consumer_group_name = self.inner_consumer_group_name.clone();
        let topic = topic.clone();
        let broker_name = broker_name.clone();

        if self.broker_runtime_inner.broker_config().broker_identity.broker_name == broker_name {
            async move {
                let result = message_store
                    .get_message(
                        &inner_consumer_group_name,
                        &topic,
                        queue_id,
                        offset,
                        1,
                        //    128 * 1024 * 1024,
                        None,
                    )
                    .await;
                if result.is_none() {
                    warn!(
                        "getMessageResult is null, innerConsumerGroupName {}, topic {}, offset {}, queueId {}",
                        inner_consumer_group_name, topic, offset, queue_id
                    );
                    return (None, "getMessageResult is null".to_string(), false);
                }
                let result1 = result.unwrap();
                let status = result1.status();
                let mut list = decode_msg_list(result1, de_compress_body);
                if list.is_empty() {
                    let need_retry = status.unwrap() == GetMessageStatus::OffsetFoundNull;
                    //   && message_store.is_tiered_message_store();
                    warn!(
                        "Can not get msg, topic {}, offset {}, queueId {}, needRetry {},",
                        topic,
                        offset,
                        queue_id,
                        need_retry,
                        //result.unwrap()
                    );
                    return (None, "Can not get msg".to_string(), need_retry);
                }
                (Some(list.remove(0)), "".to_string(), false)
            }
            .boxed()
        } else {
            self.get_message_from_remote_async(&topic, offset, queue_id, &broker_name)
        }
    }

    fn get_message_from_remote_async(
        &self,
        topic: &CheetahString,
        offset: i64,
        queue_id: i32,
        broker_name: &CheetahString,
    ) -> BoxFuture<'_, (Option<MessageExt>, String, bool)> {
        /* let topic_route_info_manager = self.topic_route_info_manager.clone();
        let broker_outer_api = self.broker_outer_api.clone();*/
        let broker_runtime_inner_ = self.broker_runtime_inner.clone();
        let inner_consumer_group_name = self.inner_consumer_group_name.clone();
        let topic = topic.clone();
        let broker_name = broker_name.clone();

        async move {
            let mut broker_addr = broker_runtime_inner_
                .topic_route_info_manager()
                .find_broker_address_in_subscribe(Some(&broker_name), 0, false);

            if broker_addr.is_none() {
                broker_runtime_inner_
                    .topic_route_info_manager()
                    .update_topic_route_info_from_name_server_ext(&topic, true, false)
                    .await;
                broker_addr = broker_runtime_inner_
                    .topic_route_info_manager()
                    .find_broker_address_in_subscribe(Some(&broker_name), 0, false);

                if broker_addr.is_none() {
                    warn!("can't find broker address for topic {}, {}", topic, broker_name);
                    return (None, "brokerAddress not found".to_string(), true);
                }
            }

            let broker_addr = broker_addr.unwrap();
            match broker_runtime_inner_
                .broker_outer_api()
                .pull_message_from_specific_broker_async(
                    &broker_name,
                    &broker_addr,
                    &inner_consumer_group_name,
                    &topic,
                    queue_id,
                    offset,
                    1,
                    10000,
                )
                .await
            {
                Ok(pull_result) => {
                    if let Some(result) = pull_result.0 {
                        if *result.pull_status() == PullStatus::Found
                            && result.msg_found_list().as_ref().is_some_and(|value| !value.is_empty())
                        {
                            return (
                                Some(result.msg_found_list().clone().unwrap()[0].clone().deref().clone()),
                                "".to_string(),
                                false,
                            );
                        }
                    }
                }
                Err(_e) => {}
            }

            (None, "Get message from remote failed".to_string(), true)
        }
        .boxed()
    }
}

fn decode_msg_list(get_message_result: GetMessageResult, de_compress_body: bool) -> Vec<MessageExt> {
    let mut found_list = Vec::new();
    for bb in get_message_result.message_mapped_list() {
        let data = &bb.mapped_file.as_ref().unwrap().get_mapped_file()
            [bb.start_offset as usize..(bb.start_offset + bb.size as u64) as usize];
        let mut bytes = Bytes::copy_from_slice(data);
        let msg_ext = message_decoder::decode(&mut bytes, true, de_compress_body, false, false, false);
        if let Some(msg_ext) = msg_ext {
            found_list.push(msg_ext);
        }
    }
    found_list
}

#[inline]
fn transform_send_result2put_result(send_result: Option<SendResult>) -> PutMessageResult {
    match send_result {
        None => PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true),
        Some(result) => match result.send_status {
            SendStatus::SendOk => PutMessageResult::new(PutMessageStatus::PutOk, None, true),
            SendStatus::FlushDiskTimeout => PutMessageResult::new(PutMessageStatus::FlushDiskTimeout, None, true),
            SendStatus::FlushSlaveTimeout => PutMessageResult::new(PutMessageStatus::FlushSlaveTimeout, None, true),
            SendStatus::SlaveNotAvailable => PutMessageResult::new(PutMessageStatus::SlaveNotAvailable, None, true),
        },
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;

    use super::*;

    #[test]
    fn transform_send_result2put_result_handles_none() {
        let result = transform_send_result2put_result(None);
        assert_eq!(result.put_message_status(), PutMessageStatus::PutToRemoteBrokerFail);
    }

    #[test]
    fn transform_send_result2put_result_handles_send_ok() {
        let send_result = SendResult {
            send_status: SendStatus::SendOk,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(result.put_message_status(), PutMessageStatus::PutOk);
    }

    #[test]
    fn transform_send_result2put_result_handles_flush_disk_timeout() {
        let send_result = SendResult {
            send_status: SendStatus::FlushDiskTimeout,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(result.put_message_status(), PutMessageStatus::FlushDiskTimeout);
    }

    #[test]
    fn transform_send_result2put_result_handles_flush_slave_timeout() {
        let send_result = SendResult {
            send_status: SendStatus::FlushSlaveTimeout,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(result.put_message_status(), PutMessageStatus::FlushSlaveTimeout);
    }

    #[test]
    fn transform_send_result2put_result_handles_slave_not_available() {
        let send_result = SendResult {
            send_status: SendStatus::SlaveNotAvailable,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(result.put_message_status(), PutMessageStatus::SlaveNotAvailable);
    }
}
