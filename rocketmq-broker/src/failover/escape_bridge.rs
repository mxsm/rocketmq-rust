/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::log_file::MessageStore;
use tracing::warn;

use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;
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
pub(crate) struct EscapeBridge<MS> {
    inner_producer_group_name: CheetahString,
    inner_consumer_group_name: CheetahString,
    escape_bridge_runtime: Option<RocketMQRuntime>,
    message_store: Option<ArcMut<MS>>,
    broker_config: Arc<BrokerConfig>,
    topic_route_info_manager: Arc<TopicRouteInfoManager>,
    broker_outer_api: Arc<BrokerOuterAPI>,
}

impl<MS> EscapeBridge<MS> {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        topic_route_info_manager: Arc<TopicRouteInfoManager>,
        broker_outer_api: Arc<BrokerOuterAPI>,
    ) -> Self {
        let inner_producer_group_name = CheetahString::from_string(format!(
            "InnerProducerGroup_{}_{}",
            broker_config.broker_name, broker_config.broker_identity.broker_id
        ));
        let inner_consumer_group_name = CheetahString::from_string(format!(
            "InnerConsumerGroup_{}_{}",
            broker_config.broker_name, broker_config.broker_identity.broker_id
        ));

        Self {
            inner_producer_group_name,
            inner_consumer_group_name,
            escape_bridge_runtime: None,
            message_store: None,
            broker_config,
            topic_route_info_manager,
            broker_outer_api,
        }
    }

    pub fn start(&mut self, message_store: Option<ArcMut<MS>>) {
        if self.broker_config.enable_slave_acting_master && self.broker_config.enable_remote_escape
        {
            self.escape_bridge_runtime = Some(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "AsyncEscapeBridgeExecutor",
            ));
            self.message_store = message_store;
        }
    }
}

impl<MS> EscapeBridge<MS>
where
    MS: MessageStore,
{
    pub async fn put_message(
        &mut self,
        mut message_ext: MessageExtBrokerInner,
    ) -> PutMessageResult {
        if self.broker_config.broker_identity.broker_id == mix_all::MASTER_ID {
            self.message_store
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_config.enable_slave_acting_master
            && self.broker_config.enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            let send_result = self.put_message_to_remote_broker(message_ext, None).await;
            transform_send_result2put_result(send_result)
        } else {
            PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable)
        }
    }

    pub async fn put_message_to_remote_broker(
        &mut self,
        message_ext: MessageExtBrokerInner,
        mut broker_name_to_send: Option<CheetahString>,
    ) -> Option<SendResult> {
        if broker_name_to_send.is_some()
            && self.broker_config.broker_identity.broker_name.as_str()
                == broker_name_to_send.as_ref().unwrap().as_str()
        {
            return None;
        }
        let is_trans_half_message =
            TransactionalMessageUtil::build_half_topic() == message_ext.get_topic();
        let mut message_to_put = if is_trans_half_message {
            TransactionalMessageUtil::build_transactional_message_from_half_message(
                &message_ext.message_ext_inner,
            )
        } else {
            message_ext
        };
        let topic_publish_info = self
            .topic_route_info_manager
            .try_to_find_topic_publish_info(message_to_put.get_topic())
            .await;
        if !topic_publish_info.as_ref().is_some_and(|value| value.ok()) {
            warn!(
                "putMessageToRemoteBroker: no route info of topic {} when escaping message, \
                 msgId={}",
                message_to_put.get_topic(),
                message_to_put.message_ext_inner.msg_id
            );
            return None;
        }
        let topic_publish_info = topic_publish_info.unwrap();
        let _mq_selected = if !broker_name_to_send
            .as_ref()
            .is_some_and(|value| !value.is_empty())
        {
            let mq = topic_publish_info
                .select_one_message_queue_by_broker(broker_name_to_send.as_ref())
                .unwrap();
            message_to_put.message_ext_inner.queue_id = mq.get_queue_id();
            broker_name_to_send = Some(mq.get_broker_name().clone());
            if self.broker_config.broker_identity.broker_name.as_str()
                == mq.get_broker_name().as_str()
            {
                warn!(
                    "putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId: \
                     {}, Broker: {}",
                    message_to_put.get_topic(),
                    message_to_put.message_ext_inner.msg_id,
                    mq.get_broker_name()
                );
                return None;
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
            .topic_route_info_manager
            .find_broker_address_in_publish(broker_name_to_send.as_ref());
        if broker_addr_to_send.is_none() {
            warn!(
                "putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId:  {}, \
                 Broker: {}",
                message_to_put.get_topic(),
                message_to_put.message_ext_inner.msg_id,
                broker_name_to_send.as_ref().unwrap()
            );
            return None;
        }
        let producer_group = self.get_producer_group(&message_to_put);
        let result = self
            .broker_outer_api
            .send_message_to_specific_broker(
                broker_addr_to_send.as_ref().unwrap(),
                broker_name_to_send.as_ref().unwrap(),
                message_to_put.message_ext_inner,
                producer_group,
                SEND_TIMEOUT,
            )
            .await;
        match result {
            Ok(value) => {
                if value.send_status == SendStatus::SendOk {
                    Some(value)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    fn get_producer_group(&self, message_ext: &MessageExtBrokerInner) -> CheetahString {
        let producer_group = message_ext.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_PRODUCER_GROUP,
        ));
        match producer_group {
            None => self.inner_producer_group_name.clone(),
            Some(value) => value,
        }
    }

    pub async fn async_put_message(
        &mut self,
        mut message_ext: MessageExtBrokerInner,
    ) -> PutMessageResult {
        if self.broker_config.broker_identity.broker_id == mix_all::MASTER_ID {
            self.message_store
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_config.enable_slave_acting_master
            && self.broker_config.enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .topic_route_info_manager
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
                .topic_route_info_manager
                .find_broker_address_in_publish(Some(broker_name_to_send));
            let producer_group = self.get_producer_group(&message_ext);
            let result = self
                .broker_outer_api
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

    pub async fn put_message_to_specific_queue(
        &mut self,
        mut message_ext: MessageExtBrokerInner,
    ) -> PutMessageResult {
        if self.broker_config.broker_identity.broker_id == mix_all::MASTER_ID {
            self.message_store
                .as_mut()
                .unwrap()
                .put_message(message_ext)
                .await
        } else if self.broker_config.enable_slave_acting_master
            && self.broker_config.enable_remote_escape
        {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .topic_route_info_manager
                .try_to_find_topic_publish_info(message_ext.get_topic())
                .await;
            if topic_publish_info.is_none() {
                return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
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
            let code = JavaStringHasher::new().hash_str(id.as_str());
            let index = code as usize % topic_publish_info.message_queue_list.len();
            let message_queue = topic_publish_info.message_queue_list[index].clone();
            message_ext.message_ext_inner.queue_id = message_queue.get_queue_id();
            let broker_name_to_send = message_queue.get_broker_name();
            let broker_addr_to_send = self
                .topic_route_info_manager
                .find_broker_address_in_publish(Some(broker_name_to_send));
            let producer_group = self.get_producer_group(&message_ext);
            let result = self
                .broker_outer_api
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
}

#[inline]
fn transform_send_result2put_result(send_result: Option<SendResult>) -> PutMessageResult {
    match send_result {
        None => PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true),
        Some(result) => match result.send_status {
            SendStatus::SendOk => PutMessageResult::new(PutMessageStatus::PutOk, None, true),
            SendStatus::FlushDiskTimeout => {
                PutMessageResult::new(PutMessageStatus::FlushDiskTimeout, None, true)
            }
            SendStatus::FlushSlaveTimeout => {
                PutMessageResult::new(PutMessageStatus::FlushSlaveTimeout, None, true)
            }
            SendStatus::SlaveNotAvailable => {
                PutMessageResult::new(PutMessageStatus::SlaveNotAvailable, None, true)
            }
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
        assert_eq!(
            result.put_message_status(),
            PutMessageStatus::PutToRemoteBrokerFail
        );
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
        assert_eq!(
            result.put_message_status(),
            PutMessageStatus::FlushDiskTimeout
        );
    }

    #[test]
    fn transform_send_result2put_result_handles_flush_slave_timeout() {
        let send_result = SendResult {
            send_status: SendStatus::FlushSlaveTimeout,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(
            result.put_message_status(),
            PutMessageStatus::FlushSlaveTimeout
        );
    }

    #[test]
    fn transform_send_result2put_result_handles_slave_not_available() {
        let send_result = SendResult {
            send_status: SendStatus::SlaveNotAvailable,
            ..Default::default()
        };
        let result = transform_send_result2put_result(Some(send_result));
        assert_eq!(
            result.put_message_status(),
            PutMessageStatus::SlaveNotAvailable
        );
    }
}
