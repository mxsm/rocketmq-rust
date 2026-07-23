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

use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::future::BoxFuture;
use futures::FutureExt;
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_model::result::PullStatus;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::query_message_result::QueryMessageResult;
use rocketmq_store::base::select_result::SelectMappedBufferResult;
use rocketmq_store::filter::ArcMessageFilter;
use rocketmq_store::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use rocketmq_store::message_store::OwnedMessageStore;
use rocketmq_store::store_api_adapter::LegacyAppendReceipt;
use rocketmq_store::store_api_adapter::LegacyStoreHealthSnapshot;
use rocketmq_store::store_error::StoreError as LegacyStoreError;
use rocketmq_store_api::StoreError;
use tracing::error;
use tracing::warn;

use crate::failover::escape_bridge_capability::EscapeBridgePolicyState;
use crate::failover::escape_bridge_capability::EscapeBridgeStoreCapability;
use crate::failover::escape_bridge_capability::LegacyEscapeStoreOwner;
use crate::failover::escape_bridge_capability::LegacyEscapeStoreReadLease;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;

const SEND_TIMEOUT: u64 = 3_000;
const DEFAULT_PULL_TIMEOUT_MILLIS: u64 = 10_000;

#[derive(Debug)]
pub(crate) struct MessageStoreUnavailable;

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
    policy_state: EscapeBridgePolicyState,
    message_store: EscapeBridgeStoreCapability<MS>,
    topic_route_info_manager: TopicRouteInfoManager,
    broker_outer_api: BrokerOuterAPI,
}

impl<MS: MessageStore> EscapeBridge<MS> {
    pub(crate) fn new(
        policy_state: EscapeBridgePolicyState,
        topic_route_info_manager: TopicRouteInfoManager,
        broker_outer_api: BrokerOuterAPI,
    ) -> Self {
        let policy = policy_state.snapshot();
        let inner_producer_group_name = CheetahString::from_string(format!(
            "InnerProducerGroup_{}_{}",
            policy.broker_name, policy.broker_id
        ));
        let inner_consumer_group_name = CheetahString::from_string(format!(
            "InnerConsumerGroup_{}_{}",
            policy.broker_name, policy.broker_id
        ));

        Self {
            inner_producer_group_name,
            inner_consumer_group_name,
            policy_state,
            message_store: EscapeBridgeStoreCapability::default(),
            topic_route_info_manager,
            broker_outer_api,
        }
    }

    pub(crate) fn store_capability(&self) -> EscapeBridgeStoreCapability<MS> {
        self.message_store.clone()
    }

    pub(crate) fn lease_message_store(&self) -> Result<LegacyEscapeStoreReadLease<MS>, MessageStoreUnavailable> {
        self.message_store.read_lease()
    }

    pub fn start(&self) {
        let policy = self.policy_state.snapshot();
        if policy.enable_slave_acting_master && policy.enable_remote_escape {

            //self.message_store = message_store;
        }
    }

    pub fn shutdown(&self) {
        warn!("EscapeBridge shutdown not implemented");
    }

    pub(crate) fn with_message_store<R>(&self, operation: impl FnOnce(&MS) -> R) -> R {
        self.message_store
            .with_store(operation)
            .expect("message store must be bound before broker services start")
    }

    pub(crate) fn try_with_message_store<R>(
        &self,
        operation: impl FnOnce(&MS) -> R,
    ) -> Result<R, MessageStoreUnavailable> {
        self.message_store.with_store(operation)
    }

    pub(crate) fn send_message_store_health_snapshot(
        &self,
    ) -> Result<LegacyStoreHealthSnapshot, MessageStoreUnavailable> {
        self.message_store.health_snapshot()
    }

    pub(crate) async fn send_append_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<LegacyAppendReceipt, StoreError> {
        self.message_store.append_message(message).await
    }

    pub(crate) async fn send_append_batch(&self, batch: MessageExtBatch) -> Result<LegacyAppendReceipt, StoreError> {
        self.message_store.append_batch(batch).await
    }

    pub(crate) fn send_append_progress(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.message_store.append_progress()
    }

    pub(crate) fn pre_online_broker_init_max_offset(&self) -> Result<i64, MessageStoreUnavailable> {
        self.try_with_message_store(MessageStore::get_broker_init_max_offset)
    }

    pub(crate) fn pre_online_master_flushed_offset(&self) -> Result<i64, MessageStoreUnavailable> {
        self.try_with_message_store(MessageStore::get_master_flushed_offset)
    }

    pub(crate) fn pre_online_set_master_flushed_offset(&self, offset: i64) -> Result<(), MessageStoreUnavailable> {
        self.message_store.set_master_flushed_offset(offset)
    }

    pub(crate) async fn pre_online_submit_ha_transfer(
        &self,
        request: HAConnectionStateNotificationRequest,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.message_store.submit_ha_transfer(request).await
    }

    pub(crate) async fn pre_online_update_master_addresses(
        &self,
        master_ha_address: &CheetahString,
        master_address: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable> {
        self.message_store
            .update_master_addresses(master_ha_address, master_address)
            .await
    }

    pub(crate) async fn query_message_from_store(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Option<QueryMessageResult>, MessageStoreUnavailable> {
        self.message_store
            .query_message(topic, key, max_num, begin_timestamp, end_timestamp)
            .await
    }

    pub(crate) fn select_message_from_store(
        &self,
        offset: i64,
    ) -> Result<Option<SelectMappedBufferResult>, MessageStoreUnavailable> {
        self.message_store.select_message(offset)
    }

    pub(crate) async fn put_message_to_local_store(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        self.message_store.put_message(message).await
    }

    pub(crate) fn set_commitlog_read_mode(&self, read_ahead_mode: i32) -> Result<(), LegacyStoreError> {
        self.message_store.set_commitlog_read_mode(read_ahead_mode)
    }

    pub(crate) fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> Result<i32, MessageStoreUnavailable> {
        self.message_store.delete_topics(delete_topics)
    }

    pub(crate) fn get_min_offset_from_local_store(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<i64, MessageStoreUnavailable> {
        self.message_store.min_offset(topic, queue_id)
    }

    pub(crate) fn get_max_offset_from_local_store(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<i64, MessageStoreUnavailable> {
        self.message_store.max_offset(topic, queue_id)
    }

    pub(crate) fn local_store_now(&self) -> Result<u64, MessageStoreUnavailable> {
        self.message_store.now()
    }

    pub(crate) async fn get_message_from_local_store(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.message_store
            .get_message(group, topic, queue_id, offset, nums)
            .await
    }

    pub(crate) async fn get_message_with_filter_from_local_store(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.message_store
            .get_message_with_filter(group, topic, queue_id, offset, nums, message_filter)
            .await
    }

    #[allow(clippy::too_many_arguments, reason = "preserves the Store pull read contract")]
    pub(crate) async fn get_message_with_size_limit_from_local_store(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_msg_bytes: i32,
        message_filter: ArcMessageFilter,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.message_store
            .get_message_with_size_limit(
                group,
                topic,
                queue_id,
                offset,
                max_msg_nums,
                max_msg_bytes,
                message_filter,
            )
            .await
    }

    #[cfg(feature = "local_file_store")]
    pub(crate) fn is_message_in_cold_area(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.try_with_message_store(|store| {
            store
                .get_commit_log()
                .get_cold_data_check_service()
                .is_msg_in_cold_area(group, topic, queue_id, queue_offset)
        })
    }

    pub(crate) fn look_message_by_offset_from_local_store(
        &self,
        offset: i64,
    ) -> Result<Option<MessageExt>, MessageStoreUnavailable> {
        self.message_store.look_message_by_offset(offset)
    }

    pub(crate) fn local_store_state_machine_version(&self) -> Result<i64, MessageStoreUnavailable> {
        self.message_store.state_machine_version()
    }

    pub(crate) async fn update_local_store_master_address(
        &self,
        master_addr: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable> {
        self.message_store.update_master_address(master_addr).await
    }

    pub(crate) fn is_message_store_slave(&self) -> bool {
        self.policy_state.snapshot().broker_role == rocketmq_common::common::broker::broker_role::BrokerRole::Slave
    }

    pub(crate) fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.message_store.check_in_mem_by_consume_offset(topic, queue_id)
    }

    pub(crate) fn local_timer_lag(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.message_store.timer_lag()
    }
}

impl EscapeBridge<OwnedMessageStore> {
    pub(crate) fn bind_message_store(&self, owner: &Arc<LegacyEscapeStoreOwner<OwnedMessageStore>>) {
        self.message_store.bind_owned(owner);
    }
}

impl<MS> EscapeBridge<MS>
where
    MS: MessageStore,
{
    pub async fn put_message(&self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        let policy = self.policy_state.snapshot();
        if policy.broker_id == mix_all::MASTER_ID {
            self.message_store
                .put_message(message_ext)
                .await
                .expect("message store must be bound before broker services start")
        } else if policy.enable_slave_acting_master && policy.enable_remote_escape {
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
                policy.enable_slave_acting_master, policy.enable_remote_escape
            );
            PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable)
        }
    }

    pub async fn put_message_to_remote_broker(
        &self,
        message_ext: MessageExtBrokerInner,
        mut broker_name_to_send: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
        let policy = self.policy_state.snapshot();
        let broker_name = policy.broker_name.as_str();
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
            .topic_route_info_manager
            .try_to_find_topic_publish_info(message_to_put.get_topic())
            .await;
        if !topic_publish_info.as_ref().is_some_and(|value| value.is_usable()) {
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
                .select_one_queue_avoiding(broker_name_to_send.as_ref())
                .unwrap();
            message_to_put.message_ext_inner.queue_id = mq.queue_id();
            broker_name_to_send = Some(mq.broker_name().clone());
            if broker_name == mq.broker_name().as_str() {
                warn!(
                    "putMessageToRemoteBroker failed, remote broker not found. Topic: {}, MsgId: {}, Broker: {}",
                    message_to_put.get_topic(),
                    message_to_put.message_ext_inner.msg_id,
                    mq.broker_name()
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
            .topic_route_info_manager
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
            .broker_outer_api
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
            message_ext.property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
        match producer_group {
            None => self.inner_producer_group_name.clone(),
            Some(value) => value,
        }
    }

    pub async fn async_put_message(&self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        let policy = self.policy_state.snapshot();
        if policy.broker_id == mix_all::MASTER_ID {
            self.message_store
                .put_message(message_ext)
                .await
                .expect("message store must be bound before broker services start")
        } else if policy.enable_slave_acting_master && policy.enable_remote_escape {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .topic_route_info_manager
                .try_to_find_topic_publish_info(message_ext.get_topic())
                .await;
            if topic_publish_info.is_none() {
                return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
            }
            let topic_publish_info = topic_publish_info.unwrap();
            let mq_selected = topic_publish_info.select_one_queue();
            if mq_selected.is_none() {
                return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
            }
            let message_queue = mq_selected.unwrap();
            message_ext.message_ext_inner.queue_id = message_queue.queue_id();
            let broker_name_to_send = message_queue.broker_name();
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

    pub async fn put_message_to_specific_queue(&self, mut message_ext: MessageExtBrokerInner) -> PutMessageResult {
        let policy = self.policy_state.snapshot();
        if policy.broker_id == mix_all::MASTER_ID {
            self.message_store
                .put_message(message_ext)
                .await
                .expect("message store must be bound before broker services start")
        } else if policy.enable_slave_acting_master && policy.enable_remote_escape {
            message_ext.set_wait_store_msg_ok(false);
            let topic_publish_info = self
                .topic_route_info_manager
                .try_to_find_topic_publish_info(message_ext.get_topic())
                .await;
            if topic_publish_info.is_none() {
                return PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true);
            }
            let topic_publish_info = topic_publish_info.unwrap();

            if topic_publish_info.message_queues().is_empty() {
                return PutMessageResult::new(PutMessageStatus::PutToRemoteBrokerFail, None, true);
            }
            //let message_queue = mq_selected.unwrap();
            let id = format!(
                "{}{}",
                message_ext.get_topic(),
                message_ext.message_ext_inner.store_host
            );
            let code = JavaStringHasher::hash_str(id.as_str());
            let index = code as usize % topic_publish_info.message_queues().len();
            let message_queue = topic_publish_info.message_queues()[index].clone();
            message_ext.message_ext_inner.queue_id = message_queue.queue_id();
            let broker_name_to_send = message_queue.broker_name();
            let broker_addr_to_send = self
                .topic_route_info_manager
                .find_broker_address_in_publish(Some(broker_name_to_send));
            let producer_group = self.get_producer_group(&message_ext);
            match self
                .broker_outer_api
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
                policy.enable_slave_acting_master, policy.enable_remote_escape
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
        let message_store = self.message_store.clone();
        if message_store.with_store(|_| ()).is_err() {
            return async { (None, "message store is unavailable".to_string(), false) }.boxed();
        }
        let inner_consumer_group_name = self.inner_consumer_group_name.clone();
        let topic = topic.clone();
        let broker_name = broker_name.clone();

        if self.policy_state.snapshot().broker_name == broker_name {
            async move {
                let result = message_store
                    .get_message(&inner_consumer_group_name, &topic, queue_id, offset, 1)
                    .await
                    .ok()
                    .flatten();
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
                    warn!(
                        "Can not get msg, topic {}, offset {}, queueId {}, needRetry {},",
                        topic, offset, queue_id, need_retry,
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
        let topic_route_info_manager = self.topic_route_info_manager.clone();
        let broker_outer_api = self.broker_outer_api.clone();
        let inner_consumer_group_name = self.inner_consumer_group_name.clone();
        let topic = topic.clone();
        let broker_name = broker_name.clone();

        async move {
            let mut broker_addr =
                topic_route_info_manager.find_broker_address_in_subscribe(Some(&broker_name), 0, false);

            if broker_addr.is_none() {
                topic_route_info_manager
                    .update_topic_route_info_from_name_server_ext(&topic, true, false)
                    .await;
                broker_addr = topic_route_info_manager.find_broker_address_in_subscribe(Some(&broker_name), 0, false);

                if broker_addr.is_none() {
                    warn!("can't find broker address for topic {}, {}", topic, broker_name);
                    return (None, "brokerAddress not found".to_string(), true);
                }
            }

            let broker_addr = broker_addr.unwrap();
            match broker_outer_api
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
                        if result.pull_status() == PullStatus::Found
                            && result.messages().is_some_and(|value| !value.is_empty())
                        {
                            return (Some(result.messages().unwrap()[0].clone()), "".to_string(), false);
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
        let mut bytes = Bytes::copy_from_slice(bb.get_buffer());
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
    use rocketmq_model::result::SendResult;
    use rocketmq_model::result::SendStatus;
    use rocketmq_store::base::message_status_enum::PutMessageStatus;

    use super::transform_send_result2put_result;

    #[test]
    fn offset_and_failover_targets_do_not_retain_the_runtime_pointer() {
        for source in [
            include_str!("escape_bridge.rs"),
            include_str!("../offset/manager/consumer_offset_manager.rs"),
        ] {
            assert!(!source.contains(concat!("Arc", "Mut")));
            assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
            assert!(!source.contains(concat!("mut", "_from_ref")));
        }
    }

    #[test]
    fn admin_store_mutations_use_named_operations_instead_of_complete_write_leases() {
        let admin_source = include_str!("../broker/broker_admin_runtime.rs");
        let bridge_source = include_str!("escape_bridge.rs");
        let capability_source = include_str!("escape_bridge_capability.rs");

        assert!(!admin_source.contains("fn message_store_mut"));
        assert!(!bridge_source.contains(concat!("fn lease_message_store", "_mut")));
        assert!(!capability_source.contains(concat!("fn write_lease(&self)", " -> Result")));
        for operation in ["put_message", "set_commitlog_read_mode", "delete_topics"] {
            assert!(
                admin_source.contains(operation),
                "Admin Store boundary must expose the named {operation} operation"
            );
        }
    }

    #[test]
    fn request_append_paths_use_the_shared_port_instead_of_complete_store_clones() {
        let capability_source = include_str!("escape_bridge_capability.rs");

        assert_eq!(
            capability_source.matches(concat!("owner.cloned_", "store()")).count(),
            1,
            "only the controller role transition may retain a legacy mutable Store clone"
        );
        for operation in [
            concat!(".put_", "message(message)"),
            concat!(".put_", "messages(batch)"),
        ] {
            assert!(
                capability_source.contains(operation),
                "request append boundary must use {operation}"
            );
        }
    }

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
