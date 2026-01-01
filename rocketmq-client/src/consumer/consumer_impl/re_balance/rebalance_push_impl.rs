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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
use rocketmq_common::common::constant::consume_init_mode::ConsumeInitMode;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::RebalanceImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

static UNLOCK_DELAY_TIME_MILLS: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.unlockDelayTimeMills")
        .unwrap_or_else(|_| "20000".into())
        .parse::<u64>()
        .unwrap_or(20000)
});

pub struct RebalancePushImpl {
    pub(crate) client_config: ClientConfig,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) rebalance_impl_inner: RebalanceImpl<RebalancePushImpl>,
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl RebalancePushImpl {
    pub fn new(client_config: ClientConfig, consumer_config: ArcMut<ConsumerConfig>) -> Self {
        RebalancePushImpl {
            client_config,
            consumer_config,
            rebalance_impl_inner: RebalanceImpl::new(None, None, None, None),
            default_mqpush_consumer_impl: None,
        }
    }
}

impl RebalancePushImpl {
    pub fn get_subscription_inner(&self) -> Arc<RwLock<HashMap<CheetahString, SubscriptionData>>> {
        self.rebalance_impl_inner.subscription_inner.clone()
    }

    pub fn set_default_mqpush_consumer_impl(
        &mut self,
        default_mqpush_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    ) {
        self.default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl);
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.rebalance_impl_inner.consumer_group = Some(consumer_group);
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.rebalance_impl_inner.message_model = Some(message_model);
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.rebalance_impl_inner.allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
    }

    pub fn set_mq_client_factory(&mut self, client_instance: ArcMut<MQClientInstance>) {
        self.rebalance_impl_inner.client_instance = Some(client_instance);
    }

    #[inline]
    pub async fn put_subscription_data(&mut self, topic: CheetahString, subscription_data: SubscriptionData) {
        let mut subscription_inner = self.rebalance_impl_inner.subscription_inner.write().await;
        subscription_inner.insert(topic, subscription_data);
    }

    pub fn set_rebalance_impl(&mut self, rebalance_impl: WeakArcMut<RebalancePushImpl>) {
        self.rebalance_impl_inner.sub_rebalance_impl = Some(rebalance_impl);
    }

    async fn try_remove_order_message_queue(&mut self, mq: &MessageQueue, pq: &ProcessQueue) -> bool {
        let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap();

        let force_unlock =
            pq.is_dropped() && (get_current_millis() > pq.get_last_lock_timestamp() + *UNLOCK_DELAY_TIME_MILLS);
        let consume_lock = pq.consume_lock.try_write_timeout(Duration::from_millis(500)).await;
        if force_unlock || consume_lock.is_some() {
            let offset_store = default_mqpush_consumer_impl.offset_store.as_mut().unwrap();
            offset_store.persist(mq).await;
            offset_store.remove_offset(mq).await;
            pq.set_locked(true);
            self.unlock(mq, true).await;
            return true;
        } else {
            pq.inc_try_unlock_times();
            warn!(
                "Failed to acquire consume_lock for {}, incrementing try_unlock_times.",
                mq
            );
        }

        false
    }
}

const UNLOCK_BATCH_MQ_TIMEOUT_MS: u64 = 1_000;

impl Rebalance for RebalancePushImpl {
    async fn message_queue_changed(
        &mut self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    ) {
        let mut subscription_inner = self.rebalance_impl_inner.subscription_inner.write().await;
        let subscription_data = subscription_inner.get_mut(topic).unwrap();
        let new_version = get_current_millis() as i64;
        info!(
            "{} Rebalance changed, also update version: {}, {}",
            topic, subscription_data.sub_version, new_version
        );
        subscription_data.sub_version = new_version;
        drop(subscription_inner);

        let process_queue_table = self.rebalance_impl_inner.process_queue_table.read().await;
        let current_queue_count = process_queue_table.len();
        if current_queue_count != 0 {
            let pull_threshold_for_topic = self.consumer_config.pull_threshold_for_topic;
            if pull_threshold_for_topic != -1 {
                let new_val = 1.max(pull_threshold_for_topic / current_queue_count as i32);
                info!(
                    "The pullThresholdForQueue is changed from {} to {}",
                    pull_threshold_for_topic, new_val
                );
                self.consumer_config.pull_threshold_for_topic = new_val;
            }
            let pull_threshold_size_for_topic = self.consumer_config.pull_threshold_size_for_topic;
            if pull_threshold_size_for_topic != -1 {
                let new_val = 1.max(pull_threshold_size_for_topic / current_queue_count as i32);
                info!(
                    "The pullThresholdSizeForQueue is changed from {} to {}",
                    pull_threshold_size_for_topic, new_val
                );
                self.consumer_config.pull_threshold_size_for_topic = new_val;
            }
        }

        //notify broker
        let _ = self
            .rebalance_impl_inner
            .client_instance
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .send_heartbeat_to_all_broker_with_lock_v2(true)
            .await;
        if let Some(ref message_queue_listener) = self.consumer_config.message_queue_listener {
            message_queue_listener.message_queue_changed(topic, mq_all, mq_divided);
        }
    }

    async fn remove_unnecessary_message_queue(&mut self, mq: &MessageQueue, pq: &ProcessQueue) -> bool {
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        let consume_orderly = default_mqpush_consumer_impl.is_consume_orderly();
        let offset_store = default_mqpush_consumer_impl.offset_store.as_mut().unwrap();

        if consume_orderly && MessageModel::Clustering == self.consumer_config.message_model {
            offset_store.persist(mq).await;
            self.try_remove_order_message_queue(mq, pq).await
        } else {
            offset_store.persist(mq).await;
            offset_store.remove_offset(mq).await;
            true
        }
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    async fn remove_dirty_offset(&mut self, mq: &MessageQueue) {
        let offset_store = self
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .offset_store
            .as_mut()
            .unwrap();
        offset_store.remove_offset(mq).await;
    }

    #[allow(deprecated)]
    async fn compute_pull_from_where_with_exception(
        &mut self,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let consume_from_where = self.consumer_config.consume_from_where;
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        let offset_store = default_mqpush_consumer_impl.offset_store.as_mut().unwrap();

        let result = match consume_from_where {
            ConsumeFromWhere::ConsumeFromLastOffset
            | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
            | ConsumeFromWhere::ConsumeFromMinOffset
            | ConsumeFromWhere::ConsumeFromMaxOffset => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    if mq.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        0
                    } else {
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .max_offset(mq)
                            .await?
                    }
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
            ConsumeFromWhere::ConsumeFromFirstOffset => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    0
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
            ConsumeFromWhere::ConsumeFromTimestamp => {
                let last_offset = offset_store.read_offset(mq, ReadOffsetType::ReadFromStore).await;
                if last_offset >= 0 {
                    last_offset
                } else if -1 == last_offset {
                    if mq.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .max_offset(mq)
                            .await?
                    } else {
                        let timestamp = util_all::parse_date(
                            self.consumer_config.consume_timestamp.as_ref().unwrap(),
                            util_all::YYYYMMDDHHMMSS,
                        )
                        .unwrap()
                        .and_utc()
                        .timestamp();
                        self.rebalance_impl_inner
                            .client_instance
                            .as_mut()
                            .unwrap()
                            .mq_admin_impl
                            .search_offset(mq, timestamp as u64)
                            .await?
                    }
                } else {
                    return Err(mq_client_err!(
                        ResponseCode::QueryNotFound as i32,
                        "Failed to query consume offset from offset store"
                    ));
                }
            }
        };
        if result < 0 {
            return Err(mq_client_err!(
                ResponseCode::SystemError as i32,
                "Failed to query consume offset from offset store"
            ));
        }
        Ok(result)
    }

    async fn compute_pull_from_where(&mut self, mq: &MessageQueue) -> i64 {
        self.compute_pull_from_where_with_exception(mq)
            .await
            .unwrap_or_else(|e| {
                warn!("Compute consume offset exception, mq={:?}", e);
                -1
            })
    }

    fn get_consume_init_mode(&self) -> i32 {
        let consume_from_where = self.consumer_config.consume_from_where;
        if consume_from_where == ConsumeFromWhere::ConsumeFromFirstOffset {
            ConsumeInitMode::MIN
        } else {
            ConsumeInitMode::MAX
        }
    }

    async fn dispatch_pull_request(&self, pull_request_list: Vec<PullRequest>, delay: u64) {
        if let Some(mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref() {
            let mqpush_consumer_impl = mqpush_consumer_impl.mut_from_ref();
            for pull_request in pull_request_list {
                if delay == 0 {
                    mqpush_consumer_impl
                        .execute_pull_request_immediately(pull_request)
                        .await;
                } else {
                    mqpush_consumer_impl.execute_pull_request_later(pull_request, delay);
                }
            }
        } else {
            error!("mqpush_consumer_impl is not initialized");
        }
    }

    async fn dispatch_pop_pull_request(&self, pop_request_list: Vec<PopRequest>, delay: u64) {
        if let Some(mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref() {
            let mqpush_consumer_impl = mqpush_consumer_impl.mut_from_ref();
            for pop_request in pop_request_list {
                if delay == 0 {
                    mqpush_consumer_impl.execute_pop_request_immediately(pop_request).await;
                } else {
                    mqpush_consumer_impl.execute_pop_request_later(pop_request, delay);
                }
            }
        } else {
            error!("mqpush_consumer_impl is not initialized");
        }
    }

    #[inline]
    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    #[inline]
    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    async fn remove_process_queue(&mut self, mq: &MessageQueue) {
        let mut process_queue_table = self.rebalance_impl_inner.process_queue_table.write().await;
        let prev = process_queue_table.remove(mq);
        drop(process_queue_table);
        if let Some(pq) = prev {
            let droped = pq.is_dropped();
            pq.set_dropped(true);
            self.remove_unnecessary_message_queue(mq, &pq).await;
            info!(
                "Fix Offset, {}, remove unnecessary mq, {} Droped: {}",
                self.rebalance_impl_inner.consumer_group.as_ref().unwrap(),
                mq,
                droped
            );
        }
    }

    async fn unlock(&mut self, mq: &MessageQueue, oneway: bool) {
        let client = match self.rebalance_impl_inner.client_instance.as_mut() {
            Some(client) => client,
            None => {
                warn!("Client instance is not available.");
                return;
            }
        };
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let find_broker_result = client
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;
        if let Some(find_broker_result) = find_broker_result {
            let mut request_body = UnlockBatchRequestBody {
                consumer_group: Some(self.rebalance_impl_inner.consumer_group.clone().unwrap()),
                client_id: Some(client.client_id.clone()),
                ..Default::default()
            };
            request_body.mq_set.insert(mq.clone());
            let result = client
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .unlock_batch_mq(
                    find_broker_result.broker_addr.as_ref(),
                    request_body,
                    UNLOCK_BATCH_MQ_TIMEOUT_MS,
                    oneway,
                )
                .await;
            if let Err(e) = result {
                warn!("unlockBatchMQ exception, {},{}", mq, e);
            } else {
                warn!(
                    "unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    self.rebalance_impl_inner.consumer_group.as_ref().unwrap(),
                    client.client_id,
                    mq
                )
            }
        }
    }

    fn lock_all(&self) {
        todo!()
    }

    fn unlock_all(&self, oneway: bool) {
        todo!()
    }

    async fn do_rebalance(&mut self, is_order: bool) -> bool {
        self.rebalance_impl_inner.do_rebalance(is_order).await
    }

    /// Determines if the client should perform rebalancing for the given topic.
    ///
    /// This function checks the following conditions to decide if rebalancing is needed:
    /// 1. If `client_rebalance` is enabled in the consumer configuration.
    /// 2. If the message model is set to `Broadcasting`.
    /// 3. If the consumer is configured to consume messages in order.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string slice that holds the name of the topic.
    ///
    /// # Returns
    ///
    /// * `true` if the client should perform rebalancing.
    /// * `false` otherwise.
    fn client_rebalance(&mut self, topic: &str) -> bool {
        //Pop message mode, order message consumer not implement, it's use
        // ConsumeMessageOrderlyService to consume
        self.consumer_config.client_rebalance
            || self.rebalance_impl_inner.message_model.unwrap() == MessageModel::Broadcasting
            || self.default_mqpush_consumer_impl.as_ref().unwrap().is_consume_orderly()
    }

    fn destroy(&mut self) {
        unimplemented!()
    }
}
