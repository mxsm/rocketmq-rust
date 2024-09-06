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

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DEFAULT_CONSUMER_GROUP;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::runtime::RPCHook;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::consumer::consumer_impl::consume_message_concurrently_service::ConsumeMessageConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_orderly_service::ConsumeMessageOrderlyService;
use crate::consumer::consumer_impl::consume_message_pop_concurrently_service::ConsumeMessagePopConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_pop_orderly_service::ConsumeMessagePopOrderlyService;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageConcurrentlyServiceGeneral;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageOrderlyServiceGeneral;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::rebalance_push_impl::RebalancePushImpl;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::message_listener::MessageListener;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::error::MQClientError;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::Result;

const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;
const BROKER_SUSPEND_MAX_TIME_MILLIS: u64 = 1000 * 15;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: u64 = 1000 * 30;
const MAX_POP_INVISIBLE_TIME: u64 = 300000;
const MIN_POP_INVISIBLE_TIME: u64 = 5000;
const ASYNC_TIMEOUT: u64 = 3000;
const DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED: bool = false;

#[derive(Clone)]
pub struct DefaultMQPushConsumerImpl {
    client_config: ClientConfig,
    consumer_config: ConsumerConfig,
    rebalance_impl: ArcRefCellWrapper<RebalancePushImpl>,
    filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    service_state: ServiceState,
    mq_client_factory: Option<ArcRefCellWrapper<MQClientInstance>>,
    pull_api_wrapper: Option<ArcRefCellWrapper<PullAPIWrapper>>,
    pause: Arc<AtomicBool>,
    consume_orderly: bool,
    message_listener: Option<ArcRefCellWrapper<MessageListener>>,
    offset_store: Option<ArcRefCellWrapper<OffsetStore>>,
    consume_message_concurrently_service: Option<
        ArcRefCellWrapper<
            ConsumeMessageConcurrentlyServiceGeneral<
                ConsumeMessageConcurrentlyService,
                ConsumeMessagePopConcurrentlyService,
            >,
        >,
    >,
    consume_message_orderly_service: Option<
        ArcRefCellWrapper<
            ConsumeMessageOrderlyServiceGeneral<
                ConsumeMessageOrderlyService,
                ConsumeMessagePopOrderlyService,
            >,
        >,
    >,
    queue_flow_control_times: u64,
    queue_max_span_flow_control_times: u64,
    pop_delay_level: Arc<[i32; 16]>,
}

impl DefaultMQPushConsumerImpl {
    pub fn new(
        client_config: ClientConfig,
        consumer_config: ConsumerConfig,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        Self {
            client_config,
            consumer_config,
            rebalance_impl: ArcRefCellWrapper::new(RebalancePushImpl),
            filter_message_hook_list: vec![],
            rpc_hook,
            service_state: ServiceState::CreateJust,
            mq_client_factory: None,
            pull_api_wrapper: None,
            pause: Arc::new(AtomicBool::new(false)),
            consume_orderly: false,
            message_listener: None,
            offset_store: None,
            consume_message_concurrently_service: None,
            consume_message_orderly_service: None,
            queue_flow_control_times: 0,
            queue_max_span_flow_control_times: 0,
            pop_delay_level: Arc::new([
                10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ]),
        }
    }
}

impl DefaultMQPushConsumerImpl {
    pub async fn start(&mut self) -> Result<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                info!(
                    "the consumer [{}] start beginning. messageModel={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                self.service_state = ServiceState::StartFailed;
                self.check_config()?;
                self.copy_subscription().await?;
                if self.consumer_config.message_model() == MessageModel::Clustering {
                    self.client_config.change_instance_name_to_pid();
                }
                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(
                        self.client_config.clone(),
                        self.rpc_hook.clone(),
                    )
                    .await;
                self.rebalance_impl
                    .set_consumer_group(self.consumer_config.consumer_group.clone());
                self.rebalance_impl
                    .set_message_model(self.consumer_config.message_model);
                self.rebalance_impl.set_allocate_message_queue_strategy(
                    self.consumer_config.allocate_message_queue_strategy.clone(),
                );
                self.rebalance_impl
                    .set_mq_client_factory(client_instance.clone());
                if self.pull_api_wrapper.is_none() {
                    self.pull_api_wrapper = Some(ArcRefCellWrapper::new(PullAPIWrapper::new(
                        client_instance.clone(),
                        self.consumer_config.consumer_group.clone(),
                        self.consumer_config.unit_mode,
                    )));
                }
                if let Some(pull_api_wrapper) = self.pull_api_wrapper.as_mut() {
                    pull_api_wrapper
                        .register_filter_message_hook(self.filter_message_hook_list.clone());
                }
                match self.consumer_config.message_model {
                    MessageModel::Broadcasting => {
                        self.offset_store = Some(ArcRefCellWrapper::new(
                            OffsetStore::new_with_local(LocalFileOffsetStore::new(
                                client_instance.clone(),
                                self.consumer_config.consumer_group.clone(),
                            )),
                        ));
                    }
                    MessageModel::Clustering => {
                        self.offset_store = Some(ArcRefCellWrapper::new(
                            OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                                client_instance.clone(),
                                self.consumer_config.consumer_group.clone(),
                            )),
                        ));
                    }
                }
                self.offset_store.as_mut().unwrap().load().await?;

                if let Some(message_listener) = self.message_listener.as_ref() {
                    if message_listener.message_listener_concurrently.is_some() {
                        self.consume_orderly = false;
                        self.consume_message_concurrently_service = Some(ArcRefCellWrapper::new(
                            ConsumeMessageConcurrentlyServiceGeneral {
                                consume_message_concurrently_service:
                                    ConsumeMessageConcurrentlyService,

                                consume_message_pop_concurrently_service:
                                    ConsumeMessagePopConcurrentlyService,
                            },
                        ));
                    } else if message_listener.message_listener_orderly.is_some() {
                        self.consume_orderly = true;
                        self.consume_message_orderly_service = Some(ArcRefCellWrapper::new(
                            ConsumeMessageOrderlyServiceGeneral {
                                consume_message_orderly_service: ConsumeMessageOrderlyService,
                                consume_message_pop_orderly_service:
                                    ConsumeMessagePopOrderlyService,
                            },
                        ));
                    }
                }

                if let Some(consume_message_concurrently_service) =
                    self.consume_message_concurrently_service.as_mut()
                {
                    consume_message_concurrently_service
                        .consume_message_concurrently_service
                        .start();
                    consume_message_concurrently_service
                        .consume_message_pop_concurrently_service
                        .start();
                }

                if let Some(consume_message_orderly_service) =
                    self.consume_message_orderly_service.as_mut()
                {
                    consume_message_orderly_service
                        .consume_message_orderly_service
                        .start();
                    consume_message_orderly_service
                        .consume_message_pop_orderly_service
                        .start();
                }
                let consumer_impl = self.clone();
                self.mq_client_factory
                    .as_mut()
                    .unwrap()
                    .register_consumer(self.consumer_config.consumer_group.as_str(), consumer_impl)
                    .await;

                self.mq_client_factory.as_mut().unwrap().start().await?;
                info!(
                    "the consumer [{}] start OK, messageModel={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                self.service_state = ServiceState::Running;
            }
            ServiceState::Running => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The PushConsumer service state is Running".to_string(),
                ));
            }
            ServiceState::ShutdownAlready => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The PushConsumer service state is ShutdownAlready".to_string(),
                ));
            }
            ServiceState::StartFailed => {
                return Err(MQClientError::MQClientException(
                    -1,
                    format!(
                        "The PushConsumer service state not OK, maybe started once,{:?},{}",
                        self.service_state,
                        FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                    ),
                ));
            }
        }
        self.update_topic_subscribe_info_when_subscription_changed();
        let client_instance = self.mq_client_factory.as_mut().unwrap();
        client_instance.check_client_in_broker().await?;
        if client_instance
            .send_heartbeat_to_all_broker_with_lock()
            .await
        {
            client_instance.rebalance_immediately().await;
        }
        Ok(())
    }

    fn update_topic_subscribe_info_when_subscription_changed(&mut self) {
        unimplemented!("updateTopicSubscribeInfoWhenSubscriptionChanged");
    }

    fn check_config(&mut self) -> Result<()> {
        Validators::check_group(self.consumer_config.consumer_group.as_str())?;
        if self.consumer_config.consumer_group.is_empty() {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumerGroup is empty, {}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.consumer_group == DEFAULT_CONSUMER_GROUP {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumerGroup can not equal {} please specify another one.{}",
                    DEFAULT_CONSUMER_GROUP,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        /*        if !util_all::parse_date(self.consumer_config.consume_timestamp.as_str(), DATE_FORMAT)
            .is_ok()
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeTimestamp is invalid, the valid format is {}, but received {}{}",
                    DATE_FORMAT,
                    self.consumer_config.consume_timestamp,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }*/

        if self
            .consumer_config
            .allocate_message_queue_strategy
            .is_none()
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "allocateMessageQueueStrategy is null{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.subscription.is_empty() {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "subscription is null{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.message_listener.is_none() {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "messageListener is null{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self
            .consumer_config
            .message_listener
            .as_ref()
            .unwrap()
            .message_listener_orderly
            .is_some()
            && self
                .consumer_config
                .message_listener
                .as_ref()
                .unwrap()
                .message_listener_concurrently
                .is_some()
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "messageListener must be instanceof MessageListenerOrderly or \
                     MessageListenerConcurrently{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        let consume_thread_min = self.consumer_config.consume_thread_min;
        let consume_thread_max = self.consumer_config.consume_thread_max;
        if !(1..=1000).contains(&consume_thread_min) {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeThreadMin Out of range [1, 1000]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }
        if !(1..=1000).contains(&consume_thread_max) {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeThreadMax Out of range [1, 1000]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }
        if consume_thread_min > consume_thread_max {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
                    consume_thread_min, consume_thread_max
                ),
            ));
        }

        if self.consumer_config.consume_concurrently_max_span < 1
            || self.consumer_config.consume_concurrently_max_span > 65535
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeConcurrentlyMaxSpan Out of range [1, 65535]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_threshold_for_queue < 1
            || self.consumer_config.pull_threshold_for_queue > 65535
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullThresholdForQueue Out of range [1, 65535]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_threshold_for_topic != -1
            && (self.consumer_config.pull_threshold_for_topic < 1
                || self.consumer_config.pull_threshold_for_topic > 6553500)
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullThresholdForTopic Out of range [1, 65535]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_threshold_size_for_queue < 1
            || self.consumer_config.pull_threshold_size_for_queue > 1024
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullThresholdSizeForQueue Out of range [1, 1024]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_threshold_size_for_topic != -1
            && (self.consumer_config.pull_threshold_size_for_topic < 1
                || self.consumer_config.pull_threshold_size_for_topic > 102400)
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullThresholdSizeForTopic Out of range [1, 102400]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_interval > 65535 {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullInterval Out of range [0, 65535]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.consume_message_batch_max_size < 1
            || self.consumer_config.consume_message_batch_max_size > 1024
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pull_batch_size < 1 || self.consumer_config.pull_batch_size > 1024 {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "pullBatchSize Out of range [1, 1024]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pop_invisible_time < MIN_POP_INVISIBLE_TIME
            || self.consumer_config.pop_invisible_time > MAX_POP_INVISIBLE_TIME
        {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "popInvisibleTime Out of range [{}, {}]{}",
                    MIN_POP_INVISIBLE_TIME,
                    MAX_POP_INVISIBLE_TIME,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        if self.consumer_config.pop_batch_nums < 1 || self.consumer_config.pop_batch_nums > 32 {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "popBatchNums Out of range [1, 32]{}",
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
                ),
            ));
        }

        Ok(())
    }

    async fn copy_subscription(&mut self) -> Result<()> {
        let sub = self.consumer_config.subscription();
        if !sub.is_empty() {
            unimplemented!()
        }
        if self.message_listener.is_none() {
            self.message_listener = self.consumer_config.message_listener.clone();
        }

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {}
            MessageModel::Clustering => {
                let retry_topic =
                    mix_all::get_retry_topic(self.consumer_config.consumer_group.as_str());
                let subscription_data = FilterAPI::build_subscription_data(
                    retry_topic.as_str(),
                    SubscriptionData::SUB_ALL,
                )
                .map_err(|e| {
                    MQClientError::MQClientException(
                        -1,
                        format!("buildSubscriptionData exception, {}", e),
                    )
                })?;
                self.rebalance_impl
                    .put_subscription_data(retry_topic.as_str(), subscription_data)
                    .await;
            }
        }
        Ok(())
    }

    pub fn register_consume_message_hook(&mut self, hook: impl ConsumeMessageHook) {
        unimplemented!("registerConsumeMessageHook");
    }

    pub fn register_message_listener(
        &mut self,
        message_listener: Option<ArcRefCellWrapper<MessageListener>>,
    ) {
        self.message_listener = message_listener;
    }
}

impl MQConsumerInner for DefaultMQPushConsumerImpl {
    fn group_name(&self) -> &str {
        todo!()
    }

    fn message_model(&self) -> MessageModel {
        todo!()
    }

    fn consume_type(&self) -> ConsumeType {
        todo!()
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        todo!()
    }

    fn subscriptions(&self) -> &HashSet<SubscriptionData> {
        todo!()
    }

    fn do_rebalance(&self) {
        todo!()
    }

    fn try_rebalance(&self) -> bool {
        todo!()
    }

    fn persist_consumer_offset(&self) {
        todo!()
    }

    fn update_topic_subscribe_info(&mut self, topic: &str, info: &HashSet<MessageQueue>) {
        todo!()
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        todo!()
    }

    fn is_unit_mode(&self) -> bool {
        todo!()
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        todo!()
    }
}
