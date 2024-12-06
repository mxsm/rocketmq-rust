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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DEFAULT_CONSUMER_GROUP;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::client_error::ClientErr;
use crate::client_error::MQClientError;
use crate::consumer::consumer_impl::consume_message_concurrently_service::ConsumeMessageConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_orderly_service::ConsumeMessageOrderlyService;
use crate::consumer::consumer_impl::consume_message_pop_concurrently_service::ConsumeMessagePopConcurrentlyService;
use crate::consumer::consumer_impl::consume_message_pop_orderly_service::ConsumeMessagePopOrderlyService;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessagePopServiceGeneral;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceGeneral;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_api_wrapper::PullAPIWrapper;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::rebalance_push_impl::RebalancePushImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::message_listener::MessageListener;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::consumer::pull_callback::DefaultPullCallback;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::mq_client_err;
use crate::producer::mq_producer::MQProducer;
use crate::Result;

const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
pub(crate) const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;
const BROKER_SUSPEND_MAX_TIME_MILLIS: u64 = 1000 * 15;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: u64 = 1000 * 30;
const MAX_POP_INVISIBLE_TIME: u64 = 300000;
const MIN_POP_INVISIBLE_TIME: u64 = 5000;
const ASYNC_TIMEOUT: u64 = 3000;
const DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED: bool = false;
const _1MB: u64 = 1024 * 1024;

pub struct DefaultMQPushConsumerImpl {
    pub(crate) global_lock: Arc<Mutex<()>>,
    pub(crate) pull_time_delay_mills_when_exception: u64,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) rebalance_impl: ArcMut<RebalancePushImpl>,
    filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
    consume_message_hook_list: Vec<Arc<Box<dyn ConsumeMessageHook + Send + Sync>>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    service_state: ArcMut<ServiceState>,
    pub(crate) client_instance: Option<ArcMut<MQClientInstance>>,
    pub(crate) pull_api_wrapper: Option<ArcMut<PullAPIWrapper>>,
    pause: Arc<AtomicBool>,
    consume_orderly: bool,
    message_listener: Option<ArcMut<MessageListener>>,
    pub(crate) offset_store: Option<ArcMut<OffsetStore>>,
    pub(crate) consume_message_service: Option<
        ArcMut<
            ConsumeMessageServiceGeneral<
                ConsumeMessageConcurrentlyService,
                ConsumeMessageOrderlyService,
            >,
        >,
    >,
    consume_message_pop_service: Option<
        ArcMut<
            ConsumeMessagePopServiceGeneral<
                ConsumeMessagePopConcurrentlyService,
                ConsumeMessagePopOrderlyService,
            >,
        >,
    >,
    queue_flow_control_times: u64,
    queue_max_span_flow_control_times: u64,
    pop_delay_level: Arc<[i32; 16]>,
    default_mqpush_consumer_impl: Option<WeakArcMut<DefaultMQPushConsumerImpl>>,
}

impl DefaultMQPushConsumerImpl {
    pub fn new(
        client_config: ClientConfig,
        consumer_config: ArcMut<ConsumerConfig>,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        let mut this = Self {
            global_lock: Arc::new(Default::default()),
            pull_time_delay_mills_when_exception: 3_000,
            client_config: ArcMut::new(client_config.clone()),
            consumer_config: consumer_config.clone(),
            rebalance_impl: ArcMut::new(RebalancePushImpl::new(client_config, consumer_config)),
            filter_message_hook_list: vec![],
            consume_message_hook_list: vec![],
            rpc_hook,
            service_state: ArcMut::new(ServiceState::CreateJust),
            client_instance: None,
            pull_api_wrapper: None,
            pause: Arc::new(AtomicBool::new(false)),
            consume_orderly: false,
            message_listener: None,
            offset_store: None,
            consume_message_service: None,
            consume_message_pop_service: None,
            queue_flow_control_times: 0,
            queue_max_span_flow_control_times: 0,
            pop_delay_level: Arc::new([
                10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ]),
            default_mqpush_consumer_impl: None,
        };
        let wrapper = ArcMut::downgrade(&this.rebalance_impl);
        this.rebalance_impl.set_rebalance_impl(wrapper);
        this
    }

    pub fn set_default_mqpush_consumer_impl(
        &mut self,
        default_mqpush_consumer_impl: WeakArcMut<DefaultMQPushConsumerImpl>,
    ) {
        self.rebalance_impl
            .set_default_mqpush_consumer_impl(default_mqpush_consumer_impl.clone());
        self.default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl.clone());
        if let Some(ref mut consume_message_concurrently_service) = self.consume_message_service {
            consume_message_concurrently_service
                .get_consume_message_concurrently_service()
                .default_mqpush_consumer_impl = Some(default_mqpush_consumer_impl);
        }
    }

    #[inline]
    pub fn is_consume_orderly(&self) -> bool {
        self.consume_orderly
    }
}

impl DefaultMQPushConsumerImpl {
    pub async fn start(&mut self) -> Result<()> {
        match *self.service_state {
            ServiceState::CreateJust => {
                info!(
                    "the consumer [{}] start beginning. message_model={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                *self.service_state = ServiceState::StartFailed;
                // check all config
                self.check_config()?;
                //copy_subscription is can be removed
                self.copy_subscription().await?;
                if self.consumer_config.message_model() == MessageModel::Clustering {
                    self.client_config.change_instance_name_to_pid();
                }
                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(
                        self.client_config.as_ref().clone(),
                        self.rpc_hook.clone(),
                    )
                    .await;
                self.client_instance = Some(client_instance.clone());
                self.rebalance_impl
                    .set_consumer_group(self.consumer_config.consumer_group.clone());
                self.rebalance_impl
                    .set_message_model(self.consumer_config.message_model);
                self.rebalance_impl.set_allocate_message_queue_strategy(
                    self.consumer_config
                        .allocate_message_queue_strategy
                        .clone()
                        .expect(
                            "allocate_message_queue_strategy is null, please set it before start",
                        ),
                );
                self.rebalance_impl
                    .set_mq_client_factory(client_instance.clone());
                if self.pull_api_wrapper.is_none() {
                    self.pull_api_wrapper = Some(ArcMut::new(PullAPIWrapper::new(
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
                        self.offset_store = Some(ArcMut::new(OffsetStore::new_with_local(
                            LocalFileOffsetStore::new(
                                client_instance.clone(),
                                self.consumer_config.consumer_group.clone(),
                            ),
                        )));
                    }
                    MessageModel::Clustering => {
                        self.offset_store = Some(ArcMut::new(OffsetStore::new_with_remote(
                            RemoteBrokerOffsetStore::new(
                                client_instance.clone(),
                                self.consumer_config.consumer_group.clone(),
                            ),
                        )));
                    }
                }
                self.offset_store.as_mut().unwrap().load().await?;

                if let Some(message_listener) = self.message_listener.as_ref() {
                    if message_listener.message_listener_concurrently.is_some() {
                        let (listener, _) = message_listener
                            .message_listener_concurrently
                            .clone()
                            .unwrap();
                        self.consume_orderly = false;
                        let consume_message_concurrently_service =
                            ArcMut::new(ConsumeMessageConcurrentlyService::new(
                                self.client_config.clone(),
                                self.consumer_config.clone(),
                                self.consumer_config.consumer_group.clone(),
                                listener.clone().expect("listener is None"),
                                self.default_mqpush_consumer_impl.clone(),
                            ));
                        self.consume_message_service =
                            Some(ArcMut::new(ConsumeMessageServiceGeneral::new(
                                Some(consume_message_concurrently_service),
                                None,
                            )));
                        let consume_message_pop_concurrently_service =
                            ArcMut::new(ConsumeMessagePopConcurrentlyService::new(
                                self.client_config.clone(),
                                self.consumer_config.clone(),
                                self.consumer_config.consumer_group.clone(),
                                listener.expect("listener is None"),
                            ));

                        self.consume_message_pop_service =
                            Some(ArcMut::new(ConsumeMessagePopServiceGeneral::new(
                                Some(consume_message_pop_concurrently_service),
                                None,
                            )));
                    } else if message_listener.message_listener_orderly.is_some() {
                        let (listener, _) =
                            message_listener.message_listener_orderly.clone().unwrap();
                        self.consume_orderly = true;
                        let consume_message_orderly_service =
                            ArcMut::new(ConsumeMessageOrderlyService::new(
                                self.client_config.clone(),
                                self.consumer_config.clone(),
                                self.consumer_config.consumer_group.clone(),
                                listener.clone().expect("listener is None"),
                                self.default_mqpush_consumer_impl.clone(),
                            ));
                        self.consume_message_service =
                            Some(ArcMut::new(ConsumeMessageServiceGeneral::new(
                                None,
                                Some(consume_message_orderly_service),
                            )));

                        let consume_message_pop_orderly_service =
                            ArcMut::new(ConsumeMessagePopOrderlyService);
                        self.consume_message_pop_service =
                            Some(ArcMut::new(ConsumeMessagePopServiceGeneral::new(
                                None,
                                Some(consume_message_pop_orderly_service),
                            )));
                    }
                }

                if let Some(consume_message_concurrently_service) =
                    self.consume_message_service.as_mut()
                {
                    consume_message_concurrently_service.start();
                }

                if let Some(consume_message_orderly_service) =
                    self.consume_message_pop_service.as_mut()
                {
                    consume_message_orderly_service.start();
                }
                self.client_instance
                    .as_mut()
                    .unwrap()
                    .register_consumer(
                        self.consumer_config.consumer_group.as_ref(),
                        MQConsumerInnerImpl {
                            default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                        },
                    )
                    .await;
                let cloned = self.client_instance.as_mut().cloned().unwrap();
                self.client_instance.as_mut().unwrap().start(cloned).await?;
                info!(
                    "the consumer [{}] start OK, message_model={}, isUnitMode={}",
                    self.consumer_config.consumer_group,
                    self.consumer_config.message_model,
                    self.consumer_config.unit_mode
                );
                *self.service_state = ServiceState::Running;
            }
            ServiceState::Running => {
                return mq_client_err!("The PushConsumer service state is Running");
            }
            ServiceState::ShutdownAlready => {
                return mq_client_err!("The PushConsumer service state is ShutdownAlready");
            }
            ServiceState::StartFailed => {
                return mq_client_err!(format!(
                    "The PushConsumer service state not OK, maybe started once,{:?},{}",
                    *self.service_state,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                ));
            }
        }
        self.update_topic_subscribe_info_when_subscription_changed()
            .await;
        let client_instance = self.client_instance.as_mut().unwrap();
        client_instance.check_client_in_broker().await?;
        if client_instance
            .send_heartbeat_to_all_broker_with_lock()
            .await
        {
            client_instance.re_balance_immediately();
        }
        Ok(())
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        let _lock = self.global_lock.lock().await;
        match *self.service_state {
            ServiceState::CreateJust => {
                warn!(
                    "the consumer [{}] do not start, so do nothing",
                    self.consumer_config.consumer_group
                );
            }
            ServiceState::Running => {
                if let Some(consume_message_concurrently_service) =
                    self.consume_message_service.as_mut()
                {
                    consume_message_concurrently_service
                        .shutdown(await_terminate_millis)
                        .await;
                }
                self.persist_consumer_offset().await;
                let client = self.client_instance.as_mut().unwrap();
                client
                    .unregister_consumer(self.consumer_config.consumer_group.as_str())
                    .await;
                client.shutdown().await;
                info!(
                    "the consumer [{}] shutdown OK",
                    self.consumer_config.consumer_group.as_str()
                );
                self.rebalance_impl.destroy();
                *self.service_state = ServiceState::ShutdownAlready;
            }
            ServiceState::ShutdownAlready => {
                warn!(
                    "the consumer [{}] has been shutdown, do nothing",
                    self.consumer_config.consumer_group
                );
            }
            ServiceState::StartFailed => {
                warn!(
                    "the consumer [{}] start failed, do nothing",
                    self.consumer_config.consumer_group
                );
            }
        }
        drop(_lock);
    }

    async fn update_topic_subscribe_info_when_subscription_changed(&mut self) {
        if DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED {
            return;
        }
        let sub_table = self.rebalance_impl.get_subscription_inner();
        let sub_table_inner = sub_table.read().await;
        let keys = sub_table_inner.keys().clone();
        let client = self.client_instance.as_mut().unwrap();
        for topic in keys {
            client
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
        }
    }

    fn check_config(&mut self) -> Result<()> {
        Validators::check_group(self.consumer_config.consumer_group.as_str())?;
        if self.consumer_config.consumer_group.is_empty() {
            return mq_client_err!(format!(
                "consumer_group is empty, {}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.consumer_group == DEFAULT_CONSUMER_GROUP {
            return mq_client_err!(format!(
                "consumer_group can not equal {} please specify another one.{}",
                DEFAULT_CONSUMER_GROUP,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self
            .consumer_config
            .allocate_message_queue_strategy
            .is_none()
        {
            return mq_client_err!(format!(
                "allocate_message_queue_strategy is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.message_listener.is_none() {
            return mq_client_err!(format!(
                "messageListener is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self
            .consumer_config
            .message_listener
            .as_ref()
            .unwrap()
            .message_listener_orderly
            .is_none()
            && self
                .consumer_config
                .message_listener
                .as_ref()
                .unwrap()
                .message_listener_concurrently
                .is_none()
        {
            return mq_client_err!(format!(
                "messageListener must be instanceof MessageListenerOrderly or \
                 MessageListenerConcurrently{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        let consume_thread_min = self.consumer_config.consume_thread_min;
        let consume_thread_max = self.consumer_config.consume_thread_max;
        if !(1..=1000).contains(&consume_thread_min) {
            return mq_client_err!(format!(
                "consumeThreadMin Out of range [1, 1000]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }
        if !(1..=1000).contains(&consume_thread_max) {
            return mq_client_err!(format!(
                "consumeThreadMax Out of range [1, 1000]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }
        if consume_thread_min > consume_thread_max {
            return mq_client_err!(format!(
                "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
                consume_thread_min, consume_thread_max
            ));
        }

        if self.consumer_config.consume_concurrently_max_span < 1
            || self.consumer_config.consume_concurrently_max_span > 65535
        {
            return mq_client_err!(format!(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_threshold_for_queue < 1
            || self.consumer_config.pull_threshold_for_queue > 65535
        {
            return mq_client_err!(format!(
                "pullThresholdForQueue Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_threshold_for_topic != -1
            && (self.consumer_config.pull_threshold_for_topic < 1
                || self.consumer_config.pull_threshold_for_topic > 6553500)
        {
            return mq_client_err!(format!(
                "pullThresholdForTopic Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_threshold_size_for_queue < 1
            || self.consumer_config.pull_threshold_size_for_queue > 1024
        {
            return mq_client_err!(format!(
                "pullThresholdSizeForQueue Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_threshold_size_for_topic != -1
            && (self.consumer_config.pull_threshold_size_for_topic < 1
                || self.consumer_config.pull_threshold_size_for_topic > 102400)
        {
            return mq_client_err!(format!(
                "pullThresholdSizeForTopic Out of range [1, 102400]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_interval > 65535 {
            return mq_client_err!(format!(
                "pullInterval Out of range [0, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.consume_message_batch_max_size < 1
            || self.consumer_config.consume_message_batch_max_size > 1024
        {
            return mq_client_err!(format!(
                "consumeMessageBatchMaxSize Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pull_batch_size < 1 || self.consumer_config.pull_batch_size > 1024 {
            return mq_client_err!(format!(
                "pullBatchSize Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pop_invisible_time < MIN_POP_INVISIBLE_TIME
            || self.consumer_config.pop_invisible_time > MAX_POP_INVISIBLE_TIME
        {
            return mq_client_err!(format!(
                "popInvisibleTime Out of range [{}, {}]{}",
                MIN_POP_INVISIBLE_TIME,
                MAX_POP_INVISIBLE_TIME,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        if self.consumer_config.pop_batch_nums < 1 || self.consumer_config.pop_batch_nums > 32 {
            return mq_client_err!(format!(
                "popBatchNums Out of range [1, 32]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ));
        }

        Ok(())
    }

    async fn copy_subscription(&mut self) -> Result<()> {
        let sub = self.consumer_config.subscription();
        if !sub.is_empty() {
            for (topic, sub_expression) in sub.as_ref() {
                let subscription_data = FilterAPI::build_subscription_data(topic, sub_expression)
                    .map_err(|e| {
                    MQClientError::MQClientErr(ClientErr::new(format!(
                        "buildSubscriptionData exception, {}",
                        e
                    )))
                })?;
                self.rebalance_impl
                    .put_subscription_data(topic.clone(), subscription_data)
                    .await;
            }
        }
        if self.message_listener.is_none() {
            self.message_listener = self.consumer_config.message_listener.clone();
        }

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {}
            MessageModel::Clustering => {
                let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(
                    self.consumer_config.consumer_group.as_str(),
                ));
                let subscription_data = FilterAPI::build_subscription_data(
                    retry_topic.as_ref(),
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                )
                .map_err(|e| {
                    MQClientError::MQClientErr(ClientErr::new(format!(
                        "buildSubscriptionData exception, {}",
                        e
                    )))
                })?;
                self.rebalance_impl
                    .put_subscription_data(retry_topic, subscription_data)
                    .await;
            }
        }
        Ok(())
    }

    pub fn register_consume_message_hook(&mut self, hook: impl ConsumeMessageHook) {
        unimplemented!("registerConsumeMessageHook");
    }

    pub fn register_message_listener(&mut self, message_listener: Option<ArcMut<MessageListener>>) {
        self.message_listener = message_listener;
    }

    pub async fn subscribe(
        &mut self,
        topic: CheetahString,
        sub_expression: CheetahString,
    ) -> Result<()> {
        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_expression);
        if let Err(e) = subscription_data {
            return mq_client_err!(format!("buildSubscriptionData exception, {}", e));
        }
        let subscription_data = subscription_data.unwrap();
        self.rebalance_impl
            .put_subscription_data(topic, subscription_data)
            .await;
        if let Some(ref mut client_instance) = self.client_instance {
            client_instance
                .send_heartbeat_to_all_broker_with_lock()
                .await;
        }
        Ok(())
    }

    pub async fn execute_pull_request_immediately(&mut self, pull_request: PullRequest) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pull_request_immediately(pull_request)
            .await;
    }
    pub fn execute_pull_request_later(&mut self, pull_request: PullRequest, time_delay: u64) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pull_request_later(pull_request, time_delay);
    }

    pub async fn execute_pop_request_immediately(&mut self, pop_request: PopRequest) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pop_pull_request_immediately(pop_request)
            .await;
    }
    pub fn execute_pop_request_later(&mut self, pop_request: PopRequest, time_delay: u64) {
        self.client_instance
            .as_mut()
            .unwrap()
            .pull_message_service
            .execute_pop_pull_request_later(pop_request, time_delay);
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        if let Some(consume_message_service) = self.consume_message_service.as_ref() {
            Some(
                consume_message_service
                    .consume_message_directly(msg, broker_name)
                    .await,
            )
        } else if let Some(consume_message_pop_service) = self.consume_message_pop_service.as_ref()
        {
            Some(
                consume_message_pop_service
                    .consume_message_directly(msg, broker_name)
                    .await,
            )
        } else {
            None
        }
    }

    pub(crate) async fn pop_message(&mut self, pop_request: PopRequest) {
        unimplemented!("popMessage");
    }

    pub(crate) async fn pull_message(&mut self, mut pull_request: PullRequest) {
        //let process_queue = pull_request.get_process_queue_mut();
        if pull_request.process_queue.is_dropped() {
            info!("the pull request[{}] is dropped.", pull_request);
            return;
        }
        pull_request
            .process_queue
            .set_last_pull_timestamp(get_current_millis());
        if let Err(e) = self.make_sure_state_ok() {
            warn!("pullMessage exception, consumer state not ok {}", e);
            self.execute_pull_request_later(
                pull_request,
                self.pull_time_delay_mills_when_exception,
            );
            return;
        }
        if self.pause.load(Ordering::Acquire) {
            warn!(
                "consumer was paused, execute pull request later. instanceName={}, group={}",
                self.client_config.instance_name, self.consumer_config.consumer_group
            );
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        let cached_message_count = pull_request.process_queue.msg_count();
        let cached_message_size_in_mib = pull_request.process_queue.msg_size() / _1MB;
        if cached_message_count > self.consumer_config.pull_threshold_for_queue as u64 {
            if self.queue_flow_control_times % 1000 == 0 {
                let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                let first_key_value = msg_tree_map.first_key_value().unwrap();
                let last_key_value = msg_tree_map.last_key_value().unwrap();
                warn!(
                    "the cached message count exceeds the threshold {}, so do flow control, \
                     minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, \
                     flowControlTimes={}",
                    self.consumer_config.pull_threshold_for_queue,
                    first_key_value.0,
                    last_key_value.0,
                    cached_message_count,
                    cached_message_size_in_mib,
                    pull_request.to_string(),
                    self.queue_flow_control_times
                );
            }
            self.execute_pull_request_later(
                pull_request,
                PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
            );

            self.queue_flow_control_times += 1;
            return;
        }

        if cached_message_size_in_mib > self.consumer_config.pull_threshold_size_for_queue as u64 {
            if self.queue_flow_control_times % 1000 == 0 {
                let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                let first_key_value = msg_tree_map.first_key_value().unwrap();
                let last_key_value = msg_tree_map.last_key_value().unwrap();
                warn!(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, \
                     minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, \
                     flowControlTimes={}",
                    self.consumer_config.pull_threshold_size_for_queue,
                    first_key_value.0,
                    last_key_value.0,
                    cached_message_count,
                    cached_message_size_in_mib,
                    pull_request.to_string(),
                    self.queue_flow_control_times
                );
            }
            self.execute_pull_request_later(
                pull_request,
                PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
            );
            self.queue_flow_control_times += 1;
            return;
        }

        if !self.consume_orderly {
            let max_span = pull_request.process_queue.get_max_span().await;
            if max_span > self.consumer_config.consume_concurrently_max_span as u64 {
                if self.queue_max_span_flow_control_times % 1000 == 0 {
                    let msg_tree_map = pull_request.process_queue.msg_tree_map.read().await;
                    let first_key_value = msg_tree_map.first_key_value().unwrap();
                    let last_key_value = msg_tree_map.last_key_value().unwrap();
                    warn!(
                        "the queue's messages, span too long, so do flow control, minOffset={}, \
                         maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        first_key_value.0,
                        last_key_value.0,
                        max_span,
                        pull_request.to_string(),
                        self.queue_max_span_flow_control_times
                    );
                }
                self.queue_max_span_flow_control_times += 1;
                self.execute_pull_request_later(
                    pull_request,
                    PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL,
                );
                return;
            }
        } else if pull_request.get_process_queue().is_locked() {
            if !pull_request.is_previously_locked() {
                let offset = match self
                    .rebalance_impl
                    .compute_pull_from_where_with_exception(pull_request.get_message_queue())
                    .await
                {
                    Ok(value) => {
                        if value < 0 {
                            error!(
                                "Failed to compute pull offset, pullResult: {}",
                                pull_request
                            );
                            self.execute_pull_request_later(
                                pull_request,
                                self.pull_time_delay_mills_when_exception,
                            );

                            return;
                        }
                        value
                    }
                    Err(e) => {
                        error!(
                            "Failed to compute pull offset, pullResult: {}, {}",
                            pull_request, e
                        );
                        self.execute_pull_request_later(
                            pull_request,
                            self.pull_time_delay_mills_when_exception,
                        );

                        return;
                    }
                };
                let broker_busy = offset < pull_request.get_next_offset();
                info!(
                    "the first time to pull message, so fix offset from broker. pullRequest: {} \
                     NewOffset: {} brokerBusy: {}",
                    pull_request.to_string(),
                    offset,
                    broker_busy
                );
                if broker_busy {
                    warn!(
                        "[NOTIFYME]the first time to pull message, but pull request offset larger \
                         than broker consume offset. pullRequest: {} NewOffset: {}",
                        pull_request.to_string(),
                        offset
                    )
                }
                pull_request.set_previously_locked(true);
                pull_request.set_next_offset(offset);
            }
        } else {
            info!(
                "pull message later because not locked in broker, {}",
                pull_request
            );
            self.execute_pull_request_later(
                pull_request,
                self.pull_time_delay_mills_when_exception,
            );
            return;
        }
        let message_queue = pull_request.get_message_queue().clone();
        let inner = self.rebalance_impl.get_subscription_inner();
        let guard = inner.read().await;
        let subscription_data = guard.get(message_queue.get_topic()).cloned();

        if subscription_data.is_none() {
            error!(
                "find the consumer's subscription failed, {}, {}",
                message_queue, self.consumer_config.consumer_group
            );
            self.execute_pull_request_later(
                pull_request,
                self.pull_time_delay_mills_when_exception,
            );
            return;
        }
        let begin_timestamp = Instant::now();
        let topic = message_queue.get_topic().to_string();

        let message_queue_inner = message_queue.clone();
        let next_offset = pull_request.next_offset;
        let mut commit_offset_enable = false;
        let mut commit_offset_value = 0;
        if MessageModel::Clustering == self.consumer_config.message_model {
            commit_offset_value = self
                .offset_store
                .as_ref()
                .unwrap()
                .read_offset(&message_queue, ReadOffsetType::ReadFromMemory)
                .await;
            if commit_offset_value > 0 {
                commit_offset_enable = true;
            }
        }
        let mut sub_expression = None;
        let mut class_filter = false;
        if subscription_data.is_some() {
            let subscription_data = subscription_data.as_ref().unwrap();
            if self.consumer_config.post_subscription_when_pull
                && !subscription_data.class_filter_mode
            {
                sub_expression = Some(subscription_data.sub_string.clone());
            }
            class_filter = subscription_data.class_filter_mode
        }
        let sys_flag = PullSysFlag::build_sys_flag(
            commit_offset_enable,
            true,
            sub_expression.is_some(),
            class_filter,
        );
        let subscription_data = subscription_data.unwrap();
        let this = self.default_mqpush_consumer_impl.clone().unwrap();
        let result = self
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .pull_kernel_impl(
                &message_queue,
                sub_expression.unwrap_or_default(),
                subscription_data.expression_type.clone(),
                subscription_data.sub_version,
                next_offset,
                self.consumer_config.pull_batch_size as i32,
                self.consumer_config.pull_batch_size_in_bytes as i32,
                sys_flag as i32,
                commit_offset_value,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                CommunicationMode::Async,
                DefaultPullCallback {
                    push_consumer_impl: this,
                    message_queue_inner: Some(message_queue_inner),
                    subscription_data: Some(subscription_data),
                    pull_request: Some(pull_request.clone()),
                },
            )
            .await;

        if let Err(e) = result {
            error!(
                "pullKernelImpl exception, {}, cost: {}ms, {}",
                pull_request,
                begin_timestamp.elapsed().as_millis(),
                e
            );
            self.execute_pull_request_later(
                pull_request,
                self.pull_time_delay_mills_when_exception,
            );
        }
    }

    fn make_sure_state_ok(&self) -> Result<()> {
        if *self.service_state != ServiceState::Running {
            return mq_client_err!(format!(
                "The consumer service state not OK, {},{}",
                *self.service_state,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
            ));
        }
        Ok(())
    }

    pub(crate) async fn correct_tags_offset(&mut self, pull_request: &PullRequest) {
        if pull_request.process_queue.msg_count() == 0 {
            self.offset_store
                .as_mut()
                .unwrap()
                .update_offset(
                    pull_request.get_message_queue(),
                    pull_request.next_offset,
                    true,
                )
                .await;
        }
    }

    pub fn try_reset_pop_retry_topic(msgs: &mut [ArcMut<MessageClientExt>], consumer_group: &str) {
        let pop_retry_prefix = format!(
            "{}{}_{}",
            mix_all::RETRY_GROUP_TOPIC_PREFIX,
            consumer_group,
            "_"
        );
        for msg in msgs.iter_mut() {
            if msg.get_topic().starts_with(&pop_retry_prefix) {
                let normal_topic = KeyBuilder::parse_normal_topic(msg.get_topic(), consumer_group);

                if !normal_topic.is_empty() {
                    msg.set_topic(CheetahString::from_string(normal_topic));
                }
            }
        }
    }

    pub fn reset_retry_and_namespace(
        &mut self,
        msgs: &mut [ArcMut<MessageClientExt>],
        consumer_group: &str,
    ) {
        let group_topic = mix_all::get_retry_topic(consumer_group);
        let namespace = self.client_config.get_namespace().unwrap_or_default();
        for msg in msgs.iter_mut() {
            if let Some(retry_topic) = msg.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_RETRY_TOPIC,
            )) {
                if group_topic == msg.get_topic().as_str() {
                    msg.set_topic(retry_topic);
                }
            }

            if !namespace.is_empty() {
                let topic = msg.get_topic().to_string();
                msg.set_topic(CheetahString::from_string(
                    NamespaceUtil::without_namespace_with_namespace(
                        topic.as_str(),
                        namespace.as_str(),
                    ),
                ));
            }
        }
    }

    #[inline]
    pub fn has_hook(&self) -> bool {
        !self.consume_message_hook_list.is_empty()
    }

    pub fn execute_hook_before(&self, context: &mut Option<ConsumeMessageContext>) {
        for hook in self.consume_message_hook_list.iter() {
            hook.consume_message_before(context.as_mut());
        }
    }

    pub fn execute_hook_after(&self, context: &mut Option<ConsumeMessageContext>) {
        for hook in self.consume_message_hook_list.iter() {
            hook.consume_message_after(context.as_mut());
        }
    }

    pub async fn send_message_back(
        &mut self,
        msg: &mut MessageExt,
        delay_level: i32,
        mq: &MessageQueue,
    ) -> Result<()> {
        self.send_message_back_with_broker_name(msg, delay_level, None, Some(mq))
            .await
    }
    pub async fn send_message_back_with_broker_name(
        &mut self,
        msg: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<CheetahString>,
        mq: Option<&MessageQueue>,
    ) -> Result<()> {
        let need_retry = true;
        if broker_name.is_some()
            && broker_name
                .as_ref()
                .unwrap()
                .starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
            && mq.is_some()
            && mq
                .unwrap()
                .get_broker_name()
                .starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
        {
            let _ = self.send_message_back_as_normal_message(msg).await;
        } else {
            let broker_addr = if let Some(ref broker_name_) = broker_name {
                self.client_instance
                    .as_mut()
                    .unwrap()
                    .find_broker_address_in_publish(broker_name_.as_ref())
                    .await
                    .unwrap()
            } else {
                CheetahString::from_string(msg.store_host.to_string())
            };
            let max_consume_retry_times = self.get_max_reconsume_times();
            let result = self
                .client_instance
                .as_mut()
                .unwrap()
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .consumer_send_message_back(
                    broker_addr.as_str(),
                    broker_name.as_ref().unwrap().as_str(),
                    msg,
                    self.consumer_config.consumer_group.as_str(),
                    delay_level,
                    5000,
                    max_consume_retry_times,
                )
                .await;
            if let Err(e) = result {
                error!("send message back error: {}", e);

                self.send_message_back_as_normal_message(msg).await?;
            }
        }
        msg.set_topic(CheetahString::from_string(
            NamespaceUtil::without_namespace_with_namespace(
                msg.get_topic().as_str(),
                self.client_config
                    .get_namespace()
                    .unwrap_or_default()
                    .as_str(),
            ),
        ));
        Ok(())
    }

    async fn send_message_back_as_normal_message(&mut self, msg: &MessageExt) -> Result<()> {
        let topic = mix_all::get_retry_topic(self.consumer_config.consumer_group());
        let body = msg.get_body().cloned();
        let mut new_msg = Message::new_body(topic.as_str(), body);
        let origin_msg_id =
            MessageAccessor::get_origin_message_id(&new_msg).unwrap_or(msg.msg_id.clone());
        MessageAccessor::set_origin_message_id(&mut new_msg, origin_msg_id);
        new_msg.set_flag(msg.get_flag());
        MessageAccessor::set_properties(&mut new_msg, msg.get_properties().clone());
        MessageAccessor::put_property(
            &mut new_msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            msg.get_topic().to_owned(),
        );
        MessageAccessor::set_reconsume_time(
            &mut new_msg,
            CheetahString::from_string((msg.reconsume_times + 1).to_string()),
        );
        MessageAccessor::set_max_reconsume_times(
            &mut new_msg,
            CheetahString::from_string(self.get_max_reconsume_times().to_string()),
        );
        MessageAccessor::clear_property(&mut new_msg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
        new_msg.set_delay_time_level(3 + msg.reconsume_times);
        self.client_instance
            .as_mut()
            .unwrap()
            .default_producer
            .send(new_msg)
            .await?;
        Ok(())
    }

    pub fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            16
        } else {
            self.consumer_config.max_reconsume_times
        }
    }
}

impl MQConsumerInner for DefaultMQPushConsumerImpl {
    fn group_name(&self) -> CheetahString {
        self.consumer_config.consumer_group().clone()
    }

    fn message_model(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        let inner = self
            .rebalance_impl
            .rebalance_impl_inner
            .subscription_inner
            .clone();

        let handle = Handle::current();
        thread::spawn(move || {
            handle.block_on(async move {
                let inner = inner.read().await;
                inner.values().cloned().collect()
            })
        })
        .join()
        .unwrap()
    }

    fn do_rebalance(&self) {
        todo!()
    }

    async fn try_rebalance(&self) -> Result<bool> {
        if !self.pause.load(Ordering::Acquire) {
            return Ok(self
                .rebalance_impl
                .mut_from_ref()
                .do_rebalance(self.consume_orderly)
                .await);
        }
        Ok(false)
    }

    async fn persist_consumer_offset(&self) {
        if let Err(err) = self.make_sure_state_ok() {
            error!(
                "group: {} persistConsumerOffset exception:{}",
                self.consumer_config.consumer_group, err
            );
        } else {
            let guard = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            let allocate_mq = guard.keys().cloned().collect::<HashSet<_>>();
            self.offset_store
                .as_ref()
                .unwrap()
                .mut_from_ref()
                .persist_all(&allocate_mq)
                .await;
        }
    }

    async fn update_topic_subscribe_info(
        &self,
        topic: CheetahString,
        info: &HashSet<MessageQueue>,
    ) {
        let sub_table = self.rebalance_impl.get_subscription_inner();
        let sub_table_inner = sub_table.read().await;
        if sub_table_inner.contains_key(&topic) {
            let mut guard = self
                .rebalance_impl
                .rebalance_impl_inner
                .topic_subscribe_info_table
                .write()
                .await;
            guard.insert(topic, info.clone());
        }
    }

    async fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        let sub_table = self.rebalance_impl.get_subscription_inner();
        let sub_table_inner = sub_table.read().await;
        if sub_table_inner.contains_key(topic) {
            drop(sub_table_inner);
            let guard = self
                .rebalance_impl
                .rebalance_impl_inner
                .topic_subscribe_info_table
                .read()
                .await;
            return !guard.contains_key(topic);
        }
        false
    }

    fn is_unit_mode(&self) -> bool {
        self.consumer_config.unit_mode
    }

    fn consumer_running_info(&self) -> ConsumerRunningInfo {
        todo!()
    }
}
