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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock as StdRwLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DEFAULT_CONSUMER_GROUP;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::utils::util_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::body::pop_process_queue_info::PopProcessQueueInfo;
use rocketmq_remoting::protocol::body::process_queue_info::ProcessQueueInfo;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::consumer::ack_callback::AckCallback;
use crate::consumer::ack_result::AckResult;
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
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::consumer::pop_callback::DefaultPopCallback;
use crate::consumer::pop_result::PopResult;
use crate::consumer::pop_status::PopStatus;
use crate::consumer::pull_callback::DefaultPullCallback;
use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::hook::consume_message_hook::ConsumeMessageHookArc;
use crate::hook::filter_message_context::FilterMessageContext;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;

const PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL: u64 = 50;
pub(crate) const PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL: u64 = 20;
const PULL_TIME_DELAY_MILLS_WHEN_SUSPEND: u64 = 1000;
const BROKER_SUSPEND_MAX_TIME_MILLIS: u64 = 1000 * 15;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: u64 = 1000 * 30;
const MAX_POP_INVISIBLE_TIME: u64 = 300000;
const MIN_POP_INVISIBLE_TIME: u64 = 5000;
const ASYNC_TIMEOUT: u64 = 3000;
const RESET_OFFSET_MAX_WAIT: Duration = Duration::from_secs(10);
const OFFSET_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const QUERY_UNIQ_KEY_LOOKBACK_MILLIS: u64 = 3 * 24 * 60 * 60 * 1000;
//const DO_NOT_UPDATE_TOPIC_SUBSCRIBE_INFO_WHEN_SUBSCRIPTION_CHANGED: bool = false;
const _1MB: u64 = 1024 * 1024;

pub struct DefaultMQPushConsumerImpl {
    pub(crate) global_lock: Arc<Mutex<()>>,
    lifecycle_transition: Mutex<()>,
    pub(crate) pull_time_delay_mills_when_exception: u64,
    client_config: ArcSwap<ClientConfig>,
    pub(crate) consumer_config: Arc<ArcSwap<ConsumerConfig>>,
    pub(crate) rebalance_impl: ArcMut<RebalancePushImpl>,
    filter_message_hook_list: StdRwLock<Vec<Arc<dyn FilterMessageHook + Send + Sync>>>,
    consume_message_hook_list: StdRwLock<Vec<ConsumeMessageHookArc>>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    service_state: StdRwLock<ServiceState>,
    client_instance: StdRwLock<Option<ArcMut<MQClientInstance>>>,
    pull_api_wrapper: StdRwLock<Option<Arc<PullAPIWrapper>>>,
    pause: Arc<AtomicBool>,
    consume_orderly: AtomicBool,
    message_listener: StdRwLock<Option<Arc<MessageListener>>>,
    offset_store: StdRwLock<Option<Arc<OffsetStore>>>,
    consume_message_service: StdRwLock<
        Option<Arc<ConsumeMessageServiceGeneral<ConsumeMessageConcurrentlyService, ConsumeMessageOrderlyService>>>,
    >,
    consume_message_pop_service: StdRwLock<
        Option<
            Arc<ConsumeMessagePopServiceGeneral<ConsumeMessagePopConcurrentlyService, ConsumeMessagePopOrderlyService>>,
        >,
    >,
    queue_flow_control_times: AtomicU64,
    queue_max_span_flow_control_times: AtomicU64,
    consumer_start_timestamp: AtomicU64,
    pub(crate) pop_delay_level: Arc<[i32; 16]>,
    self_reference: OnceLock<Weak<DefaultMQPushConsumerImpl>>,
}

impl DefaultMQPushConsumerImpl {
    pub fn new(
        client_config: ClientConfig,
        consumer_config: ConsumerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let consumer_config = Arc::new(ArcSwap::from_pointee(consumer_config));
        let rebalance_consumer_config = (*consumer_config.load_full()).clone();
        let this = Self {
            global_lock: Arc::new(Default::default()),
            lifecycle_transition: Mutex::new(()),
            pull_time_delay_mills_when_exception: 3_000,
            client_config: ArcSwap::from_pointee(client_config.clone()),
            consumer_config: consumer_config.clone(),
            rebalance_impl: ArcMut::new(RebalancePushImpl::new(client_config, rebalance_consumer_config)),
            filter_message_hook_list: StdRwLock::new(vec![]),
            consume_message_hook_list: StdRwLock::new(vec![]),
            rpc_hook,
            service_state: StdRwLock::new(ServiceState::CreateJust),
            client_instance: StdRwLock::new(None),
            pull_api_wrapper: StdRwLock::new(None),
            pause: Arc::new(AtomicBool::new(false)),
            consume_orderly: AtomicBool::new(false),
            message_listener: StdRwLock::new(None),
            offset_store: StdRwLock::new(None),
            consume_message_service: StdRwLock::new(None),
            consume_message_pop_service: StdRwLock::new(None),
            queue_flow_control_times: AtomicU64::new(0),
            queue_max_span_flow_control_times: AtomicU64::new(0),
            consumer_start_timestamp: AtomicU64::new(current_millis()),
            pop_delay_level: Arc::new([
                10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ]),
            self_reference: OnceLock::new(),
        };
        let wrapper = ArcMut::downgrade(&this.rebalance_impl);
        this.rebalance_impl.set_rebalance_impl(wrapper);
        this
    }

    pub(crate) fn new_with_config_store(
        client_config: ClientConfig,
        consumer_config: Arc<ArcSwap<ConsumerConfig>>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let initial_config = (*consumer_config.load_full()).clone();
        let mut this = Self::new(client_config, initial_config, rpc_hook);
        this.consumer_config = consumer_config;
        this
    }

    pub(crate) fn initialize_self_reference(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let _ = self.self_reference.set(weak.clone());
        self.rebalance_impl.set_default_mqpush_consumer_impl(weak);
        if let Some(consume_message_service) = self.consume_message_service() {
            if let Some(service) = consume_message_service.get_consume_message_concurrently_service() {
                *service.default_mqpush_consumer_impl.write() = Some(Arc::downgrade(self));
            }
        }
    }

    pub fn get_mq_client_factory(&self) -> Option<ArcMut<MQClientInstance>> {
        self.component_snapshot(&self.client_instance)
    }

    #[inline]
    pub(crate) fn client_id(&self) -> Option<CheetahString> {
        self.get_mq_client_factory()
            .as_ref()
            .map(|client_instance| client_instance.client_id.clone())
    }

    pub fn set_mq_client_factory(&self, client_instance: ArcMut<MQClientInstance>) {
        self.rebalance_impl.set_mq_client_factory(client_instance.clone());
        self.set_component(&self.client_instance, Some(client_instance));
    }

    pub(crate) fn set_use_tls(&self, use_tls: bool) {
        self.client_config.rcu(|current| {
            let mut next = (**current).clone();
            next.set_use_tls(use_tls);
            Arc::new(next)
        });
        self.rebalance_impl.set_use_tls(use_tls);
    }

    #[inline]
    pub fn is_consume_orderly(&self) -> bool {
        self.consume_orderly.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_consume_orderly(&self, consume_orderly: bool) {
        self.consume_orderly.store(consume_orderly, Ordering::Release);
    }

    #[inline]
    pub fn is_pause(&self) -> bool {
        self.pause.load(Ordering::Acquire)
    }

    pub fn offset_store(&self) -> Option<Arc<OffsetStore>> {
        self.component_snapshot(&self.offset_store)
    }

    pub fn set_offset_store(&self, offset_store: Option<Arc<OffsetStore>>) {
        self.set_component(&self.offset_store, offset_store);
    }

    pub(crate) fn sync_rebalance_consumer_config(&self, consumer_config: ConsumerConfig) {
        self.rebalance_impl.replace_consumer_config(consumer_config);
    }

    pub(crate) fn consumer_config_snapshot(&self) -> Arc<ConsumerConfig> {
        self.consumer_config.load_full()
    }

    pub(crate) fn client_config_snapshot(&self) -> Arc<ClientConfig> {
        self.client_config.load_full()
    }

    pub(crate) fn with_namespace(&self, resource: impl Into<CheetahString>) -> CheetahString {
        let resource = resource.into();
        let namespace = self.client_config_snapshot().resolved_namespace().unwrap_or_default();
        if namespace.is_empty() || NamespaceUtil::is_already_with_namespace(resource.as_str(), namespace.as_str()) {
            resource
        } else {
            NamespaceUtil::wrap_namespace(namespace, resource)
        }
    }

    pub(crate) fn pull_api_wrapper(&self) -> Option<Arc<PullAPIWrapper>> {
        self.component_snapshot(&self.pull_api_wrapper)
    }

    pub(crate) fn consume_message_service(
        &self,
    ) -> Option<Arc<ConsumeMessageServiceGeneral<ConsumeMessageConcurrentlyService, ConsumeMessageOrderlyService>>>
    {
        self.component_snapshot(&self.consume_message_service)
    }

    pub(crate) fn consume_message_pop_service(
        &self,
    ) -> Option<
        Arc<ConsumeMessagePopServiceGeneral<ConsumeMessagePopConcurrentlyService, ConsumeMessagePopOrderlyService>>,
    > {
        self.component_snapshot(&self.consume_message_pop_service)
    }

    fn service_state(&self) -> ServiceState {
        *self
            .service_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn set_service_state(&self, state: ServiceState) {
        *self
            .service_state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
    }

    fn component_snapshot<T: Clone>(&self, component: &StdRwLock<T>) -> T {
        component
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn set_component<T>(&self, component: &StdRwLock<T>, value: T) {
        *component.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
    }

    fn update_consumer_config(&self, update: impl Fn(&mut ConsumerConfig)) {
        self.consumer_config.rcu(|current| {
            let mut next = (**current).clone();
            update(&mut next);
            Arc::new(next)
        });
    }

    pub(crate) fn update_pull_thresholds_from_rebalance(
        &self,
        pull_threshold_for_queue: Option<u32>,
        pull_threshold_size_for_queue: Option<u32>,
    ) {
        self.update_consumer_config(|consumer_config| {
            if let Some(value) = pull_threshold_for_queue {
                consumer_config.pull_threshold_for_queue = value;
            }
            if let Some(value) = pull_threshold_size_for_queue {
                consumer_config.pull_threshold_size_for_queue = value;
            }
        });
    }
}

impl DefaultMQPushConsumerImpl {
    pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
        let _transition = self.lifecycle_transition.lock().await;
        self.start_transition().await
    }

    async fn start_transition(&self) -> rocketmq_error::RocketMQResult<()> {
        match self.service_state() {
            ServiceState::CreateJust => {
                let consumer_config = self.consumer_config_snapshot();
                info!(
                    "the consumer [{}] start beginning. message_model={}, isUnitMode={}",
                    consumer_config.consumer_group, consumer_config.message_model, consumer_config.unit_mode
                );
                self.set_service_state(ServiceState::StartFailed);
                // check all config
                self.check_config()?;
                //copy_subscription is can be removed
                self.copy_subscription().await?;
                if consumer_config.message_model() == MessageModel::Clustering {
                    self.client_config.rcu(|current| {
                        let mut next = (**current).clone();
                        next.change_instance_name_to_pid();
                        Arc::new(next)
                    });
                }
                let client_config = self.client_config_snapshot();
                let mut client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(client_config.as_ref().clone(), self.rpc_hook.clone());
                self.set_component(&self.client_instance, Some(client_instance.clone()));
                self.rebalance_impl
                    .set_consumer_group(consumer_config.consumer_group.clone());
                self.rebalance_impl.set_message_model(consumer_config.message_model);
                self.rebalance_impl.set_allocate_message_queue_strategy(
                    consumer_config.allocate_message_queue_strategy.clone().ok_or_else(|| {
                        mq_client_err!("allocate_message_queue_strategy is null, please set it before start")
                    })?,
                );
                self.rebalance_impl.set_mq_client_factory(client_instance.clone());
                if self.pull_api_wrapper().is_none() {
                    self.set_component(
                        &self.pull_api_wrapper,
                        Some(Arc::new(PullAPIWrapper::new(
                            client_instance.clone(),
                            consumer_config.consumer_group.clone(),
                            consumer_config.unit_mode,
                        ))),
                    );
                }
                if let Some(pull_api_wrapper) = self.pull_api_wrapper() {
                    pull_api_wrapper
                        .register_filter_message_hook(self.component_snapshot(&self.filter_message_hook_list));
                }
                let offset_store = if let Some(offset_store) = consumer_config.offset_store() {
                    offset_store
                } else {
                    let offset_store = Arc::new(match consumer_config.message_model {
                        MessageModel::Broadcasting => OffsetStore::new_with_local(LocalFileOffsetStore::new(
                            client_instance.clone(),
                            consumer_config.consumer_group.clone(),
                        )),
                        MessageModel::Clustering => OffsetStore::new_with_remote(RemoteBrokerOffsetStore::new(
                            client_instance.clone(),
                            consumer_config.consumer_group.clone(),
                        )),
                    });
                    self.update_consumer_config(|consumer_config| {
                        consumer_config.set_offset_store(Some(offset_store.clone()))
                    });
                    offset_store
                };
                self.set_offset_store(Some(offset_store.clone()));
                offset_store.load().await?;

                if let Some(message_listener) = self.component_snapshot(&self.message_listener) {
                    let self_reference = self
                        .self_reference
                        .get()
                        .cloned()
                        .ok_or_else(|| mq_client_err!("default_mqpush_consumer_impl is not initialized"))?;
                    let client_config_snapshot = self.client_config_snapshot();
                    let consumer_config_snapshot = Arc::new(consumer_config.as_ref().clone());
                    if let Some(listener) = message_listener.message_listener_concurrently.clone() {
                        self.set_consume_orderly(false);
                        let consume_message_concurrently_service = Arc::new(ConsumeMessageConcurrentlyService::new(
                            client_config_snapshot.clone(),
                            consumer_config_snapshot.clone(),
                            consumer_config_snapshot.consumer_group.clone(),
                            listener.clone(),
                            Some(self_reference.clone()),
                        ));
                        self.set_component(
                            &self.consume_message_service,
                            Some(Arc::new(ConsumeMessageServiceGeneral::new(
                                Some(consume_message_concurrently_service),
                                None,
                            ))),
                        );
                        let consume_message_pop_concurrently_service =
                            Arc::new(ConsumeMessagePopConcurrentlyService::new(
                                client_config_snapshot.clone(),
                                consumer_config_snapshot.clone(),
                                consumer_config_snapshot.consumer_group.clone(),
                                listener.clone(),
                                Some(self_reference.clone()),
                            ));

                        self.set_component(
                            &self.consume_message_pop_service,
                            Some(Arc::new(ConsumeMessagePopServiceGeneral::new(
                                Some(consume_message_pop_concurrently_service),
                                None,
                            ))),
                        );
                    } else if let Some(listener) = message_listener.message_listener_orderly.clone() {
                        self.set_consume_orderly(true);
                        let consume_message_orderly_service = Arc::new(ConsumeMessageOrderlyService::new(
                            client_config_snapshot.clone(),
                            consumer_config_snapshot.clone(),
                            consumer_config_snapshot.consumer_group.clone(),
                            listener.clone(),
                            Some(self_reference.clone()),
                        ));
                        self.set_component(
                            &self.consume_message_service,
                            Some(Arc::new(ConsumeMessageServiceGeneral::new(
                                None,
                                Some(consume_message_orderly_service),
                            ))),
                        );

                        let consume_message_pop_orderly_service = Arc::new(ConsumeMessagePopOrderlyService::new(
                            client_config_snapshot,
                            consumer_config_snapshot.clone(),
                            consumer_config_snapshot.consumer_group.clone(),
                            listener,
                            Some(self_reference),
                        ));
                        self.set_component(
                            &self.consume_message_pop_service,
                            Some(Arc::new(ConsumeMessagePopServiceGeneral::new(
                                None,
                                Some(consume_message_pop_orderly_service),
                            ))),
                        );
                    }
                }

                if let Some(consume_message_concurrently_service) = self.consume_message_service() {
                    consume_message_concurrently_service.start();
                }

                if let Some(consume_message_orderly_service) = self.consume_message_pop_service() {
                    consume_message_orderly_service.start();
                }
                let default_mqpush_consumer_impl = self
                    .self_reference
                    .get()
                    .and_then(Weak::upgrade)
                    .ok_or_else(|| mq_client_err!("default_mqpush_consumer_impl is None"))?;
                let register_ok = client_instance
                    .register_consumer(
                        consumer_config.consumer_group.as_ref(),
                        MQConsumerInnerImpl::from_push(default_mqpush_consumer_impl),
                    )
                    .await;
                if !register_ok {
                    self.set_service_state(ServiceState::CreateJust);
                    if let Some(consume_message_service) = self.consume_message_service() {
                        consume_message_service
                            .shutdown(consumer_config.await_termination_millis_when_shutdown)
                            .await;
                    }
                    if let Some(consume_message_pop_service) = self.consume_message_pop_service() {
                        consume_message_pop_service
                            .shutdown(consumer_config.await_termination_millis_when_shutdown)
                            .await;
                    }
                    return Err(mq_client_err!(format!(
                        "The consumer group[{}] has been created before, specify another name please.{}",
                        consumer_config.consumer_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                let client_instance_clone = client_instance.clone();
                client_instance.start(client_instance_clone).await?;
                info!(
                    "the consumer [{}] start OK, message_model={}, isUnitMode={}",
                    consumer_config.consumer_group, consumer_config.message_model, consumer_config.unit_mode
                );
                self.set_service_state(ServiceState::Running);
            }
            ServiceState::Running => {
                return Err(mq_client_err!("The PushConsumer service state is Running"));
            }
            ServiceState::ShutdownAlready => {
                return Err(mq_client_err!("The PushConsumer service state is ShutdownAlready"));
            }
            ServiceState::StartFailed => {
                return Err(mq_client_err!(format!(
                    "The PushConsumer service state not OK, maybe started once,{:?},{}",
                    self.service_state(),
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                )));
            }
        }
        self.complete_startup_after_running().await
    }

    async fn complete_startup_after_running(&self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_config = self.consumer_config_snapshot();
        if let Err(error) = self.run_startup_after_running_checks().await {
            warn!("Start the consumer {} fail. {}", consumer_config.consumer_group, error);
            self.shutdown_transition(consumer_config.await_termination_millis_when_shutdown)
                .await;
            return Err(error);
        }
        Ok(())
    }

    async fn run_startup_after_running_checks(&self) -> rocketmq_error::RocketMQResult<()> {
        self.update_topic_subscribe_info_when_subscription_changed().await;
        let mut client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.check_client_in_broker().await?;
        if client_instance.send_heartbeat_to_all_broker_with_lock().await {
            client_instance.re_balance_immediately();
        }
        Ok(())
    }

    pub async fn shutdown(&self, await_terminate_millis: u64) {
        let _transition = self.lifecycle_transition.lock().await;
        self.shutdown_transition(await_terminate_millis).await;
    }

    async fn shutdown_transition(&self, await_terminate_millis: u64) {
        match self.service_state() {
            ServiceState::CreateJust => {
                warn!(
                    "the consumer [{}] do not start, so do nothing",
                    self.consumer_config_snapshot().consumer_group
                );
            }
            ServiceState::Running => {
                if let Some(consume_message_concurrently_service) = self.consume_message_service() {
                    consume_message_concurrently_service
                        .shutdown(await_terminate_millis)
                        .await;
                }
                self.persist_consumer_offset().await;
                if let Some(offset_store) = self.offset_store() {
                    if !offset_store.shutdown(OFFSET_STORE_SHUTDOWN_TIMEOUT).await {
                        warn!(
                            "consumer [{}] offset store did not stop before timeout",
                            self.consumer_config_snapshot().consumer_group
                        );
                    }
                }
                let consumer_group = self.consumer_config_snapshot().consumer_group.clone();
                if let Some(mut client) = self.get_mq_client_factory() {
                    client.unregister_consumer(consumer_group.as_str()).await;
                    client.shutdown().await;
                } else {
                    warn!(
                        "consumer [{}] shutdown skipped client cleanup because MQClientInstance is not initialized",
                        self.consumer_config_snapshot().consumer_group
                    );
                }
                info!(
                    "the consumer [{}] shutdown OK",
                    self.consumer_config_snapshot().consumer_group.as_str()
                );
                self.rebalance_impl.destroy();
                self.set_service_state(ServiceState::ShutdownAlready);
            }
            ServiceState::ShutdownAlready => {
                warn!(
                    "the consumer [{}] has been shutdown, do nothing",
                    self.consumer_config_snapshot().consumer_group
                );
            }
            ServiceState::StartFailed => {
                warn!(
                    "the consumer [{}] start failed, do nothing",
                    self.consumer_config_snapshot().consumer_group
                );
            }
        }
    }

    async fn update_topic_subscribe_info_when_subscription_changed(&self) {
        let keys = self
            .rebalance_impl
            .get_subscription_inner()
            .iter()
            .map(|e| e.key().clone())
            .collect::<Vec<_>>();
        let Some(client) = self.get_mq_client_factory() else {
            warn!(
                "update topic subscribe info skipped because MQClientInstance is not initialized. group={}",
                self.consumer_config_snapshot().consumer_group
            );
            return;
        };
        for topic in &keys {
            client.update_topic_route_info_from_name_server_topic(topic).await;
        }
    }

    fn check_config(&self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_config = self.consumer_config_snapshot();
        Validators::check_group(consumer_config.consumer_group.as_str())?;
        if consumer_config.consumer_group.is_empty() {
            return Err(mq_client_err!(format!(
                "consumer_group is empty, {}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.consumer_group == DEFAULT_CONSUMER_GROUP {
            return Err(mq_client_err!(format!(
                "consumer_group can not equal {} please specify another one.{}",
                DEFAULT_CONSUMER_GROUP,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.allocate_message_queue_strategy.is_none() {
            return Err(mq_client_err!(format!(
                "allocate_message_queue_strategy is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.message_listener.is_none() {
            return Err(mq_client_err!(format!(
                "messageListener is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        let message_listener = consumer_config.message_listener.as_ref().ok_or_else(|| {
            mq_client_err!(format!(
                "messageListener is null{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            ))
        })?;
        if message_listener.message_listener_orderly.is_none()
            && message_listener.message_listener_concurrently.is_none()
        {
            return Err(mq_client_err!(format!(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        let valid_consume_timestamp = self
            .consumer_config_snapshot()
            .consume_timestamp
            .as_deref()
            .and_then(|timestamp| util_all::parse_date_to_millis(timestamp, util_all::YYYYMMDDHHMMSS))
            .is_some();
        if !valid_consume_timestamp {
            return Err(mq_client_err!(format!(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received {} {}",
                consumer_config.consume_timestamp.as_deref().unwrap_or("null"),
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        let consume_thread_min = consumer_config.consume_thread_min;
        let consume_thread_max = consumer_config.consume_thread_max;
        if !(1..=1000).contains(&consume_thread_min) {
            return Err(mq_client_err!(format!(
                "consumeThreadMin Out of range [1, 1000]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }
        if !(1..=1000).contains(&consume_thread_max) {
            return Err(mq_client_err!(format!(
                "consumeThreadMax Out of range [1, 1000]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }
        if consume_thread_min > consume_thread_max {
            return Err(mq_client_err!(format!(
                "consumeThreadMin ({}) is larger than consumeThreadMax ({})",
                consume_thread_min, consume_thread_max
            )));
        }

        if consumer_config.consume_concurrently_max_span < 1 || consumer_config.consume_concurrently_max_span > 65535 {
            return Err(mq_client_err!(format!(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_threshold_for_queue < 1 || consumer_config.pull_threshold_for_queue > 65535 {
            return Err(mq_client_err!(format!(
                "pullThresholdForQueue Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_threshold_for_topic != -1
            && (consumer_config.pull_threshold_for_topic < 1 || consumer_config.pull_threshold_for_topic > 6553500)
        {
            return Err(mq_client_err!(format!(
                "pullThresholdForTopic Out of range [1, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_threshold_size_for_queue < 1 || consumer_config.pull_threshold_size_for_queue > 1024 {
            return Err(mq_client_err!(format!(
                "pullThresholdSizeForQueue Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_threshold_size_for_topic != -1
            && (consumer_config.pull_threshold_size_for_topic < 1
                || consumer_config.pull_threshold_size_for_topic > 102400)
        {
            return Err(mq_client_err!(format!(
                "pullThresholdSizeForTopic Out of range [1, 102400]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_interval > 65535 {
            return Err(mq_client_err!(format!(
                "pullInterval Out of range [0, 65535]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.consume_message_batch_max_size < 1 || consumer_config.consume_message_batch_max_size > 1024 {
            return Err(mq_client_err!(format!(
                "consumeMessageBatchMaxSize Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pull_batch_size < 1 || consumer_config.pull_batch_size > 1024 {
            return Err(mq_client_err!(format!(
                "pullBatchSize Out of range [1, 1024]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pop_invisible_time < MIN_POP_INVISIBLE_TIME
            || consumer_config.pop_invisible_time > MAX_POP_INVISIBLE_TIME
        {
            return Err(mq_client_err!(format!(
                "popInvisibleTime Out of range [{}, {}]{}",
                MIN_POP_INVISIBLE_TIME,
                MAX_POP_INVISIBLE_TIME,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        if consumer_config.pop_batch_nums < 1 || consumer_config.pop_batch_nums > 32 {
            return Err(mq_client_err!(format!(
                "popBatchNums Out of range [1, 32]{}",
                FAQUrl::suggest_todo(FAQUrl::CLIENT_PARAMETER_CHECK_URL)
            )));
        }

        Ok(())
    }

    async fn copy_subscription(&self) -> rocketmq_error::RocketMQResult<()> {
        let sub = self.consumer_config_snapshot().subscription();
        if !sub.is_empty() {
            for (topic, sub_expression) in sub.as_ref() {
                let subscription_data = FilterAPI::build_subscription_data(topic, sub_expression)
                    .map_err(|e| RocketMQError::illegal_argument(format!("buildSubscriptionData exception, {e}")))?;
                self.rebalance_impl
                    .put_subscription_data(topic.clone(), subscription_data);
            }
        }
        if self.component_snapshot(&self.message_listener).is_none() {
            self.set_component(
                &self.message_listener,
                self.consumer_config_snapshot().message_listener.clone(),
            );
        }

        match self.consumer_config_snapshot().message_model {
            MessageModel::Broadcasting => {}
            MessageModel::Clustering => {
                let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(
                    self.consumer_config_snapshot().consumer_group.as_str(),
                ));
                let subscription_data = FilterAPI::build_subscription_data(
                    retry_topic.as_ref(),
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                )
                .map_err(|e| RocketMQError::illegal_argument(format!("buildSubscriptionData exception, {e}")))?;
                self.rebalance_impl
                    .put_subscription_data(retry_topic, subscription_data);
            }
        }
        Ok(())
    }

    pub fn register_consume_message_hook(&self, hook: impl ConsumeMessageHook + 'static) {
        self.consume_message_hook_list
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(Arc::new(hook) as ConsumeMessageHookArc);
    }

    #[cfg(test)]
    pub(crate) fn consume_message_hook_count(&self) -> usize {
        self.component_snapshot(&self.consume_message_hook_list).len()
    }

    pub fn register_message_listener(&self, message_listener: Option<Arc<MessageListener>>) {
        self.set_component(&self.message_listener, message_listener);
    }

    pub async fn subscribe(
        &self,
        topic: CheetahString,
        sub_expression: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let subscription_data = FilterAPI::build_subscription_data(&topic, &sub_expression)
            .map_err(|e| mq_client_err!(format!("buildSubscriptionData exception, {}", e)))?;
        self.rebalance_impl.put_subscription_data(topic, subscription_data);
        if let Some(client_instance) = self.get_mq_client_factory() {
            client_instance.send_heartbeat_to_all_broker_with_lock().await;
        }
        Ok(())
    }

    pub async fn subscribe_with_selector(
        &self,
        topic: CheetahString,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()> {
        match selector {
            Some(selector) => {
                let subscription_data = FilterAPI::build(
                    &topic,
                    selector.get_expression(),
                    Some(selector.get_expression_type().clone()),
                )
                .map_err(|e| mq_client_err!(format!("buildSubscriptionData exception, {}", e)))?;
                self.rebalance_impl.put_subscription_data(topic, subscription_data);
                if let Some(client_instance) = self.get_mq_client_factory() {
                    client_instance.send_heartbeat_to_all_broker_with_lock().await;
                }
                Ok(())
            }
            None => {
                self.subscribe(topic, CheetahString::from_static_str(SubscriptionData::SUB_ALL))
                    .await
            }
        }
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let topic = CheetahString::from(topic);
        if let Some(queues) = self.cached_subscribe_message_queues(&topic).await {
            return Ok(queues);
        }

        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance
            .update_topic_route_info_from_name_server_topic(&topic)
            .await;

        if let Some(queues) = self.cached_subscribe_message_queues(&topic).await {
            return Ok(queues);
        }

        Err(mq_client_err!(format!("The topic[{}] not exist", topic)))
    }

    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance
            .mq_admin_impl
            .create_topic(key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    pub async fn earliest_msg_store_time(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.mq_admin_impl.earliest_msg_store_time(mq).await
    }

    pub async fn max_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.mq_admin_impl.max_offset(mq).await
    }

    pub async fn min_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.mq_admin_impl.min_offset(mq).await
    }

    pub async fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.mq_admin_impl.search_offset(mq, timestamp).await
    }

    pub async fn reset_offset_by_time_stamp(&self, time_stamp: u64) -> rocketmq_error::RocketMQResult<()> {
        self.make_sure_state_ok()?;
        let topics = self
            .rebalance_impl
            .get_subscription_inner()
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();

        for topic in topics {
            let queues = {
                let topic_subscribe_info_table = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .topic_subscribe_info_table
                    .read()
                    .await;
                topic_subscribe_info_table.get(&topic).cloned()
            };
            let Some(queues) = queues else {
                continue;
            };

            let mut offset_table = HashMap::with_capacity(queues.len());
            for mq in queues {
                let offset = self.search_offset(&mq, time_stamp).await?;
                offset_table.insert(mq, offset);
            }
            MQConsumerInner::reset_offsets(self, &topic, offset_table).await;
        }
        Ok(())
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance
            .mq_admin_impl
            .query_message(topic, key, max_num, begin, end)
            .await
    }

    pub async fn query_message_by_uniq_key(
        &self,
        topic: &str,
        uniq_key: &str,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;
        let begin = current_millis().saturating_sub(QUERY_UNIQ_KEY_LOOKBACK_MILLIS);
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        let result = client_instance
            .mq_admin_impl
            .query_message_with_unique_flag(topic, uniq_key, 32, begin, i64::MAX as u64, true)
            .await?;
        result
            .message_list()
            .first()
            .cloned()
            .ok_or_else(|| mq_client_err!("query message by uniq key finished, but no message."))
    }

    pub async fn view_message(&self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        client_instance.mq_admin_impl.view_message(topic, msg_id).await
    }

    async fn cached_subscribe_message_queues(&self, topic: &CheetahString) -> Option<Vec<MessageQueue>> {
        let guard = self
            .rebalance_impl
            .rebalance_impl_inner
            .topic_subscribe_info_table
            .read()
            .await;
        guard
            .get(topic)
            .map(|queues| self.parse_subscribe_message_queues(queues))
    }

    fn parse_subscribe_message_queues(&self, message_queue_list: &HashSet<MessageQueue>) -> Vec<MessageQueue> {
        let namespace = self.client_config_snapshot().resolved_namespace().unwrap_or_default();
        let mut queues = message_queue_list
            .iter()
            .map(|mq| {
                let user_topic = NamespaceUtil::without_namespace_with_namespace(mq.topic_str(), namespace.as_str());
                MessageQueue::from_parts(user_topic, mq.broker_name(), mq.queue_id())
            })
            .collect::<Vec<_>>();
        queues.sort();
        queues
    }

    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        let Some(client_instance) = self.get_mq_client_factory() else {
            warn!(
                "execute_pull_request_immediately skipped: MQClientInstance is not initialized, {}",
                pull_request
            );
            return;
        };
        client_instance
            .pull_message_service
            .execute_pull_request_immediately(pull_request)
            .await;
    }
    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        let Some(client_instance) = self.get_mq_client_factory() else {
            warn!(
                "execute_pull_request_later skipped: MQClientInstance is not initialized, {}",
                pull_request
            );
            return;
        };
        client_instance
            .pull_message_service
            .execute_pull_request_later(pull_request, time_delay);
    }

    pub async fn execute_pop_request_immediately(&self, pop_request: PopRequest) {
        let Some(client_instance) = self.get_mq_client_factory() else {
            warn!(
                "execute_pop_request_immediately skipped: MQClientInstance is not initialized, {}",
                pop_request
            );
            return;
        };
        client_instance
            .pull_message_service
            .execute_pop_pull_request_immediately(pop_request)
            .await;
    }
    pub fn execute_pop_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        let Some(client_instance) = self.get_mq_client_factory() else {
            warn!(
                "execute_pop_request_later skipped: MQClientInstance is not initialized, {}",
                pop_request
            );
            return;
        };
        client_instance
            .pull_message_service
            .execute_pop_pull_request_later(pop_request, time_delay);
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        if let Some(consume_message_service) = self.consume_message_service() {
            Some(consume_message_service.consume_message_directly(msg, broker_name).await)
        } else if let Some(consume_message_pop_service) = self.consume_message_pop_service() {
            Some(
                consume_message_pop_service
                    .consume_message_directly(msg, broker_name)
                    .await,
            )
        } else {
            None
        }
    }

    pub(crate) async fn pop_message(&self, pop_request: PopRequest) {
        let process_queue = pop_request.get_pop_process_queue();
        if process_queue.is_dropped() {
            info!("the pop request[{}] is dropped.", pop_request);
            return;
        }
        process_queue.set_last_pop_timestamp(current_millis());

        if let Err(e) = self.make_sure_state_ok() {
            warn!("pop_message exception, consumer state not ok {}", e);
            self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            return;
        }

        if self.pause.load(Ordering::Acquire) {
            warn!(
                "consumer was paused, execute pull request later. instanceName={}, group={}",
                self.client_config_snapshot().instance_name,
                self.consumer_config_snapshot().consumer_group
            );
            self.execute_pop_request_later(pop_request, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        let pop_threshold_for_queue =
            i32::try_from(self.consumer_config_snapshot().pop_threshold_for_queue).unwrap_or(i32::MAX);
        if process_queue.get_wai_ack_msg_count() > pop_threshold_for_queue {
            self.execute_pop_request_later(pop_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            return;
        }

        let subscription_data = self
            .rebalance_impl
            .get_subscription_inner()
            .get(pop_request.get_message_queue().topic())
            .map(|v| v.value().clone());
        let Some(subscription_data) = subscription_data else {
            self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            warn!("find the consumer's subscription failed");
            return;
        };
        let expression_type = subscription_data.expression_type.clone();
        let sub_string = subscription_data.sub_string.clone();
        let mut invisible_time = self.consumer_config_snapshot().pop_invisible_time;
        if !(MIN_POP_INVISIBLE_TIME..=MAX_POP_INVISIBLE_TIME).contains(&invisible_time) {
            invisible_time = 60_000;
        }
        let mq = pop_request.get_message_queue().clone();
        let consumer_group = pop_request.get_consumer_group().clone();
        let init_mode = pop_request.get_init_mode();
        let pop_batch_nums = self.consumer_config_snapshot().pop_batch_nums;
        let Some(this) = self.self_reference.get().and_then(Weak::upgrade) else {
            warn!(
                "pop_message delayed: DefaultMQPushConsumerImpl self reference is not initialized, {}",
                pop_request
            );
            self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            return;
        };
        let Some(pull_api_wrapper) = self.pull_api_wrapper() else {
            warn!(
                "pop_message delayed: PullAPIWrapper is not initialized, {}",
                pop_request
            );
            self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            return;
        };

        match pull_api_wrapper
            .pop_async(
                pop_request.get_message_queue(),
                invisible_time,
                pop_batch_nums,
                consumer_group,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                DefaultPopCallback {
                    push_consumer_impl: this,
                    message_queue_inner: Some(mq),
                    subscription_data: Some(subscription_data),
                    pop_request: Some(pop_request.clone()),
                },
                true,
                init_mode,
                false,
                expression_type,
                sub_string,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!("popAsync error: {}", err);
                self.execute_pop_request_later(pop_request, self.pull_time_delay_mills_when_exception);
            }
        }
    }

    pub(crate) async fn pull_message(&self, mut pull_request: PullRequest) {
        //let process_queue = pull_request.get_process_queue_mut();
        if pull_request.process_queue.is_dropped() {
            info!("the pull request[{}] is dropped.", pull_request);
            return;
        }
        pull_request.process_queue.set_last_pull_timestamp(current_millis());
        if let Err(e) = self.make_sure_state_ok() {
            warn!("pullMessage exception, consumer state not ok {}", e);
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        }
        if self.pause.load(Ordering::Acquire) {
            warn!(
                "consumer was paused, execute pull request later. instanceName={}, group={}",
                self.client_config_snapshot().instance_name,
                self.consumer_config_snapshot().consumer_group
            );
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        let cached_message_count = pull_request.process_queue.msg_count();
        let cached_message_size_in_mib = pull_request.process_queue.msg_size() / _1MB;
        if cached_message_count > self.consumer_config_snapshot().pull_threshold_for_queue as u64 {
            let flow_control_times = self.queue_flow_control_times.fetch_add(1, Ordering::Relaxed);
            if flow_control_times.is_multiple_of(1000) {
                if let Some((min_offset, max_offset)) = pull_request.process_queue.get_offset_span().await {
                    warn!(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, \
                         maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        self.consumer_config_snapshot().pull_threshold_for_queue,
                        min_offset,
                        max_offset,
                        cached_message_count,
                        cached_message_size_in_mib,
                        pull_request.to_string(),
                        flow_control_times
                    );
                }
            }
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);

            return;
        }

        if cached_message_size_in_mib > self.consumer_config_snapshot().pull_threshold_size_for_queue as u64 {
            let flow_control_times = self.queue_flow_control_times.fetch_add(1, Ordering::Relaxed);
            if flow_control_times.is_multiple_of(1000) {
                if let Some((min_offset, max_offset)) = pull_request.process_queue.get_offset_span().await {
                    warn!(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, \
                         maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        self.consumer_config_snapshot().pull_threshold_size_for_queue,
                        min_offset,
                        max_offset,
                        cached_message_count,
                        cached_message_size_in_mib,
                        pull_request.to_string(),
                        flow_control_times
                    );
                }
            }
            self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            return;
        }

        if !self.is_consume_orderly() {
            let max_span = pull_request.process_queue.get_max_span().await;
            if max_span > self.consumer_config_snapshot().consume_concurrently_max_span as u64 {
                let flow_control_times = self.queue_max_span_flow_control_times.fetch_add(1, Ordering::Relaxed);
                if flow_control_times.is_multiple_of(1000) {
                    if let Some((min_offset, max_offset)) = pull_request.process_queue.get_offset_span().await {
                        warn!(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, \
                             maxSpan={}, pullRequest={}, flowControlTimes={}",
                            min_offset,
                            max_offset,
                            max_span,
                            pull_request.to_string(),
                            flow_control_times
                        );
                    }
                }
                self.execute_pull_request_later(pull_request, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
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
                            error!("Failed to compute pull offset, pullResult: {}", pull_request);
                            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);

                            return;
                        }
                        value
                    }
                    Err(e) => {
                        error!("Failed to compute pull offset, pullResult: {}, {}", pull_request, e);
                        self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);

                        return;
                    }
                };
                let broker_busy = offset < pull_request.get_next_offset();
                info!(
                    "the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} \
                     brokerBusy: {}",
                    pull_request.to_string(),
                    offset,
                    broker_busy
                );
                if broker_busy {
                    warn!(
                        "[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume \
                         offset. pullRequest: {} NewOffset: {}",
                        pull_request.to_string(),
                        offset
                    )
                }
                pull_request.set_previously_locked(true);
                pull_request.set_next_offset(offset);
            }
        } else {
            info!("pull message later because not locked in broker, {}", pull_request);
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        }
        let message_queue = pull_request.get_message_queue().clone();
        let inner = self.rebalance_impl.get_subscription_inner();
        let Some(subscription_data) = inner.get(message_queue.topic_str()).map(|v| v.value().clone()) else {
            error!(
                "find the consumer's subscription failed, {}, {}",
                message_queue,
                self.consumer_config_snapshot().consumer_group
            );
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        };
        let begin_timestamp = Instant::now();
        let topic = message_queue.topic_str().to_string();

        let message_queue_inner = message_queue.clone();
        let next_offset = pull_request.next_offset;
        let mut commit_offset_enable = false;
        let mut commit_offset_value = 0;
        if MessageModel::Clustering == self.consumer_config_snapshot().message_model {
            let Some(offset_store) = self.offset_store() else {
                warn!("pullMessage delayed: OffsetStore is not initialized, {}", pull_request);
                self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
                return;
            };
            commit_offset_value = offset_store
                .read_offset(&message_queue, ReadOffsetType::ReadFromMemory)
                .await;
            if commit_offset_value > 0 {
                commit_offset_enable = true;
            }
        }
        let mut sub_expression = None;
        if self.consumer_config_snapshot().post_subscription_when_pull && !subscription_data.class_filter_mode {
            sub_expression = Some(subscription_data.sub_string.clone());
        }
        let class_filter = subscription_data.class_filter_mode;
        let sys_flag = PullSysFlag::build_sys_flag(commit_offset_enable, true, sub_expression.is_some(), class_filter);
        let Some(this) = self.self_reference.get().and_then(Weak::upgrade) else {
            warn!(
                "pullMessage delayed: DefaultMQPushConsumerImpl self reference is not initialized, {}",
                pull_request
            );
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        };
        let pull_batch_size = self.consumer_config_snapshot().pull_batch_size as i32;
        let pull_batch_size_in_bytes = self.consumer_config_snapshot().pull_batch_size_in_bytes as i32;
        let sub_expression = sub_expression.unwrap_or_default();
        let expression_type = subscription_data.expression_type.clone();
        let sub_version = subscription_data.sub_version;
        let Some(pull_api_wrapper) = self.pull_api_wrapper() else {
            warn!(
                "pullMessage delayed: PullAPIWrapper is not initialized, {}",
                pull_request
            );
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
            return;
        };
        let result = pull_api_wrapper
            .pull_kernel_impl(
                &message_queue,
                sub_expression,
                expression_type,
                sub_version,
                next_offset,
                pull_batch_size,
                pull_batch_size_in_bytes,
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
            self.execute_pull_request_later(pull_request, self.pull_time_delay_mills_when_exception);
        }
    }

    pub(crate) async fn process_pop_result(
        &self,
        mut pop_result: PopResult,
        subscription_data: &SubscriptionData,
    ) -> PopResult {
        if pop_result.pop_status == PopStatus::Found {
            let msg_vec = pop_result.msg_found_list.take().unwrap_or_default();
            let msg_list_filter_again =
                if !subscription_data.tags_set.is_empty() && !subscription_data.class_filter_mode {
                    let mut msg_vec_again = Vec::with_capacity(msg_vec.len());
                    for msg in msg_vec {
                        if let Some(ref tag) = msg.tags() {
                            if subscription_data.tags_set.contains(tag.as_str()) {
                                msg_vec_again.push(msg);
                            }
                        }
                    }
                    msg_vec_again
                } else {
                    msg_vec
                };
            let filter_message_hooks = self.component_snapshot(&self.filter_message_hook_list);
            if !filter_message_hooks.is_empty() {
                let context = FilterMessageContext {
                    unit_mode: self.consumer_config_snapshot().unit_mode,
                    msg_list: &msg_list_filter_again,
                    ..Default::default()
                };
                for hook in &filter_message_hooks {
                    hook.filter_message(&context);
                }
            }
            let mut final_msg_list = Vec::with_capacity(msg_list_filter_again.len());
            for msg in msg_list_filter_again {
                if msg.reconsume_times > self.get_max_reconsume_times() {
                    let consumer_group = self.consumer_config_snapshot().consumer_group().clone();
                    self.ack_async(&msg, &consumer_group).await;
                } else {
                    final_msg_list.push(msg);
                }
            }
            pop_result.msg_found_list = Some(final_msg_list);
        }
        pop_result
    }

    #[inline]
    fn make_sure_state_ok(&self) -> rocketmq_error::RocketMQResult<()> {
        let service_state = self.service_state();
        if service_state != ServiceState::Running {
            return Err(mq_client_err!(format!(
                "The consumer service state not OK, {},{}",
                service_state,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
            )));
        }
        Ok(())
    }

    pub(crate) async fn correct_tags_offset(&self, pull_request: &PullRequest) {
        if pull_request.process_queue.msg_count() == 0 {
            let Some(offset_store) = self.offset_store() else {
                warn!(
                    "correct_tags_offset skipped: OffsetStore is not initialized, {}",
                    pull_request
                );
                return;
            };
            offset_store
                .update_offset(pull_request.get_message_queue(), pull_request.next_offset, true)
                .await;
        }
    }

    pub fn try_reset_pop_retry_topic(msgs: &mut [Arc<MessageExt>], consumer_group: &str) {
        let pop_retry_prefix = format!("{}{}_", mix_all::RETRY_GROUP_TOPIC_PREFIX, consumer_group);
        for msg in msgs.iter_mut() {
            if msg.topic().starts_with(&pop_retry_prefix) {
                let normal_topic = KeyBuilder::parse_normal_topic(msg.topic(), consumer_group);

                if !normal_topic.is_empty() {
                    Arc::make_mut(msg).set_topic(CheetahString::from_string(normal_topic));
                }
            }
        }
    }

    pub fn reset_retry_and_namespace(&self, msgs: &mut [Arc<MessageExt>], consumer_group: &str) {
        let group_topic = mix_all::get_retry_topic(consumer_group);
        let namespace = self.client_config_snapshot().resolved_namespace().unwrap_or_default();
        for msg in msgs.iter_mut() {
            if let Some(retry_topic) = msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC))
            {
                if group_topic == msg.topic().as_str() {
                    Arc::make_mut(msg).set_topic(retry_topic);
                }
            }

            if !namespace.is_empty() {
                let topic = msg.topic().to_string();
                Arc::make_mut(msg).set_topic(CheetahString::from_string(
                    NamespaceUtil::without_namespace_with_namespace(topic.as_str(), namespace.as_str()),
                ));
            }
        }
    }

    #[inline]
    pub fn has_hook(&self) -> bool {
        !self.component_snapshot(&self.consume_message_hook_list).is_empty()
    }

    pub fn execute_hook_before(&self, context: &mut ConsumeMessageContext) {
        for hook in self.component_snapshot(&self.consume_message_hook_list) {
            hook.consume_message_before(context);
        }
    }

    pub fn execute_hook_after(&self, context: &mut ConsumeMessageContext) {
        for hook in self.component_snapshot(&self.consume_message_hook_list) {
            hook.consume_message_after(context);
        }
    }

    pub async fn send_message_back(
        &self,
        msg: &mut MessageExt,
        delay_level: i32,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.send_message_back_with_broker_name(
            msg,
            delay_level,
            Self::message_broker_name_for_send_back(msg),
            Some(mq),
        )
        .await
    }

    fn message_broker_name_for_send_back(msg: &MessageExt) -> Option<CheetahString> {
        if msg.broker_name.is_empty() {
            None
        } else {
            Some(msg.broker_name.clone())
        }
    }

    pub async fn send_message_back_with_broker_name(
        &self,
        msg: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<CheetahString>,
        mq: Option<&MessageQueue>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let result = self
            .send_message_back_with_broker_name_inner(msg, delay_level, broker_name, mq)
            .await;
        self.reset_send_message_back_topic_namespace(msg);
        result
    }

    async fn send_message_back_with_broker_name_inner(
        &self,
        msg: &mut MessageExt,
        delay_level: i32,
        broker_name: Option<CheetahString>,
        mq: Option<&MessageQueue>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let broker_is_logical = broker_name
            .as_ref()
            .is_some_and(|name| name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX));
        let queue_is_logical =
            mq.is_some_and(|mq| mq.broker_name().starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX));
        if broker_is_logical || queue_is_logical {
            let _ = self.send_message_back_as_normal_message(msg).await;
        } else {
            let broker_addr = if let Some(ref broker_name_) = broker_name {
                let Some(client_instance) = self.get_mq_client_factory() else {
                    error!("send message back failed: MQClientInstance is not initialized");
                    return self.send_message_back_as_normal_message(msg).await;
                };
                match client_instance.find_broker_address_in_publish(broker_name_.as_ref()) {
                    Some(addr) => addr,
                    None => {
                        error!(
                            "send message back failed: broker[{}] master node does not exist",
                            broker_name_
                        );
                        return self.send_message_back_as_normal_message(msg).await;
                    }
                }
            } else {
                CheetahString::from_string(msg.store_host.to_string())
            };
            let broker_name_str = broker_name.as_ref().map(|name| name.as_str());
            let max_consume_retry_times = self.get_max_reconsume_times();
            let consumer_group = self.consumer_config_snapshot().consumer_group.clone();
            let result = if let Some(client_instance) = self.get_mq_client_factory() {
                if let Some(mq_client_api_impl) = client_instance.mq_client_api_impl.as_ref() {
                    mq_client_api_impl
                        .consumer_send_message_back(
                            broker_addr.as_str(),
                            broker_name_str,
                            msg,
                            consumer_group.as_str(),
                            delay_level,
                            5000,
                            max_consume_retry_times,
                        )
                        .await
                } else {
                    Err(mq_client_err!("MQClientAPIImpl is not initialized"))
                }
            } else {
                Err(rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))
            };
            if let Err(e) = result {
                error!("send message back error: {}", e);

                self.send_message_back_as_normal_message(msg).await?;
            }
        }
        Ok(())
    }

    fn reset_send_message_back_topic_namespace(&self, msg: &mut MessageExt) {
        msg.set_topic(CheetahString::from_string(
            NamespaceUtil::without_namespace_with_namespace(
                msg.topic().as_str(),
                self.client_config_snapshot()
                    .resolved_namespace()
                    .unwrap_or_default()
                    .as_str(),
            ),
        ));
    }

    fn build_retry_message_for_send_back(&self, msg: &MessageExt) -> rocketmq_error::RocketMQResult<Message> {
        let topic = mix_all::get_retry_topic(self.consumer_config_snapshot().consumer_group());
        let body = msg.get_body().cloned();
        let mut new_msg = if let Some(body) = body {
            Message::builder().topic(topic.as_str()).body(body).build()?
        } else {
            Message::builder().topic(topic.as_str()).empty_body().build()?
        };
        MessageAccessor::set_properties(&mut new_msg, msg.get_properties().clone());
        let origin_msg_id = MessageAccessor::get_origin_message_id(msg).unwrap_or_else(|| msg.msg_id.clone());
        MessageAccessor::set_origin_message_id(&mut new_msg, origin_msg_id);
        new_msg.set_flag(msg.get_flag());
        MessageAccessor::put_property(
            &mut new_msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            msg.topic().to_owned(),
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
        Ok(new_msg)
    }

    async fn send_message_back_as_normal_message(&self, msg: &MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let new_msg = self.build_retry_message_for_send_back(msg)?;
        self.get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?
            .default_producer
            .send(new_msg)
            .await?;
        Ok(())
    }

    pub fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config_snapshot().max_reconsume_times == -1 {
            16
        } else {
            self.consumer_config_snapshot().max_reconsume_times
        }
    }

    pub fn update_core_pool_size(&self, core_pool_size: usize) {
        if let Some(consume_message_service) = self.consume_message_service() {
            consume_message_service.update_core_pool_size(core_pool_size);
        }
        if let Some(consume_message_pop_service) = self.consume_message_pop_service() {
            consume_message_pop_service.update_core_pool_size(core_pool_size);
        }
    }

    pub(crate) async fn ack_async(&self, message: &MessageExt, consumer_group: &CheetahString) {
        let extra_info = message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
            .unwrap_or_default();
        let extra_info_strs = ExtraInfoUtil::split(extra_info.as_str());
        /*        if extra_info_strs.is_err() {
            error!("ackAsync error: {}", extra_info_strs.unwrap_err());
            return;
        }
        let extra_info_strs = extra_info_strs.unwrap();*/
        let queue_id = ExtraInfoUtil::get_queue_id(extra_info_strs.as_slice());
        let queue_id = match queue_id {
            Ok(queue_id) => queue_id,
            Err(e) => {
                error!("ackAsync error: {}", e);
                return;
            }
        };
        let queue_offset = ExtraInfoUtil::get_queue_offset(extra_info_strs.as_slice());
        let queue_offset = match queue_offset {
            Ok(queue_offset) => queue_offset,
            Err(e) => {
                error!("ackAsync error: {}", e);
                return;
            }
        };
        let broker_name =
            CheetahString::from(ExtraInfoUtil::get_broker_name(extra_info_strs.as_slice()).unwrap_or_default());
        let topic = message.topic();

        let Some(client_instance) = self.get_mq_client_factory() else {
            error!("ackAsync error: MQClientInstance is not initialized");
            return;
        };
        let des_broker_name =
            if !broker_name.is_empty() && broker_name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX) {
                let mq = self
                    .client_config_snapshot()
                    .queue_with_resolved_namespace(MessageQueue::from_parts(topic, broker_name.clone(), queue_id));
                client_instance.get_broker_name_from_message_queue(&mq).await
            } else {
                broker_name.clone()
            };

        let mut find_broker_result = client_instance
            .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
            .await;
        if find_broker_result.is_none() {
            client_instance
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            find_broker_result = client_instance
                .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
                .await;
        }
        if find_broker_result.is_none() {
            error!("The broker[{}] not exist", des_broker_name);
            return;
        }
        let Some(find_broker_result) = find_broker_result else {
            error!("The broker[{}] not exist", des_broker_name);
            return;
        };
        let request_header = AckMessageRequestHeader {
            consumer_group: consumer_group.clone(),
            topic: CheetahString::from_string(
                ExtraInfoUtil::get_real_topic(extra_info_strs.as_slice(), topic, consumer_group).unwrap_or_default(),
            ),
            queue_id,
            extra_info,
            offset: queue_offset,
            lite_topic: None,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        struct DefaultAckCallback;
        impl AckCallback for DefaultAckCallback {
            fn on_success(&self, _ack_result: AckResult) {}

            fn on_exception(&self, _e: rocketmq_error::RocketMQError) {}
        }
        let Some(mq_client_api_impl) = client_instance.mq_client_api_impl.as_ref() else {
            error!("ackAsync error: MQClientAPIImpl is not initialized");
            return;
        };
        match mq_client_api_impl
            .ack_message_async(
                &find_broker_result.broker_addr,
                request_header,
                ASYNC_TIMEOUT,
                DefaultAckCallback,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("ackAsync error: {}", e);
            }
        }
    }

    pub(crate) async fn change_pop_invisible_time_async(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        extra_info: &CheetahString,
        invisible_time: u64,
        callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let extra_info_strs = ExtraInfoUtil::split(extra_info);
        let broker_name = CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info_strs.as_slice())?);
        let queue_id = ExtraInfoUtil::get_queue_id(extra_info_strs.as_slice())?;
        let des_broker_name =
            if !broker_name.is_empty() && broker_name.starts_with(mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX) {
                let queue = self
                    .client_config_snapshot()
                    .queue_with_resolved_namespace(MessageQueue::from_parts(topic, broker_name.clone(), queue_id));
                self.get_mq_client_factory()
                    .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?
                    .get_broker_name_from_message_queue(&queue)
                    .await
            } else {
                broker_name.clone()
            };
        let client_instance = self
            .get_mq_client_factory()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        let mut find_broker_result = client_instance
            .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
            .await;
        if find_broker_result.is_none() {
            client_instance
                .update_topic_route_info_from_name_server_default(topic, false, None)
                .await;
            find_broker_result = client_instance
                .find_broker_address_in_subscribe(&des_broker_name, mix_all::MASTER_ID, true)
                .await;
        }

        if find_broker_result.is_none() {
            return Err(mq_client_err!(format!(
                "The broker[{}] not exist",
                des_broker_name.as_str()
            )));
        }
        let request_header = ChangeInvisibleTimeRequestHeader {
            consumer_group: consumer_group.clone(),
            topic: CheetahString::from_string(ExtraInfoUtil::get_real_topic(
                extra_info_strs.as_slice(),
                topic,
                consumer_group,
            )?),
            queue_id,
            extra_info: extra_info.clone(),
            offset: ExtraInfoUtil::get_queue_offset(extra_info_strs.as_slice())?,
            invisible_time: invisible_time as i64,
            lite_topic: None,
            suspend: false,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        let Some(find_broker_result) = find_broker_result else {
            return Err(mq_client_err!(format!(
                "The broker[{}] not exist",
                des_broker_name.as_str()
            )));
        };
        client_instance
            .get_mq_client_api_impl()?
            .change_invisible_time_async(
                &broker_name,
                &find_broker_result.broker_addr,
                request_header,
                ASYNC_TIMEOUT,
                callback,
            )
            .await
    }

    pub async fn unsubscribe(&self, topic: &str) {
        self.rebalance_impl
            .rebalance_impl_inner
            .subscription_inner
            .remove(topic);
    }

    pub async fn suspend(&self) {
        self.pause.store(true, Ordering::Release);
        info!(
            "Suspend the consumer, instanceName={}, group={}",
            self.client_config_snapshot().instance_name,
            self.consumer_config_snapshot().consumer_group
        );
    }

    pub async fn resume(&self) {
        self.pause.store(false, Ordering::Release);
        self.do_rebalance().await;
        info!(
            "Resume the consumer, instanceName={}, group={}",
            self.client_config_snapshot().instance_name,
            self.consumer_config_snapshot().consumer_group
        );
    }
}

impl MQConsumerInner for DefaultMQPushConsumerImpl {
    fn group_name(&self) -> CheetahString {
        self.consumer_config_snapshot().consumer_group().clone()
    }

    fn message_model(&self) -> MessageModel {
        self.consumer_config_snapshot().message_model
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config_snapshot().consume_from_where
    }

    fn subscriptions(&self) -> HashSet<SubscriptionData> {
        self.rebalance_impl
            .rebalance_impl_inner
            .subscription_inner
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    async fn do_rebalance(&self) {
        if !self.pause.load(Ordering::Acquire) {
            let orderly = self.is_consume_orderly();
            let rebalance = self.rebalance_impl.do_rebalance(orderly).await;
            if !rebalance {
                warn!(
                    "rebalance failed, maybe the consumer was paused during rebalance. instanceName={}, group={}",
                    self.client_config_snapshot().instance_name,
                    self.consumer_config_snapshot().consumer_group
                );
            }
        }
    }

    async fn try_rebalance(&self) -> rocketmq_error::RocketMQResult<bool> {
        if !self.pause.load(Ordering::Acquire) {
            return Ok(self.rebalance_impl.do_rebalance(self.is_consume_orderly()).await);
        }
        Ok(false)
    }

    async fn persist_consumer_offset(&self) {
        if let Err(err) = self.make_sure_state_ok() {
            error!(
                "group: {} persistConsumerOffset exception:{}",
                self.consumer_config_snapshot().consumer_group,
                err
            );
        } else {
            let allocate_mq = {
                let guard = self
                    .rebalance_impl
                    .rebalance_impl_inner
                    .process_queue_table
                    .read()
                    .await;
                guard.keys().cloned().collect::<HashSet<_>>()
            };
            let Some(offset_store) = self.offset_store() else {
                warn!(
                    "group: {} persistConsumerOffset skipped: OffsetStore is not initialized",
                    self.consumer_config_snapshot().consumer_group
                );
                return;
            };
            offset_store.persist_all(&allocate_mq).await;
        }
    }

    async fn update_topic_subscribe_info(&self, topic: CheetahString, info: &HashSet<MessageQueue>) {
        if self.rebalance_impl.get_subscription_inner().contains_key(&topic) {
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
        if self.rebalance_impl.get_subscription_inner().contains_key(topic) {
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
        self.consumer_config_snapshot().unit_mode
    }

    async fn reset_offsets(&self, topic: &CheetahString, offsets: HashMap<MessageQueue, i64>) {
        let Some(offset_store) = self.offset_store() else {
            warn!(
                "reset offset ignored because offset store is not initialized. group={}, topic={}",
                self.consumer_config_snapshot().consumer_group,
                topic
            );
            return;
        };

        self.suspend().await;

        let reset_queues = {
            let process_queue_table = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            process_queue_table
                .iter()
                .filter_map(|(mq, pq)| {
                    offsets
                        .get(mq)
                        .filter(|_| mq.topic() == topic)
                        .map(|offset| (mq.clone(), pq.clone(), *offset))
                })
                .collect::<Vec<_>>()
        };

        for (_, pq, _) in &reset_queues {
            pq.set_dropped(true);
            pq.clear().await;
        }

        if !reset_queues.is_empty() && !self.is_consume_orderly() {
            tokio::time::sleep(RESET_OFFSET_MAX_WAIT).await;
        }

        for (mq, pq, offset) in reset_queues {
            if self.is_consume_orderly() {
                let _ = tokio::time::timeout(RESET_OFFSET_MAX_WAIT, pq.consume_lock.write()).await;
            }

            offset_store.update_and_freeze_offset(&mq, offset).await;
            self.rebalance_impl
                .remove_unnecessary_message_queue(&mq, pq.as_ref())
                .await;

            self.rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .write()
                .await
                .remove(&mq);

            info!(
                "reset offset completed. group={}, topic={}, mq={}, offset={}",
                self.consumer_config_snapshot().consumer_group,
                topic,
                mq,
                offset
            );
        }

        self.resume().await;
    }

    async fn consumer_status(&self, topic: &CheetahString) -> HashMap<MessageQueue, i64> {
        let Some(offset_store) = self.offset_store() else {
            warn!(
                "consumer status is empty because offset store is not initialized. group={}, topic={}",
                self.consumer_config_snapshot().consumer_group,
                topic
            );
            return HashMap::new();
        };

        offset_store.clone_offset_table(topic).await
    }

    async fn consumer_running_info(&self) -> ConsumerRunningInfo {
        let consumer_config = self.consumer_config_snapshot();
        let mut info = ConsumerRunningInfo::new();
        info.consume_type = self.consume_type();
        info.consume_orderly = self.is_consume_orderly();
        info.prop_consumer_start_timestamp = self.consumer_start_timestamp.load(Ordering::Acquire);
        info.sync_properties_from_derived_fields();
        info.set_property(
            ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE,
            self.consume_message_service()
                .map(|service| service.get_core_pool_size().to_string())
                .unwrap_or_else(|| consumer_config.consume_thread_min.to_string()),
        );
        info.set_property("consumeThreadMin", consumer_config.consume_thread_min.to_string());
        info.set_property("consumeThreadMax", consumer_config.consume_thread_max.to_string());
        info.set_property("pullBatchSize", consumer_config.pull_batch_size.to_string());
        info.set_property(
            "consumeMessageBatchMaxSize",
            consumer_config.consume_message_batch_max_size.to_string(),
        );

        for entry in self.rebalance_impl.get_subscription_inner().iter() {
            info.subscription_set.insert(entry.value().clone());
        }

        if let Some(client_instance) = self.get_mq_client_factory() {
            for subscription_data in &info.subscription_set {
                let status = client_instance.consumer_stats_manager().consume_status(
                    consumer_config.consumer_group.as_str(),
                    subscription_data.topic.as_str(),
                );
                info.status_table.insert(subscription_data.topic.to_string(), status);
            }
        }

        let process_queue_table = {
            let process_queue_table = self
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            process_queue_table
                .iter()
                .map(|(mq, pq)| (mq.clone(), pq.clone()))
                .collect::<Vec<_>>()
        };
        for (mq, pq) in process_queue_table {
            let commit_offset = if let Some(offset_store) = self.offset_store() {
                offset_store
                    .read_offset(&mq, ReadOffsetType::MemoryFirstThenStore)
                    .await
            } else {
                0
            };
            let mut pq_info = ProcessQueueInfo {
                commit_offset,
                ..Default::default()
            };
            pq.fill_process_queue_info(&mut pq_info).await;
            info.mq_table.insert(mq, pq_info);
        }

        let pop_process_queue_table = {
            let pop_process_queue_table = self
                .rebalance_impl
                .rebalance_impl_inner
                .pop_process_queue_table
                .read()
                .await;
            pop_process_queue_table
                .iter()
                .map(|(mq, pq)| (mq.clone(), pq.clone()))
                .collect::<Vec<_>>()
        };
        for (mq, pq) in pop_process_queue_table {
            let mut pq_info = PopProcessQueueInfo::new(0, false, 0);
            pq.fill_pop_process_queue_info(&mut pq_info);
            info.mq_pop_table.insert(mq, pq_info);
        }

        info.user_consumer_info.insert(
            "consumeThreadMin".to_string(),
            consumer_config.consume_thread_min.to_string(),
        );
        info.user_consumer_info.insert(
            "consumeThreadMax".to_string(),
            consumer_config.consume_thread_max.to_string(),
        );
        info.user_consumer_info
            .insert("pullBatchSize".to_string(), consumer_config.pull_batch_size.to_string());
        info.user_consumer_info.insert(
            "consumeMessageBatchMaxSize".to_string(),
            consumer_config.consume_message_batch_max_size.to_string(),
        );

        info
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_common::common::filter::expression_type::ExpressionType;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

    use super::*;
    use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
    use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
    use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;

    fn new_unstarted_impl() -> DefaultMQPushConsumerImpl {
        DefaultMQPushConsumerImpl::new(ClientConfig::default(), ConsumerConfig::default(), None)
    }

    fn new_running_impl() -> DefaultMQPushConsumerImpl {
        let consumer = new_unstarted_impl();
        consumer.set_service_state(ServiceState::Running);
        consumer
    }

    struct NoopConcurrentListener;

    impl MessageListenerConcurrently for NoopConcurrentListener {
        fn consume_message(
            &self,
            _msgs: &[&MessageExt],
            _context: &ConsumeConcurrentlyContext,
        ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        }
    }

    fn new_check_config_consumer(consume_timestamp: Option<CheetahString>) -> DefaultMQPushConsumerImpl {
        let mut consumer_config = ConsumerConfig {
            consumer_group: CheetahString::from_static_str("PushGroup"),
            consume_timestamp,
            ..Default::default()
        };
        consumer_config.message_listener = Some(Arc::new(MessageListener::new(
            Some(Arc::new(NoopConcurrentListener)),
            None,
        )));
        DefaultMQPushConsumerImpl::new(ClientConfig::default(), consumer_config, None)
    }

    fn new_startable_push_consumer(
        client_config: ClientConfig,
        consumer_group: CheetahString,
    ) -> Arc<DefaultMQPushConsumerImpl> {
        let mut consumer_config = ConsumerConfig {
            consumer_group,
            ..Default::default()
        };
        consumer_config.message_listener = Some(Arc::new(MessageListener::new(
            Some(Arc::new(NoopConcurrentListener)),
            None,
        )));
        let consumer = Arc::new(DefaultMQPushConsumerImpl::new(client_config, consumer_config, None));
        consumer.initialize_self_reference();
        consumer
    }

    #[test]
    fn rebalance_threshold_updates_publish_to_owner_config() {
        let consumer = new_unstarted_impl();
        let initial_config = consumer.consumer_config_snapshot();

        consumer.update_pull_thresholds_from_rebalance(Some(25), Some(10));

        let consumer_config = consumer.consumer_config_snapshot();
        assert!(!Arc::ptr_eq(&initial_config, &consumer_config));
        assert_eq!(initial_config.pull_threshold_for_queue, 1000);
        assert_eq!(initial_config.pull_threshold_size_for_queue, 100);
        assert_eq!(consumer_config.pull_threshold_for_queue, 25);
        assert_eq!(consumer_config.pull_threshold_size_for_queue, 10);
    }

    #[test]
    fn root_owned_services_and_rebalance_back_reference_are_weak() {
        let consumer = Arc::new(new_unstarted_impl());
        consumer.initialize_self_reference();
        let weak_consumer = Arc::downgrade(&consumer);
        let consumer_config = Arc::new(ConsumerConfig::default());
        let service = Arc::new(ConsumeMessageConcurrentlyService::new(
            Arc::new(ClientConfig::default()),
            consumer_config.clone(),
            consumer_config.consumer_group.clone(),
            Arc::new(|_msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
                Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
            }),
            Some(weak_consumer.clone()),
        ));
        consumer.set_component(
            &consumer.consume_message_service,
            Some(Arc::new(ConsumeMessageServiceGeneral::new(Some(service), None))),
        );

        assert_eq!(Arc::strong_count(&consumer), 1);
        drop(consumer);
        assert!(weak_consumer.upgrade().is_none());
    }

    #[tokio::test]
    async fn shutdown_waits_for_the_lifecycle_transition_owner() {
        let consumer = Arc::new(new_unstarted_impl());
        consumer.initialize_self_reference();
        let transition = consumer.lifecycle_transition.lock().await;
        let consumer_for_shutdown = consumer.clone();
        let shutdown = tokio::spawn(async move {
            consumer_for_shutdown.shutdown(0).await;
        });

        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        drop(transition);
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown should acquire the released lifecycle transition")
            .expect("shutdown task should not panic");
    }

    fn message_queue() -> MessageQueue {
        MessageQueue::from_parts("topic", "broker-a", 0)
    }

    fn pull_request() -> PullRequest {
        PullRequest::new(
            CheetahString::from_static_str("group"),
            message_queue(),
            Arc::new(ProcessQueue::new()),
            0,
        )
    }

    fn pop_request() -> PopRequest {
        PopRequest::new(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("group"),
            message_queue(),
            PopProcessQueue::new(),
            0,
        )
    }

    fn message_ext_for_send_back(topic: &str) -> MessageExt {
        let mut message = Message::builder()
            .topic(topic)
            .body_slice(b"retry-body")
            .build_unchecked();
        message.set_flag(7);
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
            CheetahString::from_static_str("true"),
        );
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_msg_id(CheetahString::from_static_str("msg-id-current"));
        message_ext.set_reconsume_times(2);
        message_ext
    }

    #[tokio::test]
    async fn start_duplicate_consumer_group_rolls_back_like_java() {
        let group = CheetahString::from_string(format!("PushDuplicateGroup{}", current_millis()));
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(CheetahString::from_string(format!(
            "push-duplicate-instance-{}",
            current_millis()
        )));
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let client_instance =
            MQClientManager::get_instance().get_or_create_mq_client_instance(client_config.clone(), None);

        let existing_consumer = new_startable_push_consumer(client_config.clone(), group.clone());
        assert!(
            client_instance
                .mut_from_ref()
                .register_consumer(&group, MQConsumerInnerImpl::from_push(existing_consumer))
                .await
        );

        let duplicate_consumer = new_startable_push_consumer(client_config, group.clone());
        let result = duplicate_consumer.start().await;

        assert!(result
            .err()
            .is_some_and(|error| error.to_string().contains("has been created before")));
        assert_eq!(duplicate_consumer.service_state(), ServiceState::CreateJust);
        assert!(duplicate_consumer
            .consume_message_service()
            .and_then(|service| service.get_consume_message_concurrently_service())
            .is_some_and(|service| service.is_shutdown()));

        client_instance.mut_from_ref().unregister_consumer(group).await;
        MQClientManager::get_instance().remove_client_factory(&client_id);
    }

    #[tokio::test]
    async fn startup_after_running_failure_shutdowns_and_unregisters_like_java() {
        let group = CheetahString::from_string(format!("PushPostStartFailGroup{}", current_millis()));
        let topic = CheetahString::from_static_str("sql-topic");
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(CheetahString::from_string(format!(
            "push-post-start-fail-instance-{}",
            current_millis()
        )));
        let client_instance = MQClientInstance::new_arc(client_config.clone(), 0, "push-post-start-fail-client", None);
        let consumer = new_startable_push_consumer(client_config, group.clone());
        consumer.set_service_state(ServiceState::Running);
        consumer.set_mq_client_factory(client_instance.clone());

        let mut subscription = SubscriptionData {
            topic: topic.clone(),
            sub_string: CheetahString::from_static_str("a > 1"),
            expression_type: CheetahString::from_static_str(ExpressionType::SQL92),
            ..Default::default()
        };
        subscription.tags_set.clear();
        consumer
            .rebalance_impl
            .put_subscription_data(topic.clone(), subscription);
        assert!(
            client_instance
                .mut_from_ref()
                .register_consumer(&group, MQConsumerInnerImpl::from_push(consumer.clone()))
                .await
        );

        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(mix_all::MASTER_ID, CheetahString::from_static_str("127.0.0.1:10911"));
        client_instance.mut_from_ref().topic_route_table.insert(
            topic,
            TopicRouteData {
                broker_datas: vec![BrokerData::new(
                    CheetahString::from_static_str("cluster-a"),
                    CheetahString::from_static_str("broker-a"),
                    broker_addrs,
                    None,
                )],
                ..Default::default()
            },
        );
        client_instance.mut_from_ref().mq_client_api_impl = None;

        let error = consumer
            .complete_startup_after_running()
            .await
            .expect_err("missing MQClientAPIImpl should fail post-start broker compatibility check");
        assert!(
            matches!(error, rocketmq_error::RocketMQError::ClientNotStarted),
            "unexpected post-start error: {error:?}"
        );
        assert_eq!(consumer.service_state(), ServiceState::ShutdownAlready);
        let replacement = new_startable_push_consumer(ClientConfig::default(), group.clone());
        assert!(
            client_instance
                .mut_from_ref()
                .register_consumer(&group, MQConsumerInnerImpl::from_push(replacement))
                .await,
            "shutdown rollback should unregister the failed consumer group"
        );
        client_instance.mut_from_ref().unregister_consumer(group).await;
    }

    #[test]
    fn check_config_accepts_java_consume_timestamp_format() {
        let consumer = new_check_config_consumer(Some(CheetahString::from_static_str("20250102030405")));

        consumer
            .check_config()
            .expect("Java yyyyMMddHHmmss consume timestamp should pass config validation");
    }

    #[test]
    fn check_config_rejects_invalid_consume_timestamp_like_java() {
        let consumer = new_check_config_consumer(Some(CheetahString::from_static_str("2025-01-02 03:04:05")));

        let error = consumer
            .check_config()
            .expect_err("invalid consume timestamp should fail before start");

        let message = error.to_string();
        assert!(message.contains("consumeTimestamp is invalid"));
        assert!(message.contains("yyyyMMddHHmmss"));
    }

    #[test]
    fn check_config_rejects_missing_consume_timestamp_without_panic() {
        let consumer = new_check_config_consumer(None);

        let error = consumer
            .check_config()
            .expect_err("missing consume timestamp should return a typed config error");

        assert!(error.to_string().contains("consumeTimestamp is invalid"));
    }

    #[tokio::test]
    async fn execute_pull_request_immediately_without_client_instance_does_not_panic() {
        let consumer = new_unstarted_impl();

        consumer.execute_pull_request_immediately(pull_request()).await;
    }

    #[tokio::test]
    async fn reset_offset_by_time_stamp_with_no_subscriptions_is_noop_like_java() {
        let consumer = new_running_impl();

        consumer
            .reset_offset_by_time_stamp(123456)
            .await
            .expect("no subscriptions should not require broker lookup");
    }

    #[test]
    fn execute_pull_request_later_without_client_instance_does_not_panic() {
        let consumer = new_unstarted_impl();

        consumer.execute_pull_request_later(pull_request(), 1);
    }

    #[tokio::test]
    async fn execute_pop_request_immediately_without_client_instance_does_not_panic() {
        let consumer = new_unstarted_impl();

        consumer.execute_pop_request_immediately(pop_request()).await;
    }

    #[test]
    fn execute_pop_request_later_without_client_instance_does_not_panic() {
        let consumer = new_unstarted_impl();

        consumer.execute_pop_request_later(pop_request(), 1);
    }

    #[test]
    fn send_back_retry_message_preserves_existing_origin_message_id_like_java() {
        let consumer = new_unstarted_impl();
        consumer.update_consumer_config(|config| config.consumer_group = CheetahString::from_static_str("PushGroup"));
        let mut msg = message_ext_for_send_back("ns%TopicA");
        MessageAccessor::set_origin_message_id(&mut msg, CheetahString::from_static_str("origin-msg-id"));

        let retry_message = consumer
            .build_retry_message_for_send_back(&msg)
            .expect("retry message should build");

        assert_eq!(retry_message.topic().as_str(), "%RETRY%PushGroup");
        assert_eq!(
            retry_message.get_body().map(|body| body.as_ref()),
            Some(b"retry-body".as_slice())
        );
        assert_eq!(retry_message.get_flag(), 7);
        assert_eq!(
            MessageAccessor::get_origin_message_id(&retry_message).as_deref(),
            Some("origin-msg-id")
        );
        assert_eq!(
            retry_message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC)),
            Some("ns%TopicA")
        );
        assert_eq!(
            MessageAccessor::get_reconsume_time(&retry_message).as_deref(),
            Some("3")
        );
        assert_eq!(
            MessageAccessor::get_max_reconsume_times(&retry_message).as_deref(),
            Some("16")
        );
        assert_eq!(
            retry_message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL)),
            Some("5")
        );
        assert!(retry_message
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_TRANSACTION_PREPARED
            ))
            .is_none());
    }

    #[test]
    fn send_back_retry_message_uses_msg_id_when_origin_missing_like_java() {
        let consumer = new_unstarted_impl();
        consumer.update_consumer_config(|config| config.consumer_group = CheetahString::from_static_str("PushGroup"));
        let msg = message_ext_for_send_back("TopicA");

        let retry_message = consumer
            .build_retry_message_for_send_back(&msg)
            .expect("retry message should build");

        assert_eq!(
            MessageAccessor::get_origin_message_id(&retry_message).as_deref(),
            Some("msg-id-current")
        );
    }

    #[test]
    fn send_message_back_treats_empty_message_broker_name_as_java_null() {
        let msg = message_ext_for_send_back("TopicA");

        assert!(DefaultMQPushConsumerImpl::message_broker_name_for_send_back(&msg).is_none());
    }

    #[test]
    fn send_message_back_preserves_non_empty_message_broker_name_like_java() {
        let mut msg = message_ext_for_send_back("TopicA");
        msg.broker_name = CheetahString::from_static_str("broker-a");

        assert_eq!(
            DefaultMQPushConsumerImpl::message_broker_name_for_send_back(&msg).as_deref(),
            Some("broker-a")
        );
    }

    #[tokio::test]
    async fn send_message_back_restores_topic_when_retry_fallback_errors_like_java_finally() {
        let client_config = ClientConfig {
            namespace: Some(CheetahString::from_static_str("ns")),
            ..Default::default()
        };
        let consumer_config = ConsumerConfig {
            consumer_group: CheetahString::from_static_str("PushGroup"),
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumerImpl::new(client_config, consumer_config, None);
        let mut msg = message_ext_for_send_back("ns%TopicA");

        let error = consumer
            .send_message_back_with_broker_name(&mut msg, 3, Some(CheetahString::from_static_str("broker-a")), None)
            .await
            .expect_err("missing client instance should make fallback send fail");

        assert!(error.to_string().contains("MQClientInstance"));
        assert_eq!(msg.topic().as_str(), "TopicA");
    }

    #[tokio::test]
    async fn send_message_back_queue_overload_uses_message_broker_name_like_java() {
        let consumer = new_unstarted_impl();
        let mut msg = message_ext_for_send_back("TopicA");
        msg.broker_name = CheetahString::from_string(format!("{}broker-a", mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX));
        let mq = MessageQueue::from_parts("TopicA", "broker-a", 0);

        consumer
            .send_message_back(&mut msg, 3, &mq)
            .await
            .expect("logical broker name from message should use Java normal-message fallback path");
    }

    #[test]
    fn try_reset_pop_retry_topic_restores_java_v1_pop_retry_topic() {
        let mut message = MessageExt::default();
        message.set_topic(CheetahString::from_static_str("%RETRY%PushGroup_TopicA"));
        let queued_message = Arc::new(message);
        let mut messages = vec![queued_message.clone()];

        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(&mut messages, "PushGroup");

        assert_eq!(messages[0].topic().as_str(), "TopicA");
        assert_eq!(queued_message.topic().as_str(), "%RETRY%PushGroup_TopicA");
        assert!(!Arc::ptr_eq(&messages[0], &queued_message));
    }

    #[tokio::test]
    async fn fetch_subscribe_message_queues_prefers_cached_route_like_java() {
        let client_config = ClientConfig {
            namespace: Some(CheetahString::from_static_str("ns")),
            ..Default::default()
        };
        let consumer = DefaultMQPushConsumerImpl::new(client_config, ConsumerConfig::default(), None);
        let topic = CheetahString::from_static_str("ns%TopicA");
        let cached_queues = HashSet::from([
            MessageQueue::from_parts("ns%TopicA", "broker-b", 1),
            MessageQueue::from_parts("ns%TopicA", "broker-a", 0),
        ]);
        consumer
            .rebalance_impl
            .rebalance_impl_inner
            .topic_subscribe_info_table
            .write()
            .await
            .insert(topic.clone(), cached_queues);

        let queues = consumer
            .fetch_subscribe_message_queues(topic.as_str())
            .await
            .expect("cached subscribe route should be returned without namesrv access");

        assert_eq!(
            queues,
            vec![
                MessageQueue::from_parts("TopicA", "broker-a", 0),
                MessageQueue::from_parts("TopicA", "broker-b", 1),
            ]
        );
    }

    #[tokio::test]
    async fn consumer_running_info_includes_process_queue_snapshots() {
        let consumer = new_unstarted_impl();
        let mq = message_queue();
        let process_queue = Arc::new(ProcessQueue::new());
        consumer
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), process_queue);

        let pop_process_queue = Arc::new(PopProcessQueue::new());
        pop_process_queue.inc_found_msg(2);
        consumer
            .rebalance_impl
            .rebalance_impl_inner
            .pop_process_queue_table
            .write()
            .await
            .insert(mq.clone(), pop_process_queue);

        let info = consumer.consumer_running_info().await;

        assert!(info.prop_consumer_start_timestamp > 0);
        assert!(info.mq_table.contains_key(&mq));
        assert_eq!(info.mq_table.get(&mq).map(|pq| pq.commit_offset), Some(0));
        assert_eq!(info.mq_pop_table.get(&mq).map(|pq| pq.wait_ack_count()), Some(2));
    }

    #[tokio::test]
    async fn consumer_running_info_includes_push_status_table() {
        let consumer = new_unstarted_impl();
        let topic = CheetahString::from_static_str("topic-status");
        let subscription_data =
            FilterAPI::build_subscription_data(&topic, &CheetahString::from_static_str(SubscriptionData::SUB_ALL))
                .expect("subscription data should build");
        consumer
            .rebalance_impl
            .put_subscription_data(topic.clone(), subscription_data);
        consumer.set_mq_client_factory(MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            "push-running-info-status-test",
            None,
        ));

        let info = consumer.consumer_running_info().await;

        assert!(info.status_table.contains_key(topic.as_str()));
        assert_eq!(
            info.status_table
                .get(topic.as_str())
                .map(|status| status.consume_failed_msgs),
            Some(0)
        );
    }

    #[tokio::test]
    async fn reset_offsets_drops_clears_persists_and_removes_assigned_queue_like_java() {
        let consumer = Arc::new(new_unstarted_impl());
        consumer.initialize_self_reference();
        consumer.set_consume_orderly(true);
        consumer.update_consumer_config(|config| config.consumer_group = CheetahString::from_static_str("ResetGroup"));

        let offset_store = Arc::new(OffsetStore::new_test());
        consumer.set_offset_store(Some(offset_store.clone()));

        let topic = CheetahString::from_static_str("reset-topic");
        let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
        let pq = Arc::new(ProcessQueue::new());
        let mut message = MessageExt {
            queue_offset: 7,
            ..Default::default()
        };
        message.set_topic(topic.clone());
        message.set_body(Bytes::from_static(b"cached-body"));
        pq.put_message(&[Arc::new(message)]).await;
        assert_eq!(pq.msg_count(), 1);

        consumer
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), pq.clone());

        MQConsumerInner::reset_offsets(consumer.as_ref(), &topic, HashMap::from([(mq.clone(), 321)])).await;

        assert!(pq.is_dropped());
        assert_eq!(pq.msg_count(), 0);
        assert!(!pq.has_temp_message().await);
        assert!(!consumer
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await
            .contains_key(&mq));
        assert_eq!(offset_store.read_offset(&mq, ReadOffsetType::ReadFromMemory).await, -1);
        assert_eq!(offset_store.test_persisted_offset(&mq), Some(321));
        assert_eq!(offset_store.test_persist_all_count(), 2);
    }

    #[tokio::test]
    async fn copy_subscription_reads_immutable_config_snapshot() {
        let topic = CheetahString::from_static_str("TopicSnapshotCopy");
        let expression = CheetahString::from_static_str("TagA || TagB");
        let consumer = new_check_config_consumer(None);
        consumer.update_consumer_config(|config| {
            config.subscription = Arc::new(HashMap::from([(topic.clone(), expression.clone())]))
        });

        consumer
            .copy_subscription()
            .await
            .expect("subscription snapshot should copy into rebalance state");

        let subscriptions = consumer.rebalance_impl.get_subscription_inner();
        let copied = subscriptions
            .get(&topic)
            .expect("configured topic should be present after copy");
        assert_eq!(copied.sub_string, expression);
    }
}
