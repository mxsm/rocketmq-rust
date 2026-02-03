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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use futures::future;
use rand::seq::IndexedRandom;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::base::connection_net_event::ConnectionNetEvent;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::consumer_data::ConsumerData;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::producer_data::ProducerData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::admin::mq_admin_ext_async_inner::MQAdminExtInnerImpl;
use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::pull_message_service::PullMessageService;
use crate::consumer::consumer_impl::re_balance::rebalance_service::RebalanceService;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::find_broker_result::FindBrokerResult;
use crate::implementation::mq_admin_impl::MQAdminImpl;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

const LOCK_TIMEOUT_MILLIS: u64 = 3000;

pub struct MQClientInstance {
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) client_id: CheetahString,
    boot_timestamp: u64,
    /**
     * The container of the producer in the current client. The key is the name of
     * producerGroup.
     */
    producer_table: Arc<DashMap<CheetahString, MQProducerInnerImpl>>,
    /**
     * The container of the consumer in the current client. The key is the name of
     * consumer_group.
     */
    consumer_table: Arc<DashMap<CheetahString, MQConsumerInnerImpl>>,
    /**
     * The container of the adminExt in the current client. The key is the name of
     * adminExtGroup.
     */
    admin_ext_table: Arc<DashMap<CheetahString, MQAdminExtInnerImpl>>,
    pub(crate) mq_client_api_impl: Option<ArcMut<MQClientAPIImpl>>,
    pub(crate) mq_admin_impl: ArcMut<MQAdminImpl>,
    pub(crate) topic_route_table: Arc<DashMap<CheetahString /* Topic */, TopicRouteData>>,
    topic_end_points_table:
        Arc<DashMap<CheetahString /* Topic */, HashMap<MessageQueue, CheetahString /* brokerName */>>>,
    lock_namesrv: Arc<RocketMQTokioMutex<()>>,
    lock_heartbeat: Arc<RocketMQTokioMutex<()>>,

    service_state: ServiceState,
    pub(crate) pull_message_service: ArcMut<PullMessageService>,
    rebalance_service: RebalanceService,
    pub(crate) default_producer: ArcMut<DefaultMQProducer>,
    broker_addr_table: Arc<DashMap<CheetahString, HashMap<u64, CheetahString>>>,
    broker_version_table: Arc<DashMap<CheetahString /* Broker Name */, HashMap<CheetahString /* address */, i32>>>,
    send_heartbeat_times_total: Arc<AtomicI64>,
    scheduled_task_manager: ScheduledTaskManager,
    /// HeartbeatV2: Cache of broker address -> last fingerprint
    broker_heartbeat_fingerprint_table: Arc<DashMap<CheetahString, i32>>,
    /// HeartbeatV2: Set of brokers that support V2 protocol
    broker_support_v2_heartbeat_set: Arc<DashMap<CheetahString, ()>>,
}

impl MQClientInstance {
    pub fn new_arc(
        client_config: ClientConfig,
        _instance_index: i32,
        client_id: impl Into<CheetahString>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> ArcMut<MQClientInstance> {
        let client_id = client_id.into();
        let shared_config = ArcMut::new(client_config.clone());

        let broker_addr_table = Arc::new(DashMap::default());
        let producer_table = Arc::new(DashMap::default());
        let consumer_table = Arc::new(DashMap::default());
        let admin_ext_table = Arc::new(DashMap::default());
        let topic_route_table = Arc::new(DashMap::default());
        let topic_end_points_table = Arc::new(DashMap::default());
        let broker_version_table = Arc::new(DashMap::default());
        let broker_heartbeat_fingerprint_table = Arc::new(DashMap::default());
        let broker_support_v2_heartbeat_set = Arc::new(DashMap::default());

        let default_producer = ArcMut::new(
            DefaultMQProducer::builder()
                .producer_group(mix_all::CLIENT_INNER_PRODUCER_GROUP)
                .client_config(client_config.clone())
                .build(),
        );
        let mut instance = ArcMut::new(MQClientInstance {
            client_config: shared_config,
            client_id,
            boot_timestamp: get_current_millis(),
            producer_table,
            consumer_table,
            admin_ext_table,
            mq_client_api_impl: None,
            mq_admin_impl: ArcMut::new(MQAdminImpl::new()),
            topic_route_table,
            topic_end_points_table,
            lock_namesrv: Arc::default(),
            lock_heartbeat: Arc::default(),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcMut::new(PullMessageService::new()),
            rebalance_service: RebalanceService::new(),
            default_producer,
            broker_addr_table,
            broker_version_table,
            send_heartbeat_times_total: Arc::new(AtomicI64::new(0)),
            scheduled_task_manager: ScheduledTaskManager::new(),
            broker_heartbeat_fingerprint_table,
            broker_support_v2_heartbeat_set,
        });

        // Clone instance first to avoid borrow checker issues
        let instance_clone = instance.clone();
        instance.mq_admin_impl.set_client(instance_clone);

        let (tx, mut rx) = tokio::sync::broadcast::channel::<ConnectionNetEvent>(16);

        let mq_client_api_impl = ArcMut::new(MQClientAPIImpl::new(
            Arc::new(TokioClientConfig::default()),
            ClientRemotingProcessor::new(instance.clone()),
            rpc_hook,
            client_config.clone(),
            Some(tx),
        ));

        if let Some(namesrv_addr) = client_config.namesrv_addr.as_deref() {
            let api_impl = mq_client_api_impl.clone();
            let addr = namesrv_addr.to_string();
            // Use block_in_place for synchronous initialization in async context
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    api_impl.update_name_server_address_list(&addr).await;
                })
            });
        }

        instance.mq_client_api_impl = Some(mq_client_api_impl);

        // Use weak reference to avoid circular dependencies
        let weak_instance = ArcMut::downgrade(&instance);
        tokio::spawn(async move {
            while let Ok(value) = rx.recv().await {
                if let Some(instance_) = weak_instance.upgrade() {
                    match value {
                        ConnectionNetEvent::CONNECTED(remote_address) => {
                            info!("ConnectionNetEvent CONNECTED");

                            let matched_brokers: Vec<_> = instance_
                                .broker_addr_table
                                .iter()
                                .flat_map(|entry| {
                                    let (name, addrs) = entry.pair();
                                    addrs
                                        .iter()
                                        .filter(|(_, addr)| addr.as_str() == remote_address.to_string().as_str())
                                        .map(|(id, addr)| (*id, name.clone(), addr.clone()))
                                        .collect::<Vec<_>>()
                                })
                                .collect();

                            for (id, broker_name, addr) in matched_brokers {
                                if instance_.send_heartbeat_to_broker(id, &broker_name, &addr, false).await {
                                    instance_.re_balance_immediately();
                                }
                            }
                        }
                        ConnectionNetEvent::DISCONNECTED => {}
                        ConnectionNetEvent::EXCEPTION => {}
                        ConnectionNetEvent::IDLE => {}
                    }
                }
            }
            warn!("ConnectionNetEvent recv error");
        });
        instance
    }

    pub fn re_balance_immediately(&self) {
        self.rebalance_service.wakeup();
    }

    pub fn re_balance_later(&self, delay_millis: Duration) {
        if delay_millis <= Duration::from_millis(0) {
            self.rebalance_service.wakeup();
        } else {
            let service = self.rebalance_service.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay_millis).await;
                service.wakeup();
            });
        }
    }

    pub async fn start(&mut self, this: ArcMut<Self>) -> rocketmq_error::RocketMQResult<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                // If not specified,looking address from name remoting_server
                if self.client_config.namesrv_addr.is_none() {
                    self.mq_client_api_impl
                        .as_mut()
                        .expect("mq_client_api_impl is None")
                        .fetch_name_server_addr()
                        .await;
                }
                // Start request-response channel
                self.mq_client_api_impl
                    .as_mut()
                    .expect("mq_client_api_impl is None")
                    .start()
                    .await;
                // Start various schedule tasks
                self.start_scheduled_task(this.clone());
                // Start pull service
                let instance = this.clone();
                if let Err(e) = self.pull_message_service.start(instance).await {
                    error!("Failed to start pull message service: {:?}", e);
                }
                // Start rebalance service
                if let Err(e) = self.rebalance_service.start(this).await {
                    error!("Failed to start rebalance service: {:?}", e);
                }
                // Start push service

                self.default_producer
                    .default_mqproducer_impl
                    .as_mut()
                    .unwrap()
                    .start_with_factory(false)
                    .await?;
                info!("the client factory[{}] start OK", self.client_id);
                self.service_state = ServiceState::Running;
            }
            ServiceState::Running => {}
            ServiceState::ShutdownAlready => {}
            ServiceState::StartFailed => {
                return Err(mq_client_err!(format!(
                    "The Factory object[{}] has been created before, and failed.",
                    self.client_id
                )));
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        match self.service_state {
            ServiceState::CreateJust | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                warn!(
                    "MQClientInstance shutdown called but state is {:?}, ignoring shutdown request",
                    self.service_state
                );
                return;
            }
            ServiceState::Running => {
                info!(
                    "MQClientInstance[{}] shutdown starting, current state: Running",
                    self.client_id
                );
            }
        }

        info!("MQClientInstance[{}] shutting down rebalance service", self.client_id);
        if let Err(e) = self.rebalance_service.shutdown(3000).await {
            warn!("Failed to shutdown rebalance service: {:?}", e);
        }

        info!(
            "MQClientInstance[{}] shutting down pull message service",
            self.client_id
        );
        if let Err(e) = self.pull_message_service.shutdown_default().await {
            warn!("Failed to shutdown pull message service: {:?}", e);
        }

        info!("MQClientInstance[{}] persisting all consumer offsets", self.client_id);
        self.persist_all_consumer_offset().await;

        info!("MQClientInstance[{}] shutting down default producer", self.client_id);
        if let Some(producer_impl) = self.default_producer.default_mqproducer_impl.as_mut() {
            let shutdown_future = producer_impl.shutdown_with_factory(false);
            if let Err(e) = Box::pin(shutdown_future).await {
                warn!("Failed to shutdown default producer: {:?}", e);
            }
        }

        info!("MQClientInstance[{}] unregistering all consumers", self.client_id);
        self.unregister_all_consumers().await;

        info!("MQClientInstance[{}] unregistering all producers", self.client_id);
        self.unregister_all_producers().await;

        if self.mq_client_api_impl.is_some() {
            info!(
                "MQClientInstance[{}] network client shutdown (deferred to Drop)",
                self.client_id
            );
        }

        info!("MQClientInstance[{}] canceling scheduled tasks", self.client_id);
        self.scheduled_task_manager.cancel_all();
        info!(
            "MQClientInstance[{}] scheduled tasks canceled, total canceled: {}",
            self.client_id,
            self.scheduled_task_manager.task_count()
        );

        info!("MQClientInstance[{}] clearing all registration tables", self.client_id);
        self.producer_table.clear();
        self.consumer_table.clear();
        self.admin_ext_table.clear();

        self.topic_route_table.clear();
        self.topic_end_points_table.clear();
        self.broker_addr_table.clear();
        self.broker_version_table.clear();

        self.service_state = ServiceState::ShutdownAlready;

        info!("MQClientInstance[{}] shutdown completed successfully", self.client_id);
    }

    /// Unregister all producers from broker
    async fn unregister_all_producers(&mut self) {
        // Get all producer groups before removing from table
        let producer_groups: Vec<CheetahString> = self.producer_table.iter().map(|entry| entry.key().clone()).collect();

        // Unregister each producer (removes from table and notifies broker)
        for group in producer_groups {
            info!("Unregistering producer group: {}", group);
            self.unregister_producer(group).await;
        }
    }

    /// Unregister all consumers from broker
    async fn unregister_all_consumers(&mut self) {
        // Get all consumer groups before removing from table
        let consumer_groups: Vec<CheetahString> = self.consumer_table.iter().map(|entry| entry.key().clone()).collect();

        // Unregister each consumer (removes from table and notifies broker)
        for group in consumer_groups {
            info!("Unregistering consumer group: {}", group);
            self.unregister_consumer(group).await;
        }
    }

    pub async fn register_producer(&mut self, group: &str, producer: MQProducerInnerImpl) -> bool {
        if group.is_empty() {
            return false;
        }
        if self.producer_table.contains_key(group) {
            warn!("the producer group[{}] exist already.", group);
            return false;
        }
        self.producer_table.insert(group.into(), producer);
        true
    }

    pub async fn register_admin_ext(&mut self, group: &str, admin: MQAdminExtInnerImpl) -> bool {
        if group.is_empty() {
            return false;
        }
        if self.admin_ext_table.contains_key(group) {
            warn!("the admin group[{}] exist already.", group);
            return false;
        }
        self.admin_ext_table.insert(group.into(), admin);
        true
    }

    fn start_scheduled_task(&mut self, this: ArcMut<Self>) {
        info!("Starting scheduled tasks with ScheduledTaskManager");

        if self.client_config.namesrv_addr.is_none() {
            let mq_client_api_impl = self.mq_client_api_impl.as_ref().unwrap().clone();
            self.scheduled_task_manager.add_fixed_rate_task_async(
                Duration::from_secs(10),
                Duration::from_secs(120),
                async move |_token| {
                    let mut api = mq_client_api_impl.clone();
                    info!("ScheduledTask: fetchNameServerAddr");
                    api.fetch_name_server_addr().await;
                    Ok(())
                },
            );
        }

        let client_instance = this.clone();
        let poll_name_server_interval = self.client_config.poll_name_server_interval;
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_millis(10),
            Duration::from_millis(poll_name_server_interval as u64),
            async move |_token| {
                let mut instance = client_instance.clone();
                info!("ScheduledTask: update_topic_route_info_from_name_server");
                instance.update_topic_route_info_from_name_server().await;
                Ok(())
            },
        );

        let client_instance = this.clone();
        let heartbeat_broker_interval = self.client_config.heartbeat_broker_interval;
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(1),
            Duration::from_millis(heartbeat_broker_interval as u64),
            async move |_token| {
                let mut instance = client_instance.clone();
                info!("ScheduledTask: clean_offline_broker and send_heartbeat");
                instance.clean_offline_broker().await;
                instance.send_heartbeat_to_all_broker_with_lock().await;
                Ok(())
            },
        );

        let client_instance = this;
        let persist_consumer_offset_interval = self.client_config.persist_consumer_offset_interval as u64;
        self.scheduled_task_manager.add_fixed_rate_task_async(
            Duration::from_secs(10),
            Duration::from_millis(persist_consumer_offset_interval),
            async move |_token| {
                let mut instance = client_instance.clone();
                info!("ScheduledTask: persistAllConsumerOffset");
                instance.persist_all_consumer_offset().await;
                Ok(())
            },
        );

        info!(
            "All scheduled tasks started, total tasks: {}",
            self.scheduled_task_manager.task_count()
        );
    }

    pub async fn update_topic_route_info_from_name_server(&mut self) {
        let mut topic_list = HashSet::new();

        for entry in self.producer_table.iter() {
            topic_list.extend(entry.value().get_publish_topic_list());
        }

        for entry in self.consumer_table.iter() {
            entry.value().subscriptions().iter().for_each(|sub| {
                topic_list.insert(sub.topic.clone());
            });
        }

        for topic in topic_list.iter() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
        }
    }

    #[inline]
    pub async fn update_topic_route_info_from_name_server_topic(&mut self, topic: &CheetahString) -> bool {
        self.update_topic_route_info_from_name_server_default(topic, false, None)
            .await
    }

    pub async fn find_consumer_id_list(
        &mut self,
        topic: &CheetahString,
        group: &CheetahString,
    ) -> Option<Vec<CheetahString>> {
        let mut broker_addr = self.find_broker_addr_by_topic(topic).await;
        if broker_addr.is_none() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
            broker_addr = self.find_broker_addr_by_topic(topic).await;
        }
        if let Some(broker_addr) = broker_addr {
            match self
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .get_consumer_id_list_by_group(broker_addr.as_str(), group, self.client_config.mq_client_api_timeout)
                .await
            {
                Ok(value) => return Some(value),
                Err(e) => {
                    warn!(
                        "getConsumerIdListByGroup exception,{}  {}, err:{}",
                        broker_addr,
                        group,
                        e.to_string()
                    );
                }
            }
        }
        None
    }

    pub async fn find_broker_addr_by_topic(&self, topic: &str) -> Option<CheetahString> {
        if let Some(topic_route_data) = self.topic_route_table.get(topic) {
            let brokers = &topic_route_data.value().broker_datas;
            if !brokers.is_empty() {
                let bd = brokers.choose(&mut rand::rng());
                if let Some(bd) = bd {
                    return bd.select_broker_addr();
                }
            }
        }
        None
    }

    pub async fn update_topic_route_info_from_name_server_default(
        &mut self,
        topic: &CheetahString,
        is_default: bool,
        producer_config: Option<&Arc<ProducerConfig>>,
    ) -> bool {
        let lock = self.lock_namesrv.lock().await;
        let topic_route_data = if let (true, Some(producer_config)) = (is_default, producer_config) {
            let mut result = match self
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .get_default_topic_route_info_from_name_server(self.client_config.mq_client_api_timeout)
                .await
            {
                Ok(value) => value,
                Err(e) => {
                    error!(
                        "get_default_topic_route_info_from_name_server failed, topic: {}, error: {}",
                        topic, e
                    );
                    None
                }
            };
            if let Some(topic_route_data) = result.as_mut() {
                for data in topic_route_data.queue_datas.iter_mut() {
                    let queue_nums = producer_config.default_topic_queue_nums().max(data.read_queue_nums);
                    data.read_queue_nums = queue_nums;
                    data.write_queue_nums = queue_nums;
                }
            }
            result
        } else {
            self.mq_client_api_impl
                .as_mut()
                .unwrap()
                .get_topic_route_info_from_name_server(topic, self.client_config.mq_client_api_timeout)
                .await
                .unwrap_or(None)
        };
        if let Some(mut topic_route_data) = topic_route_data {
            let old = self.topic_route_table.get(topic).map(|entry| entry.value().clone());
            let mut changed = topic_route_data.topic_route_data_changed(old.as_ref());
            if !changed {
                changed = self.is_need_update_topic_route_info(topic).await;
            } else {
                info!(
                    "the topic[{}] route info changed, old[{:?}] ,new[{:?}]",
                    topic, old, topic_route_data
                )
            }
            if changed {
                for bd in topic_route_data.broker_datas.iter() {
                    self.broker_addr_table
                        .insert(bd.broker_name().clone(), bd.broker_addrs().clone());
                }

                // Update endpoint map
                {
                    let mq_end_points =
                        ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, &topic_route_data);
                    if let Some(mq_end_points) = mq_end_points {
                        if !mq_end_points.is_empty() {
                            self.topic_end_points_table.insert(topic.into(), mq_end_points);
                        }
                    }
                }

                // Update Pub info
                {
                    let mut publish_info = topic_route_data2topic_publish_info(topic, &mut topic_route_data);
                    publish_info.have_topic_router_info = true;
                    for mut entry in self.producer_table.iter_mut() {
                        entry
                            .value_mut()
                            .update_topic_publish_info(topic.to_string(), Some(publish_info.clone()));
                    }
                }

                // Update sub info
                if !self.consumer_table.is_empty() {
                    let subscribe_info = topic_route_data2topic_subscribe_info(topic, &topic_route_data);

                    let consumers: Vec<_> = self.consumer_table.iter().map(|entry| entry.value().clone()).collect();

                    for consumer in consumers {
                        consumer
                            .update_topic_subscribe_info(topic.clone(), &subscribe_info)
                            .await;
                    }
                }
                let clone_topic_route_data = TopicRouteData::from_existing(&topic_route_data);
                self.topic_route_table.insert(topic.clone(), clone_topic_route_data);
                return true;
            }
        } else {
            warn!(
                "updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]",
                topic, self.client_id
            );
        }

        drop(lock);
        false
    }

    async fn is_need_update_topic_route_info(&self, topic: &CheetahString) -> bool {
        for entry in self.producer_table.iter() {
            if entry.value().is_publish_topic_need_update(topic) {
                return true;
            }
        }

        for entry in self.consumer_table.iter() {
            if entry.value().is_subscribe_topic_need_update(topic).await {
                return true;
            }
        }
        false
    }

    pub async fn persist_all_consumer_offset(&mut self) {
        for entry in self.consumer_table.iter() {
            entry.value().persist_consumer_offset().await;
        }
    }

    pub async fn clean_offline_broker(&mut self) {
        let lock = self
            .lock_namesrv
            .try_lock_timeout(Duration::from_millis(LOCK_TIMEOUT_MILLIS))
            .await;
        if let Some(lock) = lock {
            let mut updated_table = HashMap::new();
            let mut broker_name_set = HashSet::new();

            for broker_entry in self.broker_addr_table.iter() {
                let (broker_name, one_table) = broker_entry.pair();
                let mut clone_addr_table = one_table.clone();
                let mut remove_id_set = HashSet::new();
                for (id, addr) in one_table.iter() {
                    if !self.is_broker_addr_exist_in_topic_route_table(addr).await {
                        remove_id_set.insert(*id);
                    }
                }
                clone_addr_table.retain(|k, _| !remove_id_set.contains(k));
                if clone_addr_table.is_empty() {
                    info!("the broker[{}] name's host is offline, remove it", broker_name);
                    broker_name_set.insert(broker_name.clone());
                } else {
                    updated_table.insert(broker_name.clone(), clone_addr_table);
                }
            }

            // Remove offline brokers and update with new data
            for broker_name in broker_name_set {
                self.broker_addr_table.remove(&broker_name);
            }
            for (broker_name, addrs) in updated_table {
                self.broker_addr_table.insert(broker_name, addrs);
            }
        }
    }
    pub async fn send_heartbeat_to_all_broker_with_lock(&self) -> bool {
        let _guard = match self.lock_heartbeat.try_lock().await {
            Some(g) => g,
            None => {
                warn!("lock heartBeat, but failed. [{}]", self.client_id);
                return false;
            }
        };

        // Check if concurrent heartbeat is enabled
        if self.client_config.enable_concurrent_heartbeat {
            return self.send_heartbeat_to_all_broker_concurrently().await;
        }

        // Use V2 or V1 protocol based on configuration
        if self.client_config.use_heartbeat_v2 {
            self.send_heartbeat_to_all_broker_v2(false).await
        } else {
            self.send_heartbeat_to_all_broker().await
        }
    }

    pub async fn send_heartbeat_to_all_broker_with_lock_v2(&self, is_rebalance: bool) -> bool {
        let _guard = match self.lock_heartbeat.try_lock_timeout(Duration::from_secs(2)).await {
            Some(g) => g,
            None => {
                warn!("lock heartBeat, but failed. [{}]", self.client_id);
                return false;
            }
        };

        if self.client_config.use_heartbeat_v2 {
            self.send_heartbeat_to_all_broker_v2(is_rebalance).await
        } else {
            self.send_heartbeat_to_all_broker().await
        }
    }

    pub fn get_mq_client_api_impl(&self) -> ArcMut<MQClientAPIImpl> {
        self.mq_client_api_impl.as_ref().unwrap().clone()
    }

    pub async fn get_broker_name_from_message_queue(&self, message_queue: &MessageQueue) -> CheetahString {
        if let Some(broker_name) = self.topic_end_points_table.get(message_queue.get_topic()) {
            if let Some(addr) = broker_name.value().get(message_queue) {
                return addr.clone();
            }
        }
        message_queue.get_broker_name().clone()
    }

    pub async fn find_broker_address_in_publish(&self, broker_name: &CheetahString) -> Option<CheetahString> {
        if broker_name.is_empty() {
            return None;
        }
        let map = self.broker_addr_table.get(broker_name);
        if let Some(map) = map {
            return map.get(&(mix_all::MASTER_ID)).cloned();
        }
        None
    }

    async fn send_heartbeat_to_all_broker_v2(&self, is_rebalance: bool) -> bool {
        let heartbeat_data_with_sub = self.prepare_heartbeat_data(false).await;
        let producer_empty = heartbeat_data_with_sub.producer_data_set.is_empty();
        let consumer_empty = heartbeat_data_with_sub.consumer_data_set.is_empty();

        if producer_empty && consumer_empty {
            warn!(
                "sendHeartbeatToAllBrokerV2 sending heartbeat, but no consumer and no producer. [{}]",
                self.client_id
            );
            return false;
        }

        if self.broker_addr_table.is_empty() {
            return false;
        }

        // Reset fingerprint map on rebalance
        if is_rebalance {
            self.reset_broker_addr_heartbeat_fingerprint_map().await;
        }

        // Compute fingerprint for current heartbeat
        let current_fingerprint = heartbeat_data_with_sub.compute_heartbeat_fingerprint();

        // Send to all brokers
        for broker_entry in self.broker_addr_table.iter() {
            let (broker_name, broker_addrs) = broker_entry.pair();
            if broker_addrs.is_empty() {
                continue;
            }

            for (id, addr) in broker_addrs.iter() {
                if addr.is_empty() {
                    continue;
                }
                // Skip non-master brokers for consumer-only clients
                if consumer_empty && *id != mix_all::MASTER_ID {
                    continue;
                }

                // Clone heartbeat data for this broker
                let mut heartbeat_data = heartbeat_data_with_sub.clone();
                heartbeat_data.heartbeat_fingerprint = current_fingerprint;

                self.send_heartbeat_to_broker_v2(*id, broker_name, addr, heartbeat_data)
                    .await;
            }
        }

        true
    }

    async fn reset_broker_addr_heartbeat_fingerprint_map(&self) {
        self.broker_heartbeat_fingerprint_table.clear();
    }

    /// Send heartbeat to all brokers concurrently
    async fn send_heartbeat_to_all_broker_concurrently(&self) -> bool {
        let heartbeat_data = self.prepare_heartbeat_data(false).await;
        let producer_empty = heartbeat_data.producer_data_set.is_empty();
        let consumer_empty = heartbeat_data.consumer_data_set.is_empty();

        if producer_empty && consumer_empty {
            warn!(
                "sending heartbeat concurrently, but no consumer and no producer. [{}]",
                self.client_id
            );
            return false;
        }

        // Collect broker list without holding lock during task execution
        let broker_list: Vec<(u64, CheetahString, CheetahString)> = {
            if self.broker_addr_table.is_empty() {
                return false;
            }

            let mut list = Vec::new();
            for broker_entry in self.broker_addr_table.iter() {
                let (broker_name, broker_addrs) = broker_entry.pair();
                if broker_addrs.is_empty() {
                    continue;
                }

                for (id, addr) in broker_addrs.iter() {
                    if addr.is_empty() {
                        continue;
                    }
                    // Skip non-master brokers for consumer-only clients
                    if consumer_empty && *id != mix_all::MASTER_ID {
                        continue;
                    }
                    list.push((*id, broker_name.clone(), addr.clone()));
                }
            }
            list
        };

        if broker_list.is_empty() {
            return false;
        }

        let task_count = broker_list.len();

        // Collect all heartbeat tasks
        let mut tasks = Vec::new();
        for (broker_id, broker_name, addr) in broker_list {
            // Clone necessary data for the async task
            let heartbeat_data = heartbeat_data.clone();
            let client_id = self.client_id.clone();
            let mq_client_api = self.mq_client_api_impl.as_ref().unwrap().clone();
            let timeout = self.client_config.mq_client_api_timeout;
            let send_heartbeat_times_total = self.send_heartbeat_times_total.clone();
            let topic_route_table_clone = self.topic_route_table.clone();

            // Spawn independent task for each broker
            // Returns: (broker_name, addr, version, success)
            let task = tokio::spawn(async move {
                match mq_client_api
                    .mut_from_ref()
                    .send_heartbeat(&addr, &heartbeat_data, timeout)
                    .await
                {
                    Ok((version, _response)) => {
                        let times = send_heartbeat_times_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if times % 20 == 0 {
                            info!(
                                "send heart beat to broker[{} {} {}] success (concurrent)",
                                broker_name, broker_id, addr
                            );
                        }
                        (broker_name, addr, Some(version), true)
                    }
                    Err(_) => {
                        // Check if broker is in name server
                        let is_in_ns = topic_route_table_clone.iter().any(|route_entry| {
                            route_entry.value().broker_datas.iter().any(|bd| {
                                bd.broker_addrs()
                                    .iter()
                                    .any(|(_, broker_addr)| broker_addr.as_str() == addr.as_str())
                            })
                        });

                        if is_in_ns {
                            warn!(
                                "send heart beat to broker[{} {} {}] failed (concurrent)",
                                broker_name, broker_id, addr
                            );
                        } else {
                            warn!(
                                "send heart beat to broker[{} {} {}] exception, because the broker not up, forget it \
                                 (concurrent)",
                                broker_name, broker_id, addr
                            );
                        }
                        (broker_name, addr, None, false)
                    }
                }
            });

            tasks.push(task);
        }

        // Wait for all tasks with timeout (3 seconds)
        let results = match tokio::time::timeout(Duration::from_millis(3000), future::join_all(tasks)).await {
            Ok(results) => results,
            Err(_) => {
                warn!(
                    "Concurrent heartbeat timeout after 3000ms for client [{}]",
                    self.client_id
                );
                return false;
            }
        };

        // Update broker version table after all tasks complete
        // This avoids lock contention during task execution
        let mut success_count = 0;

        for result in results {
            match result {
                Ok((broker_name, addr, version_opt, success)) => {
                    if success {
                        success_count += 1;
                        if let Some(version) = version_opt {
                            self.broker_version_table
                                .entry(broker_name.clone())
                                .or_default()
                                .insert(addr, version);
                        }
                    }
                }
                Err(e) => {
                    warn!("Concurrent heartbeat task panicked: {:?}", e);
                }
            }
        }

        info!(
            "Concurrent heartbeat completed for client [{}]: {}/{} succeeded",
            self.client_id, success_count, task_count
        );

        // Return true if at least one heartbeat succeeded
        success_count > 0
    }

    async fn send_heartbeat_to_all_broker(&self) -> bool {
        let heartbeat_data = self.prepare_heartbeat_data(false).await;
        let producer_empty = heartbeat_data.producer_data_set.is_empty();
        let consumer_empty = heartbeat_data.consumer_data_set.is_empty();
        if producer_empty && consumer_empty {
            warn!(
                "sending heartbeat, but no consumer and no producer. [{}]",
                self.client_id
            );
            return false;
        }
        if self.broker_addr_table.is_empty() {
            return false;
        }
        for broker_entry in self.broker_addr_table.iter() {
            let (broker_name, broker_addrs) = broker_entry.pair();
            if broker_addrs.is_empty() {
                continue;
            }
            for (id, addr) in broker_addrs.iter() {
                if addr.is_empty() {
                    continue;
                }
                if consumer_empty && *id != mix_all::MASTER_ID {
                    continue;
                }
                self.send_heartbeat_to_broker_inner(*id, broker_name, addr, &heartbeat_data)
                    .await;
            }
        }

        true
    }

    pub async fn send_heartbeat_to_broker(
        &self,
        id: u64,
        broker_name: &CheetahString,
        addr: &CheetahString,
        strict_lock_mode: bool,
    ) -> bool {
        let _guard = match self.lock_heartbeat.try_lock().await {
            Some(g) => g,
            None => {
                if strict_lock_mode {
                    warn!("lock heartBeat, but failed. [{}]", self.client_id);
                }
                return false;
            }
        };

        let heartbeat_data = self.prepare_heartbeat_data(false).await;
        let producer_empty = heartbeat_data.producer_data_set.is_empty();
        let consumer_empty = heartbeat_data.consumer_data_set.is_empty();
        if producer_empty && consumer_empty {
            warn!(
                "sending heartbeat, but no consumer and no producer. [{}]",
                self.client_id
            );
            return false;
        }

        if self.client_config.use_heartbeat_v2 {
            self.send_heartbeat_to_broker_v2(id, broker_name, addr, heartbeat_data)
                .await
        } else {
            let (result, _) = self
                .send_heartbeat_to_broker_inner(id, broker_name, addr, &heartbeat_data)
                .await;
            result
        }
    }

    /// Send HeartbeatV2 to broker
    async fn send_heartbeat_to_broker_v2(
        &self,
        id: u64,
        broker_name: &CheetahString,
        addr: &CheetahString,
        mut heartbeat_data_with_sub: HeartbeatData,
    ) -> bool {
        // Calculate current fingerprint
        let current_fingerprint = heartbeat_data_with_sub.compute_heartbeat_fingerprint();
        heartbeat_data_with_sub.heartbeat_fingerprint = current_fingerprint;

        // Check if this broker supports V2
        let broker_support_v2 = self.broker_support_v2_heartbeat_set.contains_key(addr);

        // Get last fingerprint for this broker
        let last_fingerprint = self
            .broker_heartbeat_fingerprint_table
            .get(addr)
            .map(|entry| *entry.value());

        // Determine if we should send minimal heartbeat (without subscription)
        let should_send_minimal =
            broker_support_v2 && last_fingerprint.is_some() && last_fingerprint.unwrap() == current_fingerprint;

        let heartbeat_data = if should_send_minimal {
            // Send minimal heartbeat without subscription data
            let mut minimal = self.prepare_heartbeat_data(true).await;
            minimal.heartbeat_fingerprint = current_fingerprint;
            minimal
        } else {
            // Send full heartbeat with subscription data
            heartbeat_data_with_sub.clone()
        };

        // Send heartbeat and get result with V2 support info
        let (result, support_v2) = self
            .send_heartbeat_to_broker_inner(id, broker_name, addr, &heartbeat_data)
            .await;

        if result {
            if let Some(support_v2) = support_v2 {
                if support_v2 {
                    // Update V2 support set
                    self.broker_support_v2_heartbeat_set.insert(addr.clone(), ());

                    // Update fingerprint cache only when sending full data
                    if !should_send_minimal {
                        self.broker_heartbeat_fingerprint_table
                            .insert(addr.clone(), current_fingerprint);
                    }
                } else {
                    // Broker doesn't support V2, remove from set and clear fingerprint
                    self.broker_support_v2_heartbeat_set.remove(addr);
                    self.broker_heartbeat_fingerprint_table.remove(addr);

                    warn!("Broker {} does not support HeartbeatV2, downgrading to V1", addr);
                }
            }
        }

        result
    }

    async fn send_heartbeat_to_broker_inner(
        &self,
        id: u64,
        broker_name: &CheetahString,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
    ) -> (bool, Option<bool>) {
        match self
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .send_heartbeat(addr, heartbeat_data, self.client_config.mq_client_api_timeout)
            .await
        {
            Ok((version, response)) => {
                self.broker_version_table
                    .entry(broker_name.clone())
                    .or_default()
                    .insert(addr.clone(), version);

                let times = self
                    .send_heartbeat_times_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if times % 20 == 0 {
                    info!("send heart beat to broker[{} {} {}] success", broker_name, id, addr);
                }

                // Check if broker supports HeartbeatV2
                let support_v2 = response.and_then(|resp| {
                    resp.ext_fields().and_then(|fields| {
                        fields
                            .get(rocketmq_common::common::mix_all::IS_SUPPORT_HEART_BEAT_V2)
                            .and_then(|v| v.parse::<bool>().ok())
                    })
                });

                (true, support_v2)
            }
            Err(_) => {
                if self.is_broker_in_name_server(addr).await {
                    warn!("send heart beat to broker[{} {} {}] failed", broker_name, id, addr);
                } else {
                    warn!(
                        "send heart beat to broker[{} {} {}] exception, because the broker not up, forget it",
                        broker_name, id, addr
                    );
                }
                (false, None)
            }
        }
    }

    async fn is_broker_in_name_server(&self, broker_name: &str) -> bool {
        for entry in self.topic_route_table.iter() {
            for bd in entry.value().broker_datas.iter() {
                for (_, value) in bd.broker_addrs().iter() {
                    if value.as_str() == broker_name {
                        return true;
                    }
                }
            }
        }
        false
    }

    async fn prepare_heartbeat_data(&self, is_without_sub: bool) -> HeartbeatData {
        let mut heartbeat_data = HeartbeatData {
            client_id: self.client_id.clone(),
            ..Default::default()
        };

        for entry in self.consumer_table.iter() {
            let mut consumer_data = ConsumerData {
                group_name: entry.value().group_name(),
                consume_type: entry.value().consume_type(),
                message_model: entry.value().message_model(),
                consume_from_where: entry.value().consume_from_where(),
                subscription_data_set: entry.value().subscriptions(),
                unit_mode: entry.value().is_unit_mode(),
            };
            if !is_without_sub {
                entry.value().subscriptions().iter().for_each(|sub| {
                    consumer_data.subscription_data_set.insert(sub.clone());
                });
            }
            heartbeat_data.consumer_data_set.insert(consumer_data);
        }
        for entry in self.producer_table.iter() {
            let producer_data = ProducerData {
                group_name: entry.key().clone(),
            };
            heartbeat_data.producer_data_set.insert(producer_data);
        }
        heartbeat_data.is_without_sub = is_without_sub;
        heartbeat_data
    }

    pub async fn register_consumer(&mut self, group: &CheetahString, consumer: MQConsumerInnerImpl) -> bool {
        if self.consumer_table.contains_key(group) {
            warn!("the consumer group[{}] exist already.", group);
            return false;
        }
        self.consumer_table.insert(group.clone(), consumer);
        true
    }

    pub async fn check_client_in_broker(&mut self) -> rocketmq_error::RocketMQResult<()> {
        for entry in self.consumer_table.iter() {
            let subscription_inner = entry.value().subscriptions();
            if subscription_inner.is_empty() {
                return Ok(());
            }
            for subscription_data in subscription_inner.iter() {
                if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
                    continue;
                }
                let addr = self.find_broker_addr_by_topic(subscription_data.topic.as_str()).await;
                if let Some(addr) = addr {
                    match self
                        .mq_client_api_impl
                        .as_mut()
                        .unwrap()
                        .check_client_in_broker(
                            addr.as_str(),
                            entry.key().as_str(),
                            self.client_id.as_str(),
                            subscription_data,
                            self.client_config.mq_client_api_timeout,
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => match e {
                            rocketmq_error::RocketMQError::IllegalArgument(_) => {
                                return Err(e);
                            }
                            _ => {
                                let desc = format!(
                                    "Check client in broker error, maybe because you use {} to filter message, but \
                                     server has not been upgraded to support!This error would not affect the launch \
                                     of consumer, but may has impact on message receiving if you have use the new \
                                     features which are not supported by server, please check the log!",
                                    subscription_data.expression_type
                                );
                            }
                        },
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn do_rebalance(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut balanced = true;
        for entry in self.consumer_table.iter() {
            match entry.value().try_rebalance().await {
                Ok(result) => {
                    if !result {
                        balanced = false;
                    }
                }
                Err(e) => {
                    error!(
                        "doRebalance for consumer group [{}] exception:{}",
                        entry.key(),
                        e.to_string()
                    );
                    balanced = false;
                }
            }
        }
        Ok(balanced)
    }

    pub fn rebalance_later(&mut self, delay_millis: u64) {
        if delay_millis == 0 {
            self.rebalance_service.wakeup();
        } else {
            let service = self.rebalance_service.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay_millis)).await;
                service.wakeup();
            });
        }
    }

    pub async fn find_broker_address_in_subscribe(
        &mut self,
        broker_name: &CheetahString,
        broker_id: u64,
        only_this_broker: bool,
    ) -> Option<FindBrokerResult> {
        if broker_name.is_empty() {
            return None;
        }

        let mut broker_addr: Option<CheetahString> = None;
        let mut slave = false;
        let mut found = false;

        if let Some(map_ref) = self.broker_addr_table.get(broker_name) {
            let map = map_ref.value();
            if let Some(addr) = map.get(&broker_id) {
                broker_addr = Some(addr.clone());
                slave = broker_id != mix_all::MASTER_ID;
                found = true;
            } else if !found && broker_id != mix_all::MASTER_ID {
                if let Some(addr) = map.get(&(broker_id + 1)) {
                    broker_addr = Some(addr.clone());
                    found = true;
                }
            }
            if !found && !only_this_broker {
                if let Some((key, value)) = map.iter().next() {
                    broker_addr = Some(value.clone());
                    slave = *key != mix_all::MASTER_ID;
                    found = !value.is_empty();
                }
            }
        }

        if found {
            let broker_addr = broker_addr?;
            let broker_version = self.find_broker_version(broker_name, broker_addr.as_str()).await;
            Some(FindBrokerResult {
                broker_addr,
                slave,
                broker_version,
            })
        } else {
            None
        }
    }
    async fn find_broker_version(&self, broker_name: &str, broker_addr: &str) -> i32 {
        if let Some(map) = self.broker_version_table.get(broker_name) {
            if let Some(version) = map.value().get(broker_addr) {
                return *version;
            }
        }
        0
    }

    pub async fn select_consumer(&self, group: &str) -> Option<MQConsumerInnerImpl> {
        self.consumer_table.get(group).map(|entry| entry.value().clone())
    }

    pub async fn select_producer(&self, group: &str) -> Option<MQProducerInnerImpl> {
        self.producer_table.get(group).map(|entry| entry.value().clone())
    }

    pub async fn unregister_consumer(&mut self, group: impl Into<CheetahString>) -> bool {
        let group = group.into();
        if group.is_empty() {
            warn!("unregister_consumer: group name is empty");
            return false;
        }

        let removed = self.consumer_table.remove(&group).is_some();

        if removed {
            info!("unregister consumer [{}] OK", group);
            self.unregister_client(None, Some(group)).await;
            true
        } else {
            warn!("unregister consumer [{}] failed: not found in consumer table", group);
            false
        }
    }

    pub async fn unregister_producer(&mut self, group: impl Into<CheetahString>) -> bool {
        let group = group.into();
        if group.is_empty() {
            warn!("unregister_producer: group name is empty");
            return false;
        }

        let removed = self.producer_table.remove(&group).is_some();

        if removed {
            info!("unregister producer [{}] OK", group);
            self.unregister_client(Some(group), None).await;
            true
        } else {
            warn!("unregister producer [{}] failed: not found in producer table", group);
            false
        }
    }

    pub async fn unregister_admin_ext(&mut self, group: impl Into<CheetahString>) {
        let _ = self.admin_ext_table.remove(&group.into());
    }
    async fn unregister_client(
        &mut self,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
    ) {
        for broker_entry in self.broker_addr_table.iter() {
            let (broker_name, broker_addrs) = broker_entry.pair();
            for (id, addr) in broker_addrs.iter() {
                if let Err(err) = self
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .unregister_client(
                        addr,
                        self.client_id.clone(),
                        producer_group.clone(),
                        consumer_group.clone(),
                        self.client_config.mq_client_api_timeout,
                    )
                    .await
                {
                } else {
                    info!(
                        "unregister client[Producer: {:?} Consumer: {:?}] from broker[{} {} {}] success",
                        producer_group, consumer_group, broker_name, id, addr,
                    );
                }
            }
        }
    }

    async fn is_broker_addr_exist_in_topic_route_table(&self, addr: &str) -> bool {
        for entry in self.topic_route_table.iter() {
            for bd in entry.value().broker_datas.iter() {
                for (_, value) in bd.broker_addrs().iter() {
                    if value.as_str() == addr {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Queries the assignment for a given topic.
    ///
    /// This function attempts to find the broker address for the specified topic. If the broker
    /// address is not found, it updates the topic route information from the name server and
    /// retries. If the broker address is found, it queries the assignment from the broker.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    /// * `consumer_group` - A reference to a `CheetahString` representing the consumer group.
    /// * `strategy_name` - A reference to a `CheetahString` representing the allocation strategy
    ///   name.
    /// * `message_model` - The message model to use for the query.
    /// * `timeout` - The timeout duration for the query.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with a `HashSet` of `MessageQueueAssignment` if the query
    /// is successful, or an error if it fails.
    pub async fn query_assignment(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        strategy_name: &CheetahString,
        message_model: MessageModel,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<HashSet<MessageQueueAssignment>>> {
        // Try to find broker address
        let mut broker_addr = self.find_broker_addr_by_topic(topic).await;

        // If not found, update and retry
        if broker_addr.is_none() {
            self.update_topic_route_info_from_name_server_topic(topic).await;
            broker_addr = self.find_broker_addr_by_topic(topic).await;
        }
        if let Some(broker_addr) = broker_addr {
            let client_id = self.client_id.clone();
            match self.mq_client_api_impl.as_mut() {
                Some(api_impl) => {
                    api_impl
                        .query_assignment(
                            &broker_addr,
                            topic.clone(),
                            consumer_group.clone(),
                            client_id,
                            strategy_name.clone(),
                            message_model,
                            timeout,
                        )
                        .await
                }
                None => Err(mq_client_err!("mq_client_api_impl is None")),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn consume_message_directly(
        &self,
        message: MessageExt,
        consumer_group: &CheetahString,
        broker_name: Option<CheetahString>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        let consumer_inner = self.consumer_table.get(consumer_group);
        if let Some(entry) = consumer_inner {
            entry.value().consume_message_directly(message, broker_name).await;
        }

        None
    }
}

#[allow(clippy::unnecessary_unwrap)]
pub fn topic_route_data2topic_publish_info(topic: &str, route: &mut TopicRouteData) -> TopicPublishInfo {
    let mut info = TopicPublishInfo {
        topic_route_data: Some(route.clone()),
        ..Default::default()
    };
    if route.order_topic_conf.is_some() && !route.order_topic_conf.as_ref().unwrap().is_empty() {
        let brokers = route
            .order_topic_conf
            .as_ref()
            .unwrap()
            .split(";")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        for broker in brokers {
            let item = broker.split(":").collect::<Vec<&str>>();
            if item.len() == 2 {
                let queue_num = item[1].parse::<i32>().unwrap();
                for i in 0..queue_num {
                    let mq = MessageQueue::from_parts(topic, item[0], i);
                    info.message_queue_list.push(mq);
                }
            }
        }
        info.order_topic = true;
    } else if route.order_topic_conf.is_none()
        && route.topic_queue_mapping_by_broker.is_some()
        && !route.topic_queue_mapping_by_broker.as_ref().unwrap().is_empty()
    {
        info.order_topic = false;
        let mq_end_points = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route);
        if let Some(mq_end_points) = mq_end_points {
            for (mq, broker_name) in mq_end_points {
                info.message_queue_list.push(mq);
            }
        }
        info.message_queue_list
            .sort_by(|a, b| match a.get_queue_id().cmp(&b.get_queue_id()) {
                Ordering::Less => std::cmp::Ordering::Less,
                Ordering::Equal => std::cmp::Ordering::Equal,
                Ordering::Greater => std::cmp::Ordering::Greater,
            });
    } else {
        route.queue_datas.sort();
        for queue_data in route.queue_datas.iter() {
            if PermName::is_writeable(queue_data.perm) {
                let mut broker_data = None;
                for bd in route.broker_datas.iter() {
                    if bd.broker_name() == queue_data.broker_name.as_str() {
                        broker_data = Some(bd.clone());
                        break;
                    }
                }
                if broker_data.is_none() {
                    continue;
                }
                if !broker_data
                    .as_ref()
                    .unwrap()
                    .broker_addrs()
                    .contains_key(&(mix_all::MASTER_ID))
                {
                    continue;
                }
                for i in 0..queue_data.write_queue_nums {
                    let mq = MessageQueue::from_parts(topic, queue_data.broker_name.as_str(), i as i32);
                    info.message_queue_list.push(mq);
                }
            }
        }
    }
    info
}

pub fn topic_route_data2topic_subscribe_info(topic: &str, route: &TopicRouteData) -> HashSet<MessageQueue> {
    if let Some(ref topic_queue_mapping_by_broker) = route.topic_queue_mapping_by_broker {
        if !topic_queue_mapping_by_broker.is_empty() {
            let mq_endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route);
            return mq_endpoints
                .unwrap_or_default()
                .keys()
                .cloned()
                .collect::<HashSet<MessageQueue>>();
        }
    }
    let mut mq_list = HashSet::new();
    for qd in &route.queue_datas {
        if PermName::is_readable(qd.perm) {
            for i in 0..qd.read_queue_nums {
                let mq = MessageQueue::from_parts(topic, qd.broker_name.as_str(), i as i32);
                mq_list.insert(mq);
            }
        }
    }
    mq_list
}
