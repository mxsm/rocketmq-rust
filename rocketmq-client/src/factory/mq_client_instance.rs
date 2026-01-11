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
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
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
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
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
    producer_table: Arc<RwLock<HashMap<CheetahString, MQProducerInnerImpl>>>,
    /**
     * The container of the consumer in the current client. The key is the name of
     * consumer_group.
     */
    consumer_table: Arc<RwLock<HashMap<CheetahString, MQConsumerInnerImpl>>>,
    /**
     * The container of the adminExt in the current client. The key is the name of
     * adminExtGroup.
     */
    admin_ext_table: Arc<RwLock<HashMap<CheetahString, MQAdminExtInnerImpl>>>,
    pub(crate) mq_client_api_impl: Option<ArcMut<MQClientAPIImpl>>,
    pub(crate) mq_admin_impl: ArcMut<MQAdminImpl>,
    pub(crate) topic_route_table: Arc<RwLock<HashMap<CheetahString /* Topic */, TopicRouteData>>>,
    topic_end_points_table:
        Arc<RwLock<HashMap<CheetahString /* Topic */, HashMap<MessageQueue, CheetahString /* brokerName */>>>>,
    lock_namesrv: Arc<RocketMQTokioMutex<()>>,
    lock_heartbeat: Arc<RocketMQTokioMutex<()>>,

    service_state: ServiceState,
    pub(crate) pull_message_service: ArcMut<PullMessageService>,
    rebalance_service: RebalanceService,
    pub(crate) default_producer: ArcMut<DefaultMQProducer>,
    instance_runtime: Arc<RocketMQRuntime>,
    broker_addr_table: Arc<RwLock<HashMap<CheetahString, HashMap<u64, CheetahString>>>>,
    broker_version_table:
        Arc<RwLock<HashMap<CheetahString /* Broker Name */, HashMap<CheetahString /* address */, i32>>>>,
    send_heartbeat_times_total: Arc<AtomicI64>,
    scheduled_task_manager: ScheduledTaskManager,
}

impl MQClientInstance {
    pub fn new(
        client_config: ClientConfig,
        instance_index: i32,
        client_id: String,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        /* let broker_addr_table = Arc::new(Default::default());
        let (tx, _) = tokio::sync::broadcast::channel::<ConnectionNetEvent>(16);
        let rx = tx.subscribe();
        let mq_client_api_impl = ArcMut::new(MQClientAPIImpl::new(
            Arc::new(TokioClientConfig::default()),
            ClientRemotingProcessor::new(),
            rpc_hook,
            client_config.clone(),
            Some(tx),
        ));
        if let Some(namesrv_addr) = client_config.namesrv_addr.as_deref() {
            let handle = Handle::current();
            let mq_client_api_impl_cloned = mq_client_api_impl.clone();
            let namesrv_addr = namesrv_addr.to_string();
            thread::spawn(move || {
                handle.block_on(async move {
                    mq_client_api_impl_cloned
                        .update_name_server_address_list(namesrv_addr.as_str())
                        .await;
                })
            });
        }
        let instance = MQClientInstance {
            client_config: Arc::new(client_config.clone()),
            client_id,
            boot_timestamp: get_current_millis(),
            producer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_table: Arc::new(Default::default()),
            admin_ext_table: Arc::new(Default::default()),
            mq_client_api_impl:None,
            mq_admin_impl: ArcMut::new(MQAdminImpl::new()),
            topic_route_table: Arc::new(Default::default()),
            topic_end_points_table: Arc::new(Default::default()),
            lock_namesrv: Default::default(),
            lock_heartbeat: Default::default(),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcMut::new(PullMessageService::new()),
            rebalance_service: RebalanceService::new(),
            default_producer: ArcMut::new(
                DefaultMQProducer::builder()
                    .producer_group(mix_all::CLIENT_INNER_PRODUCER_GROUP)
                    .client_config(client_config.clone())
                    .build(),
            ),
            instance_runtime: Arc::new(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "mq-client-instance",
            )),
            broker_addr_table,
            broker_version_table: Arc::new(Default::default()),
            send_heartbeat_times_total: Arc::new(AtomicI64::new(0)),
            tx: Some(rx),
        };
        // let instance_ = instance.clone();

        instance*/
        unimplemented!()
    }

    pub fn new_arc(
        client_config: ClientConfig,
        instance_index: i32,
        client_id: impl Into<CheetahString>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> ArcMut<MQClientInstance> {
        let broker_addr_table = Arc::new(Default::default());
        let mut instance = ArcMut::new(MQClientInstance {
            client_config: ArcMut::new(client_config.clone()),
            client_id: client_id.into(),
            boot_timestamp: get_current_millis(),
            producer_table: Arc::new(RwLock::new(HashMap::new())),
            consumer_table: Arc::new(Default::default()),
            admin_ext_table: Arc::new(Default::default()),
            mq_client_api_impl: None,
            mq_admin_impl: ArcMut::new(MQAdminImpl::new()),
            topic_route_table: Arc::new(Default::default()),
            topic_end_points_table: Arc::new(Default::default()),
            lock_namesrv: Default::default(),
            lock_heartbeat: Default::default(),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcMut::new(PullMessageService::new()),
            rebalance_service: RebalanceService::new(),
            default_producer: ArcMut::new(
                DefaultMQProducer::builder()
                    .producer_group(mix_all::CLIENT_INNER_PRODUCER_GROUP)
                    .client_config(client_config.clone())
                    .build(),
            ),
            instance_runtime: Arc::new(RocketMQRuntime::new_multi(num_cpus::get(), "mq-client-instance")),
            broker_addr_table,
            broker_version_table: Arc::new(Default::default()),
            send_heartbeat_times_total: Arc::new(AtomicI64::new(0)),
            scheduled_task_manager: ScheduledTaskManager::new(),
        });
        let instance_clone = instance.clone();
        instance.mq_admin_impl.set_client(instance_clone);
        let weak_instance = ArcMut::downgrade(&instance);
        let (tx, mut rx) = tokio::sync::broadcast::channel::<ConnectionNetEvent>(16);

        let mq_client_api_impl = ArcMut::new(MQClientAPIImpl::new(
            Arc::new(TokioClientConfig::default()),
            ClientRemotingProcessor::new(instance.clone()),
            rpc_hook,
            client_config.clone(),
            Some(tx),
        ));
        instance.mq_client_api_impl = Some(mq_client_api_impl.clone());
        if let Some(namesrv_addr) = client_config.namesrv_addr.as_deref() {
            let handle = Handle::current();

            let namesrv_addr = namesrv_addr.to_string();
            thread::spawn(move || {
                handle.block_on(async move {
                    mq_client_api_impl
                        .update_name_server_address_list(namesrv_addr.as_str())
                        .await;
                })
            });
        }
        tokio::spawn(async move {
            while let Ok(value) = rx.recv().await {
                if let Some(instance_) = weak_instance.upgrade() {
                    match value {
                        ConnectionNetEvent::CONNECTED(remote_address) => {
                            info!("ConnectionNetEvent CONNECTED");
                            let broker_addr_table = instance_.broker_addr_table.read().await;
                            for (broker_name, broker_addrs) in broker_addr_table.iter() {
                                for (id, addr) in broker_addrs.iter() {
                                    if addr == remote_address.to_string().as_str()
                                        && instance_.send_heartbeat_to_broker(*id, broker_name, addr).await
                                    {
                                        instance_.re_balance_immediately();
                                    }
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
        self.producer_table.write().await.clear();
        self.consumer_table.write().await.clear();
        self.admin_ext_table.write().await.clear();

        self.topic_route_table.write().await.clear();
        self.topic_end_points_table.write().await.clear();
        self.broker_addr_table.write().await.clear();
        self.broker_version_table.write().await.clear();

        self.service_state = ServiceState::ShutdownAlready;

        info!("MQClientInstance[{}] shutdown completed successfully", self.client_id);
    }

    /// Unregister all producers from broker
    async fn unregister_all_producers(&mut self) {
        // Get all producer groups before removing from table
        let producer_groups: Vec<CheetahString> = {
            let producer_table = self.producer_table.read().await;
            producer_table.keys().cloned().collect()
        };

        // Unregister each producer (removes from table and notifies broker)
        for group in producer_groups {
            info!("Unregistering producer group: {}", group);
            self.unregister_producer(group).await;
        }
    }

    /// Unregister all consumers from broker
    async fn unregister_all_consumers(&mut self) {
        // Get all consumer groups before removing from table
        let consumer_groups: Vec<CheetahString> = {
            let consumer_table = self.consumer_table.read().await;
            consumer_table.keys().cloned().collect()
        };

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
        let mut producer_table = self.producer_table.write().await;
        if producer_table.contains_key(group) {
            warn!("the producer group[{}] exist already.", group);
            return false;
        }
        producer_table.insert(group.into(), producer);
        true
    }

    pub async fn register_admin_ext(&mut self, group: &str, admin: MQAdminExtInnerImpl) -> bool {
        if group.is_empty() {
            return false;
        }
        let mut admin_ext_table = self.admin_ext_table.write().await;
        if admin_ext_table.contains_key(group) {
            warn!("the admin group[{}] exist already.", group);
            return false;
        }
        admin_ext_table.insert(group.into(), admin);
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

        {
            let producer_table = self.producer_table.read().await;
            for (_, value) in producer_table.iter() {
                topic_list.extend(value.get_publish_topic_list());
            }
        }

        {
            let consumer_table = self.consumer_table.read().await;
            for (_, value) in consumer_table.iter() {
                value.subscriptions().iter().for_each(|sub| {
                    topic_list.insert(sub.topic.clone());
                });
            }
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
        let topic_route_table = self.topic_route_table.read().await;
        if let Some(topic_route_data) = topic_route_table.get(topic) {
            let brokers = &topic_route_data.broker_datas;
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
            let mut topic_route_table = self.topic_route_table.write().await;
            let old = topic_route_table.get(topic);
            let mut changed = topic_route_data.topic_route_data_changed(old);
            if !changed {
                changed = self.is_need_update_topic_route_info(topic).await;
            } else {
                info!(
                    "the topic[{}] route info changed, old[{:?}] ,new[{:?}]",
                    topic, old, topic_route_data
                )
            }
            if changed {
                let mut broker_addr_table = self.broker_addr_table.write().await;
                for bd in topic_route_data.broker_datas.iter() {
                    broker_addr_table.insert(bd.broker_name().clone(), bd.broker_addrs().clone());
                }
                drop(broker_addr_table);

                // Update endpoint map
                {
                    let mq_end_points =
                        ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, &topic_route_data);
                    if let Some(mq_end_points) = mq_end_points {
                        if !mq_end_points.is_empty() {
                            let mut topic_end_points_table = self.topic_end_points_table.write().await;
                            topic_end_points_table.insert(topic.into(), mq_end_points);
                        }
                    }
                }

                // Update Pub info
                {
                    let mut publish_info = topic_route_data2topic_publish_info(topic, &mut topic_route_data);
                    publish_info.have_topic_router_info = true;
                    let mut producer_table = self.producer_table.write().await;
                    for (_, value) in producer_table.iter_mut() {
                        value.update_topic_publish_info(topic.to_string(), Some(publish_info.clone()));
                    }
                }

                // Update sub info
                {
                    let consumer_table = self.consumer_table.read().await;
                    if !consumer_table.is_empty() {
                        let subscribe_info = topic_route_data2topic_subscribe_info(topic, &topic_route_data);
                        for (_, value) in consumer_table.iter() {
                            value.update_topic_subscribe_info(topic.clone(), &subscribe_info).await;
                        }
                    }
                }
                let clone_topic_route_data = TopicRouteData::from_existing(&topic_route_data);
                topic_route_table.insert(topic.clone(), clone_topic_route_data);
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
        let mut result = false;
        let producer_table = self.producer_table.read().await;
        for (key, value) in producer_table.iter() {
            if !result {
                result = value.is_publish_topic_need_update(topic);
                break;
            }
        }
        if result {
            return true;
        }

        let consumer_table = self.consumer_table.read().await;
        for (key, value) in consumer_table.iter() {
            if !result {
                result = value.is_subscribe_topic_need_update(topic).await;
                break;
            }
        }
        result
    }

    pub async fn persist_all_consumer_offset(&mut self) {
        let consumer_table = self.consumer_table.read().await;
        for (_, value) in consumer_table.iter() {
            value.persist_consumer_offset().await;
        }
    }

    pub async fn clean_offline_broker(&mut self) {
        let lock = self
            .lock_namesrv
            .try_lock_timeout(Duration::from_millis(LOCK_TIMEOUT_MILLIS))
            .await;
        if let Some(lock) = lock {
            let mut broker_addr_table = self.broker_addr_table.write().await;
            let mut updated_table = HashMap::with_capacity(broker_addr_table.len());
            let mut broker_name_set = HashSet::new();
            for (broker_name, one_table) in broker_addr_table.iter() {
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
            broker_addr_table.retain(|k, _| !broker_name_set.contains(k));
            if !updated_table.is_empty() {
                broker_addr_table.extend(updated_table);
            }
        }
    }
    pub async fn send_heartbeat_to_all_broker_with_lock(&mut self) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock().await {
            if self.client_config.use_heartbeat_v2 {
                self.send_heartbeat_to_all_broker_v2(false).await
            } else {
                self.send_heartbeat_to_all_broker().await
            }
        } else {
            warn!("lock heartBeat, but failed. [{}]", self.client_id);
            false
        }
    }

    pub async fn send_heartbeat_to_all_broker_with_lock_v2(&mut self, is_rebalance: bool) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock_timeout(Duration::from_secs(2)).await {
            if self.client_config.use_heartbeat_v2 {
                self.send_heartbeat_to_all_broker_v2(is_rebalance).await
            } else {
                self.send_heartbeat_to_all_broker().await
            }
        } else {
            warn!("lock heartBeat, but failed. [{}]", self.client_id);
            false
        }
    }

    pub fn get_mq_client_api_impl(&self) -> ArcMut<MQClientAPIImpl> {
        self.mq_client_api_impl.as_ref().unwrap().clone()
    }

    pub async fn get_broker_name_from_message_queue(&self, message_queue: &MessageQueue) -> CheetahString {
        let guard = self.topic_end_points_table.read().await;
        if let Some(broker_name) = guard.get(message_queue.get_topic()) {
            if let Some(addr) = broker_name.get(message_queue) {
                return addr.clone();
            }
        }
        message_queue.get_broker_name().clone()
    }

    pub async fn find_broker_address_in_publish(&self, broker_name: &CheetahString) -> Option<CheetahString> {
        if broker_name.is_empty() {
            return None;
        }
        let guard = self.broker_addr_table.read().await;
        let map = guard.get(broker_name);
        if let Some(map) = map {
            return map.get(&(mix_all::MASTER_ID)).cloned();
        }
        None
    }

    async fn send_heartbeat_to_all_broker_v2(&self, is_rebalance: bool) -> bool {
        unimplemented!()
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
        let broker_addr_table = self.broker_addr_table.read().await;
        if broker_addr_table.is_empty() {
            return false;
        }
        for (broker_name, broker_addrs) in broker_addr_table.iter() {
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

    pub async fn send_heartbeat_to_broker(&self, id: u64, broker_name: &CheetahString, addr: &CheetahString) -> bool {
        if let Some(lock) = self.lock_heartbeat.try_lock().await {
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
                unimplemented!("sendHeartbeatToBrokerV2")
            } else {
                self.send_heartbeat_to_broker_inner(id, broker_name, addr, &heartbeat_data)
                    .await
            }
        } else {
            false
        }
    }

    async fn send_heartbeat_to_broker_inner(
        &self,
        id: u64,
        broker_name: &CheetahString,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
    ) -> bool {
        if let Ok(version) = self
            .mq_client_api_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .send_heartbeat(addr, heartbeat_data, self.client_config.mq_client_api_timeout)
            .await
        {
            let mut broker_version_table = self.broker_version_table.write().await;
            let map = broker_version_table.get_mut(broker_name);
            if let Some(map) = map {
                map.insert(addr.clone(), version);
            } else {
                let mut map = HashMap::new();
                map.insert(addr.clone(), version);
                broker_version_table.insert(broker_name.clone(), map);
            }

            let times = self
                .send_heartbeat_times_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if times % 20 == 0 {
                info!("send heart beat to broker[{} {} {}] success", broker_name, id, addr,);
            }
            return true;
        }
        if self.is_broker_in_name_server(addr).await {
            warn!("send heart beat to broker[{} {} {}] failed", broker_name, id, addr);
        } else {
            warn!(
                "send heart beat to broker[{} {} {}] exception, because the broker not up, forget it",
                broker_name, id, addr
            )
        }
        false
    }

    async fn is_broker_in_name_server(&self, broker_name: &str) -> bool {
        let broker_addr_table = self.topic_route_table.read().await;
        for (_, value) in broker_addr_table.iter() {
            for bd in value.broker_datas.iter() {
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

        let consumer_table = self.consumer_table.read().await;
        for (_, value) in consumer_table.iter() {
            let mut consumer_data = ConsumerData {
                group_name: value.group_name(),
                consume_type: value.consume_type(),
                message_model: value.message_model(),
                consume_from_where: value.consume_from_where(),
                subscription_data_set: value.subscriptions(),
                unit_mode: value.is_unit_mode(),
            };
            if !is_without_sub {
                value.subscriptions().iter().for_each(|sub| {
                    consumer_data.subscription_data_set.insert(sub.clone());
                });
            }
            heartbeat_data.consumer_data_set.insert(consumer_data);
        }
        drop(consumer_table);
        let producer_table = self.producer_table.read().await;
        for (group_name, _) in producer_table.iter() {
            let producer_data = ProducerData {
                group_name: group_name.clone(),
            };
            heartbeat_data.producer_data_set.insert(producer_data);
        }
        drop(producer_table);
        heartbeat_data.is_without_sub = is_without_sub;
        heartbeat_data
    }

    pub async fn register_consumer(&mut self, group: &CheetahString, consumer: MQConsumerInnerImpl) -> bool {
        let mut consumer_table = self.consumer_table.write().await;
        if consumer_table.contains_key(group) {
            warn!("the consumer group[{}] exist already.", group);
            return false;
        }
        consumer_table.insert(group.clone(), consumer);
        true
    }

    pub async fn check_client_in_broker(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_table = self.consumer_table.read().await;
        for (key, value) in consumer_table.iter() {
            let subscription_inner = value.subscriptions();
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
                            key,
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
        let consumer_table = self.consumer_table.read().await;
        for (key, value) in consumer_table.iter() {
            match value.try_rebalance().await {
                Ok(result) => {
                    if !result {
                        balanced = false;
                    }
                }
                Err(e) => {
                    error!("doRebalance for consumer group [{}] exception:{}", key, e.to_string());
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
            self.instance_runtime.get_handle().spawn(async move {
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
        let broker_addr_table = self.broker_addr_table.read().await;
        let map = broker_addr_table.get(broker_name);
        let mut broker_addr = None;
        let mut slave = false;
        let mut found = false;

        if let Some(map) = map {
            broker_addr = map.get(&broker_id);
            slave = broker_id != mix_all::MASTER_ID;
            found = broker_addr.is_some();
            if !found && slave {
                broker_addr = map.get(&(broker_id + 1));
                found = broker_addr.is_some();
            }
            if !found && !only_this_broker {
                if let Some((key, value)) = map.iter().next() {
                    //broker_addr = Some(value.clone());
                    slave = *key != mix_all::MASTER_ID;
                    found = !value.is_empty();
                }
            }
        }
        if found {
            let broker_addr = broker_addr.cloned()?;
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
        let broker_version_table = self.broker_version_table.read().await;
        if let Some(map) = broker_version_table.get(broker_name) {
            if let Some(version) = map.get(broker_addr) {
                return *version;
            }
        }
        0
    }

    pub async fn select_consumer(&self, group: &str) -> Option<MQConsumerInnerImpl> {
        let consumer_table = self.consumer_table.read().await;
        consumer_table.get(group).cloned()
    }

    pub async fn select_producer(&self, group: &str) -> Option<MQProducerInnerImpl> {
        let producer_table = self.producer_table.read().await;
        producer_table.get(group).cloned()
    }

    pub async fn unregister_consumer(&mut self, group: impl Into<CheetahString>) -> bool {
        let group = group.into();
        if group.is_empty() {
            warn!("unregister_consumer: group name is empty");
            return false;
        }

        let mut consumer_table = self.consumer_table.write().await;
        let removed = consumer_table.remove(&group).is_some();
        drop(consumer_table);

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

        let mut producer_table = self.producer_table.write().await;
        let removed = producer_table.remove(&group).is_some();
        drop(producer_table);

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
        let mut write_guard = self.admin_ext_table.write().await;
        let _ = write_guard.remove(&group.into());
    }
    async fn unregister_client(
        &mut self,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
    ) {
        let broker_addr_table = self.broker_addr_table.read().await;
        for (broker_name, broker_addrs) in broker_addr_table.iter() {
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
        let topic_route_table = self.topic_route_table.read().await;
        for (_, value) in topic_route_table.iter() {
            for bd in value.broker_datas.iter() {
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
        let consumer_table = self.consumer_table.read().await;
        let consumer_inner = consumer_table.get(consumer_group);
        if let Some(consumer) = consumer_inner {
            consumer.consume_message_directly(message, broker_name).await;
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
