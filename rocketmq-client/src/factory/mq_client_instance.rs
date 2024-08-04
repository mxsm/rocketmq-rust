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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_runtime::RocketMQRuntime;
use tokio::sync::RwLock;
use tracing::info;
use tracing::warn;

use crate::admin::mq_admin_ext_inner::MQAdminExtInner;
use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::pull_message_service::PullMessageService;
use crate::consumer::consumer_impl::rebalance_service::RebalanceService;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::error::MQClientError::MQClientException;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::mq_admin_impl::MQAdminImpl;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::Result;

#[derive(Clone)]
pub struct MQClientInstance {
    client_config: Arc<ClientConfig>,
    client_id: String,
    boot_timestamp: u64,
    /**
     * The container of the producer in the current client. The key is the name of
     * producerGroup.
     */
    producer_table: Arc<RwLock<HashMap<String, Box<dyn MQProducerInner>>>>,
    /**
     * The container of the consumer in the current client. The key is the name of
     * consumerGroup.
     */
    consumer_table: Arc<RwLock<HashMap<String, Box<dyn MQConsumerInner>>>>,
    /**
     * The container of the adminExt in the current client. The key is the name of
     * adminExtGroup.
     */
    admin_ext_table: Arc<RwLock<HashMap<String, Box<dyn MQAdminExtInner>>>>,
    mq_client_api_impl: ArcRefCellWrapper<MQClientAPIImpl>,
    mq_admin_impl: Arc<MQAdminImpl>,
    topic_route_table: Arc<RwLock<HashMap<String /* Topic */, TopicRouteData>>>,
    topic_end_points_table:
        Arc<RwLock<HashMap<String /* Topic */, HashMap<MessageQueue, String /* brokerName */>>>>,
    lock_namesrv: Arc<RwLock<()>>,
    lock_heartbeat: Arc<RwLock<()>>,

    service_state: ServiceState,
    pull_message_service: ArcRefCellWrapper<PullMessageService>,
    rebalance_service: ArcRefCellWrapper<RebalanceService>,
    default_mqproducer: ArcRefCellWrapper<DefaultMQProducer>,
    instance_runtime: Arc<RocketMQRuntime>,
}

impl MQClientInstance {
    pub fn new(
        client_config: ClientConfig,
        instance_index: i32,
        client_id: String,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        MQClientInstance {
            client_config: Arc::new(client_config.clone()),
            client_id,
            boot_timestamp: get_current_millis(),
            producer_table: Arc::new(Default::default()),
            consumer_table: Arc::new(Default::default()),
            admin_ext_table: Arc::new(Default::default()),
            mq_client_api_impl: ArcRefCellWrapper::new(MQClientAPIImpl::new(
                Arc::new(TokioClientConfig::default()),
                ClientRemotingProcessor {},
                rpc_hook,
                client_config,
            )),
            mq_admin_impl: Arc::new(MQAdminImpl {}),
            topic_route_table: Arc::new(Default::default()),
            topic_end_points_table: Arc::new(Default::default()),
            lock_namesrv: Default::default(),
            lock_heartbeat: Default::default(),
            service_state: ServiceState::CreateJust,
            pull_message_service: ArcRefCellWrapper::new(PullMessageService {}),
            rebalance_service: ArcRefCellWrapper::new(RebalanceService {}),
            default_mqproducer: Default::default(),
            instance_runtime: Arc::new(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "mq-client-instance",
            )),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                // If not specified,looking address from name server
                if self.client_config.namesrv_addr.is_none() {
                    self.mq_client_api_impl.fetch_name_server_addr().await;
                }
                // Start request-response channel
                self.mq_client_api_impl.start().await;
                // Start various schedule tasks
                self.start_scheduled_task();
                // Start pull service
                self.pull_message_service.start().await;
                // Start rebalance service
                self.rebalance_service.start().await;
                // Start push service
                self.default_mqproducer
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
                return Err(MQClientException(
                    -1,
                    format!(
                        "The Factory object[{}] has been created before, and failed.",
                        self.client_id
                    ),
                ));
            }
        }
        Ok(())
    }

    pub async fn register_producer(&mut self, group: &str, producer: impl MQProducerInner) -> bool {
        if group.is_empty() {
            return false;
        }
        let mut producer_table = self.producer_table.write().await;
        if producer_table.contains_key(group) {
            warn!("the producer group[{}] exist already.", group);
            return false;
        }
        producer_table.insert(group.to_string(), Box::new(producer));
        true
    }

    fn start_scheduled_task(&mut self) {
        if self.client_config.namesrv_addr.is_none() {
            let mut mq_client_api_impl = self.mq_client_api_impl.clone();
            self.instance_runtime.get_handle().spawn(async move {
                info!("ScheduledTask fetchNameServerAddr started");
                tokio::time::sleep(Duration::from_secs(10)).await;
                loop {
                    let current_execution_time = tokio::time::Instant::now();
                    mq_client_api_impl.fetch_name_server_addr().await;
                    let next_execution_time = current_execution_time + Duration::from_secs(120);
                    let delay =
                        next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                    tokio::time::sleep(delay).await;
                }
            });
        }

        let mut client_instance = self.clone();
        let poll_name_server_interval = self.client_config.poll_name_server_interval;
        self.instance_runtime.get_handle().spawn(async move {
            info!("ScheduledTask updateTopicRouteInfoFromNameServer started");
            tokio::time::sleep(Duration::from_millis(10)).await;
            loop {
                let current_execution_time = tokio::time::Instant::now();
                client_instance
                    .update_topic_route_info_from_name_server()
                    .await;
                let next_execution_time = current_execution_time
                    + Duration::from_millis(poll_name_server_interval as u64);
                let delay =
                    next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(delay).await;
            }
        });

        let mut client_instance = self.clone();
        let heartbeat_broker_interval = self.client_config.heartbeat_broker_interval;
        self.instance_runtime.get_handle().spawn(async move {
            info!("ScheduledTask sendHeartbeatToAllBroker started");
            tokio::time::sleep(Duration::from_secs(1)).await;
            loop {
                let current_execution_time = tokio::time::Instant::now();
                client_instance.clean_offline_broker().await;
                client_instance
                    .send_heartbeat_to_all_broker_with_lock()
                    .await;
                let next_execution_time = current_execution_time
                    + Duration::from_millis(heartbeat_broker_interval as u64);
                let delay =
                    next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(delay).await;
            }
        });

        let mut client_instance = self.clone();
        let persist_consumer_offset_interval =
            self.client_config.persist_consumer_offset_interval as u64;
        self.instance_runtime.get_handle().spawn(async move {
            info!("ScheduledTask persistAllConsumerOffset started");
            tokio::time::sleep(Duration::from_secs(10)).await;
            loop {
                let current_execution_time = tokio::time::Instant::now();
                client_instance.persist_all_consumer_offset().await;
                let next_execution_time = current_execution_time
                    + Duration::from_millis(persist_consumer_offset_interval);
                let delay =
                    next_execution_time.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(delay).await;
            }
        });
    }

    pub async fn update_topic_route_info_from_name_server(&mut self) {
        println!("updateTopicRouteInfoFromNameServer")
    }

    pub async fn persist_all_consumer_offset(&mut self) {
        println!("updateTopicRouteInfoFromNameServer")
    }

    pub async fn clean_offline_broker(&mut self) {
        println!("cleanOfflineBroker")
    }
    pub async fn send_heartbeat_to_all_broker_with_lock(&mut self) {
        println!("sendHeartbeatToAllBroker")
    }
}
