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
use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_common::common::mix_all::CLIENT_INNER_PRODUCER_GROUP;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;

use crate::base::client_config::ClientConfig;
use crate::error::MQClientError;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::transaction_listener::TransactionListener;
use crate::Result;

pub struct DefaultMQProducerImpl {
    client_config: ClientConfig,
    producer_config: ProducerConfig,
    topic_publish_info_table: Arc<RwLock<HashMap<String /* topic */, TopicPublishInfo>>>,
    send_message_hook_list: Vec<Box<dyn SendMessageHook>>,
    end_transaction_hook_list: Vec<Box<dyn EndTransactionHook>>,
    check_forbidden_hook_list: Vec<Box<dyn CheckForbiddenHook>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    service_state: ServiceState,
    client_instance: ArcRefCellWrapper<MQClientInstance>,
    mq_fault_strategy: MQFaultStrategy,
    semaphore_async_send_num: Semaphore,
    semaphore_async_send_size: Semaphore,
}

impl DefaultMQProducerImpl {
    pub fn new(
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        let semaphore_async_send_num =
            Semaphore::new(producer_config.back_pressure_for_async_send_num().max(10) as usize);
        let semaphore_async_send_size = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_size()
                .max(1024 * 1024) as usize,
        );
        let client_instance = ArcRefCellWrapper::new(MQClientInstance {});
        let topic_publish_info_table = Arc::new(RwLock::new(HashMap::new()));

        let service_detector = DefaultServiceDetector {
            client_instance: client_instance.clone(),
            topic_publish_info_table: topic_publish_info_table.clone(),
        };
        let resolver = DefaultResolver {
            client_instance: client_instance.clone(),
        };
        DefaultMQProducerImpl {
            client_config: client_config.clone(),
            producer_config,
            topic_publish_info_table,
            send_message_hook_list: vec![],
            end_transaction_hook_list: vec![],
            check_forbidden_hook_list: vec![],
            rpc_hook: None,
            service_state: ServiceState::CreateJust,
            client_instance,
            mq_fault_strategy: MQFaultStrategy::new(client_config, resolver, service_detector),
            semaphore_async_send_num,
            semaphore_async_send_size,
        }
    }
}

impl MQProducerInner for DefaultMQProducerImpl {
    fn get_publish_topic_list(&self) -> HashSet<String> {
        todo!()
    }

    fn is_publish_topic_need_update(&self, topic: &str) -> bool {
        todo!()
    }

    fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>> {
        todo!()
    }

    fn check_transaction_state(
        &self,
        addr: &str,
        msg: &MessageExt,
        check_request_header: &CheckTransactionStateRequestHeader,
    ) {
        todo!()
    }

    fn update_topic_publish_info(&self, topic: &str, info: &TopicPublishInfo) {
        todo!()
    }

    fn is_unit_mode(&self) -> bool {
        todo!()
    }
}

impl DefaultMQProducerImpl {
    pub async fn start(&mut self) -> Result<()> {
        self.start_with_factory(true).await
    }

    pub async fn start_with_factory(&mut self, start_factory: bool) -> Result<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.check_config()?;

                if self.producer_config.producer_group() != CLIENT_INNER_PRODUCER_GROUP {
                    self.client_config.change_instance_name_to_pid();
                }

                self.client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(
                        self.client_config.clone(),
                        self.rpc_hook.clone(),
                    )
                    .await;
            }
            ServiceState::Running => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The producer service state is Running".to_string(),
                ));
            }
            ServiceState::ShutdownAlready => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The producer service state is ShutdownAlready".to_string(),
                ));
            }
            ServiceState::StartFailed => {
                return Err(MQClientError::MQClientException(
                    -1,
                    format!(
                        "The producer service state not OK, maybe started once,{:?},{}",
                        self.service_state,
                        FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn register_end_transaction_hook(&mut self, hook: impl EndTransactionHook) {
        todo!()
    }

    pub fn register_send_message_hook(&mut self, hook: impl SendMessageHook) {
        todo!()
    }

    #[inline]
    fn check_config(&self) -> Result<()> {
        Ok(())
    }
}

struct DefaultServiceDetector {
    client_instance: ArcRefCellWrapper<MQClientInstance>,
    topic_publish_info_table: Arc<RwLock<HashMap<String /* topic */, TopicPublishInfo>>>,
}

impl ServiceDetector for DefaultServiceDetector {
    fn detect(&self, endpoint: &str, timeout_millis: u64) -> bool {
        todo!()
    }
}

struct DefaultResolver {
    client_instance: ArcRefCellWrapper<MQClientInstance>,
}

impl Resolver for DefaultResolver {
    fn resolve(&self, name: &str) -> String {
        todo!()
    }
}
