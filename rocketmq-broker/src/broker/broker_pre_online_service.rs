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
use std::sync::atomic::Ordering;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct BrokerPreOnlineService<MS: MessageStore> {
    service_manager: ServiceManager<BrokerPreOnlineServiceInner<MS>>,
}

struct BrokerPreOnlineServiceInner<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> ServiceTask for BrokerPreOnlineServiceInner<MS>
where
    MS: MessageStore,
{
    fn get_service_name(&self) -> String {
        "BrokerPreOnlineService".to_string()
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            if !self
                .broker_runtime_inner
                .is_isolated()
                .load(Ordering::SeqCst)
            {
                info!(
                    "broker {} is online",
                    self.broker_runtime_inner
                        .broker_config()
                        .broker_identity
                        .get_canonical_name()
                );
                break;
            }
            match self.prepare_for_broker_online().await {
                Ok(is_success) => {
                    if is_success {
                        break;
                    } else {
                        let _ = context.wait_for_running(Duration::from_millis(1000)).await;
                    }
                }
                Err(e) => {
                    error!(
                        "prepare for broker online failed, retry later. error: {:?}",
                        e
                    );
                }
            }
        }
    }
}

impl<MS> BrokerPreOnlineServiceInner<MS>
where
    MS: MessageStore,
{
    async fn prepare_for_broker_online(&self) -> RocketMQResult<bool> {
        let broker_cluster_name = &self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_cluster_name;
        let broker_name = &self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_name;
        let compatible_with_old_name_srv = self
            .broker_runtime_inner
            .broker_config()
            .compatible_with_old_name_srv;
        let broker_member_group = match self
            .broker_runtime_inner
            .broker_outer_api()
            .sync_broker_member_group(
                broker_cluster_name,
                broker_name,
                compatible_with_old_name_srv,
            )
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!(
                    "syncBrokerMemberGroup from namesrv error, start service failed, will try \
                     later, {}",
                    e
                );
                return Ok(false);
            }
        };
        let broker_id = self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_id;
        if let Some(broker_member_group) = broker_member_group {
            let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
            if !broker_member_group.broker_addrs.is_empty() {
                if broker_id == MASTER_ID {
                    return Ok(self.prepare_for_master_online(broker_member_group));
                } else if min_broker_id == MASTER_ID {
                    return Ok(self.prepare_for_slave_online(broker_member_group));
                } else {
                    info!("no master online, start service directly");
                    self.broker_runtime_inner.mut_from_ref().start_service(
                        min_broker_id,
                        broker_member_group
                            .broker_addrs
                            .get(&min_broker_id)
                            .cloned(),
                    );
                    return Ok(true);
                }
            }
        }
        info!("no other broker online, will start service directly");
        let broker_addr = self.broker_runtime_inner.get_broker_addr().clone();
        self.broker_runtime_inner
            .mut_from_ref()
            .start_service(broker_id, Some(broker_addr));
        Ok(true)
    }

    fn get_min_broker_id(
        &self,
        _broker_addr_map: &HashMap<
            u64,           /* brokerId */
            CheetahString, /* broker address */
        >,
    ) -> u64 {
        unimplemented!("get_min_broker_id unimplemented")
    }

    fn prepare_for_master_online(&self, _broker_member_group: BrokerMemberGroup) -> bool {
        unimplemented!("prepare_for_master_online unimplemented")
    }
    fn prepare_for_slave_online(&self, _broker_member_group: BrokerMemberGroup) -> bool {
        unimplemented!("prepare_for_slave_online unimplemented")
    }
}

impl<MS> BrokerPreOnlineService<MS>
where
    MS: MessageStore,
{
    pub async fn start(&mut self) {
        self.service_manager.start().await.unwrap();
    }

    pub async fn shutdown(&mut self) {
        self.service_manager.shutdown().await.unwrap();
    }
}
