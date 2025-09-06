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
use rocketmq_error::RocketMQResult;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

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
        while !context.is_stopped() {}
    }
}

impl<MS> BrokerPreOnlineServiceInner<MS>
where
    MS: MessageStore,
{
    fn prepare_for_broker_online(&self) -> RocketMQResult<bool> {
        unimplemented!()
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
