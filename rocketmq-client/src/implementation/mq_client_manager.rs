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
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use lazy_static::lazy_static;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::runtime::RPCHook;
use tokio::sync::RwLock;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::producer::produce_accumulator::ProduceAccumulator;

type ClientInstanceHashMap =
    HashMap<String /* clientId */, ArcRefCellWrapper<MQClientInstance>>;
type AccumulatorHashMap =
    HashMap<String /* clientId */, ArcRefCellWrapper<ProduceAccumulator>>;

#[derive(Default)]
pub struct MQClientManager {
    factory_table: Arc<RwLock<ClientInstanceHashMap>>,
    accumulator_table: Arc<RwLock<AccumulatorHashMap>>,
    factory_index_generator: AtomicI32,
}

impl MQClientManager {
    fn new() -> Self {
        MQClientManager {
            factory_index_generator: AtomicI32::new(0),
            factory_table: Arc::new(RwLock::new(HashMap::new())),
            accumulator_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_instance() -> &'static MQClientManager {
        lazy_static! {
            static ref INSTANCE: MQClientManager = MQClientManager::new();
        }
        &INSTANCE
    }

    pub async fn get_or_create_mq_client_instance(
        &self,
        client_config: ClientConfig,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> ArcRefCellWrapper<MQClientInstance> {
        let client_id = client_config.build_mq_client_id();
        let mut factory_table = self.factory_table.write().await;
        let instance = factory_table.entry(client_id.clone()).or_insert_with(|| {
            let instance = MQClientInstance::new(
                client_config.clone(),
                self.factory_index_generator.fetch_add(1, Ordering::SeqCst),
                client_id.clone(),
                rpc_hook,
            );
            info!("Created new MQClientInstance for clientId: [{}]", client_id);
            ArcRefCellWrapper::new(instance)
        });
        instance.clone()
    }

    pub async fn get_or_create_produce_accumulator(
        &self,
        client_config: ClientConfig,
    ) -> ArcRefCellWrapper<ProduceAccumulator> {
        let client_id = client_config.build_mq_client_id();
        let mut accumulator_table = self.accumulator_table.write().await;
        let accumulator = accumulator_table
            .entry(client_id.clone())
            .or_insert_with(|| {
                let accumulator = ProduceAccumulator::new(client_id.as_str());
                info!(
                    "Created new ProduceAccumulator for clientId: [{}]",
                    client_id
                );
                ArcRefCellWrapper::new(accumulator)
            });
        accumulator.clone()
    }

    pub async fn remove_client_factory(&self, client_id: &str) {
        self.factory_table.write().await.remove(client_id);
    }
}
