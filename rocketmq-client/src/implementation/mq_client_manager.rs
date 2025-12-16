//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::producer::produce_accumulator::ProduceAccumulator;

type ClientInstanceHashMap = HashMap<CheetahString /* clientId */, ArcMut<MQClientInstance>>;
type AccumulatorHashMap = HashMap<CheetahString /* clientId */, ArcMut<ProduceAccumulator>>;

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

    pub fn get_or_create_mq_client_instance(
        &self,
        client_config: ClientConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> ArcMut<MQClientInstance> {
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let mut factory_table = self.factory_table.write();

        if let Some(instance) = factory_table.get(&client_id) {
            return instance.clone();
        }

        let instance = MQClientInstance::new_arc(
            client_config,
            self.factory_index_generator.fetch_add(1, Ordering::SeqCst),
            client_id.clone(),
            rpc_hook,
        );
        info!("Created new MQClientInstance for clientId: [{}]", client_id);
        factory_table.insert(client_id, instance.clone());
        instance
    }

    pub fn get_or_create_produce_accumulator(
        &self,
        client_config: ClientConfig,
    ) -> ArcMut<ProduceAccumulator> {
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());
        let mut accumulator_table = self.accumulator_table.write();

        if let Some(accumulator) = accumulator_table.get(&client_id) {
            return accumulator.clone();
        }

        let accumulator = ArcMut::new(ProduceAccumulator::new(client_id.as_str()));
        info!(
            "Created new ProduceAccumulator for clientId:[{}]",
            client_id
        );
        accumulator_table.insert(client_id, accumulator.clone());

        accumulator
    }

    pub async fn remove_client_factory(&self, client_id: &CheetahString) {
        self.factory_table.write().remove(client_id);
    }
}
