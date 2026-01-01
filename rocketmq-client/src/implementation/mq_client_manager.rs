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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::info;

use crate::base::client_config::ClientConfig;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::producer::produce_accumulator::ProduceAccumulator;

type ClientInstanceHashMap = DashMap<CheetahString /* clientId */, ArcMut<MQClientInstance>>;
type AccumulatorHashMap = DashMap<CheetahString /* clientId */, ArcMut<ProduceAccumulator>>;

#[derive(Default)]
pub struct MQClientManager {
    factory_table: Arc<ClientInstanceHashMap>,
    accumulator_table: Arc<AccumulatorHashMap>,
    factory_index_generator: AtomicI32,
}

impl MQClientManager {
    fn new() -> Self {
        MQClientManager {
            factory_index_generator: AtomicI32::new(0),
            factory_table: Arc::new(DashMap::with_capacity(128)),
            accumulator_table: Arc::new(DashMap::with_capacity(128)),
        }
    }

    #[inline]
    pub fn get_instance() -> &'static MQClientManager {
        static INSTANCE: LazyLock<MQClientManager> = LazyLock::new(MQClientManager::new);
        &INSTANCE
    }

    pub fn get_or_create_mq_client_instance(
        &self,
        client_config: ClientConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> ArcMut<MQClientInstance> {
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());

        self.factory_table
            .entry(client_id.clone())
            .or_insert_with(|| {
                let index = self.factory_index_generator.fetch_add(1, Ordering::Relaxed);

                info!("Created new MQClientInstance for clientId: [{}]", client_id);

                MQClientInstance::new_arc(client_config, index, client_id, rpc_hook)
            })
            .clone()
    }

    pub fn get_or_create_produce_accumulator(&self, client_config: ClientConfig) -> ArcMut<ProduceAccumulator> {
        let client_id = CheetahString::from_string(client_config.build_mq_client_id());

        self.accumulator_table
            .entry(client_id.clone())
            .or_insert_with(|| {
                info!("Created new ProduceAccumulator for clientId:[{}]", client_id);
                ArcMut::new(ProduceAccumulator::new(client_id.as_str()))
            })
            .clone()
    }

    pub fn remove_client_factory(&self, client_id: &CheetahString) {
        self.factory_table.remove(client_id);
    }
}
