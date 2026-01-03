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

use std::sync::Arc;
use std::time::Instant;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_filter::utils::bits_array::BitsArray;
use rocketmq_store::base::commit_log_dispatcher::CommitLogDispatcher;
use rocketmq_store::base::dispatch_request::DispatchRequest;
use tracing::debug;
use tracing::error;
use tracing::warn;

use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::filter::message_evaluation_context::MessageEvaluationContext;

pub struct CommitLogDispatcherCalcBitMap {
    broker_config: Arc<BrokerConfig>,
    consumer_filter_manager: ConsumerFilterManager,
}

impl CommitLogDispatcherCalcBitMap {
    pub fn new(broker_config: Arc<BrokerConfig>, consumer_filter_manager: ConsumerFilterManager) -> Self {
        Self {
            broker_config,
            consumer_filter_manager,
        }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherCalcBitMap {
    fn dispatch(&self, request: &mut DispatchRequest) {
        if !self.broker_config.enable_calc_filter_bit_map {
            return;
        }

        // Main logic
        let filter_datas = self.consumer_filter_manager.get(&request.topic);
        if filter_datas.is_none() || filter_datas.as_ref().unwrap().is_empty() {
            return;
        }
        let filter_datas = filter_datas.unwrap();
        let mut filter_bit_map = BitsArray::create(self.consumer_filter_manager.bloom_filter().unwrap().m() as usize);

        let start_time = Instant::now();
        for filter_data in filter_datas.iter() {
            if filter_data.compiled_expression().is_none() {
                error!(
                    "[BUG] Consumer in filter manager has no compiled expression! {:?}",
                    filter_data
                );
                continue;
            }
            if filter_data.bloom_filter_data().is_none() {
                error!("[BUG] Consumer in filter manager has no bloom data! {:?}", filter_data);
                continue;
            }

            let context = MessageEvaluationContext::new(&request.properties_map);
            let ret = filter_data.compiled_expression().as_ref().unwrap().evaluate(&context);

            debug!(
                "Result of Calc bit map: ret={:?}, data={:?}, props={:?}, offset={}",
                ret, filter_data, request.properties_map, request.commit_log_offset
            );

            // eval true
            if let Ok(ret) = ret {
                if let Some(b) = ret.downcast_ref::<bool>() {
                    if *b {
                        let _ = self
                            .consumer_filter_manager
                            .bloom_filter()
                            .unwrap()
                            .hash_to(filter_data.bloom_filter_data().unwrap(), &mut filter_bit_map);
                    }
                }
            }
        }

        request.bit_map = Some(filter_bit_map.bytes().to_vec());

        let elapsed = start_time.elapsed().as_millis();
        if elapsed >= 1 {
            warn!(
                "Spend {} ms to calc bit map, consumerNum={}, topic={}",
                elapsed,
                filter_datas.len(),
                request.topic
            );
        }
    }
}
