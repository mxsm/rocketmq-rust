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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::store::controllable_offset::ControllableOffset;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

pub struct RemoteBrokerOffsetStore {
    client_instance: ArcMut<MQClientInstance>,
    group_name: CheetahString,
    offset_table: Arc<Mutex<HashMap<MessageQueue, ControllableOffset>>>,
}

impl RemoteBrokerOffsetStore {
    pub fn new(client_instance: ArcMut<MQClientInstance>, group_name: CheetahString) -> Self {
        Self {
            client_instance,
            group_name,
            offset_table: Arc::new(Mutex::new(HashMap::with_capacity(64))),
        }
    }

    async fn fetch_consume_offset_from_broker(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let mut find_broker_result = self
            .client_instance
            .mut_from_ref()
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;

        if find_broker_result.is_none() {
            self.client_instance
                .mut_from_ref()
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
            find_broker_result = self
                .client_instance
                .mut_from_ref()
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
                .await;
        }
        if let Some(find_broker_result) = find_broker_result {
            let request_header = QueryConsumerOffsetRequestHeader {
                consumer_group: self.group_name.clone(),
                topic: CheetahString::from_string(mq.get_topic().to_string()),
                queue_id: mq.get_queue_id(),
                set_zero_if_not_found: None,
                topic_request_header: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
            };
            self.client_instance
                .mut_from_ref()
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .query_consumer_offset(find_broker_result.broker_addr.as_str(), request_header, 5_000)
                .await
        } else {
            Err(mq_client_err!(format!("broker not found, {}", mq.get_broker_name())))
        }
    }
}

impl OffsetStoreTrait for RemoteBrokerOffsetStore {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        let mut offset_table = self.offset_table.lock().await;
        let offset_old = offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset));
        if increase_only {
            offset_old.update(offset, true);
        } else {
            offset_old.update_unconditionally(offset);
        }
    }

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset))
            .update_and_freeze(offset);
    }

    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        match type_ {
            ReadOffsetType::ReadFromMemory | ReadOffsetType::MemoryFirstThenStore => {
                let offset_table = self.offset_table.lock().await;
                if let Some(offset) = offset_table.get(mq) {
                    return offset.get_offset();
                } else if type_ == ReadOffsetType::ReadFromMemory {
                    return -1;
                }
            }
            ReadOffsetType::ReadFromStore => {
                return match self.fetch_consume_offset_from_broker(mq).await {
                    Ok(value) => {
                        self.update_offset(mq, value, false).await;
                        value
                    }
                    Err(e) => match e {
                        rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. }
                            if code == rocketmq_remoting::code::response_code::ResponseCode::QueryNotFound as i32 =>
                        {
                            -1
                        }
                        _ => {
                            warn!("fetchConsumeOffsetFromBroker exception: {:?}", mq);
                            -2
                        }
                    },
                };
            }
        };
        -3
    }

    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        if mqs.is_empty() {
            return;
        }
        let mut unused_mq = HashSet::new();
        let mut used_mq = Vec::new();
        let mut offset_table = self.offset_table.lock().await;
        for (mq, offset) in offset_table.iter_mut() {
            if mqs.contains(mq) {
                let offset = offset.get_offset();
                used_mq.push((mq.clone(), offset));
            } else {
                unused_mq.insert(mq.clone());
            }
        }
        for mq in unused_mq {
            offset_table.remove(&mq);
            info!("remove unused mq, {}, {}", mq, self.group_name);
        }
        drop(offset_table);

        for (mq, offset) in used_mq {
            match self.update_consume_offset_to_broker(&mq, offset, true).await {
                Ok(_) => {
                    info!(
                        "[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                        self.group_name, self.client_instance.client_id, mq, offset
                    );
                }
                Err(e) => {
                    error!("updateConsumeOffsetToBroker exception, {},{}", mq, e);
                }
            }
        }
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        let offset_table = self.offset_table.lock().await;
        let offset = offset_table.get(mq);
        if offset.is_none() {
            return;
        }
        let offset = offset.unwrap().get_offset();
        drop(offset_table);
        match self.update_consume_offset_to_broker(mq, offset, true).await {
            Ok(_) => {
                info!(
                    "[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    self.group_name, self.client_instance.client_id, mq, offset
                );
            }
            Err(e) => {
                error!("updateConsumeOffsetToBroker exception, {},{}", mq, e);
            }
        }
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table.remove(mq);
        info!(
            "remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}",
            mq,
            self.group_name,
            offset_table.len()
        );
    }

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        let offset_table = self.offset_table.lock().await;
        offset_table
            .iter()
            .filter(|(mq, _)| mq.get_topic() == topic)
            .map(|(mq, offset)| (mq.clone(), offset.get_offset()))
            .collect()
    }

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let mut find_broker_result = self
            .client_instance
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
            .await;

        if find_broker_result.is_none() {
            self.client_instance
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
            find_broker_result = self
                .client_instance
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, false)
                .await;
        }

        if let Some(find_broker_result) = find_broker_result {
            let request_header = UpdateConsumerOffsetRequestHeader {
                consumer_group: self.group_name.clone(),
                topic: mq.get_topic_cs().clone(),
                queue_id: mq.get_queue_id(),
                commit_offset: offset,
                topic_request_header: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
            };
            if is_oneway {
                self.client_instance
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .update_consumer_offset_oneway(find_broker_result.broker_addr.as_str(), request_header, 5_000)
                    .await?;
            } else {
                self.client_instance
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .update_consumer_offset(&find_broker_result.broker_addr, request_header, 5_000)
                    .await?;
            };
            Ok(())
        } else {
            Err(mq_client_err!(format!("broker not found, {}", mq.get_broker_name())))
        }
    }
}
