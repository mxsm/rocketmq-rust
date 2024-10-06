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
use std::ops::Deref;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::WeakCellWrapper;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::factory::mq_client_instance::MQClientInstance;

const TIMEOUT_CHECK_TIMES: u32 = 3;
const QUERY_ASSIGNMENT_TIMEOUT: u32 = 3000;

pub(crate) struct RebalanceImpl<R> {
    pub(crate) process_queue_table: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    pub(crate) pop_process_queue_table: Arc<RwLock<HashMap<MessageQueue, PopProcessQueue>>>,
    pub(crate) topic_subscribe_info_table: Arc<RwLock<HashMap<String, HashSet<MessageQueue>>>>,
    pub(crate) subscription_inner: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    pub(crate) consumer_group: Option<String>,
    pub(crate) message_model: Option<MessageModel>,
    pub(crate) allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
    pub(crate) client_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    pub(crate) sub_rebalance_impl: Option<WeakCellWrapper<R>>,
    pub(crate) topic_broker_rebalance: Arc<RwLock<HashMap<String, String>>>,
    pub(crate) topic_client_rebalance: Arc<RwLock<HashMap<String, String>>>,
}

impl<R> RebalanceImpl<R>
where
    R: Rebalance,
{
    pub fn new(
        consumer_group: Option<String>,
        message_model: Option<MessageModel>,
        allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
        mqclient_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    ) -> Self {
        RebalanceImpl {
            process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            pop_process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_subscribe_info_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            subscription_inner: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            consumer_group,
            message_model,
            allocate_message_queue_strategy,
            client_instance: mqclient_instance,
            sub_rebalance_impl: None,
            topic_broker_rebalance: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_client_rebalance: Arc::new(RwLock::new(HashMap::with_capacity(64))),
        }
    }

    pub async fn put_subscription_data(&self, topic: &str, subscription_data: SubscriptionData) {
        let mut subscription_inner = self.subscription_inner.write().await;
        subscription_inner.insert(topic.to_string(), subscription_data);
    }

    #[inline]
    pub async fn do_rebalance(&mut self, is_order: bool) -> bool {
        let mut balanced = true;
        let sub_table = self.subscription_inner.read().await;
        if !sub_table.is_empty() {
            let topics = sub_table.keys().cloned().collect::<HashSet<String>>();
            drop(sub_table);
            for topic in &topics {
                //try_query_assignment unimplemented
                if !self.client_rebalance(topic) && self.try_query_assignment(topic).await {
                    if !self.get_rebalance_result_from_broker(topic, is_order).await {
                        balanced = false;
                    }
                } else if !self.rebalance_by_topic(topic, is_order).await {
                    balanced = false;
                }
            }
        }
        self.truncate_message_queue_not_my_topic().await;
        balanced
    }

    #[inline]
    pub fn client_rebalance(&mut self, topic: &str) -> bool {
        match self.sub_rebalance_impl.as_mut().unwrap().upgrade() {
            None => true,
            Some(mut value) => value.client_rebalance(topic),
        }
    }

    async fn try_query_assignment(&self, topic: &str) -> bool {
        unimplemented!("try_query_assignment")
    }

    async fn truncate_message_queue_not_my_topic(&self) {
        let sub_table = self.subscription_inner.read().await;

        let mut process_queue_table = self.process_queue_table.write().await;
        process_queue_table.retain(|mq, pq| {
            if !sub_table.contains_key(mq.get_topic()) {
                pq.set_dropped(true);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}",
                    self.consumer_group.as_ref().unwrap(),
                    mq.get_topic()
                );
                false
            } else {
                true
            }
        });
        let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
        pop_process_queue_table.retain(|mq, pq| {
            if !sub_table.contains_key(mq.get_topic()) {
                pq.set_dropped(true);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}",
                    self.consumer_group.as_ref().unwrap(),
                    mq.get_topic()
                );
                false
            } else {
                true
            }
        });

        let mut topic_client_rebalance = self.topic_client_rebalance.write().await;
        topic_client_rebalance.retain(|topic, _| sub_table.contains_key(topic));
        let mut topic_broker_rebalance = self.topic_broker_rebalance.write().await;
        topic_broker_rebalance.retain(|topic, _| sub_table.contains_key(topic));
    }

    async fn get_rebalance_result_from_broker(&self, topic: &str, is_order: bool) -> bool {
        unimplemented!("get_rebalance_result_from_broker")
    }

    async fn update_process_queue_table_in_rebalance(
        &mut self,
        topic: &str,
        mq_set: &HashSet<MessageQueue>,
        is_order: bool,
    ) -> bool {
        let mut changed = false;
        let mut remove_queue_map = HashMap::new();
        let process_queue_table_cloned = self.process_queue_table.clone();
        {
            let process_queue_table = process_queue_table_cloned.read().await;
            // Drop process queues no longer belong to me
            for (mq, pq) in process_queue_table.iter() {
                if mq.get_topic() == topic {
                    if !mq_set.contains(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) =
                            self.sub_rebalance_impl.as_mut().unwrap().upgrade()
                        {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, \
                                     because pull is pause, so try to fixed it",
                                    self.consumer_group,
                                    mq.get_topic()
                                );
                            }
                        }
                    }
                }
            }
        }

        {
            if !remove_queue_map.is_empty() {
                let mut process_queue_table = process_queue_table_cloned.write().await;
                // Remove message queues no longer belong to me
                for (mq, pq) in remove_queue_map {
                    if let Some(mut sub_rebalance) =
                        self.sub_rebalance_impl.as_mut().unwrap().upgrade()
                    {
                        if sub_rebalance
                            .remove_unnecessary_message_queue(&mq, &pq)
                            .await
                        {
                            process_queue_table.remove(&mq);
                            changed = true;
                            info!(
                                "doRebalance, {:?}, remove unnecessary mq, {}",
                                self.consumer_group,
                                mq.get_topic()
                            );
                        }
                    }
                }
            }
        }
        // Add new message queue
        let mut all_mq_locked = true;
        let mut pull_request_list = Vec::new();
        let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap().upgrade();
        if sub_rebalance_impl.is_none() {
            return false;
        }
        let mut sub_rebalance_impl = sub_rebalance_impl.unwrap();
        let mut process_queue_table = process_queue_table_cloned.write().await;
        for mq in mq_set {
            if !process_queue_table.contains_key(mq) {
                if is_order && !self.lock_with(mq, process_queue_table.deref()).await {
                    warn!(
                        "doRebalance, {:?}, add a new mq failed, {}, because lock failed",
                        self.consumer_group,
                        mq.get_topic()
                    );
                    all_mq_locked = false;
                    continue;
                }

                sub_rebalance_impl.remove_dirty_offset(mq).await;
                let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                pq.set_locked(true);
                let next_offset = sub_rebalance_impl.compute_pull_from_where(mq).await;
                if next_offset >= 0 {
                    if process_queue_table.insert(mq.clone(), pq.clone()).is_none() {
                        info!(
                            "doRebalance, {:?}, add a new mq, {}",
                            self.consumer_group,
                            mq.get_topic()
                        );
                        pull_request_list.push(PullRequest::new(
                            self.consumer_group.as_ref().unwrap().clone(),
                            mq.clone(),
                            pq,
                            next_offset,
                        ));
                        changed = true;
                    } else {
                        info!(
                            "doRebalance, {:?}, mq already exists, {}",
                            self.consumer_group,
                            mq.get_topic()
                        );
                    }
                } else {
                    warn!(
                        "doRebalance, {:?}, add new mq failed, {}",
                        self.consumer_group,
                        mq.get_topic()
                    );
                }
            }
        }

        if !all_mq_locked {
            self.client_instance.as_mut().unwrap().rebalance_later(500);
        }
        sub_rebalance_impl
            .dispatch_pull_request(pull_request_list, 500)
            .await;

        changed
    }

    async fn rebalance_by_topic(&mut self, topic: &str, is_order: bool) -> bool {
        match self.message_model.unwrap() {
            MessageModel::Broadcasting => {
                let topic_sub_cloned = self.topic_subscribe_info_table.clone();
                let topic_subscribe_info_table = topic_sub_cloned.read().await;
                let mq_set = topic_subscribe_info_table.get(topic);
                if let Some(mq_set) = mq_set {
                    let changed = self
                        .update_process_queue_table_in_rebalance(topic, mq_set, is_order)
                        .await;
                    if changed {
                        let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap();
                        if let Some(mut sub_rebalance_impl) = sub_rebalance_impl.upgrade() {
                            sub_rebalance_impl
                                .message_queue_changed(topic, mq_set, mq_set)
                                .await;
                        }
                    }
                    mq_set.eq(&self.get_working_message_queue(topic).await)
                } else {
                    let sub_rebalance_impl = self.sub_rebalance_impl.as_mut().unwrap();
                    if let Some(mut sub_rebalance_impl) = sub_rebalance_impl.upgrade() {
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group.as_ref().unwrap(),
                            topic
                        );
                    }
                    true
                }
            }
            MessageModel::Clustering => {
                let topic_sub_cloned = self.topic_subscribe_info_table.clone();
                let topic_subscribe_info_table_inner = topic_sub_cloned.read().await;
                let mq_set = topic_subscribe_info_table_inner.get(topic);
                //get consumer id list from broker
                let cid_all = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .find_consumer_id_list(topic, self.consumer_group.as_ref().unwrap())
                    .await;
                if mq_set.is_none() && !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    if let Some(mut sub_rebalance_impl) =
                        self.sub_rebalance_impl.as_ref().unwrap().upgrade()
                    {
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group.as_ref().unwrap(),
                            topic
                        );
                    }
                }
                if cid_all.is_none() {
                    warn!(
                        "doRebalance, {}, {}, get consumer id list failed.",
                        self.consumer_group.as_ref().unwrap(),
                        topic
                    );
                    return true;
                }
                if mq_set.is_some() && cid_all.is_some() {
                    let mq_set = mq_set.unwrap();
                    let mut mq_all = mq_set.iter().cloned().collect::<Vec<MessageQueue>>();
                    mq_all.sort();
                    let mut ci_all = cid_all.unwrap();
                    ci_all.sort();

                    let strategy = self.allocate_message_queue_strategy.as_ref().unwrap();
                    let strategy_name = strategy.get_name();
                    let allocate_result = match strategy.allocate(
                        self.consumer_group.as_ref().unwrap(),
                        self.client_instance.as_ref().unwrap().client_id.as_ref(),
                        mq_all.as_slice(),
                        ci_all.as_slice(),
                    ) {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                "allocate message queue exception. strategy name: {}, ex: {}",
                                self.allocate_message_queue_strategy
                                    .as_ref()
                                    .unwrap()
                                    .get_name(),
                                e.to_string()
                            );
                            return false;
                        }
                    };
                    let allocate_result_set = allocate_result
                        .into_iter()
                        .collect::<HashSet<MessageQueue>>();
                    let changed = self
                        .update_process_queue_table_in_rebalance(
                            topic,
                            &allocate_result_set,
                            is_order,
                        )
                        .await;
                    if changed {
                        info!(
                            "client rebalanced result changed. \
                             allocateMessageQueueStrategyName={}, group={}, topic={}, \
                             clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, \
                             rebalanceResultSet={:?}",
                            strategy_name,
                            self.consumer_group.as_ref().unwrap(),
                            topic,
                            self.client_instance.as_ref().unwrap().client_id,
                            mq_set.len(),
                            ci_all.len(),
                            allocate_result_set.len(),
                            allocate_result_set
                        );

                        if let Some(mut sub_rebalance_impl) =
                            self.sub_rebalance_impl.as_ref().unwrap().upgrade()
                        {
                            sub_rebalance_impl
                                .message_queue_changed(topic, mq_set, &allocate_result_set)
                                .await;
                        }
                    }
                    return allocate_result_set.eq(&self.get_working_message_queue(topic).await);
                }
                true
            }
        }
    }

    pub async fn get_working_message_queue(&self, topic: &str) -> HashSet<MessageQueue> {
        let mut queue_set = HashSet::new();
        let process_queue_table = self.process_queue_table.read().await;
        for (mq, pq) in process_queue_table.iter() {
            if mq.get_topic() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        let pop_process_queue_table = self.pop_process_queue_table.read().await;
        for (mq, pq) in pop_process_queue_table.iter() {
            if mq.get_topic() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        queue_set
    }

    pub async fn lock(&mut self, mq: &MessageQueue) -> bool {
        let process_queue_table_ = self.process_queue_table.clone();
        let process_queue_table = process_queue_table_.read().await;
        let table = process_queue_table.deref();
        self.lock_with(mq, table).await
    }

    pub async fn lock_with(
        &mut self,
        mq: &MessageQueue,
        process_queue_table: &HashMap<MessageQueue, Arc<ProcessQueue>>,
    ) -> bool {
        let client = self.client_instance.as_mut().unwrap();
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let find_broker_result = client
            .find_broker_address_in_subscribe(broker_name.as_str(), mix_all::MASTER_ID, true)
            .await;
        if let Some(find_broker_result) = find_broker_result {
            let mut request_body = LockBatchRequestBody {
                consumer_group: Some(self.consumer_group.clone().unwrap()),
                client_id: Some(client.client_id.clone()),
                ..Default::default()
            };
            request_body.mq_set.insert(mq.clone());
            let result = client
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                .await;
            match result {
                Ok(locked_mq) => {
                    for mq in &locked_mq {
                        if let Some(pq) = process_queue_table.get(mq) {
                            pq.set_locked(true);
                            pq.set_last_pull_timestamp(get_current_millis());
                        }
                    }
                    let lock_ok = locked_mq.contains(mq);
                    info!(
                        "message queue lock {}, {:?} {}",
                        lock_ok, self.consumer_group, mq
                    );
                    lock_ok
                }
                Err(e) => {
                    error!("lockBatchMQ exception {},{}", mq, e);
                    false
                }
            }
        } else {
            false
        }
    }

    pub async fn lock_all(&mut self) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;

        let map = broker_mqs
            .into_iter()
            .map(|(broker_name, mqs)| {
                let mut client_instance = self.client_instance.clone();
                let process_queue_table = self.process_queue_table.clone();
                let consumer_group = self.consumer_group.clone().unwrap();
                async move {
                    if mqs.is_empty() {
                        return;
                    }
                    let client = client_instance.as_mut().unwrap();
                    let find_broker_result = client
                        .find_broker_address_in_subscribe(
                            broker_name.as_str(),
                            mix_all::MASTER_ID,
                            true,
                        )
                        .await;
                    if let Some(find_broker_result) = find_broker_result {
                        let request_body = LockBatchRequestBody {
                            consumer_group: Some(consumer_group.to_owned()),
                            client_id: Some(client.client_id.clone()),
                            mq_set: mqs.clone(),
                            ..Default::default()
                        };
                        let result = client
                            .mq_client_api_impl
                            .as_mut()
                            .unwrap()
                            .lock_batch_mq(
                                find_broker_result.broker_addr.as_str(),
                                request_body,
                                1_000,
                            )
                            .await;
                        match result {
                            Ok(lock_okmqset) => {
                                let process_queue_table = process_queue_table.read().await;
                                for mq in &mqs {
                                    if let Some(pq) = process_queue_table.get(mq) {
                                        if lock_okmqset.contains(mq) {
                                            if pq.is_locked() {
                                                info!(
                                                    "the message queue locked OK, Group: {:?} {}",
                                                    consumer_group, mq
                                                );
                                            }
                                            pq.set_locked(true);
                                            pq.set_last_lock_timestamp(get_current_millis());
                                        } else {
                                            pq.set_locked(false);
                                            warn!(
                                                "the message queue locked Failed, Group: {:?} {}",
                                                consumer_group, mq
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("lockBatchMQ exception {}", e);
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        futures::future::join_all(map).await;

        /*        for (broker_name, mqs) in broker_mqs {
            if mqs.is_empty() {
                continue;
            }
            let client = self.client_instance.as_mut().unwrap();
            let find_broker_result = client
                .find_broker_address_in_subscribe(broker_name.as_str(), mix_all::MASTER_ID, true)
                .await;
            if let Some(find_broker_result) = find_broker_result {
                let request_body = LockBatchRequestBody {
                    consumer_group: Some(self.consumer_group.clone().unwrap()),
                    client_id: Some(client.client_id.clone()),
                    mq_set: mqs.clone(),
                    ..Default::default()
                };
                let result = client
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                    .await;
                match result {
                    Ok(lock_okmqset) => {
                        let process_queue_table = self.process_queue_table.read().await;
                        for mq in &mqs {
                            if let Some(pq) = process_queue_table.get(mq) {
                                if lock_okmqset.contains(mq) {
                                    if pq.is_locked() {
                                        info!(
                                            "the message queue locked OK, Group: {:?} {}",
                                            self.consumer_group, mq
                                        );
                                    }
                                    pq.set_locked(true);
                                    pq.set_last_lock_timestamp(get_current_millis());
                                } else {
                                    pq.set_locked(false);
                                    warn!(
                                        "the message queue locked Failed, Group: {:?} {}",
                                        self.consumer_group, mq
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("lockBatchMQ exception {}", e);
                    }
                }
            }
        }*/
    }

    pub async fn unlock_all(&mut self, oneway: bool) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;
        for (broker_name, mqs) in broker_mqs {
            if mqs.is_empty() {
                continue;
            }
            let client = self.client_instance.as_mut().unwrap();
            let find_broker_result = client
                .find_broker_address_in_subscribe(broker_name.as_str(), mix_all::MASTER_ID, true)
                .await;
            if let Some(find_broker_result) = find_broker_result {
                let request_body = UnlockBatchRequestBody {
                    consumer_group: Some(self.consumer_group.clone().unwrap()),
                    client_id: Some(client.client_id.clone()),
                    mq_set: mqs.clone(),
                    ..Default::default()
                };
                let result = client
                    .mq_client_api_impl
                    .as_mut()
                    .unwrap()
                    .unlock_batch_mq(
                        find_broker_result.broker_addr.as_str(),
                        request_body,
                        1_000,
                        oneway,
                    )
                    .await;
                match result {
                    Ok(_) => {
                        let process_queue_table = self.process_queue_table.read().await;
                        for mq in &mqs {
                            if let Some(pq) = process_queue_table.get(mq) {
                                pq.set_locked(false);
                                info!(
                                    "the message queue unlock OK, Group: {:?} {}",
                                    self.consumer_group, mq
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("unlockBatchMQ exception {}", e);
                    }
                }
            }
        }
    }

    async fn build_process_queue_table_by_broker_name(
        &self,
    ) -> HashMap<String /* brokerName */, HashSet<MessageQueue>> {
        let mut result = HashMap::new();
        let process_queue_table = self.process_queue_table.read().await;
        let client = self.client_instance.as_ref().unwrap();
        for (mq, pq) in process_queue_table.iter() {
            if pq.is_dropped() {
                continue;
            }
            let broker_name = client.get_broker_name_from_message_queue(mq).await;
            let entry = result.entry(broker_name).or_insert(HashSet::new());
            entry.insert(mq.to_owned());
        }
        result
    }
}
