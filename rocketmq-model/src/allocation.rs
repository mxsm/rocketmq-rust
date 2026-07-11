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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tracing::info;

use crate::consistent_hash::ConsistentHashRouter;
use crate::consistent_hash::HashFunction;
use crate::consistent_hash::Node;
use crate::message::MessageQueue;

/// Runtime-neutral queue allocation contract.
pub trait AllocateMessageQueueStrategy: Send + Sync {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>>;

    fn get_name(&self) -> &'static str;
}

pub type AbstractAllocateMessageQueueStrategy = dyn AllocateMessageQueueStrategy;

/// Validates the shared preconditions used by allocation strategies.
pub fn check(
    consumer_group: &CheetahString,
    current_cid: &CheetahString,
    mq_all: &[MessageQueue],
    cid_all: &[CheetahString],
) -> RocketMQResult<bool> {
    if current_cid.is_empty() {
        return Err(RocketMQError::illegal_argument("currentCID is empty"));
    }
    if mq_all.is_empty() {
        return Err(RocketMQError::illegal_argument("mqAll is null or mqAll empty"));
    }
    if cid_all.is_empty() {
        return Err(RocketMQError::illegal_argument("cidAll is null or cidAll empty"));
    }
    if !cid_all.iter().any(|cid| cid == current_cid) {
        info!(
            consumer_group = %consumer_group,
            current_cid = %current_cid,
            "consumer id is not present in the allocation set"
        );
        return Ok(false);
    }
    Ok(true)
}

/// Allocates contiguous queue ranges as evenly as possible.
pub struct AllocateMessageQueueAveragely;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragely {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        let index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);
        let remainder = mq_all.len() % cid_all.len();
        let average = if mq_all.len() <= cid_all.len() {
            1
        } else if remainder > 0 && index < remainder {
            mq_all.len() / cid_all.len() + 1
        } else {
            mq_all.len() / cid_all.len()
        };
        let start = if remainder > 0 && index < remainder {
            index * average
        } else {
            index * average + remainder
        };
        Ok(mq_all.iter().skip(start).take(average).cloned().collect())
    }

    fn get_name(&self) -> &'static str {
        "AVG"
    }
}

/// Allocates every Nth queue to each consumer.
pub struct AllocateMessageQueueAveragelyByCircle;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragelyByCircle {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        let index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);
        Ok(mq_all
            .iter()
            .enumerate()
            .filter(|(queue_index, _)| queue_index % cid_all.len() == index)
            .map(|(_, queue)| queue.clone())
            .collect())
    }

    fn get_name(&self) -> &'static str {
        "AVG_BY_CIRCLE"
    }
}

/// Returns a statically configured set of queues.
pub struct AllocateMessageQueueByConfig {
    message_queue_list: Vec<MessageQueue>,
}

impl AllocateMessageQueueByConfig {
    pub fn new(message_queue_list: Vec<MessageQueue>) -> Self {
        Self { message_queue_list }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByConfig {
    fn allocate(
        &self,
        _consumer_group: &CheetahString,
        _current_cid: &CheetahString,
        _mq_all: &[MessageQueue],
        _cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        Ok(self.message_queue_list.clone())
    }

    fn get_name(&self) -> &'static str {
        "CONFIG"
    }
}

/// Limits allocation to selected broker machine rooms.
pub struct AllocateMessageQueueByMachineRoom {
    consumer_idcs: HashSet<CheetahString>,
}

impl AllocateMessageQueueByMachineRoom {
    pub fn new(consumer_idcs: HashSet<CheetahString>) -> Self {
        Self { consumer_idcs }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoom {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        let index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);
        let queues: Vec<_> = mq_all
            .iter()
            .filter(|queue| {
                queue.broker_name().split_once('@').is_some_and(|(room, broker)| {
                    !broker.is_empty() && !broker.contains('@') && self.consumer_idcs.contains(room)
                })
            })
            .cloned()
            .collect();
        let per_consumer = queues.len() / cid_all.len();
        let remainder = queues.len() % cid_all.len();
        let mut result: Vec<_> = queues
            .iter()
            .skip(per_consumer * index)
            .take(per_consumer)
            .cloned()
            .collect();
        if remainder > index {
            let extra = index + per_consumer * cid_all.len();
            if let Some(queue) = queues.get(extra) {
                result.push(queue.clone());
            }
        }
        Ok(result)
    }

    fn get_name(&self) -> &'static str {
        "MACHINE_ROOM"
    }
}

/// Resolves the deployment room of brokers and consumers.
pub trait MachineRoomResolver: Send + Sync {
    fn broker_deploy_in(&self, message_queue: &MessageQueue) -> Option<CheetahString>;
    fn consumer_deploy_in(&self, client_id: &CheetahString) -> Option<CheetahString>;
}

/// Prefers queues local to a consumer and shares queues from rooms without consumers.
pub struct AllocateMessageQueueByMachineRoomNearby {
    strategy: Box<dyn AllocateMessageQueueStrategy>,
    resolver: Box<dyn MachineRoomResolver>,
    name: &'static str,
}

pub type AllocateMachineRoomNearby = AllocateMessageQueueByMachineRoomNearby;

impl AllocateMessageQueueByMachineRoomNearby {
    pub fn new(strategy: Box<dyn AllocateMessageQueueStrategy>, resolver: Box<dyn MachineRoomResolver>) -> Self {
        let name = cached_strategy_name(strategy.get_name());
        Self {
            strategy,
            resolver,
            name,
        }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoomNearby {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        let mut room_queues: BTreeMap<CheetahString, Vec<MessageQueue>> = BTreeMap::new();
        for queue in mq_all {
            let room = self
                .resolver
                .broker_deploy_in(queue)
                .filter(|room| !room.is_empty())
                .ok_or_else(|| RocketMQError::illegal_argument(format!("Machine room is null for mq {queue}")))?;
            room_queues.entry(room).or_default().push(queue.clone());
        }
        let mut room_consumers: BTreeMap<CheetahString, Vec<CheetahString>> = BTreeMap::new();
        for cid in cid_all {
            let room = self
                .resolver
                .consumer_deploy_in(cid)
                .filter(|room| !room.is_empty())
                .ok_or_else(|| {
                    RocketMQError::illegal_argument(format!("Machine room is null for consumer id {cid}"))
                })?;
            room_consumers.entry(room).or_default().push(cid.clone());
        }
        let Some(current_room) = self.resolver.consumer_deploy_in(current_cid) else {
            return Ok(Vec::new());
        };
        let mut result = Vec::new();
        if let Some(local_queues) = room_queues.remove(&current_room) {
            if let Some(local_consumers) = room_consumers.get(&current_room) {
                result.extend(
                    self.strategy
                        .allocate(consumer_group, current_cid, &local_queues, local_consumers)?,
                );
            }
        }
        for (room, queues) in room_queues {
            if !room_consumers.contains_key(&room) {
                result.extend(self.strategy.allocate(consumer_group, current_cid, &queues, cid_all)?);
            }
        }
        Ok(result)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

fn cached_strategy_name(inner: &'static str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashMap<&'static str, &'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    names
        .entry(inner)
        .or_insert_with(|| Box::leak(format!("MACHINE_ROOM_NEARBY-{inner}").into_boxed_str()))
}

/// Assigns queues through a consistent hash ring of consumer ids.
#[derive(Clone)]
pub struct AllocateMessageQueueConsistentHash {
    virtual_node_count: i32,
    hash_function: Option<Arc<dyn HashFunction + Send + Sync>>,
}

impl fmt::Debug for AllocateMessageQueueConsistentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllocateMessageQueueConsistentHash")
            .field("virtual_node_count", &self.virtual_node_count)
            .field("custom_hash_function", &self.hash_function.is_some())
            .finish()
    }
}

impl Default for AllocateMessageQueueConsistentHash {
    fn default() -> Self {
        Self::new(10)
    }
}

impl AllocateMessageQueueConsistentHash {
    pub fn new(virtual_node_count: i32) -> Self {
        Self {
            virtual_node_count,
            hash_function: None,
        }
    }

    pub fn try_new(virtual_node_count: i32) -> RocketMQResult<Self> {
        if virtual_node_count < 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "illegal virtualNodeCnt :{virtual_node_count}"
            )));
        }
        Ok(Self::new(virtual_node_count))
    }

    pub fn with_hash_function(
        virtual_node_count: i32,
        hash_function: Arc<dyn HashFunction + Send + Sync>,
    ) -> RocketMQResult<Self> {
        if virtual_node_count < 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "illegal virtualNodeCnt :{virtual_node_count}"
            )));
        }
        Ok(Self {
            virtual_node_count,
            hash_function: Some(hash_function),
        })
    }
}

#[derive(Clone)]
struct ClientNode {
    client_id: CheetahString,
}

impl Node for ClientNode {
    fn get_key(&self) -> &str {
        &self.client_id
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueConsistentHash {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> RocketMQResult<Vec<MessageQueue>> {
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(Vec::new());
        }
        if self.virtual_node_count < 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "illegal virtualNodeCnt :{}",
                self.virtual_node_count
            )));
        }
        let nodes = cid_all
            .iter()
            .cloned()
            .map(|client_id| ClientNode { client_id })
            .collect();
        let router = if let Some(hash_function) = &self.hash_function {
            let hash_function: Arc<dyn HashFunction> = hash_function.clone();
            ConsistentHashRouter::try_new_with_hash_function(nodes, self.virtual_node_count, hash_function)?
        } else {
            ConsistentHashRouter::try_new(nodes, self.virtual_node_count)?
        };
        Ok(mq_all
            .iter()
            .filter(|queue| {
                router
                    .route_node_ref(&queue.to_string())
                    .is_some_and(|node| node.client_id == *current_cid)
            })
            .cloned()
            .collect())
    }

    fn get_name(&self) -> &'static str {
        "CONSISTENT_HASH"
    }
}
