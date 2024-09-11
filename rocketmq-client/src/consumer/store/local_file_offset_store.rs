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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::ArcRefCellWrapper;

use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

pub struct LocalFileOffsetStore;

impl LocalFileOffsetStore {
    pub fn new(mq_client_factory: ArcRefCellWrapper<MQClientInstance>, group_name: String) -> Self {
        Self
    }
}

impl OffsetStoreTrait for LocalFileOffsetStore {
    async fn load(&self) -> crate::Result<()> {
        todo!()
    }

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        todo!()
    }

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        todo!()
    }

    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        todo!()
    }

    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        todo!()
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        todo!()
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        todo!()
    }

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        todo!()
    }

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> crate::Result<()> {
        todo!()
    }
}
