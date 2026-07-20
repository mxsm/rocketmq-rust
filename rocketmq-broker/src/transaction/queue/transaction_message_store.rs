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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;

/// Transitional direct store owner for the transaction subsystem.
///
/// This isolates the remaining `ArcMut` dependency to the MessageStore boundary instead of
/// retaining the complete broker runtime. It is not a soundness exemption: Store owner batches
/// must replace it with a standard owner or actor before PR-M11-12 can close.
pub(crate) struct TransactionMessageStore<MS: MessageStore> {
    owner: ArcMut<MS>,
}

impl<MS: MessageStore> Clone for TransactionMessageStore<MS> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
        }
    }
}

impl<MS: MessageStore> TransactionMessageStore<MS> {
    pub(crate) fn new(owner: ArcMut<MS>) -> Self {
        Self { owner }
    }

    pub(crate) fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.owner.get_min_offset_in_queue(topic, queue_id)
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Option<GetMessageResult> {
        self.owner.get_message(group, topic, queue_id, offset, nums, None).await
    }

    pub(crate) fn look_message_by_offset(&self, offset: i64) -> Option<MessageExt> {
        self.owner.look_message_by_offset(offset)
    }

    pub(crate) async fn put_message(&mut self, message: MessageExtBrokerInner) -> PutMessageResult {
        self.owner.put_message(message).await
    }

    pub(crate) fn state_machine_version(&self) -> i64 {
        self.owner.get_state_machine_version()
    }

    pub(crate) async fn update_master_address(&self, master_addr: &CheetahString) {
        self.owner.update_ha_master_address(master_addr.as_str()).await;
        self.owner.update_master_address(master_addr);
    }
}
