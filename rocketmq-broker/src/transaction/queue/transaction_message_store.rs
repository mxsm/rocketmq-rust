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
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;

use crate::failover::escape_bridge::EscapeBridge;

/// Narrow, non-owning Store capability for the transaction subsystem.
pub(crate) struct TransactionMessageStore<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> Clone for TransactionMessageStore<MS> {
    fn clone(&self) -> Self {
        Self {
            escape_bridge: Weak::clone(&self.escape_bridge),
        }
    }
}

impl<MS: MessageStore> TransactionMessageStore<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    pub(crate) fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.escape_bridge
            .upgrade()
            .and_then(|provider| provider.get_min_offset_from_local_store(topic, queue_id).ok())
            .unwrap_or(-1)
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Option<GetMessageResult> {
        let provider = self.escape_bridge.upgrade()?;
        provider
            .get_message_from_local_store(group, topic, queue_id, offset, nums)
            .await
            .ok()
            .flatten()
    }

    pub(crate) fn look_message_by_offset(&self, offset: i64) -> Option<MessageExt> {
        self.escape_bridge
            .upgrade()
            .and_then(|provider| provider.look_message_by_offset_from_local_store(offset).ok())
            .flatten()
    }

    pub(crate) async fn put_message(&self, message: MessageExtBrokerInner) -> PutMessageResult {
        let Some(provider) = self.escape_bridge.upgrade() else {
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        };
        provider
            .put_message_to_local_store(message)
            .await
            .unwrap_or_else(|_| PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable))
    }

    pub(crate) fn state_machine_version(&self) -> Option<i64> {
        self.escape_bridge
            .upgrade()
            .and_then(|provider| provider.local_store_state_machine_version().ok())
    }

    pub(crate) async fn update_master_address(&self, master_addr: &CheetahString) {
        let Some(provider) = self.escape_bridge.upgrade() else {
            return;
        };
        let _ = provider.update_local_store_master_address(master_addr).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::message_store::OwnedMessageStore;

    #[tokio::test]
    async fn transaction_store_fails_closed_after_provider_shutdown() {
        let store = TransactionMessageStore::<OwnedMessageStore> {
            escape_bridge: Weak::new(),
        };
        let topic = CheetahString::from_static_str("transaction-topic");
        let group = CheetahString::from_static_str("transaction-group");

        assert_eq!(store.get_min_offset_in_queue(&topic, 0), -1);
        assert!(store.get_message(&group, &topic, 0, 0, 1).await.is_none());
        assert!(store.look_message_by_offset(0).is_none());
        assert!(store.state_machine_version().is_none());
        assert_eq!(
            store
                .put_message(MessageExtBrokerInner::default())
                .await
                .put_message_status(),
            PutMessageStatus::ServiceNotAvailable
        );
        store
            .update_master_address(&CheetahString::from_static_str("127.0.0.1:10912"))
            .await;
    }

    #[test]
    fn transaction_store_source_uses_weak_provider() {
        let source = include_str!("transaction_message_store.rs");

        assert!(source.contains("Weak<EscapeBridge<MS>>"));
        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("owner: Arc", "Mut<MS>")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
    }
}
