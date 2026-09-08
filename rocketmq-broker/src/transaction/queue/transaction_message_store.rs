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
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store::BrokerMasterAddressStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::PutMessageResult;
use rocketmq_store::PutMessageStatus;

use crate::broker_error::BrokerResult;
use crate::failover::escape_bridge::EscapeBridge;

fn transaction_store_unavailable() -> rocketmq_error::SharedError {
    crate::broker_error::from_canonical(rocketmq_error::Error::new(
        &rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED,
    ))
}

/// Narrow, non-owning Store capability for the transaction subsystem.
pub(crate) struct TransactionMessageStore<MS> {
    escape_bridge: Weak<EscapeBridge<MS>>,
    #[cfg(test)]
    read_results: Arc<parking_lot::Mutex<std::collections::VecDeque<BrokerResult<Option<GetMessageResult>>>>>,
}

impl<MS> Clone for TransactionMessageStore<MS> {
    fn clone(&self) -> Self {
        Self {
            escape_bridge: Weak::clone(&self.escape_bridge),
            #[cfg(test)]
            read_results: Arc::clone(&self.read_results),
        }
    }
}

impl<MS: BrokerReadStore> TransactionMessageStore<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
            #[cfg(test)]
            read_results: Arc::default(),
        }
    }

    pub(crate) fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> BrokerResult<i64> {
        self.escape_bridge
            .upgrade()
            .ok_or_else(transaction_store_unavailable)?
            .get_min_offset_from_local_store(topic, queue_id)
            .map_err(|_| transaction_store_unavailable())
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> BrokerResult<Option<GetMessageResult>> {
        let provider = self.escape_bridge.upgrade().ok_or_else(transaction_store_unavailable)?;
        #[cfg(test)]
        if let Some(result) = self.read_results.lock().pop_front() {
            return result;
        }
        provider
            .get_message_from_local_store(group, topic, queue_id, offset, nums)
            .await
            .map_err(|_| transaction_store_unavailable())
    }

    #[cfg(test)]
    pub(super) fn set_read_results(&self, results: Vec<BrokerResult<Option<GetMessageResult>>>) {
        *self.read_results.lock() = results.into();
    }

    pub(crate) fn look_message_by_offset(&self, offset: i64) -> Option<MessageExt> {
        self.escape_bridge
            .upgrade()
            .and_then(|provider| provider.look_message_by_offset_from_local_store(offset).ok())
            .flatten()
    }

    pub(crate) async fn put_message(&self, message: MessageExtBrokerInner) -> PutMessageResult
    where
        MS: BrokerWriteStore,
    {
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

    pub(crate) async fn update_master_addresses(&self, master_ha_addr: &CheetahString, master_addr: &CheetahString)
    where
        MS: BrokerMasterAddressStore,
    {
        let Some(provider) = self.escape_bridge.upgrade() else {
            return;
        };
        let _ = provider
            .update_local_store_master_addresses(master_ha_addr, master_addr)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::StorePorts;

    #[tokio::test]
    async fn transaction_store_fails_closed_after_provider_shutdown() {
        let store = TransactionMessageStore::<StorePorts> {
            escape_bridge: Weak::new(),
            read_results: Arc::default(),
        };
        let topic = CheetahString::from_static_str("transaction-topic");
        let group = CheetahString::from_static_str("transaction-group");

        let offset_error = store.get_min_offset_in_queue(&topic, 0).unwrap_err();
        let read_error = store
            .get_message(&group, &topic, 0, 0, 1)
            .await
            .err()
            .expect("provider unavailable");
        assert_eq!(
            offset_error.descriptor(),
            &rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED
        );
        assert_eq!(read_error.descriptor(), offset_error.descriptor());
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
            .update_master_addresses(
                &CheetahString::from_static_str("127.0.0.1:10913"),
                &CheetahString::from_static_str("127.0.0.1:10912"),
            )
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
