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

use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_store_local::consume_queue::root::ConsumeQueueDispatchRoot;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::queue::consume_queue_store::ConsumeQueueStoreInterface;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;

pub struct CommitLogDispatcherBuildConsumeQueue {
    root: ConsumeQueueDispatchRoot<ConsumeQueueStore>,
}

impl CommitLogDispatcherBuildConsumeQueue {
    pub fn new(consume_queue_store: ConsumeQueueStore) -> Self {
        Self {
            root: ConsumeQueueDispatchRoot::new(consume_queue_store),
        }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildConsumeQueue {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let tran_type = MessageSysFlag::get_transaction_value(dispatch_request.sys_flag);
        let eligible = matches!(
            tran_type,
            MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE
        );
        self.root.dispatch(eligible, dispatch_request, |store, request| {
            store.put_message_position_info_wrapper(request);
        });
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        self.root
            .progress_offset(ConsumeQueueStore::get_max_phy_offset_in_consume_queue_global)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;

    #[test]
    fn prepared_and_rollback_transactions_skip_consume_queue_dispatch() {
        let store = ConsumeQueueStore::new(
            Arc::new(MessageStoreConfig::default()),
            Arc::new(BrokerConfig::default()),
        );
        let dispatcher = CommitLogDispatcherBuildConsumeQueue::new(store);

        for transaction_type in [
            MessageSysFlag::TRANSACTION_PREPARED_TYPE,
            MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
        ] {
            let mut request = DispatchRequest {
                sys_flag: transaction_type,
                ..DispatchRequest::default()
            };
            dispatcher.dispatch(&mut request);
        }

        assert_eq!(dispatcher.dispatch_progress_offset(0), None);
    }
}
