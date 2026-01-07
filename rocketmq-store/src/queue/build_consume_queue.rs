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

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::queue::consume_queue_store::ConsumeQueueStoreInterface;
use crate::queue::local_file_consume_queue_store::ConsumeQueueStore;

pub struct CommitLogDispatcherBuildConsumeQueue {
    consume_queue_store: ConsumeQueueStore,
}

impl CommitLogDispatcherBuildConsumeQueue {
    pub fn new(consume_queue_store: ConsumeQueueStore) -> Self {
        Self { consume_queue_store }
    }
}

impl CommitLogDispatcher for CommitLogDispatcherBuildConsumeQueue {
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        let tran_type = MessageSysFlag::get_transaction_value(dispatch_request.sys_flag);
        match tran_type {
            MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE => {
                self.consume_queue_store
                    .put_message_position_info_wrapper(dispatch_request);
            }
            _ => {}
        }
    }
}
