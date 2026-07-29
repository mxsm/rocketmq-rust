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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use tokio::sync::Notify;
use tokio::time::timeout;

use super::RebalanceImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::Rebalance;

struct BlockingRemovalRebalance {
    callback_started: Notify,
    release_callback: Notify,
    offset_started: Notify,
    release_offset: Notify,
}

impl Rebalance for BlockingRemovalRebalance {
    async fn message_queue_changed(
        &self,
        _topic: &str,
        _mq_all: &HashSet<MessageQueue>,
        _mq_divided: &HashSet<MessageQueue>,
    ) {
    }

    async fn remove_unnecessary_message_queue(&self, _mq: &MessageQueue, _pq: &ProcessQueue) -> bool {
        self.callback_started.notify_one();
        self.release_callback.notified().await;
        true
    }

    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumePassively
    }

    async fn remove_dirty_offset(&self, _mq: &MessageQueue) {
        self.offset_started.notify_one();
        self.release_offset.notified().await;
    }

    async fn compute_pull_from_where_with_exception(&self, _mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        Ok(0)
    }

    async fn compute_pull_from_where(&self, _mq: &MessageQueue) -> i64 {
        0
    }

    fn get_consume_init_mode(&self) -> i32 {
        0
    }

    async fn dispatch_pull_request(&self, _pull_request_list: Vec<PullRequest>, _delay: u64) {}

    async fn dispatch_pop_pull_request(&self, _pop_request_list: Vec<PopRequest>, _delay: u64) {}

    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    async fn remove_process_queue(&self, _mq: &MessageQueue) {}

    async fn unlock(&self, _mq: &MessageQueue, _oneway: bool) {}

    async fn lock_all(&self) {}

    async fn unlock_all(&self, _oneway: bool) {}

    async fn do_rebalance(&self, _is_order: bool) -> bool {
        true
    }

    fn client_rebalance(&self, _topic: &str) -> bool {
        true
    }

    fn destroy(&self) {}
}

#[tokio::test]
async fn blocked_removal_callback_does_not_hold_queue_table_or_remove_replacement() {
    let topic = CheetahString::from_static_str("topic-a");
    let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
    let original = Arc::new(ProcessQueue::new());
    let replacement = Arc::new(ProcessQueue::new());
    let callback = Arc::new(BlockingRemovalRebalance {
        callback_started: Notify::new(),
        release_callback: Notify::new(),
        offset_started: Notify::new(),
        release_offset: Notify::new(),
    });
    let rebalance = Arc::new(RebalanceImpl::new(
        Some(CheetahString::from_static_str("group-a")),
        None,
        None,
        None,
    ));
    let callback_set = rebalance.sub_rebalance_impl.set(Arc::downgrade(&callback));
    assert!(callback_set.is_ok(), "test callback should be initialized once");
    rebalance.process_queue_table.write().await.insert(mq.clone(), original);

    let removal = tokio::spawn({
        let rebalance = rebalance.clone();
        let topic = topic.clone();
        async move {
            rebalance
                .update_process_queue_table_in_rebalance(&topic, &HashSet::new(), false)
                .await
        }
    });

    let callback_started = timeout(Duration::from_secs(1), callback.callback_started.notified()).await;
    assert!(callback_started.is_ok(), "removal callback should start");
    let replacement_inserted = timeout(Duration::from_millis(100), async {
        rebalance
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), replacement.clone());
    })
    .await
    .is_ok();

    callback.release_callback.notify_one();
    let removal_result = timeout(Duration::from_secs(1), removal).await;
    assert!(
        matches!(&removal_result, Ok(Ok(_))),
        "rebalance should finish after releasing the callback without panicking"
    );
    let Ok(Ok(changed)) = removal_result else {
        return;
    };

    assert!(
        replacement_inserted,
        "a blocked removal callback must not retain the process-queue table lock"
    );
    let current = rebalance.process_queue_table.read().await.get(&mq).cloned();
    assert!(current.is_some(), "replacement queue must survive the stale callback");
    let Some(current) = current else {
        return;
    };
    assert!(Arc::ptr_eq(&current, &replacement));
    assert!(!changed, "a stale callback must not report removal of a replacement");
}

#[tokio::test]
async fn blocked_offset_lookup_does_not_hold_queue_table_or_overwrite_concurrent_insert() {
    let topic = CheetahString::from_static_str("topic-a");
    let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
    let replacement = Arc::new(ProcessQueue::new());
    let callback = Arc::new(BlockingRemovalRebalance {
        callback_started: Notify::new(),
        release_callback: Notify::new(),
        offset_started: Notify::new(),
        release_offset: Notify::new(),
    });
    let rebalance = Arc::new(RebalanceImpl::new(
        Some(CheetahString::from_static_str("group-a")),
        None,
        None,
        None,
    ));
    let callback_set = rebalance.sub_rebalance_impl.set(Arc::downgrade(&callback));
    assert!(callback_set.is_ok(), "test callback should be initialized once");

    let update = tokio::spawn({
        let rebalance = rebalance.clone();
        let topic = topic.clone();
        let mq = mq.clone();
        async move {
            rebalance
                .update_process_queue_table_in_rebalance(&topic, &HashSet::from([mq]), false)
                .await
        }
    });

    let offset_started = timeout(Duration::from_secs(1), callback.offset_started.notified()).await;
    assert!(offset_started.is_ok(), "offset lookup should start");
    let replacement_inserted = timeout(Duration::from_millis(100), async {
        rebalance
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), replacement.clone());
    })
    .await
    .is_ok();

    callback.release_offset.notify_one();
    let update_result = timeout(Duration::from_secs(1), update).await;
    assert!(
        matches!(&update_result, Ok(Ok(_))),
        "rebalance should finish after releasing the offset lookup without panicking"
    );
    let Ok(Ok(changed)) = update_result else {
        return;
    };

    assert!(
        replacement_inserted,
        "a blocked offset lookup must not retain the process-queue table lock"
    );
    let current = rebalance.process_queue_table.read().await.get(&mq).cloned();
    assert!(current.is_some(), "concurrently inserted queue must remain present");
    let Some(current) = current else {
        return;
    };
    assert!(Arc::ptr_eq(&current, &replacement));
    assert!(!changed, "a concurrent insertion must win the conditional commit");
}
