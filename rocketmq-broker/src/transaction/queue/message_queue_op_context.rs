// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::UnifiedServiceError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueueSnapshot;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use tokio::time;

pub struct MessageQueueOpContext {
    total_size: AtomicU32,
    last_write_timestamp: AtomicU64,
    pending_operations: BudgetedQueue<String>,
}

impl MessageQueueOpContext {
    pub fn try_new(
        timestamp: u64,
        queue_length: usize,
        queue_id: i32,
        parent_budget: &ResourceBudget,
    ) -> RocketMQResult<Self> {
        let queue_bytes = parent_budget.limit().capacity.bytes;
        let budget = parent_budget
            .child(
                format!("queue-{queue_id}"),
                BudgetLimit::new(queue_length, queue_bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(queue_length as u64, queue_length as u64))
                    .with_max_age(Duration::from_secs(30)),
            )
            .map_err(|error| RocketMQError::ConfigInvalidValue {
                key: "broker.transaction.operationQueue",
                value: queue_id.to_string(),
                reason: error.to_string(),
            })?;
        Ok(Self {
            total_size: AtomicU32::new(0),
            last_write_timestamp: AtomicU64::new(timestamp),
            pending_operations: BudgetedQueue::new(budget),
        })
    }

    pub async fn get_total_size(&self) -> u32 {
        self.total_size.load(Ordering::Relaxed)
    }

    pub async fn total_size_add_and_get(&self, delta: u32) -> u32 {
        self.total_size.fetch_add(delta, Ordering::AcqRel) + delta
    }

    pub async fn get_last_write_timestamp(&self) -> u64 {
        self.last_write_timestamp.load(Ordering::Relaxed)
    }

    pub async fn set_last_write_timestamp(&self, timestamp: u64) {
        self.last_write_timestamp.store(timestamp, Ordering::Release);
    }

    pub async fn push(&self, msg: String) -> RocketMQResult<()> {
        let retained_bytes = std::mem::size_of::<String>().saturating_add(msg.capacity());
        match self.pending_operations.try_push_data(msg, retained_bytes) {
            rocketmq_runtime::QueuePushOutcome::Rejected { .. } => Err(RocketMQError::broker_operation_failed(
                "message_queue_push",
                ResponseCode::SystemBusy as i32,
                "transaction operation queue is full",
            )),
            _ => Ok(()),
        }
    }
    pub async fn offer(&self, item: String, timeout: std::time::Duration) -> RocketMQResult<()> {
        if let Ok(res) = time::timeout(timeout, self.push(item)).await {
            return res;
        }
        Err(RocketMQError::Timeout {
            operation: "message_queue_offer",
            timeout_ms: timeout.as_millis() as u64,
        })
    }
    pub async fn pull(&self) -> RocketMQResult<String> {
        if let Some(item) = self.pending_operations.recv().await {
            return Ok(item);
        }
        Err(RocketMQError::Service(UnifiedServiceError::Interrupted))
    }
    pub async fn is_empty(&self) -> bool {
        self.pending_operations.is_empty()
    }

    pub fn queue_snapshot(&self) -> QueueSnapshot {
        self.pending_operations.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_runtime::ResourceBudgetTree;

    #[tokio::test]
    async fn overload_rejects_excess_transaction_operations() {
        let item_bytes = std::mem::size_of::<String>() + 1;
        let root = ResourceBudgetTree::new(
            "broker-transaction-overload",
            BudgetLimit::new(2, item_bytes * 2, FullPolicy::Reject),
        )
        .expect("root budget")
        .root();
        let queue = MessageQueueOpContext::try_new(0, 2, 0, &root).expect("operation queue");

        assert!(queue.push("a".to_owned()).await.is_ok());
        assert!(queue.push("b".to_owned()).await.is_ok());
        assert!(queue.push("c".to_owned()).await.is_err());
        assert!(queue.push("d".to_owned()).await.is_err());

        let snapshot = queue.queue_snapshot();
        assert_eq!(snapshot.depth, 2);
        assert_eq!(snapshot.retained_bytes, item_bytes * 2);
        assert_eq!(snapshot.rejected_count, 2);
    }
}
