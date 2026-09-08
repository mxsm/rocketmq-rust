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

use std::time::Duration;

use crate::broker_error::BrokerResult as Result;
use bytes::Bytes;
use parking_lot::Mutex;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueueSnapshot;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::MutexGuard;

pub struct MessageQueueOpContext {
    state: Mutex<OperationQueueState>,
    drainer: AsyncMutex<()>,
    pending_operations: BudgetedQueue<String>,
}

struct OperationQueueState {
    total_size: usize,
    last_write_timestamp: u64,
    deferred: Option<(String, ResourcePermit)>,
    pending: Option<Bytes>,
    last_append_evidence: Option<rocketmq_store::AppendExecutionEvidence>,
}

struct OperationBody {
    bytes: Vec<u8>,
    _permits: Vec<ResourcePermit>,
}

impl AsRef<[u8]> for OperationBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

// Dropping an interrupted attempt only releases the drainer. The queue retains
// the pending body, and every Store alias retains its resource permits as well.
pub(crate) struct OperationBatch<'a> {
    queue: &'a MessageQueueOpContext,
    body: Bytes,
    _drainer: MutexGuard<'a, ()>,
}

impl OperationBatch<'_> {
    pub(crate) fn body(&self) -> Bytes {
        self.body.clone()
    }

    pub(crate) fn complete(self, timestamp: u64) {
        let mut state = self.queue.state.lock();
        state.pending = None;
        state.last_append_evidence = None;
        state.total_size -= self.body.len();
        state.last_write_timestamp = timestamp;
    }

    pub(crate) fn record_append_evidence(&self, evidence: &rocketmq_store::AppendExecutionEvidence) {
        self.queue.state.lock().last_append_evidence = Some(evidence.clone());
    }

    pub(crate) fn append_evidence(&self) -> Option<rocketmq_store::AppendExecutionEvidence> {
        self.queue.state.lock().last_append_evidence.clone()
    }
}

fn retained_operation_bytes(message: &String) -> usize {
    // Reserve the source and its eventual batch copy at admission. The payload
    // is copied once; subsequent Store bodies share the budget-owning Bytes.
    std::mem::size_of::<String>()
        .saturating_add(std::mem::size_of::<ResourcePermit>())
        .saturating_add(message.capacity().saturating_mul(2))
}

impl MessageQueueOpContext {
    pub fn try_new(timestamp: u64, queue_length: usize, queue_id: i32, parent_budget: &ResourceBudget) -> Result<Self> {
        let queue_bytes = parent_budget.limit().capacity.bytes;
        let budget = parent_budget
            .child(
                format!("queue-{queue_id}"),
                BudgetLimit::new(queue_length, queue_bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(queue_length as u64, queue_length as u64))
                    .with_max_age(Duration::from_secs(30)),
            )
            .map_err(|_error| crate::broker_error::configuration_invalid("broker.transaction.operationQueue"))?;
        Ok(Self {
            state: Mutex::new(OperationQueueState {
                total_size: 0,
                last_write_timestamp: timestamp,
                deferred: None,
                pending: None,
                last_append_evidence: None,
            }),
            drainer: AsyncMutex::new(()),
            pending_operations: BudgetedQueue::new(budget),
        })
    }

    pub fn get_total_size(&self) -> usize {
        self.state.lock().total_size
    }

    pub fn get_last_write_timestamp(&self) -> u64 {
        self.state.lock().last_write_timestamp
    }

    pub fn push(&self, msg: String) -> Result<()> {
        let retained_bytes = retained_operation_bytes(&msg);
        let length = msg.len();
        let mut state = self.state.lock();
        match self.pending_operations.try_push_data(msg, retained_bytes) {
            rocketmq_runtime::QueuePushOutcome::Rejected { .. } => Err(crate::broker_error::broker_operation_failed(
                "message_queue_push",
                ResponseCode::SystemBusy as i32,
                "transaction operation queue is full",
            )),
            _ => {
                state.total_size += length;
                Ok(())
            }
        }
    }

    pub(crate) fn try_take_batch(&self, max_bytes: usize) -> Option<OperationBatch<'_>> {
        let drainer = self.drainer.try_lock().ok()?;
        let mut state = self.state.lock();
        let body = if let Some(body) = &state.pending {
            body.clone()
        } else {
            let mut entries = Vec::new();
            let mut length = 0;
            while length < max_bytes {
                let next = state.deferred.take().or_else(|| {
                    self.pending_operations.try_pop_budgeted().map(|item| {
                        let (message, permit, _) = item.into_parts();
                        (message, permit)
                    })
                });
                let Some((message, permit)) = next else { break };
                if message.len() > max_bytes - length {
                    state.deferred = Some((message, permit));
                    break;
                }
                length += message.len();
                entries.push((message, permit));
            }
            if entries.is_empty() {
                return None;
            }
            let mut bytes = Vec::with_capacity(length);
            let mut permits = Vec::with_capacity(entries.len());
            for (message, permit) in entries {
                bytes.extend_from_slice(message.as_bytes());
                permits.push(permit);
            }
            let body = Bytes::from_owner(OperationBody {
                bytes,
                _permits: permits,
            });
            state.pending = Some(body.clone());
            body
        };
        // Until the Store returns, dropping the attempt cannot prove whether
        // the append worker committed. Keep that uncertainty with the batch.
        state.last_append_evidence = Some(rocketmq_store::AppendExecutionEvidence::Unknown);
        drop(state);
        Some(OperationBatch {
            queue: self,
            body,
            _drainer: drainer,
        })
    }

    pub fn queue_snapshot(&self) -> QueueSnapshot {
        self.pending_operations.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rocketmq_runtime::ResourceBudgetTree;

    fn test_budget() -> ResourceBudget {
        ResourceBudgetTree::new("transaction-batches", BudgetLimit::new(16, 4096, FullPolicy::Reject))
            .unwrap()
            .root()
    }

    #[tokio::test]
    async fn blocked_queue_does_not_block_other_queue_and_cancel_preserves_batch() {
        let root = test_budget();
        let first = Arc::new(MessageQueueOpContext::try_new(0, 8, 0, &root).unwrap());
        let second = MessageQueueOpContext::try_new(0, 8, 1, &root).unwrap();
        first.push("123,".into()).unwrap();
        second.push("456,".into()).unwrap();
        let admitted_bytes = root.snapshot().current_bytes;
        let (started, received) = tokio::sync::oneshot::channel();
        let owner = Arc::clone(&first);
        let attempt = tokio::spawn(async move {
            let batch = owner.try_take_batch(32).unwrap();
            started.send(batch.body()).unwrap();
            std::future::pending::<()>().await;
            batch.complete(1);
        });
        let store_alias = received.await.unwrap();
        assert!(first.try_take_batch(32).is_none());
        let other_batch = second.try_take_batch(32).unwrap();
        assert_eq!(other_batch.body().as_ref(), b"456,");
        assert_eq!(root.snapshot().current_bytes, admitted_bytes);
        other_batch.complete(1);
        assert_eq!(second.get_total_size(), 0);
        assert_eq!(root.snapshot().current_count, 1);

        attempt.abort();
        assert!(attempt.await.unwrap_err().is_cancelled());
        assert_eq!(first.get_total_size(), 4);
        assert_eq!(
            first.state.lock().last_append_evidence,
            Some(rocketmq_store::AppendExecutionEvidence::Unknown)
        );
        assert_eq!(root.snapshot().current_count, 1);
        let retry = first.try_take_batch(32).unwrap();
        assert_eq!(retry.body(), store_alias);
        retry.complete(2);
        assert_eq!(first.get_total_size(), 0);
        assert_eq!(first.get_last_write_timestamp(), 2);
        assert!(first.try_take_batch(32).is_none());
        assert_eq!(root.snapshot().current_count, 1);
        drop(store_alias);
        assert_eq!(root.snapshot().current_count, 0);
        assert_eq!(root.snapshot().current_bytes, 0);
    }

    #[test]
    fn bounded_batch_retries_before_draining_later_operations() {
        let root = test_budget();
        let queue = MessageQueueOpContext::try_new(7, 8, 0, &root).unwrap();
        queue.push("12,".into()).unwrap();
        queue.push("345,".into()).unwrap();
        let batch = queue.try_take_batch(6).unwrap();
        assert_eq!(batch.body().as_ref(), b"12,");
        batch.record_append_evidence(&rocketmq_store::AppendExecutionEvidence::NotAppended);
        assert_eq!(
            batch.append_evidence(),
            Some(rocketmq_store::AppendExecutionEvidence::NotAppended)
        );
        assert_eq!(queue.get_total_size(), 7);
        assert_eq!(root.snapshot().current_count, 2);
        drop(batch); // Append failure retains the original body for retry.
        queue.push("6,".into()).unwrap();
        let retry = queue.try_take_batch(6).unwrap();
        assert_eq!(retry.body().as_ref(), b"12,");
        retry.complete(8);
        assert_eq!(queue.get_total_size(), 6);
        assert_eq!(root.snapshot().current_count, 2);
        let next = queue.try_take_batch(6).unwrap();
        assert_eq!(next.body().as_ref(), b"345,6,");
        next.complete(9);
        assert_eq!(queue.get_total_size(), 0);
        assert_eq!(root.snapshot().current_count, 0);
        assert_eq!(root.snapshot().current_bytes, 0);
    }

    #[tokio::test]
    async fn overload_rejects_excess_transaction_operations() {
        let item_bytes = retained_operation_bytes(&"a".to_owned());
        let root = ResourceBudgetTree::new(
            "broker-transaction-overload",
            BudgetLimit::new(2, item_bytes * 2, FullPolicy::Reject),
        )
        .expect("root budget")
        .root();
        let queue = MessageQueueOpContext::try_new(0, 2, 0, &root).expect("operation queue");

        assert!(queue.push("a".to_owned()).is_ok());
        assert!(queue.push("b".to_owned()).is_ok());
        assert!(queue.push("c".to_owned()).is_err());
        assert!(queue.push("d".to_owned()).is_err());

        let snapshot = queue.queue_snapshot();
        assert_eq!(snapshot.depth, 2);
        assert_eq!(snapshot.retained_bytes, item_bytes * 2);
        assert_eq!(snapshot.rejected_count, 2);
    }
}
