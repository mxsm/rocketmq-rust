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

//! Rebalance implementation for the LitePull (active-pull) consumer mode.
//!
//! [`RebalanceLitePullImpl`] specialises the shared [`RebalanceImpl`] core for the scenario
//! where the consumer actively drives message fetching via `poll()`.  The key behavioural
//! differences from [`RebalancePushImpl`] are:
//!
//! - Queue removal is unconditional (no ordering lock). The offset is persisted and cleared.
//! - `consume_type` returns [`ConsumeType::ConsumeActively`].
//! - Initial pull-offset computation uses [`ReadOffsetType::MemoryFirstThenStore`] rather than
//!   [`ReadOffsetType::ReadFromStore`], because LitePull consumers cache offsets in memory.
//! - `dispatch_pull_request` and `dispatch_pop_pull_request` are no-ops; the consumer drives
//!   fetching directly through `poll()`.
//! - Lock/unlock operations are no-ops; LitePull does not use broker-side distributed queue locks.

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::utils::util_all;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::rebalance_impl::RebalanceImpl;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

/// Rebalance logic for the LitePull consumer.
///
/// Holds the shared [`RebalanceImpl`] core alongside the consumer configuration and the
/// offset store.  The offset store is injected by the consumer after construction.
pub struct RebalanceLitePullImpl {
    /// Shared rebalance state: process-queue table, subscription map, broker connections, etc.
    pub(crate) rebalance_impl_inner: RebalanceImpl<RebalanceLitePullImpl>,
    /// Consumer configuration providing `ConsumeFromWhere`, `ConsumeTimestamp`, and the
    /// optional [`MessageQueueListener`][crate::consumer::message_queue_listener::MessageQueueListener].
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    /// The active offset storage backend, injected after the consumer starts.
    pub(crate) offset_store: Option<OffsetStore>,
}

impl RebalanceLitePullImpl {
    /// Constructs a new `RebalanceLitePullImpl` with the supplied configuration.
    ///
    /// The offset store, client instance, and subscription strategy must be set separately
    /// before the first rebalance cycle runs.
    pub fn new(consumer_config: ArcMut<ConsumerConfig>) -> Self {
        Self {
            consumer_config,
            rebalance_impl_inner: RebalanceImpl::new(None, None, None, None),
            offset_store: None,
        }
    }
}

impl RebalanceLitePullImpl {
    /// Returns a shared reference to the subscription map, keyed by topic.
    pub fn get_subscription_inner(&self) -> Arc<DashMap<CheetahString, SubscriptionData>> {
        self.rebalance_impl_inner.subscription_inner.clone()
    }

    /// Sets the consumer group name on the underlying rebalance core.
    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.rebalance_impl_inner.consumer_group = Some(consumer_group);
    }

    /// Sets the message model (Clustering / Broadcasting) on the underlying rebalance core.
    pub fn set_message_model(
        &mut self,
        message_model: rocketmq_remoting::protocol::heartbeat::message_model::MessageModel,
    ) {
        self.rebalance_impl_inner.message_model = Some(message_model);
    }

    /// Sets the queue-allocation strategy used during rebalancing.
    pub fn set_allocate_message_queue_strategy(
        &mut self,
        strategy: Arc<dyn crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy>,
    ) {
        self.rebalance_impl_inner.allocate_message_queue_strategy = Some(strategy);
    }

    /// Injects the `MQClientInstance` used for broker communication.
    pub fn set_mq_client_factory(&mut self, client_instance: ArcMut<MQClientInstance>) {
        self.rebalance_impl_inner.client_instance = Some(client_instance);
    }

    /// Registers this [`RebalanceLitePullImpl`] as its own sub-rebalance implementation.
    ///
    /// The weak reference prevents a reference cycle between the outer `ArcMut` wrapper and
    /// the inner rebalance core.
    pub fn set_rebalance_impl(&mut self, rebalance_impl: WeakArcMut<RebalanceLitePullImpl>) {
        self.rebalance_impl_inner.sub_rebalance_impl = Some(rebalance_impl);
    }

    /// Inserts or replaces a topic subscription.
    #[inline]
    pub fn put_subscription_data(&mut self, topic: CheetahString, subscription_data: SubscriptionData) {
        self.rebalance_impl_inner
            .subscription_inner
            .insert(topic, subscription_data);
    }

    /// Sets the offset store backend used to persist and query per-queue consume offsets.
    pub fn set_offset_store(&mut self, offset_store: OffsetStore) {
        self.offset_store = Some(offset_store);
    }
}

impl Rebalance for RebalanceLitePullImpl {
    /// Notifies the user-registered [`MessageQueueListener`] of a queue-allocation change.
    ///
    /// Any panic or error raised by the listener is caught and logged; the rebalance cycle
    /// is never interrupted by listener failures.
    async fn message_queue_changed(
        &mut self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    ) {
        let listener = self.consumer_config.message_queue_listener.clone();
        if let Some(listener) = listener {
            if let Err(err) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener.message_queue_changed(topic, mq_all, mq_divided);
            })) {
                error!("messageQueueChanged listener panicked for topic={}: {:?}", topic, err);
            }
        }
    }

    /// Persists and then removes the consume offset for a queue being dropped from this consumer.
    ///
    /// LitePull consumers have no ordering lock, so the queue can always be removed immediately.
    /// Returns `true` unconditionally.
    async fn remove_unnecessary_message_queue(&mut self, mq: &MessageQueue, _pq: &ProcessQueue) -> bool {
        let Some(ref mut offset_store) = self.offset_store else {
            warn!("Offset store not initialised; skipping persist/remove for {}", mq);
            return true;
        };
        offset_store.persist(mq).await;
        offset_store.remove_offset(mq).await;
        true
    }

    /// Returns [`ConsumeType::ConsumeActively`], declaring LitePull as an active-pull consumer.
    fn consume_type(&self) -> ConsumeType {
        ConsumeType::ConsumeActively
    }

    /// Removes the in-memory offset entry for the given queue without persisting it.
    ///
    /// Called to discard a stale offset before a fresh initial-offset computation.
    async fn remove_dirty_offset(&mut self, mq: &MessageQueue) {
        let Some(ref mut offset_store) = self.offset_store else {
            warn!("Offset store not initialised; cannot remove dirty offset for {}", mq);
            return;
        };
        offset_store.remove_offset(mq).await;
    }

    /// Computes the initial consume offset for a newly assigned queue.
    ///
    /// Uses [`ReadOffsetType::MemoryFirstThenStore`] to check the in-memory cache before
    /// falling back to the persistent store, matching the LitePull consumer's offset management
    /// model.
    ///
    /// Returns `-1` when the offset store returns an unexpected value (not `>= 0` and not `-1`),
    /// indicating to the caller that the queue should be skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker is unreachable when a remote offset query is required.
    #[allow(deprecated)]
    async fn compute_pull_from_where_with_exception(
        &mut self,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let consume_from_where = self.consumer_config.consume_from_where;

        let offset_store = self.offset_store.as_ref().ok_or_else(|| {
            error!("Offset store not initialised for mq: {}", mq);
            mq_client_err!(
                ResponseCode::SystemError as i32,
                format!("Offset store not initialised for mq: {}", mq)
            )
        })?;

        // LitePull checks the in-memory cache first; a cached offset is returned directly.
        let last_offset = offset_store.read_offset(mq, ReadOffsetType::MemoryFirstThenStore).await;

        let result = match consume_from_where {
            ConsumeFromWhere::ConsumeFromLastOffset
            | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
            | ConsumeFromWhere::ConsumeFromMinOffset
            | ConsumeFromWhere::ConsumeFromMaxOffset => {
                if last_offset >= 0 {
                    last_offset
                } else if last_offset == -1 {
                    // No stored offset: retry topics start at 0; others start at the broker tail.
                    if mq.topic_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        0
                    } else {
                        let client = self.rebalance_impl_inner.client_instance.as_mut().ok_or_else(|| {
                            error!("Client instance not initialised for mq: {}", mq);
                            mq_client_err!(
                                ResponseCode::SystemError as i32,
                                format!("Client instance not initialised for mq: {}", mq)
                            )
                        })?;
                        client.mq_admin_impl.max_offset(mq).await?
                    }
                } else {
                    -1
                }
            }

            ConsumeFromWhere::ConsumeFromFirstOffset => {
                if last_offset >= 0 {
                    last_offset
                } else if last_offset == -1 {
                    0
                } else {
                    -1
                }
            }

            ConsumeFromWhere::ConsumeFromTimestamp => {
                if last_offset >= 0 {
                    last_offset
                } else if last_offset == -1 {
                    if mq.topic_str().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                        let client = self.rebalance_impl_inner.client_instance.as_mut().ok_or_else(|| {
                            error!("Client instance not initialised for mq: {}", mq);
                            mq_client_err!(
                                ResponseCode::SystemError as i32,
                                format!("Client instance not initialised for mq: {}", mq)
                            )
                        })?;
                        client.mq_admin_impl.max_offset(mq).await?
                    } else {
                        let timestamp = util_all::parse_date(
                            self.consumer_config
                                .consume_timestamp
                                .as_deref()
                                .unwrap_or(util_all::YYYYMMDDHHMMSS),
                            util_all::YYYYMMDDHHMMSS,
                        )
                        .unwrap()
                        .and_utc()
                        .timestamp();
                        let client = self.rebalance_impl_inner.client_instance.as_mut().ok_or_else(|| {
                            error!("Client instance not initialised for mq: {}", mq);
                            mq_client_err!(
                                ResponseCode::SystemError as i32,
                                format!("Client instance not initialised for mq: {}", mq)
                            )
                        })?;
                        client.mq_admin_impl.search_offset(mq, timestamp as u64).await?
                    }
                } else {
                    -1
                }
            }
        };

        Ok(result)
    }

    /// Computes the initial consume offset, returning `-1` on any error.
    async fn compute_pull_from_where(&mut self, mq: &MessageQueue) -> i64 {
        self.compute_pull_from_where_with_exception(mq)
            .await
            .unwrap_or_else(|e| {
                warn!("Compute consume offset exception, mq={}, err={:?}", mq, e);
                -1
            })
    }

    /// Not supported for LitePull consumers; always returns `-1`.
    ///
    /// LitePull consumers manage their own consume position via explicit `seek` or `poll`
    /// calls, so the `ConsumeInitMode` concept (MIN/MAX) does not apply.
    fn get_consume_init_mode(&self) -> i32 {
        warn!("get_consume_init_mode is not supported for LitePull consumers");
        -1
    }

    /// No-op for LitePull consumers.
    ///
    /// LitePull consumers drive message fetching directly through `poll()`; they do not use
    /// the `PullRequest` queue mechanism employed by Push consumers.
    async fn dispatch_pull_request(&self, _pull_request_list: Vec<PullRequest>, _delay: u64) {}

    /// No-op for LitePull consumers.
    ///
    /// Pop-mode pull requests are not used in the LitePull consumer model.
    async fn dispatch_pop_pull_request(&self, _pop_request_list: Vec<PopRequest>, _delay: u64) {}

    /// Creates an empty [`ProcessQueue`] for a newly assigned message queue.
    #[inline]
    fn create_process_queue(&self) -> ProcessQueue {
        ProcessQueue::new()
    }

    /// Creates an empty [`PopProcessQueue`].
    ///
    /// Pop mode is not actively used by LitePull consumers; this method exists to satisfy
    /// the [`Rebalance`] contract.
    #[inline]
    fn create_pop_process_queue(&self) -> PopProcessQueue {
        PopProcessQueue::new()
    }

    /// Marks the process queue as dropped, removes it from the table, and cleans up the offset.
    async fn remove_process_queue(&mut self, mq: &MessageQueue) {
        let mut process_queue_table = self.rebalance_impl_inner.process_queue_table.write().await;
        let prev = process_queue_table.remove(mq);
        drop(process_queue_table);
        if let Some(pq) = prev {
            let dropped = pq.is_dropped();
            pq.set_dropped(true);
            self.remove_unnecessary_message_queue(mq, &pq).await;
            if let Some(ref consumer_group) = self.rebalance_impl_inner.consumer_group {
                info!(
                    "Fix Offset, {}, remove unnecessary mq, {} Dropped: {}",
                    consumer_group, mq, dropped
                );
            }
        }
    }

    /// No-op for LitePull consumers.
    ///
    /// LitePull consumers do not hold broker-side distributed queue locks; there is nothing
    /// to unlock.
    async fn unlock(&mut self, _mq: &MessageQueue, _oneway: bool) {}

    /// No-op for LitePull consumers.
    ///
    /// Broker-side queue locking is a Push-consumer concept and is not used here.
    async fn lock_all(&mut self) {}

    /// No-op for LitePull consumers.
    ///
    /// Broker-side queue unlocking is a Push-consumer concept and is not used here.
    async fn unlock_all(&mut self, _oneway: bool) {}

    /// Drives the rebalance cycle by delegating to the shared [`RebalanceImpl`] core.
    async fn do_rebalance(&mut self, is_order: bool) -> bool {
        self.rebalance_impl_inner.do_rebalance(is_order).await
    }

    /// Returns `true`; LitePull consumers always perform client-side rebalancing.
    fn client_rebalance(&mut self, _topic: &str) -> bool {
        true
    }

    /// Drops all process queues and clears the internal tables.
    ///
    /// Called during consumer shutdown to ensure no stale state remains.
    fn destroy(&mut self) {
        info!(
            "Destroying RebalanceLitePullImpl for consumer group: {:?}",
            self.rebalance_impl_inner.consumer_group
        );
        if let Ok(mut table) = self.rebalance_impl_inner.process_queue_table.try_write() {
            for pq in table.values() {
                pq.set_dropped(true);
            }
            table.clear();
        } else {
            warn!("destroy: could not acquire write lock on process_queue_table; queues may not be dropped cleanly");
        }
        if let Ok(mut pop_table) = self.rebalance_impl_inner.pop_process_queue_table.try_write() {
            for pq in pop_table.values() {
                pq.set_dropped(true);
            }
            pop_table.clear();
        } else {
            warn!(
                "destroy: could not acquire write lock on pop_process_queue_table; queues may not be dropped cleanly"
            );
        }
    }
}
