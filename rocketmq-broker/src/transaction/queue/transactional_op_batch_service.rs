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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::service_task::ServiceTaskContext;
use rocketmq_runtime::task::ServiceManager;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::BrokerMasterAddressStore;
use rocketmq_store::BrokerWriteStore;
use tracing::info;
use tracing::warn;

use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;

pub struct TransactionalOpBatchService<MS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
{
    service_manager: ServiceManager<TransactionalOpBatchServiceInner<MS>>,
    #[cfg(test)]
    pub(super) parent_for_test: Option<TaskGroup>,
}

impl<MS> TransactionalOpBatchService<MS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
{
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: Weak<DefaultTransactionalMessageService<MS>>,
    ) -> Self {
        Self::with_owner(broker_config, transactional_message_service, None)
    }

    /// Owns the batch loop and its final queue drain under the broker group.
    ///
    /// A cooperative exit closes operation admission and attempts to flush
    /// accepted partial batches while the Store is available. A failed append
    /// or forced cancellation retains its payload and resource reservations;
    /// task completion alone does not prove the business drain succeeded.
    pub fn new_with_task_group(
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: Weak<DefaultTransactionalMessageService<MS>>,
        parent: TaskGroup,
    ) -> Self {
        Self::with_owner(broker_config, transactional_message_service, Some(parent))
    }

    fn with_owner(
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: Weak<DefaultTransactionalMessageService<MS>>,
        parent: Option<TaskGroup>,
    ) -> Self {
        let inner = TransactionalOpBatchServiceInner {
            cancellation: parent.as_ref().map(TaskGroup::cancellation_token).unwrap_or_default(),
            broker_config,
            transactional_message_service,
            wakeup_timestamp: AtomicU64::new(0),
        };
        #[cfg(test)]
        let parent_for_test = parent.clone();
        let service_manager = match parent {
            Some(parent) => ServiceManager::new_with_task_group(inner, parent),
            None => ServiceManager::new_legacy_compatibility(inner),
        };
        TransactionalOpBatchService {
            service_manager,
            #[cfg(test)]
            parent_for_test,
        }
    }

    pub async fn start(&self) -> crate::broker_error::BrokerResult<()> {
        self.service_manager
            .start()
            .await
            .map_err(|source| crate::broker_error::broker_task_failed("TransactionalOpBatchService", source))
    }

    pub async fn shutdown(&self) {
        if let Err(error) = self.service_manager.shutdown().await {
            warn!(error = %error, "TransactionalOpBatchService shutdown failed");
        }
    }

    pub(super) async fn shutdown_report(&self) -> Option<rocketmq_runtime::ShutdownReport> {
        self.service_manager.last_task_group_shutdown_report().await
    }

    pub fn wakeup(&self) {
        self.service_manager.wakeup();
    }
}

struct TransactionalOpBatchServiceInner<MS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
{
    cancellation: tokio_util::sync::CancellationToken,
    broker_config: Arc<BrokerConfig>,
    transactional_message_service: Weak<DefaultTransactionalMessageService<MS>>,
    wakeup_timestamp: AtomicU64,
}

impl<MS> ServiceTask for TransactionalOpBatchServiceInner<MS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
{
    fn get_service_name(&self) -> String {
        "TransactionalOpBatchService".to_string()
    }

    async fn run(&self, context: &ServiceTaskContext) {
        info!("TransactionalOpBatchService started");
        let transaction_op_batch_interval = self.broker_config.transaction_op_batch_interval;
        self.wakeup_timestamp.store(
            current_millis() + transaction_op_batch_interval,
            std::sync::atomic::Ordering::Relaxed,
        );
        while !context.is_stopped() && !self.cancellation.is_cancelled() {
            let mut interval =
                self.wakeup_timestamp.load(std::sync::atomic::Ordering::Relaxed) as i64 - current_millis() as i64;
            if interval <= 0 {
                interval = 0;
                context.wakeup();
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break,
                _ = context.wait_for_running(Duration::from_millis(interval as u64)) => {}
            }
            if !context.is_stopped() && !self.cancellation.is_cancelled() {
                self.on_wait_end().await;
            }
        }
        if let Some(service) = self.transactional_message_service.upgrade() {
            service.drain_operation_queues().await;
        }
    }

    async fn on_wait_end(&self) {
        if let Some(transactional_message_service) = self.transactional_message_service.upgrade() {
            let time = transactional_message_service.batch_send_op_message().await;
            self.wakeup_timestamp.store(time, std::sync::atomic::Ordering::Relaxed);
        } else {
            const WARN_MESSAGE: &str =
                "TransactionalMessageService has been dropped, skipping batch send operation message.";
            warn!(WARN_MESSAGE);
        }
    }
}
