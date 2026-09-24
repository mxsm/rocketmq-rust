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
                let Some(service) = self.transactional_message_service.upgrade() else {
                    // A dropped owner cannot return. Retrying with the expired
                    // wakeup timestamp would self-notify forever without yielding.
                    break;
                };
                let time = service.batch_send_op_message().await;
                self.wakeup_timestamp.store(time, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if let Some(service) = self.transactional_message_service.upgrade() {
            service.drain_operation_queues().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_store::LocalFileMessageStore;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[test]
    fn orphaned_batch_service_exits_without_starving_its_runtime() {
        let owner = RuntimeOwner::plan(RuntimeConfig::for_parallelism("orphaned-transaction-batch", 1))
            .expect("valid test runtime")
            .build()
            .expect("start test runtime");
        let cancellation = CancellationToken::new();
        let service = TransactionalOpBatchServiceInner::<LocalFileMessageStore> {
            cancellation: cancellation.clone(),
            broker_config: Arc::new(BrokerConfig {
                transaction_op_batch_interval: 1,
                ..BrokerConfig::default()
            }),
            transactional_message_service: Weak::new(),
            wakeup_timestamp: AtomicU64::new(0),
        };
        let context = ServiceTaskContext::new(
            Arc::new(Notify::new()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (finished_tx, finished_rx) = mpsc::channel();
        owner
            .root_context()
            .component("transaction-batch")
            .task_group()
            .spawn_service("orphaned-batch", async move {
                service.run(&context).await;
                let _ = finished_tx.send(());
            })
            .expect("spawn batch service");

        // The observer must be outside the runtime: a self-waking loop can
        // monopolize its only worker and prevent a Tokio timeout from firing.
        let exited = finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        cancellation.cancel();
        let report = owner
            .shutdown_runtime_blocking_with_timeout(Duration::from_secs(5))
            .expect("shut down test runtime");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(exited, "batch service must exit after its transaction owner disappears");
    }
}
